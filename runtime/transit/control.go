package transit

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
)

// ControlHandlers is the set of callbacks the central frame dispatcher
// invokes when a control.* / viewsync.* frame arrives. Each callback
// has a nil-safe default: missing handler → frame logged and dropped.
//
// runtime/lifecycle wires CreateChannel + UnbindChannel + Heartbeat /
// Reclaim handlers; runtime/transit ack_handler wires Ack; runtime/
// daemon wires WriteMessage via a per-channel router (FIX-T2 /
// server→daemon write-through path).
type ControlHandlers struct {
	OnViewsyncAck           func(ctx context.Context, frame viewsync.AckFrame) error
	OnViewsyncResyncRequest func(ctx context.Context, frame viewsync.ResyncRequest) (viewsync.ResyncResponse, error)

	OnCreateChannel   func(ctx context.Context, frame daemonbus.Frame, req placement.CreateChannelRequest) error
	OnDaemonReclaim   func(ctx context.Context, frame daemonbus.Frame, req placement.DaemonReclaimRequest) error
	OnUnbindChannel   func(ctx context.Context, frame daemonbus.Frame) error
	OnReclaimAccepted func(ctx context.Context, frame daemonbus.Frame) error
	OnReclaimRejected func(ctx context.Context, frame daemonbus.Frame) error
	OnHeartbeatAck    func(ctx context.Context, frame daemonbus.Frame) error
	OnDeviceTransit   func(ctx context.Context, frame daemonbus.Frame) error

	// OnWriteMessage handles the daemon-side `control.write_message`
	// dispatch path (FIX-T2). The Dispatcher decodes the frame body
	// into WriteMessageBody, invokes this callback, and SENDS the
	// returned ack as `control.write_message_ack`. The callback is
	// nil-safe — when unset, the frame is dropped silently (M1.5
	// development bootstrap path).
	OnWriteMessage func(ctx context.Context, frame daemonbus.Frame, body WriteMessageBody) WriteMessageAckBody

	// OnBindDeviceSession / OnUnbindDeviceSession handle the
	// server → daemon device-session lifecycle frames (T147 §A-S2).
	// The Dispatcher decodes the body, invokes the callback, and SENDS
	// the returned ack as `control.bind_device_session_ack` /
	// `control.unbind_device_session_ack` so the gateway HTTP request
	// waiting on SendAndAwait wakes up. Both callbacks are nil-safe:
	// when unset the Dispatcher synthesises an Accepted=false ack with
	// Reason=bind_internal_error / unbind_internal_error.
	OnBindDeviceSession   func(ctx context.Context, frame daemonbus.Frame, body BindDeviceSessionBody) BindDeviceSessionAckBody
	OnUnbindDeviceSession func(ctx context.Context, frame daemonbus.Frame, body UnbindDeviceSessionBody) UnbindDeviceSessionAckBody

	// Unknown is invoked for any frame_type not handled above. May be
	// nil — default is to drop silently.
	Unknown func(ctx context.Context, frame daemonbus.Frame) error
}

// Dispatcher routes a single incoming frame to the right handler.
//
// The Dispatcher is stateless beyond the handlers it was built with; it
// can be reused across reconnects.
type Dispatcher struct {
	handlers ControlHandlers
	client   *Client
	frameID  FrameIDGen
}

// DispatcherConfig wires a Dispatcher.
type DispatcherConfig struct {
	Handlers ControlHandlers
	Client   *Client
	FrameID  FrameIDGen
}

// NewDispatcher builds a Dispatcher.
func NewDispatcher(cfg DispatcherConfig) (*Dispatcher, error) {
	if cfg.Client == nil {
		return nil, errors.New("transit: DispatcherConfig.Client nil")
	}
	if cfg.FrameID == nil {
		return nil, errors.New("transit: DispatcherConfig.FrameID nil")
	}
	return &Dispatcher{
		handlers: cfg.Handlers,
		client:   cfg.Client,
		frameID:  cfg.FrameID,
	}, nil
}

// replyFrameID returns the envelope frame_id the daemon should use when
// emitting an ack for the given inbound frame. We echo the inbound
// frame_id so the server-side daemonbus.Connection.matchAck pending
// map (keyed under the originating SendAndAwait envelope frame_id) can
// pair the ack with the waiting caller (FIX-2026-05-18). A generated
// fallback is used only when the inbound frame_id is empty — that
// branch is unreachable in production but keeps the helper safe under
// fuzz/test inputs.
func (d *Dispatcher) replyFrameID(frame daemonbus.Frame) string {
	if frame.FrameID != "" {
		return string(frame.FrameID)
	}
	return d.frameID()
}

// ErrStaleEpoch is returned by Dispatch when a frame's
// DaemonConnectionEpoch does not match the daemon's current epoch.
// FIX-T8: a frame from an older WS session must not be applied after
// the daemon reconnects with a fresh epoch (L2 §9.4). The Loop swallows
// this sentinel — it is observable, not fatal.
var ErrStaleEpoch = errors.New("transit: stale connection epoch")

// Dispatch one incoming frame to the right handler. Returns the handler
// error (or nil) so the caller (Loop) can decide whether to disconnect.
func (d *Dispatcher) Dispatch(ctx context.Context, frame daemonbus.Frame) error {
	// FIX-T8 — drop frames whose epoch does not match the current
	// daemonbus connection epoch (L2 §9.4 stale-frame guard). Epoch 0
	// means "client never connected" — Loop only fires after Connect,
	// so a zero current-epoch is treated as "accept anything", keeping
	// the test stub that ignores epoch alive without weakening prod.
	if cur := d.client.Epoch(); cur != 0 && frame.DaemonConnectionEpoch != cur {
		return ErrStaleEpoch
	}

	switch frame.FrameKind {
	case daemonbus.FrameTypeViewsyncAck:
		if d.handlers.OnViewsyncAck == nil {
			return nil
		}
		var ack viewsync.AckFrame
		if err := DecodePayload(frame, &ack); err != nil {
			return fmt.Errorf("transit: decode viewsync.ack: %w", err)
		}
		return d.handlers.OnViewsyncAck(ctx, ack)

	case daemonbus.FrameTypeViewsyncResyncRequest:
		if d.handlers.OnViewsyncResyncRequest == nil {
			return nil
		}
		var req viewsync.ResyncRequest
		if err := DecodePayload(frame, &req); err != nil {
			return fmt.Errorf("transit: decode viewsync.resync_request: %w", err)
		}
		resp, err := d.handlers.OnViewsyncResyncRequest(ctx, req)
		if err != nil {
			return err
		}
		// FIX-2026-05-18: when replying to a frame the server is
		// awaiting via SendAndAwait, the ack envelope frame_id MUST
		// match the inbound frame_id — that is the key the server's
		// pending-map is registered under (server/daemonbus/
		// connection.go:96). Generating a fresh frame_id here makes
		// the server's matchAck miss forever and SendAndAwait times
		// out (the production 524 incident root cause). The protocol
		// underdefines envelope vs. body frame_id semantics; until
		// L2 §9 explicitly specifies "envelope frame_id = in-reply-to
		// frame_id for ack frames", every ack-reply path on the
		// daemon side echoes the inbound envelope frame_id.
		return d.client.Send(ctx, d.replyFrameID(frame),
			daemonbus.FrameTypeViewsyncResyncResponse, resp)

	case daemonbus.FrameTypeControlCreateChannel:
		if d.handlers.OnCreateChannel == nil {
			return nil
		}
		var req placement.CreateChannelRequest
		if err := DecodePayload(frame, &req); err != nil {
			return fmt.Errorf("transit: decode control.create_channel: %w", err)
		}
		return d.handlers.OnCreateChannel(ctx, frame, req)

	case daemonbus.FrameTypeControlDaemonReclaim:
		if d.handlers.OnDaemonReclaim == nil {
			return nil
		}
		var req placement.DaemonReclaimRequest
		if err := DecodePayload(frame, &req); err != nil {
			return fmt.Errorf("transit: decode control.daemon_reclaim: %w", err)
		}
		return d.handlers.OnDaemonReclaim(ctx, frame, req)

	case daemonbus.FrameTypeControlUnbindChannel:
		if d.handlers.OnUnbindChannel == nil {
			return nil
		}
		return d.handlers.OnUnbindChannel(ctx, frame)

	case daemonbus.FrameTypeControlReclaimAccepted:
		if d.handlers.OnReclaimAccepted != nil {
			return d.handlers.OnReclaimAccepted(ctx, frame)
		}
		return nil

	case daemonbus.FrameTypeControlReclaimRejected:
		if d.handlers.OnReclaimRejected != nil {
			return d.handlers.OnReclaimRejected(ctx, frame)
		}
		return nil

	case daemonbus.FrameTypeControlHeartbeatAck:
		if d.handlers.OnHeartbeatAck != nil {
			return d.handlers.OnHeartbeatAck(ctx, frame)
		}
		return nil

	case daemonbus.FrameTypeControlWriteMessage:
		if d.handlers.OnWriteMessage == nil {
			return nil
		}
		var body WriteMessageBody
		if err := DecodePayload(frame, &body); err != nil {
			return fmt.Errorf("transit: decode control.write_message: %w", err)
		}
		if body.FrameID == "" {
			// Fall back to the wrapper frame_id so the gateway can still
			// pair the ack with the HTTP request.
			body.FrameID = frame.FrameID
		}
		ack := d.handlers.OnWriteMessage(ctx, frame, body)
		if ack.FrameID == "" {
			ack.FrameID = body.FrameID
		}
		// FIX-2026-05-18: echo inbound envelope frame_id. See viewsync
		// resync_response branch for the full root-cause comment.
		return d.client.Send(ctx, d.replyFrameID(frame),
			daemonbus.FrameTypeControlWriteMessageAck, ack)

	case daemonbus.FrameTypeControlBindDeviceSession:
		var body BindDeviceSessionBody
		if err := DecodePayload(frame, &body); err != nil {
			return fmt.Errorf("transit: decode control.bind_device_session: %w", err)
		}
		if body.FrameID == "" {
			body.FrameID = frame.FrameID
		}
		var ack BindDeviceSessionAckBody
		if d.handlers.OnBindDeviceSession == nil {
			// Handler not wired — emit a structured reject so the
			// server can tell "daemon does not implement bind" apart
			// from "daemon refused this specific bind".
			ack = BindDeviceSessionAckBody{
				FrameID:   body.FrameID,
				SessionID: body.SessionID,
				Accepted:  false,
				Reason:    DeviceSessionRejectBindInternalError,
				Detail:    "OnBindDeviceSession handler is nil",
			}
		} else {
			ack = d.handlers.OnBindDeviceSession(ctx, frame, body)
			if ack.FrameID == "" {
				ack.FrameID = body.FrameID
			}
			if ack.SessionID == "" {
				ack.SessionID = body.SessionID
			}
		}
		// FIX-2026-05-18: echo inbound envelope frame_id. See viewsync
		// resync_response branch for the full root-cause comment.
		return d.client.Send(ctx, d.replyFrameID(frame),
			daemonbus.FrameTypeControlBindDeviceSessionAck, ack)

	case daemonbus.FrameTypeControlUnbindDeviceSession:
		var body UnbindDeviceSessionBody
		if err := DecodePayload(frame, &body); err != nil {
			return fmt.Errorf("transit: decode control.unbind_device_session: %w", err)
		}
		if body.FrameID == "" {
			body.FrameID = frame.FrameID
		}
		var ack UnbindDeviceSessionAckBody
		if d.handlers.OnUnbindDeviceSession == nil {
			ack = UnbindDeviceSessionAckBody{
				FrameID:   body.FrameID,
				SessionID: body.SessionID,
				Accepted:  false,
				Reason:    DeviceSessionRejectUnbindInternalError,
				Detail:    "OnUnbindDeviceSession handler is nil",
			}
		} else {
			ack = d.handlers.OnUnbindDeviceSession(ctx, frame, body)
			if ack.FrameID == "" {
				ack.FrameID = body.FrameID
			}
			if ack.SessionID == "" {
				ack.SessionID = body.SessionID
			}
		}
		// FIX-2026-05-18: echo inbound envelope frame_id. See viewsync
		// resync_response branch for the full root-cause comment.
		return d.client.Send(ctx, d.replyFrameID(frame),
			daemonbus.FrameTypeControlUnbindDeviceSessionAck, ack)
	}

	if daemonbus.CategoryOf(frame.FrameKind) == daemonbus.CategoryDeviceTransit {
		if d.handlers.OnDeviceTransit != nil {
			return d.handlers.OnDeviceTransit(ctx, frame)
		}
		return nil
	}

	if d.handlers.Unknown != nil {
		return d.handlers.Unknown(ctx, frame)
	}
	return nil
}

// Loop drives Recv → Dispatch in a tight loop until ctx is cancelled or
// transport reports an error.
func (d *Dispatcher) Loop(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		frame, err := d.client.Recv(ctx)
		if err != nil {
			return err
		}
		if err := d.Dispatch(ctx, frame); err != nil {
			// Dispatch errors are typically transient; keep looping
			// unless the context is dead.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			// FIX-T8: stale-epoch frames are an expected race during
			// reconnect — drop without invoking Unknown.
			if errors.Is(err, ErrStaleEpoch) {
				continue
			}
			// Surface for observability but continue.
			if d.handlers.Unknown != nil {
				_ = d.handlers.Unknown(ctx, frame)
			}
		}
	}
}
