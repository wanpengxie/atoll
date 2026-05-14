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
// scheduler may wire WriteMessage (server->daemon write-through).
type ControlHandlers struct {
	OnViewsyncAck           func(ctx context.Context, frame viewsync.AckFrame) error
	OnViewsyncResyncRequest func(ctx context.Context, frame viewsync.ResyncRequest) (viewsync.ResyncResponse, error)

	OnCreateChannel   func(ctx context.Context, frame daemonbus.Frame, req placement.CreateChannelRequest) error
	OnUnbindChannel   func(ctx context.Context, frame daemonbus.Frame) error
	OnReclaimAccepted func(ctx context.Context, frame daemonbus.Frame) error
	OnReclaimRejected func(ctx context.Context, frame daemonbus.Frame) error
	OnHeartbeatAck    func(ctx context.Context, frame daemonbus.Frame) error
	OnDeviceTransit   func(ctx context.Context, frame daemonbus.Frame) error

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

// Dispatch one incoming frame to the right handler. Returns the handler
// error (or nil) so the caller (Loop) can decide whether to disconnect.
func (d *Dispatcher) Dispatch(ctx context.Context, frame daemonbus.Frame) error {
	switch frame.FrameType {
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
		return d.client.Send(ctx, d.frameID(),
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
	}

	if daemonbus.CategoryOf(frame.FrameType) == daemonbus.CategoryDeviceTransit {
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
			// Surface for observability but continue.
			if d.handlers.Unknown != nil {
				_ = d.handlers.Unknown(ctx, frame)
			}
		}
	}
}
