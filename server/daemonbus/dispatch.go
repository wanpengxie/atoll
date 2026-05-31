package daemonbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/framework/multiuser/daemonbus"
	"github.com/wanpengxie/ActOS/framework/multiuser/placement"
	"github.com/wanpengxie/ActOS/framework/multiuser/viewsync"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
)

// Handlers is the set of hooks dispatch invokes when daemon-sent
// frames arrive. The gateway wires these to the placements +
// viewcache + devicebus services.
type Handlers struct {
	// OnPush is called for viewsync.push — implementation should
	// hand the frame to viewcache.Apply and then send back ack via
	// SendAck (or use the return value).
	OnPush func(ctx context.Context, conn *Connection, frame viewsync.PushFrame) (viewsync.AckFrame, error)

	// OnCreateChannelAck handles control.create_channel_ack.
	OnCreateChannelAck func(ctx context.Context, conn *Connection, ack placement.CreateChannelAck) error

	// OnRejectChannel handles control.reject_channel.
	OnRejectChannel func(ctx context.Context, conn *Connection, rej placement.RejectChannel) error

	// OnHeartbeat handles control.heartbeat — refresh daemon
	// last_heartbeat_at + per-channel heartbeat for active
	// placements.
	OnHeartbeat func(ctx context.Context, conn *Connection, payload HeartbeatPayload) (placement.HeartbeatAckPayload, error)

	// OnHeldChannelsReport handles control.held_channels_report —
	// server validates each held channel and replies
	// control.held_channels_ack.
	OnHeldChannelsReport func(ctx context.Context, conn *Connection, req placement.HeldChannelsReport) error

	// OnDeviceTransitRecv handles device_transit.recv frames pushed BY
	// the daemon (daemon → server → device). Per impl-layer2 §5.3.2
	// this is the outbound (adapter → device) direction: daemon adapter
	// emits the payload, server relays it to the device WS. The gateway
	// decodes the devicetransit.SendFrame body and asks the
	// devicebus.Service to relay it to the device WS keyed by
	// channel_id + adapter_actor_id.
	OnDeviceTransitRecv func(ctx context.Context, conn *Connection, frame daemonbus.Frame) error
}

// HeartbeatPayload is the wire shape of `control.heartbeat`.
type HeartbeatPayload = placement.HeartbeatPayload

// Run consumes frames from the transport until ctx is cancelled or
// the transport closes. Each frame is dispatched to the
// corresponding Handler hook. ACK frames are matched against
// pending SendAndAwait calls first; if a hook is also registered for
// that frame_type it runs after the ack-match (so the gateway can
// observe ACK arrival even when nothing is waiting).
func (c *Connection) Run(ctx context.Context, h Handlers) error {
	defer func() { _ = c.Close() }()

	for {
		frame, err := c.transport.ReadFrame(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		frameCtx := ctx
		if frame.RequestID != "" {
			frameCtx = requestctx.WithRequestID(ctx, frame.RequestID)
		}
		// Stale-connection frame guard (L2 §9.4): drop frames whose
		// epoch is less than the current connection epoch.
		if frame.DaemonConnectionEpoch != c.ConnectionEpoch {
			if c.log != nil {
				c.log.Warn("daemonbus.frame_rejected",
					"reason", "stale_connection_epoch",
					"request_id", requestctx.RequestID(frameCtx),
					"daemon_id", string(c.DaemonID),
					"frame_id", string(frame.FrameID),
					"frame_kind", string(frame.FrameKind),
					"connection_epoch", int64(c.ConnectionEpoch),
					"frame_connection_epoch", int64(frame.DaemonConnectionEpoch),
				)
			}
			continue
		}
		// Wake any SendAndAwait waiters first.
		matched := c.matchAck(frame)

		if err := c.dispatchFrame(frameCtx, h, frame, matched); err != nil {
			if c.log != nil {
				c.log.Warn("daemonbus.frame_dispatch_failed",
					"request_id", requestctx.RequestID(frameCtx),
					"daemon_id", string(c.DaemonID),
					"frame_id", string(frame.FrameID),
					"frame_kind", string(frame.FrameKind),
					"connection_epoch", int64(c.ConnectionEpoch),
					"error", err.Error(),
				)
			}
			return err
		}
	}
}

func (c *Connection) dispatchFrame(ctx context.Context, h Handlers, frame daemonbus.Frame, matched bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("daemonbus: dispatch panic on %s: %v\n%s", frame.FrameKind, r, debug.Stack())
		}
	}()
	switch frame.FrameKind {
	case daemonbus.FrameTypeViewsyncPush:
		if h.OnPush != nil {
			push, perr := DecodeViewsyncPush(frame)
			if perr != nil {
				return perr
			}
			ack, err := h.OnPush(ctx, c, push)
			if err != nil {
				return err
			}
			if ack.ChannelID == "" {
				ack.ChannelID = push.ChannelID
			}
			if _, err := c.SendFrame(ctx, daemonbus.FrameTypeViewsyncAck, ack); err != nil {
				return err
			}
		}
	case daemonbus.FrameTypeControlCreateChannelAck:
		if h.OnCreateChannelAck != nil {
			ack, perr := DecodeCreateAck(frame)
			if perr != nil {
				return perr
			}
			if err := h.OnCreateChannelAck(ctx, c, ack); err != nil {
				return err
			}
		}
	case daemonbus.FrameTypeControlRejectChannel:
		if h.OnRejectChannel != nil {
			rej, perr := DecodeRejectChannel(frame)
			if perr != nil {
				return perr
			}
			if err := h.OnRejectChannel(ctx, c, rej); err != nil {
				return err
			}
		}
	case daemonbus.FrameTypeControlHeartbeat:
		var p HeartbeatPayload
		if err := json.Unmarshal(frame.Payload, &p); err != nil {
			return fmt.Errorf("daemonbus: unmarshal heartbeat: %w", err)
		}
		ackPayload := placement.HeartbeatAckPayload{
			HeartbeatSeq: p.HeartbeatSeq,
		}
		if h.OnHeartbeat != nil {
			out, err := h.OnHeartbeat(ctx, c, p)
			if err != nil {
				return err
			}
			ackPayload = out
			if ackPayload.HeartbeatSeq == 0 {
				ackPayload.HeartbeatSeq = p.HeartbeatSeq
			}
		}
		// Always reply heartbeat_ack so daemon RTT stays calibrated.
		if _, err := c.SendFrame(ctx, daemonbus.FrameTypeControlHeartbeatAck, ackPayload); err != nil {
			return err
		}
	case daemonbus.FrameTypeControlHeldChannelsReport:
		if h.OnHeldChannelsReport != nil {
			req, perr := DecodeHeldChannelsReport(frame)
			if perr != nil {
				return perr
			}
			if err := h.OnHeldChannelsReport(ctx, c, req); err != nil {
				return err
			}
		}
	case daemonbus.FrameTypeDeviceTransitRecv:
		if h.OnDeviceTransitRecv != nil {
			if err := h.OnDeviceTransitRecv(ctx, c, frame); err != nil {
				return err
			}
		}
	default:
		// Unhandled frame types fall through — the ack-match above
		// already handled SendAndAwait pairings.
		_ = matched
	}
	return nil
}

var monotonic atomic.Uint64

func newFrameID() string {
	// UUID v4 for unforgeability + a monotonic suffix as a tiebreaker
	// inside one process so identical-second collisions never happen.
	return uuid.NewString() + "-" + fmtUint(monotonic.Add(1))
}

func fmtUint(n uint64) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

// nowMs returns wall clock as ms. Indirected for tests; default is
// time.Now.
var nowMs = func() int64 { return time.Now().UnixMilli() }
