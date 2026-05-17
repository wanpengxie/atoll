package transit

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/daemonbus"
)

// HeartbeatTracker records the daemon's last successful heartbeat ack
// watermark. It is the daemon-side companion to the server's
// `control.heartbeat_ack` reply — every ack updates LastAckAt so the
// daemon's own heartbeat sender (a forthcoming M1-FIX-T2 follow-up)
// knows to back off retries vs ramp up reconnect.
//
// M1.6-T0.3 scope: the tracker is a watermark sink — it does not own
// the heartbeat sender loop itself. The OnHeartbeatAck handler in
// runtime/daemon.go calls AckReceived(now) each time a
// control.heartbeat_ack frame arrives; downstream wakers (idle backoff
// reset, reclaim path) read LastAckAt to decide whether to act.
type HeartbeatTracker struct {
	mu        sync.Mutex
	lastAckAt int64
	lastFrame string
}

// NewHeartbeatTracker constructs a zero-state tracker.
func NewHeartbeatTracker() *HeartbeatTracker { return &HeartbeatTracker{} }

// AckReceived records that the server acked a heartbeat at unix-ms now.
// frameID is the daemonbus.Frame.FrameID of the ack — kept for
// diagnostics; M1.6 doesn't pair acks to sends, so the value is purely
// observational.
func (h *HeartbeatTracker) AckReceived(now int64, frameID string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if now > h.lastAckAt {
		h.lastAckAt = now
	}
	if frameID != "" {
		h.lastFrame = frameID
	}
}

// LastAckAt returns the most recently recorded ack timestamp (unix-ms).
// Returns 0 before the first ack.
func (h *HeartbeatTracker) LastAckAt() int64 {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastAckAt
}

// LastFrameID returns the FrameID of the most recently observed ack
// frame. Returns "" before the first ack.
func (h *HeartbeatTracker) LastFrameID() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastFrame
}

// Handle implements a control.heartbeat_ack handler signature compatible
// with transit.ControlHandlers.OnHeartbeatAck. nowFn supplies the unix-ms
// clock so callers can plug a deterministic clock in tests.
func (h *HeartbeatTracker) Handle(nowFn func() int64) func(context.Context, daemonbus.Frame) error {
	if nowFn == nil {
		return func(context.Context, daemonbus.Frame) error {
			return errors.New("transit: HeartbeatTracker nowFn nil")
		}
	}
	return func(_ context.Context, frame daemonbus.Frame) error {
		h.AckReceived(nowFn(), frame.FrameID)
		return nil
	}
}
