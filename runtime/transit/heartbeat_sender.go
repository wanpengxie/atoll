package transit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
)

// HeartbeatBody is the wire shape of `control.heartbeat`. Mirrors
// server/daemonbus.HeartbeatPayload (json tag `channels`) so the
// existing server-side decoder accepts it without touching server/.
//
// Per L2 placements semantics, the channels list is the snapshot of
// channel ids the daemon currently still owns; the server refreshes
// per-channel last_heartbeat_at as well as daemon-level last_heartbeat_at.
type HeartbeatBody struct {
	Channels []channel.ID `json:"channels"`
}

// ChannelsFn returns the daemon's current owned-channel snapshot. The
// HeartbeatSender invokes this on every tick; implementations MUST be
// cheap and safe to call from a goroutine.
type ChannelsFn func() []channel.ID

// HeartbeatSenderConfig wires a HeartbeatSender.
type HeartbeatSenderConfig struct {
	// Client is the daemon → server transit funnel. REQUIRED.
	Client *Client
	// Period is the heartbeat cadence (default 15s; spec range 15-30s).
	Period time.Duration
	// FrameID generates a unique frame id per emit. REQUIRED.
	FrameID func() string
	// Channels supplies the owned-channel snapshot. May be nil (empty list).
	Channels ChannelsFn
}

// HeartbeatSender drives the daemon → server `control.heartbeat` ticker.
//
// M1.6-T1 scope: complements the existing HeartbeatTracker (receiver of
// `control.heartbeat_ack`). Without this sender, server placements drift
// to `stale` 90s after daemon boot even though the daemon process is
// alive.
//
// Design notes:
//   - Sender lives in runtime/transit so it shares a package with Client
//     (the single Send funnel). Policy knobs (period, channels snapshot)
//     stay on the caller side — daemon.go assembles this in startPhase3.
//   - Send errors are logged-and-keep-going: outbox/replay logic absorbs
//     transient transport faults; a missed heartbeat tick just gets
//     covered by the next one.
//   - We do NOT pair acks to sends. The tracker.LastAckAt() watermark is
//     the only feedback signal (used by reclaim path).
type HeartbeatSender struct {
	cfg HeartbeatSenderConfig
}

// DefaultHeartbeatPeriod is the cadence used when HeartbeatSenderConfig.Period
// is zero. Picked at the lower end of the spec range (15-30s) so the
// server's 90s placements stale threshold has at least 6 chances to
// observe liveness during a network blip.
const DefaultHeartbeatPeriod = 15 * time.Second

// NewHeartbeatSender constructs a HeartbeatSender.
func NewHeartbeatSender(cfg HeartbeatSenderConfig) (*HeartbeatSender, error) {
	if cfg.Client == nil {
		return nil, errors.New("transit: HeartbeatSenderConfig.Client nil")
	}
	if cfg.FrameID == nil {
		return nil, errors.New("transit: HeartbeatSenderConfig.FrameID nil")
	}
	if cfg.Period <= 0 {
		cfg.Period = DefaultHeartbeatPeriod
	}
	if cfg.Channels == nil {
		cfg.Channels = func() []channel.ID { return nil }
	}
	return &HeartbeatSender{cfg: cfg}, nil
}

// Period returns the configured heartbeat cadence (for tests).
func (s *HeartbeatSender) Period() time.Duration { return s.cfg.Period }

// Emit sends a single heartbeat frame using the current owned-channel
// snapshot. Exposed so tests can drive a deterministic single emit
// without running the Period ticker.
func (s *HeartbeatSender) Emit(ctx context.Context) error {
	body := HeartbeatBody{Channels: s.cfg.Channels()}
	if err := s.cfg.Client.Send(ctx, s.cfg.FrameID(), daemonbus.FrameTypeControlHeartbeat, body); err != nil {
		return fmt.Errorf("transit: heartbeat emit: %w", err)
	}
	return nil
}

// Run loops Emit at the configured Period until ctx is cancelled.
// Returns ctx.Err() on cancellation. Transient Send errors are swallowed
// (the next tick retries).
func (s *HeartbeatSender) Run(ctx context.Context) error {
	// Emit immediately on start so server placements get a fresh
	// last_heartbeat_at without waiting one Period (~15s) for the first
	// tick — this matters during the 90s stale-threshold race after a
	// daemon restart.
	if err := s.Emit(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, ErrBusClosed) {
			return err
		}
		// transient — keep going
	}

	tk := time.NewTicker(s.cfg.Period)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tk.C:
			if err := s.Emit(ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, ErrBusClosed) {
					return err
				}
				// transient — let the next tick retry.
			}
		}
	}
}
