package scheduler

import (
	"context"
	"errors"
	"time"
)

// LongPendingScanFn is implemented by the daemon: scan the messages
// table for pending requests whose expires_at <= now and emit the
// fallback envelope (L1 §6 / L2 §3.7).
type LongPendingScanFn func(ctx context.Context, nowMs int64) error

// TimerConfig wires Timer.
type TimerConfig struct {
	// Period is the scan cadence (default 1s per L2 §3.7).
	Period time.Duration
	// Scan is the per-tick callback invoked by Run / Tick.
	Scan LongPendingScanFn
	// NowFn returns unix-ms.
	NowFn func() int64
}

// Timer drives a periodic long-pending scan.
type Timer struct {
	cfg TimerConfig
}

// NewTimer builds a Timer.
func NewTimer(cfg TimerConfig) (*Timer, error) {
	if cfg.Scan == nil {
		return nil, errors.New("scheduler: TimerConfig.Scan nil")
	}
	if cfg.NowFn == nil {
		return nil, errors.New("scheduler: TimerConfig.NowFn nil")
	}
	if cfg.Period <= 0 {
		cfg.Period = time.Second
	}
	return &Timer{cfg: cfg}, nil
}

// Tick performs a single scan.
func (t *Timer) Tick(ctx context.Context) error {
	return t.cfg.Scan(ctx, t.cfg.NowFn())
}

// Run loops Tick at the configured Period until ctx is cancelled.
func (t *Timer) Run(ctx context.Context) error {
	tk := time.NewTicker(t.cfg.Period)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tk.C:
			if err := t.Tick(ctx); err != nil {
				// transient — keep going; caller can decide via metrics.
				if errors.Is(err, context.Canceled) {
					return err
				}
			}
		}
	}
}
