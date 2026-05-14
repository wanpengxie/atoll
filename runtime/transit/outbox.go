package transit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/coagent-ai/coagent/kernel/channel"
	"github.com/coagent-ai/coagent/kernel/daemonbus"
	"github.com/coagent-ai/coagent/kernel/viewsync"
)

// OutboxReader is the subset of runtime/store.ViewSyncOutbox the pusher
// needs. Declared as an interface so tests can swap in fakes.
type OutboxReader interface {
	ChannelID() channel.ID
	PendingPage(ctx context.Context, limit int) ([]viewsync.PushFrame, error)
	MarkPushed(ctx context.Context, seq viewsync.Seq, pushedAt int64) error
	ResetPushed(ctx context.Context, seq viewsync.Seq) error
	AckUpTo(ctx context.Context, lastAckedSeq viewsync.Seq) error
}

// FrameIDGen returns a stable-ish frame id. Production injects uuid;
// tests can inject a counter.
type FrameIDGen func() string

// Pusher drives the persistent view-sync outbox: it pulls pending rows,
// emits viewsync.push frames via the daemonbus Client, marks them as
// pushed, and watches the cursor tracker advance via incoming acks.
//
// One Pusher serves one channel.
type Pusher struct {
	outbox    OutboxReader
	client    *Client
	cursors   *CursorTracker
	frameID   FrameIDGen
	pageSize  int
	nowFn     func() int64
	failHook  func(channel.ID, viewsync.Seq, error)
	pollEvery time.Duration
}

// PusherConfig wires a Pusher.
type PusherConfig struct {
	Outbox   OutboxReader
	Client   *Client
	Cursors  *CursorTracker
	FrameID  FrameIDGen
	PageSize int
	NowFn    func() int64
	// PollEvery is the cadence at which Pump pulls a fresh batch when
	// no progress is being made. Default = 50ms.
	PollEvery time.Duration
	// FailHook is called when a push of a specific seq returns an error.
	// May be nil — caller can use this to emit system.event
	// view_sync_failed (L1 §8.1.5).
	FailHook func(channel.ID, viewsync.Seq, error)
}

// NewPusher builds a Pusher.
func NewPusher(cfg PusherConfig) (*Pusher, error) {
	if cfg.Outbox == nil {
		return nil, errors.New("transit: PusherConfig.Outbox nil")
	}
	if cfg.Client == nil {
		return nil, errors.New("transit: PusherConfig.Client nil")
	}
	if cfg.Cursors == nil {
		return nil, errors.New("transit: PusherConfig.Cursors nil")
	}
	if cfg.FrameID == nil {
		return nil, errors.New("transit: PusherConfig.FrameID nil")
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = 32
	}
	if cfg.NowFn == nil {
		cfg.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.PollEvery <= 0 {
		cfg.PollEvery = 50 * time.Millisecond
	}
	return &Pusher{
		outbox:    cfg.Outbox,
		client:    cfg.Client,
		cursors:   cfg.Cursors,
		frameID:   cfg.FrameID,
		pageSize:  cfg.PageSize,
		nowFn:     cfg.NowFn,
		failHook:  cfg.FailHook,
		pollEvery: cfg.PollEvery,
	}, nil
}

// Drain pulls one batch of pending rows and pushes them. Returns
// (n, err) where n is the number of frames successfully sent.
// Production callers usually run Pump (Drain in a loop).
func (p *Pusher) Drain(ctx context.Context) (int, error) {
	pending, err := p.outbox.PendingPage(ctx, p.pageSize)
	if err != nil {
		return 0, fmt.Errorf("transit: drain pending: %w", err)
	}
	var sent int
	for _, frame := range pending {
		if err := p.client.Send(ctx, p.frameID(), daemonbus.FrameTypeViewsyncPush, frame); err != nil {
			if p.failHook != nil {
				p.failHook(frame.ChannelID, frame.Seq, err)
			}
			return sent, fmt.Errorf("transit: send seq=%d: %w", frame.Seq, err)
		}
		if err := p.outbox.MarkPushed(ctx, frame.Seq, p.nowFn()); err != nil {
			return sent, fmt.Errorf("transit: mark pushed seq=%d: %w", frame.Seq, err)
		}
		p.cursors.AdvancePushed(frame.ChannelID, viewsync.LastPushedSeq(frame.Seq))
		sent++
	}
	return sent, nil
}

// Pump runs Drain in a loop until ctx is cancelled. Empty batches
// trigger a pollEvery sleep before the next attempt.
func (p *Pusher) Pump(ctx context.Context) error {
	timer := time.NewTimer(p.pollEvery)
	defer timer.Stop()
	for {
		n, err := p.Drain(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return ctx.Err()
			}
			return err
		}
		if n == 0 {
			// Reset the timer and wait.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(p.pollEvery)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}
