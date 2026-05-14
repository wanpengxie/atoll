package transit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
)

// OutboxReader is the subset of runtime/store.ViewSyncOutbox the pusher
// needs. Declared as an interface so tests can swap in fakes.
//
// PendingCount is OPTIONAL — implementations that cannot answer cheaply
// may return 0 and (nil) error; the Pusher only consults the count to
// emit the watermark warn-event and treats 0 as "no warning needed".
type OutboxReader interface {
	ChannelID() channel.ID
	PendingPage(ctx context.Context, limit int) ([]viewsync.PushFrame, error)
	MarkPushed(ctx context.Context, seq viewsync.Seq, pushedAt int64) error
	ResetPushed(ctx context.Context, seq viewsync.Seq) error
	AckUpTo(ctx context.Context, lastAckedSeq viewsync.Seq) error
	// PendingCount reports the number of rows currently in status='pending'
	// (used for backlog watermark observability — L1 §8.1.5).
	PendingCount(ctx context.Context) (int, error)
}

// ViewSyncFailedEvent carries the L1 §8.1.5 view_sync_failed observation.
// Emitted when a single seq exceeds MaxRetriesBeforeEvent within a Pusher.
//
// The caller (daemon composition root) decides how to surface this — the
// production path runs it through the harness as a `system.event` envelope
// with payload.kind="view_sync_failed". transit only invokes the callback
// so the package stays free of harness dependencies (kernel/arch-lint).
type ViewSyncFailedEvent struct {
	ChannelID     channel.ID
	Seq           viewsync.Seq
	Attempts      int
	LastPushedSeq viewsync.LastPushedSeq
	LastAckedSeq  viewsync.LastAckedSeq
	LastError     string
	NowMs         int64
}

// ViewSyncBacklogEvent carries the outbox high-watermark warn signal.
// Emitted whenever a Drain ends with pendingCount > BacklogHighWatermark.
//
// The default threshold is 10000 rows (L1 §8.1.5 monitoring rec.).
// Callers route this through harness as a `system.event` envelope with
// payload.kind="view_sync_backlog_warn".
type ViewSyncBacklogEvent struct {
	ChannelID     channel.ID
	PendingCount  int
	Watermark     int
	LastPushedSeq viewsync.LastPushedSeq
	LastAckedSeq  viewsync.LastAckedSeq
	NowMs         int64
}

// EventEmitter is the (optional) callback the Pusher uses to surface
// L1 §8.1.5 view-sync observability events. Both fields may be nil.
type EventEmitter struct {
	OnViewSyncFailed  func(ev ViewSyncFailedEvent)
	OnViewSyncBacklog func(ev ViewSyncBacklogEvent)
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

	// retry-counter + watermark state (L1 §8.1.5 observability).
	maxRetries   int
	watermark    int
	emitter      EventEmitter
	mu           sync.Mutex
	failCount    map[viewsync.Seq]int
	failedNotify map[viewsync.Seq]bool // seq → emitted view_sync_failed already
	backlogActive bool                  // de-dupe consecutive backlog warns
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
	// view_sync_failed (L1 §8.1.5). Kept as a back-compat seam alongside
	// the richer EventEmitter — both fire when present.
	FailHook func(channel.ID, viewsync.Seq, error)

	// MaxRetriesBeforeEvent controls when a sustained push failure
	// triggers Emitter.OnViewSyncFailed. Default = 5. Zero or negative
	// disables the threshold and the callback fires only via FailHook
	// (legacy behavior).
	MaxRetriesBeforeEvent int

	// BacklogHighWatermark caps the outbox pending-row count before
	// Emitter.OnViewSyncBacklog fires. Default = 10000 (L1 §8.1.5).
	// Zero or negative disables the watermark check.
	BacklogHighWatermark int

	// Emitter is the optional set of system-event callbacks the Pusher
	// invokes when retry threshold or backlog watermark trip. May leave
	// individual fields nil — they are no-ops.
	Emitter EventEmitter
}

// Defaults for the observability knobs — exported so tests + the daemon
// composition root reference one source of truth.
const (
	DefaultMaxRetriesBeforeEvent = 5
	DefaultBacklogHighWatermark  = 10000
)

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
	if cfg.MaxRetriesBeforeEvent == 0 {
		cfg.MaxRetriesBeforeEvent = DefaultMaxRetriesBeforeEvent
	}
	if cfg.BacklogHighWatermark == 0 {
		cfg.BacklogHighWatermark = DefaultBacklogHighWatermark
	}
	return &Pusher{
		outbox:       cfg.Outbox,
		client:       cfg.Client,
		cursors:      cfg.Cursors,
		frameID:      cfg.FrameID,
		pageSize:     cfg.PageSize,
		nowFn:        cfg.NowFn,
		failHook:     cfg.FailHook,
		pollEvery:    cfg.PollEvery,
		maxRetries:   cfg.MaxRetriesBeforeEvent,
		watermark:    cfg.BacklogHighWatermark,
		emitter:      cfg.Emitter,
		failCount:    make(map[viewsync.Seq]int),
		failedNotify: make(map[viewsync.Seq]bool),
	}, nil
}

// recordFailure increments the per-seq retry counter and, when the
// threshold is crossed, invokes Emitter.OnViewSyncFailed exactly once
// per seq until the next successful push.
func (p *Pusher) recordFailure(chID channel.ID, seq viewsync.Seq, pushErr error) {
	p.mu.Lock()
	p.failCount[seq]++
	attempts := p.failCount[seq]
	emit := !p.failedNotify[seq] && p.maxRetries > 0 && attempts >= p.maxRetries
	if emit {
		p.failedNotify[seq] = true
	}
	p.mu.Unlock()

	if !emit || p.emitter.OnViewSyncFailed == nil {
		return
	}
	lastPushed, lastAcked, _ := p.cursors.Get(chID)
	detail := ""
	if pushErr != nil {
		detail = pushErr.Error()
	}
	p.emitter.OnViewSyncFailed(ViewSyncFailedEvent{
		ChannelID:     chID,
		Seq:           seq,
		Attempts:      attempts,
		LastPushedSeq: lastPushed,
		LastAckedSeq:  lastAcked,
		LastError:     detail,
		NowMs:         p.nowFn(),
	})
}

// recordSuccess clears the retry counters for a seq that just pushed
// successfully so a future transient failure doesn't trip on stale
// counts.
func (p *Pusher) recordSuccess(seq viewsync.Seq) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.failCount, seq)
	delete(p.failedNotify, seq)
}

// checkBacklog reads the outbox pending count and emits a
// ViewSyncBacklogEvent when it crosses BacklogHighWatermark. Re-armed
// after the count drops back below the watermark.
func (p *Pusher) checkBacklog(ctx context.Context, chID channel.ID) {
	if p.watermark <= 0 || p.emitter.OnViewSyncBacklog == nil {
		return
	}
	n, err := p.outbox.PendingCount(ctx)
	if err != nil {
		return
	}
	p.mu.Lock()
	over := n > p.watermark
	emit := over && !p.backlogActive
	if emit {
		p.backlogActive = true
	} else if !over && p.backlogActive {
		// re-arm
		p.backlogActive = false
	}
	p.mu.Unlock()

	if !emit {
		return
	}
	lastPushed, lastAcked, _ := p.cursors.Get(chID)
	p.emitter.OnViewSyncBacklog(ViewSyncBacklogEvent{
		ChannelID:     chID,
		PendingCount:  n,
		Watermark:     p.watermark,
		LastPushedSeq: lastPushed,
		LastAckedSeq:  lastAcked,
		NowMs:         p.nowFn(),
	})
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
			p.recordFailure(frame.ChannelID, frame.Seq, err)
			// Outbox row stays in pending state; backlog watermark may
			// also be tripping right now — surface it before returning.
			p.checkBacklog(ctx, frame.ChannelID)
			return sent, fmt.Errorf("transit: send seq=%d: %w", frame.Seq, err)
		}
		if err := p.outbox.MarkPushed(ctx, frame.Seq, p.nowFn()); err != nil {
			return sent, fmt.Errorf("transit: mark pushed seq=%d: %w", frame.Seq, err)
		}
		p.cursors.AdvancePushed(frame.ChannelID, viewsync.LastPushedSeq(frame.Seq))
		p.recordSuccess(frame.Seq)
		sent++
	}
	// Even on a clean drain, check the backlog so a slow-ack situation
	// (push succeeded but ack hasn't arrived) still trips the watermark.
	p.checkBacklog(ctx, p.outbox.ChannelID())
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
