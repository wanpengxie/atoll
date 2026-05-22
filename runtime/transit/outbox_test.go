package transit_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

// fakeOutbox is a memory-only OutboxReader fixture used by the Pusher
// observability tests. It returns the same pending page on every call
// (so retries against the same seq accumulate failures) and records the
// state-changing methods.
type fakeOutbox struct {
	mu         sync.Mutex
	chID       channel.ID
	pending    []viewsync.PushFrame
	pushed     map[viewsync.Seq]bool
	pendingN   int // PendingCount stub — caller mutates directly
	pendingErr error
	pageCalls  int
}

func newFakeOutbox(chID channel.ID, n int) *fakeOutbox {
	pending := make([]viewsync.PushFrame, 0, n)
	for i := 1; i <= n; i++ {
		pending = append(pending, viewsync.PushFrame{
			ChannelID: chID,
			Seq:       viewsync.Seq(i),
			MessageID: message.ID("m-" + itoa(i)),
			Envelope: message.Envelope{
				ID:         message.ID("m-" + itoa(i)),
				ChannelID:  chID,
				Type:       "tick",
				Visibility: message.VisibilityPublic,
				Kind:       message.KindEvent,
				Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
				Payload:    json.RawMessage(`{}`),
				Audience:   message.Audience{"*"},
			},
		})
	}
	return &fakeOutbox{
		chID:     chID,
		pending:  pending,
		pushed:   map[viewsync.Seq]bool{},
		pendingN: n,
	}
}

func (f *fakeOutbox) ChannelID() channel.ID { return f.chID }

func (f *fakeOutbox) PendingPage(_ context.Context, limit int) ([]viewsync.PushFrame, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pageCalls++
	out := make([]viewsync.PushFrame, 0, limit)
	for _, p := range f.pending {
		if f.pushed[p.Seq] {
			continue
		}
		if len(out) >= limit {
			break
		}
		out = append(out, p)
	}
	return out, nil
}

func (f *fakeOutbox) MarkPushed(_ context.Context, seq viewsync.Seq, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pushed[seq] = true
	f.pendingN--
	return nil
}

func (f *fakeOutbox) ResetPushed(_ context.Context, seq viewsync.Seq) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pushed[seq] {
		delete(f.pushed, seq)
		f.pendingN++
	}
	return nil
}

func (f *fakeOutbox) AckUpTo(_ context.Context, _ viewsync.Seq) error { return nil }

func (f *fakeOutbox) PendingCount(_ context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pendingErr != nil {
		return 0, f.pendingErr
	}
	return f.pendingN, nil
}

func (f *fakeOutbox) pendingPageCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pageCalls
}

func (f *fakeOutbox) pushedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pushed)
}

// flakyTransport implements daemonbus.Transport but fails the first N
// Send invocations (simulating intermittent network errors).
type flakyTransport struct {
	failsLeft atomic.Int32
	failErr   error
	good      *transit.MockBus
}

func newFlakyTransport(fails int) *flakyTransport {
	t := &flakyTransport{
		failErr: errors.New("ws: write reset by peer"),
		good:    transit.NewMockBus(64),
	}
	t.failsLeft.Store(int32(fails))
	return t
}

func (t *flakyTransport) Connect(ctx context.Context) (daemonbus.ConnectionEpoch, error) {
	return t.good.Connect(ctx)
}

func (t *flakyTransport) Send(ctx context.Context, frame daemonbus.Frame) error {
	if t.failsLeft.Add(-1) >= 0 {
		return t.failErr
	}
	return t.good.Send(ctx, frame)
}

func (t *flakyTransport) Recv(ctx context.Context) (daemonbus.Frame, error) {
	return t.good.Recv(ctx)
}

func (t *flakyTransport) Close() error { return t.good.Close() }

// TestPusher_ViewSyncFailedAfterRetryThreshold — when push of the same
// seq fails N >= MaxRetriesBeforeEvent times, the Emitter.OnViewSyncFailed
// callback fires exactly once with the failure context per L1 §8.1.5.
func TestPusher_ViewSyncFailedAfterRetryThreshold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	outbox := newFakeOutbox("ch-1", 1)
	transport := newFlakyTransport(100) // always fails for this test
	client, err := transit.NewClient(transit.ClientConfig{
		DaemonID:  "daemon-A",
		Transport: transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	var (
		gotEvents []transit.ViewSyncFailedEvent
		eventMu   sync.Mutex
	)
	emitter := transit.EventEmitter{
		OnViewSyncFailed: func(ev transit.ViewSyncFailedEvent) {
			eventMu.Lock()
			gotEvents = append(gotEvents, ev)
			eventMu.Unlock()
		},
	}

	pusher, err := transit.NewPusher(transit.PusherConfig{
		Outbox:                outbox,
		Client:                client,
		Cursors:               transit.NewCursorTracker(),
		FrameID:               atomicFrameID(),
		MaxRetriesBeforeEvent: 3,
		BacklogHighWatermark:  -1, // disabled
		Emitter:               emitter,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 3 failed drains → threshold tripped on the 3rd attempt → event fires.
	for i := 0; i < 5; i++ {
		_, _ = pusher.Drain(ctx)
	}

	eventMu.Lock()
	defer eventMu.Unlock()
	if len(gotEvents) != 1 {
		t.Fatalf("expected exactly 1 view_sync_failed event; got %d", len(gotEvents))
	}
	ev := gotEvents[0]
	if ev.ChannelID != "ch-1" {
		t.Errorf("ChannelID=%v want ch-1", ev.ChannelID)
	}
	if ev.Seq != 1 {
		t.Errorf("Seq=%d want 1", ev.Seq)
	}
	if ev.Attempts < 3 {
		t.Errorf("Attempts=%d want >= 3", ev.Attempts)
	}
	if ev.LastError == "" {
		t.Error("LastError empty; want non-empty error description")
	}
}

// TestPusher_SuccessClearsFailCounter — a transient error followed by
// success must not leave the threshold armed for that seq.
func TestPusher_SuccessClearsFailCounter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	outbox := newFakeOutbox("ch-1", 1)
	transport := newFlakyTransport(2) // fails twice, succeeds afterwards
	client, _ := transit.NewClient(transit.ClientConfig{
		DaemonID:  "daemon-A",
		Transport: transport,
	})
	_, _ = client.Connect(ctx)

	var firedCount atomic.Int32
	emitter := transit.EventEmitter{
		OnViewSyncFailed: func(transit.ViewSyncFailedEvent) {
			firedCount.Add(1)
		},
	}

	pusher, _ := transit.NewPusher(transit.PusherConfig{
		Outbox:                outbox,
		Client:                client,
		Cursors:               transit.NewCursorTracker(),
		FrameID:               atomicFrameID(),
		MaxRetriesBeforeEvent: 5,
		BacklogHighWatermark:  -1,
		Emitter:               emitter,
	})

	for i := 0; i < 10; i++ {
		_, _ = pusher.Drain(ctx)
	}

	if firedCount.Load() != 0 {
		t.Errorf("view_sync_failed fired %d times; want 0 (under threshold)", firedCount.Load())
	}
}

// TestPusher_BacklogWatermarkFiresOnceAboveThreshold — PendingCount over
// the watermark trips OnViewSyncBacklog once; staying above does not
// repeat the alert; dropping below re-arms.
func TestPusher_BacklogWatermarkFiresOnceAboveThreshold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	outbox := newFakeOutbox("ch-1", 5)
	// Synthetically inflate the pending count so it exceeds the watermark.
	outbox.pendingN = 100

	bus := transit.NewMockBus(64)
	client, _ := transit.NewClient(transit.ClientConfig{DaemonID: "daemon-A", Transport: bus})
	_, _ = client.Connect(ctx)

	var (
		events  []transit.ViewSyncBacklogEvent
		eventMu sync.Mutex
	)
	emitter := transit.EventEmitter{
		OnViewSyncBacklog: func(ev transit.ViewSyncBacklogEvent) {
			eventMu.Lock()
			events = append(events, ev)
			eventMu.Unlock()
		},
	}

	pusher, _ := transit.NewPusher(transit.PusherConfig{
		Outbox:                outbox,
		Client:                client,
		Cursors:               transit.NewCursorTracker(),
		FrameID:               atomicFrameID(),
		MaxRetriesBeforeEvent: -1,
		BacklogHighWatermark:  50,
		Emitter:               emitter,
	})

	// Drain once with pending=100>50 → trips.
	if _, err := pusher.Drain(ctx); err != nil {
		t.Fatalf("Drain1: %v", err)
	}
	// Drain again — counts after the 5 pushes drop to ~95; still above
	// watermark; MUST NOT re-fire (alert is edge-triggered).
	outbox.pendingN = 95
	if _, err := pusher.Drain(ctx); err != nil {
		t.Fatalf("Drain2: %v", err)
	}

	eventMu.Lock()
	if len(events) != 1 {
		eventMu.Unlock()
		t.Fatalf("expected exactly 1 backlog event; got %d", len(events))
	}
	if events[0].PendingCount < 50 {
		t.Errorf("PendingCount=%d want >= 50", events[0].PendingCount)
	}
	if events[0].Watermark != 50 {
		t.Errorf("Watermark=%d want 50", events[0].Watermark)
	}
	eventMu.Unlock()

	// Drop below watermark, then back above — alert re-arms.
	outbox.pendingN = 10
	if _, err := pusher.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	outbox.pendingN = 200
	if _, err := pusher.Drain(ctx); err != nil {
		t.Fatal(err)
	}

	eventMu.Lock()
	defer eventMu.Unlock()
	if len(events) != 2 {
		t.Fatalf("after drop+rise expected 2 events; got %d", len(events))
	}
}

func TestPusher_BackpressureOnHighWatermark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	outbox := newFakeOutbox("ch-1", 5)
	outbox.pendingN = 51

	bus := transit.NewMockBus(64)
	client, _ := transit.NewClient(transit.ClientConfig{DaemonID: "daemon-A", Transport: bus})
	_, _ = client.Connect(ctx)

	pusher, _ := transit.NewPusher(transit.PusherConfig{
		Outbox:               outbox,
		Client:               client,
		Cursors:              transit.NewCursorTracker(),
		FrameID:              atomicFrameID(),
		BacklogHighWatermark: 50,
	})

	sent, err := pusher.Drain(ctx)
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if sent != 0 {
		t.Fatalf("sent=%d want 0 while backlog exceeds high watermark", sent)
	}
	if calls := outbox.pendingPageCalls(); calls != 0 {
		t.Fatalf("PendingPage calls=%d want 0 under backpressure", calls)
	}
	if pushed := outbox.pushedCount(); pushed != 0 {
		t.Fatalf("pushed rows=%d want 0 under backpressure", pushed)
	}
}

// TestPusher_DefaultThresholdsApplied — zero values in PusherConfig
// resolve to the documented defaults.
func TestPusher_DefaultThresholdsApplied(t *testing.T) {
	if transit.DefaultMaxRetriesBeforeEvent != 5 {
		t.Errorf("DefaultMaxRetriesBeforeEvent=%d want 5",
			transit.DefaultMaxRetriesBeforeEvent)
	}
	if transit.DefaultBacklogHighWatermark != 10000 {
		t.Errorf("DefaultBacklogHighWatermark=%d want 10000",
			transit.DefaultBacklogHighWatermark)
	}
}

// TestPusher_FailHookStillFiresAlongsideEmitter — back-compat: legacy
// FailHook callers must keep working even with the new Emitter wired.
func TestPusher_FailHookStillFiresAlongsideEmitter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	outbox := newFakeOutbox("ch-1", 1)
	transport := newFlakyTransport(1)
	client, _ := transit.NewClient(transit.ClientConfig{DaemonID: "daemon-A", Transport: transport})
	_, _ = client.Connect(ctx)

	var hookHits atomic.Int32
	pusher, _ := transit.NewPusher(transit.PusherConfig{
		Outbox:                outbox,
		Client:                client,
		Cursors:               transit.NewCursorTracker(),
		FrameID:               atomicFrameID(),
		FailHook:              func(channel.ID, viewsync.Seq, error) { hookHits.Add(1) },
		MaxRetriesBeforeEvent: 10,
		BacklogHighWatermark:  -1,
	})

	if _, err := pusher.Drain(ctx); err == nil {
		t.Fatal("first Drain should fail (flaky transport)")
	}
	if hookHits.Load() != 1 {
		t.Errorf("FailHook hits=%d want 1", hookHits.Load())
	}
}

// itoa + atomicFrameID are shared with transit_test.go (same package).
