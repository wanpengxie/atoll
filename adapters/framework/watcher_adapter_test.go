package framework

import (
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/adapter/futurereg"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// fakeWatcher is a minimal futurereg.Watcher whose inner event stream the test
// drives directly. It records Close() so the test can assert the single
// forwarder is torn down exactly once.
type fakeWatcher struct {
	events chan futurereg.WatchEvent

	mu     sync.Mutex
	closed int
}

func newFakeWatcher() *fakeWatcher {
	return &fakeWatcher{events: make(chan futurereg.WatchEvent, 16)}
}

func (f *fakeWatcher) Events() <-chan futurereg.WatchEvent { return f.events }

func (f *fakeWatcher) Close() error {
	f.mu.Lock()
	f.closed++
	f.mu.Unlock()
	return nil
}

func (f *fakeWatcher) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// TestWatcherAdapter_ConcurrentEvents_NoRace hammers Events() from many
// goroutines on a freshly constructed watcherAdapter. Pre-fix, Events() did a
// lock-free lazy init (`if w.events == nil { ... }`), so concurrent first
// callers raced on w.events/w.done and could start two forwarders that both
// ranged the same inner stream (one event to each → lost events). Eager
// construction in newWatcherAdapter means the channel is written once before
// any caller observes it; this test must be clean under `-race` and every
// caller must see the identical channel value (a single forwarder).
//
// Run with: go test -race ./adapters/framework/...
func TestWatcherAdapter_ConcurrentEvents_NoRace(t *testing.T) {
	inner := newFakeWatcher()
	wa := newWatcherAdapter(inner)

	const callers = 64
	got := make([]<-chan adapter.WatchEvent, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer wg.Done()
			got[i] = wa.Events()
		}(i)
	}
	wg.Wait()

	// All callers must observe the exact same channel (single forwarder).
	for i := 1; i < callers; i++ {
		if got[i] != got[0] {
			t.Fatalf("Events() returned different channels across concurrent callers (caller %d)", i)
		}
	}

	// Feed an event through the single forwarder; exactly one reader must see it.
	inner.events <- futurereg.WatchEvent{Envelope: &message.Envelope{ID: "e1"}, IsFinal: false}
	select {
	case ev, ok := <-wa.Events():
		if !ok {
			t.Fatalf("events channel closed unexpectedly")
		}
		if ev.Envelope == nil || ev.Envelope.ID != "e1" {
			t.Fatalf("unexpected forwarded event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatalf("forwarder did not deliver the event")
	}

	// Closing the inner stream must drain the forwarder and close the adapter
	// stream exactly once (single forwarder ⇒ single close(events)).
	close(inner.events)
	select {
	case _, ok := <-wa.Events():
		if ok {
			t.Fatalf("expected adapter events channel to be closed after inner drained")
		}
	case <-time.After(time.Second):
		t.Fatalf("forwarder did not close adapter events channel after inner stream ended")
	}
}

// TestWatcherAdapter_ConcurrentClose_NoDoublePanic asserts that concurrent
// Close() calls are safe: the sync.Once guard means done is closed exactly once
// (a naive double close(w.done) would panic).
func TestWatcherAdapter_ConcurrentClose_NoDoublePanic(t *testing.T) {
	inner := newFakeWatcher()
	wa := newWatcherAdapter(inner)

	const closers = 32
	var wg sync.WaitGroup
	wg.Add(closers)
	for i := 0; i < closers; i++ {
		go func() {
			defer wg.Done()
			_ = wa.Close()
		}()
	}
	wg.Wait()

	if got := inner.closeCount(); got != closers {
		t.Fatalf("inner.Close() call count = %d, want %d (Close must delegate every call)", got, closers)
	}
}
