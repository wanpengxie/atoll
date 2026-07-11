package link

// TRA (transport-面卫生) DoD red lines (施工单 S9 / 修复批 F-8):
//   - the control substream has exactly TWO death arms, both = "session dies,
//     process lives": a full dispatch queue and a decode error each kill the
//     WHOLE session (never a silent hang, never a process-wide crash);
//   - the control-RPC correlation table (pendingReplies) pairs each RequestID
//     to its OWN waiter even when replies arrive out of order — a reply never
//     crosses to the wrong caller.
// The control-worker-panic arm (the THIRD would-be death, folded back into
// "session dies") is pinned separately in control_failure_test.go.

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

func TestControlDeathDropsQueuedFramesBeforeTeardown(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	ls, writer, _ := newControlKillRig(t, func([]byte) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
	})
	if _, err := writer.Write([]byte("{\"n\":1}\n")); err != nil {
		t.Fatal(err)
	}
	<-entered
	if _, err := writer.Write([]byte("{\"n\":2}\n")); err != nil {
		t.Fatal(err)
	}
	ls.kill("test_session_death", nil)
	close(release)
	_ = writer.Close()
	ls.waitControlWorkers(2 * time.Second)
	if got := calls.Load(); got != 1 {
		t.Fatalf("control handlers after death=%d want only the already-running handler", got)
	}
}

func TestDuplicateControlStreamKillsSession(t *testing.T) {
	ls, _, log := newControlKillRig(t, func([]byte) {})
	existingA, existingB := net.Pipe()
	t.Cleanup(func() { _ = existingA.Close(); _ = existingB.Close() })
	ls.ctrlMu.Lock()
	ls.ctrl = existingA
	ls.ctrlMu.Unlock()
	dupA, dupB := net.Pipe()
	t.Cleanup(func() { _ = dupB.Close() })
	done := make(chan struct{})
	go func() { ls.dispatch(dupA); close(done) }()
	if err := writeStreamHeader(dupB, streamControl); err != nil {
		t.Fatalf("write header: %v", err)
	}
	waitSessionDead(t, ls)
	<-done
	if got := log.String(); !bytes.Contains([]byte(got), []byte("reason=control_stream_duplicate")) {
		t.Fatalf("duplicate control killed for wrong reason: %q", got)
	}
}

// TestMissingStreamHeaderClosesOnlyThatStream pins the honest header-admission
// contract: a substream that opens but never sends its streamHeader is closed by
// the EXISTING laneHeaderReadTimeout bound (readLaneJSON's admission deadline,
// lane.go) — dispatch returns and closes that ONE substream on its own, without
// escalating into session death. The bound is shortened here purely so the test
// runs in <1s instead of waiting out the real 30s (the old form of this test
// leaned on a caller-side read deadline to *appear* to pass while dispatch still
// blocked the full 30s inside readLaneJSON — the 30s link-package test cost).
func TestMissingStreamHeaderClosesOnlyThatStream(t *testing.T) {
	orig := laneHeaderReadTimeout
	laneHeaderReadTimeout = 30 * time.Millisecond
	defer func() { laneHeaderReadTimeout = orig }()

	ls, _, _ := newControlKillRig(t, func([]byte) {})
	streamA, streamB := net.Pipe()
	t.Cleanup(func() { _ = streamB.Close() })
	dispatched := make(chan struct{})
	go func() { ls.dispatch(streamA); close(dispatched) }()

	// dispatch returns of its own accord once laneHeaderReadTimeout fires — the
	// headerless substream is bounded-closed, no external deadline needed.
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("headerless substream was not closed by laneHeaderReadTimeout")
	}
	// dispatch closed streamA, so the peer end now observes the close — proof the
	// substream was actually torn down, not merely abandoned.
	_ = streamB.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	if _, err := streamB.Read(one[:]); err == nil {
		t.Fatal("headerless substream was not closed after its admission bound")
	}
	// The bounded close of ONE substream is not a session death — the session (and
	// thus any sibling substream) stays healthy.
	select {
	case <-ls.closed():
		t.Fatal("header timeout escalated into session death")
	default:
	}
}

// syncBuffer is a concurrency-safe io.Writer for a capturing slog handler (kill
// may log from either the decoder goroutine or the dispatch worker goroutine).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// newControlKillRig builds a linkSession over a REAL yamux session (so kill's
// yamux.Close makes CloseChan fire) with a capturing logger, plus a net.Pipe
// standing in for the control substream. onControl is the caller's handler.
func newControlKillRig(t *testing.T, onControl func([]byte)) (ls *linkSession, ctrlWriter net.Conn, log *syncBuffer) {
	t.Helper()
	carrierA, carrierB := net.Pipe()
	client, err := yamux.Client(carrierA, linkYamuxConfig())
	if err != nil {
		t.Fatalf("yamux client: %v", err)
	}
	server, err := yamux.Server(carrierB, linkYamuxConfig())
	if err != nil {
		t.Fatalf("yamux server: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	log = &syncBuffer{}
	ls = &linkSession{
		ys:        client,
		onControl: onControl,
		logger:    slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	reader, writer := net.Pipe()
	if !ls.beginControlWorker() {
		t.Fatal("control worker unexpectedly stopped")
	}
	go ls.readControl(reader)
	t.Cleanup(func() { _ = writer.Close(); _ = reader.Close() })
	return ls, writer, log
}

func waitSessionDead(t *testing.T, ls *linkSession) {
	t.Helper()
	select {
	case <-ls.closed():
	case <-time.After(2 * time.Second):
		t.Fatal("control failure never killed the session")
	}
}

// TestControlQueueFull_KillsSession pins the "队满 → session 死" arm: when the
// single control dispatch worker wedges (onControl never returns) and the
// bounded dispatch queue backs up past controlQueueDepth, the reader kills the
// whole session with reason control_queue_full — it never silently drops frames
// or unbounded-buffers them. The process itself stays up (kill closes yamux, it
// does not panic/exit).
func TestControlQueueFull_KillsSession(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	// The worker wedges on the FIRST frame, so the queue fills and overflows.
	ls, writer, log := newControlKillRig(t, func([]byte) { <-release })

	// Push far more than controlQueueDepth frames; the overflow trips the kill.
	go func() {
		frame := []byte("{}\n")
		for i := 0; i < controlQueueDepth*4; i++ {
			if _, err := writer.Write(frame); err != nil {
				return // pipe closed by the kill — expected
			}
		}
	}()

	waitSessionDead(t, ls)
	if got := log.String(); !bytes.Contains([]byte(got), []byte("reason=control_queue_full")) {
		t.Fatalf("session killed but not for control_queue_full; log=%q", got)
	}
}

// TestControlDecodeError_KillsSession pins the "解码错 → session 死" arm: a
// non-JSON byte on the control stream is the substream dying, and the whole
// session is killed with reason control_decode (a corrupt control plane is
// never limped along).
func TestControlDecodeError_KillsSession(t *testing.T) {
	ls, writer, log := newControlKillRig(t, func([]byte) {})

	go func() { _, _ = writer.Write([]byte("this-is-not-json!!!\n")) }()

	waitSessionDead(t, ls)
	if got := log.String(); !bytes.Contains([]byte(got), []byte("reason=control_decode")) {
		t.Fatalf("session killed but not for control_decode; log=%q", got)
	}
}

// TestPendingReplies_ConcurrentOutOfOrderPairing pins the control-RPC
// correlation invariant: N in-flight requests, each with its own RequestID and
// its own waiter, are answered in a SHUFFLED order, and every waiter receives
// exactly ITS reply — a reply for req-k never surfaces at the waiter for req-j.
// This is the property that lets many storage/attach RPCs share one control
// substream without their answers crossing.
func TestPendingReplies_ConcurrentOutOfOrderPairing(t *testing.T) {
	const n = 64
	p := newPendingReplies[int]()

	ids := make([]string, n)
	got := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("req-%d", i)
		ch := p.register(ids[i])
		wg.Add(1)
		go func(i int, ch chan int) {
			defer wg.Done()
			v, err := p.wait(context.Background(), ids[i], ch, nil)
			if err != nil {
				t.Errorf("waiter %d: %v", i, err)
				return
			}
			got[i] = v
		}(i, ch)
	}

	// Deliver replies in REVERSE order; each reply's value = its own index * 7,
	// so a crossed pairing is detectable.
	for i := n - 1; i >= 0; i-- {
		p.deliver(ids[i], i*7)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waiters did not all receive their replies")
	}

	for i := 0; i < n; i++ {
		if got[i] != i*7 {
			t.Fatalf("waiter for %s got %d, want %d — replies crossed", ids[i], got[i], i*7)
		}
	}
}
