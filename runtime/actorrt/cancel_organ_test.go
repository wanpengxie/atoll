package actorrt

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

// gatedCanceller is a cell occupant that implements both Actor and
// RequestCanceller, recording every CancelRequest dispatch and the peak number
// of CONCURRENT dispatches (the §16 organ invariant: a single cell has ≤1 drain
// goroutine, so its RequestCanceller is never entered concurrently). Optional
// gates let a test freeze the drainer (gate) or the cell's Receive line
// (receiveGate) at a chosen point.
type gatedCanceller struct {
	mu         sync.Mutex
	calls      []message.ID
	concurrent int
	maxConc    int

	gate        chan struct{} // if non-nil, CancelRequest blocks until closed
	receiveGate chan struct{} // if non-nil, Receive blocks until closed
	entered     chan struct{} // signalled (buffered 1) on each CancelRequest entry
}

func (g *gatedCanceller) Receive(ctx context.Context, env *message.Envelope) error {
	if g.receiveGate != nil {
		<-g.receiveGate
	}
	return nil
}

func (g *gatedCanceller) CancelRequest(id message.ID) {
	g.mu.Lock()
	g.concurrent++
	if g.concurrent > g.maxConc {
		g.maxConc = g.concurrent
	}
	g.calls = append(g.calls, id)
	g.mu.Unlock()
	if g.entered != nil {
		select {
		case g.entered <- struct{}{}:
		default:
		}
	}
	if g.gate != nil {
		<-g.gate
	}
	g.mu.Lock()
	g.concurrent--
	g.mu.Unlock()
}

func (g *gatedCanceller) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}

// waitDrainIdle blocks until the cell's pending-cancel drainer has quiesced (no
// drainer flagged, set empty) or the deadline passes.
func waitDrainIdle(t *testing.T, c *cell) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !c.cancelDraining.Load() {
			c.cancelMu.Lock()
			empty := len(c.pendingCancel) == 0
			c.cancelMu.Unlock()
			if empty {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("pending-cancel drainer never quiesced")
}

// TestCancelOrgan_MergeIdempotent: while the drainer is frozen mid-dispatch (set
// already emptied), 99 re-cancels of the SAME id collapse into a single set
// entry, so the occupant sees exactly one further dispatch — not 99. (1 initial
// dispatch + 1 for the merged batch = 2 total.)
func TestCancelOrgan_MergeIdempotent(t *testing.T) {
	t.Parallel()
	g := &gatedCanceller{gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	c := newCell(context.Background(), "a", g, 4, nil, nil, nil, time.Now(), nil)

	c.cancelRequest("x") // spawns drainer; it clears the set and blocks inside CancelRequest("x")
	<-g.entered          // drainer is now frozen mid-dispatch, set empty
	for i := 0; i < 99; i++ {
		c.cancelRequest("x") // all merge into the single set entry {x}
	}
	close(g.gate) // release: drainer dispatches the merged {x} once more, then exits

	deadline := time.Now().Add(2 * time.Second)
	for g.callCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Let any erroneous extra dispatch surface before asserting the final count.
	time.Sleep(50 * time.Millisecond)
	if n := g.callCount(); n != 2 {
		t.Fatalf("same-id cancels dispatched %d times, want 2 (1 initial + 99 merged into 1)", n)
	}
}

// TestCancelOrgan_OverflowCounted: with the drainer frozen, filling the set to
// cancelSetCap and then pushing more distinct ids counts each surplus as a
// dropped overflow (best-effort — the request's own deadline backstops it).
func TestCancelOrgan_OverflowCounted(t *testing.T) {
	t.Parallel()
	g := &gatedCanceller{gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	c := newCell(context.Background(), "a", g, 4, nil, nil, nil, time.Now(), nil)

	c.cancelRequest("seed") // drainer clears the set, then freezes inside CancelRequest("seed")
	<-g.entered             // set is now empty, drainer frozen

	for i := 0; i < cancelSetCap; i++ {
		c.cancelRequest(message.ID(fmt.Sprintf("id-%d", i))) // fills the set exactly to cap
	}
	const extra = 10
	for i := 0; i < extra; i++ {
		c.cancelRequest(message.ID(fmt.Sprintf("over-%d", i))) // each is a counted, dropped overflow
	}
	if got := c.cancelOverflow.Load(); got != extra {
		t.Fatalf("overflow count = %d, want %d", got, extra)
	}
	close(g.gate)
}

// TestCancelOrgan_StormSingleDrainerPerCell: 10k concurrent cancel frames at one
// cell must never spin up more than one drain goroutine — the RequestCanceller,
// invoked only from that single drainer, is never entered concurrently
// (maxConc <= 1). This is the §16 修正案 storm invariant (每 cell ≤1 消费者).
func TestCancelOrgan_StormSingleDrainerPerCell(t *testing.T) {
	t.Parallel()
	g := &gatedCanceller{} // no gate: dispatch runs freely
	c := newCell(context.Background(), "a", g, 4, nil, nil, nil, time.Now(), nil)

	const frames = 10000
	var wg sync.WaitGroup
	for i := 0; i < frames; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.cancelRequest(message.ID(fmt.Sprintf("r-%d", i%512)))
		}(i)
	}
	wg.Wait()
	waitDrainIdle(t, c)

	g.mu.Lock()
	mc := g.maxConc
	g.mu.Unlock()
	if mc > 1 {
		t.Fatalf("peak concurrent drainers for one cell = %d, want <=1", mc)
	}
}

// TestCancelOrgan_ArrivesWhileReceiveBlocked: a cancel must reach the occupant
// even while the cell's serial Receive line is occupied by a blocking Receive —
// the organ dispatches OFF the cell goroutine, so a wedged Receive can never
// stall a cancel (the invariant link_test's cross-wire guard also locks).
func TestCancelOrgan_ArrivesWhileReceiveBlocked(t *testing.T) {
	t.Parallel()
	g := &gatedCanceller{receiveGate: make(chan struct{}), entered: make(chan struct{}, 1)}
	c := newCell(context.Background(), "a", g, 4, nil, nil, nil, time.Now(), nil)
	c.start()
	defer func() {
		close(g.receiveGate) // unblock Receive so the cell goroutine can exit
		c.initiateStop()
		<-c.done
	}()

	// Occupy the serial Receive line with a Receive that blocks on receiveGate.
	if err := c.Deliver(env("work")); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	c.cancelRequest("req-1")
	select {
	case <-g.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel never dispatched while Receive was blocked")
	}
}
