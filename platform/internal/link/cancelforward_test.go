package link

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// blockingRWC is an io.ReadWriteCloser whose Read/Write never return until Close
// is called — the stand-in for a wedged carrier (peer not draining: TCP black
// hole, or a peer that never reads its receive buffer). writeUnblocked receives
// once each blocked Write actually returns, so a test can prove a write goroutine
// was released (not merely that some other path moved on).
type blockingRWC struct {
	closeOnce      sync.Once
	closed         chan struct{}
	writeUnblocked chan struct{}
}

func newBlockingRWC() *blockingRWC {
	return &blockingRWC{
		closed:         make(chan struct{}),
		writeUnblocked: make(chan struct{}, 8),
	}
}

func (c *blockingRWC) Read([]byte) (int, error) {
	<-c.closed
	return 0, errors.New("link: closed (test)")
}

func (c *blockingRWC) Write([]byte) (int, error) {
	<-c.closed
	select {
	case c.writeUnblocked <- struct{}{}:
	default:
	}
	return 0, errors.New("link: closed (test)")
}

func (c *blockingRWC) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

// TestSendCancelRequest_StuckWriteDoesNotBlockCaller is the review-finding
// regression: a cancel-forward write onto a wedged link (peer not reading, TCP
// black hole) must never leave the caller — the actor worker/ledger goroutine
// abandoning its own request — hostage to a write that will not drain (violates
// the fire-and-forget best-effort contract SendCancelRequest itself documents).
// It must also not leak the goroutine carrying the write: past
// cancelForwardWriteGrace the link is torn down, which — session Close → carrier
// close → the blocked substream write errors out — force-unblocks it.
func TestSendCancelRequest_StuckWriteDoesNotBlockCaller(t *testing.T) {
	origGrace := cancelForwardWriteGrace
	cancelForwardWriteGrace = 50 * time.Millisecond
	defer func() { cancelForwardWriteGrace = origGrace }()

	// A REAL yamux session over the wedged carrier: this is exactly how a link
	// teardown unblocks a stuck write in production — ls.Close() → ys.Close() →
	// carrier.Close(), which errors the blocked write out from underneath it. The
	// cancel frame itself is issued directly onto the carrier via the codec (a
	// substream would need a live peer to open; the carrier IS the stuck seam).
	block := newBlockingRWC()
	ys, err := yamux.Client(block, linkYamuxConfig())
	if err != nil {
		t.Fatalf("yamux client: %v", err)
	}
	ls := &linkSession{ys: ys}

	const id = actor.ActorID("actor:stuck")
	codec := ipc.NewCodec(block, block)
	as := &actorStream{id: id, stream: block, codec: codec}

	d := testDialer()
	d.lc = ls
	d.streams[id] = as

	start := time.Now()
	done := make(chan struct{})
	go func() {
		d.SendCancelRequest(id, message.ID("req-1"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SendCancelRequest blocked the caller on a stuck link write")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("SendCancelRequest took %v to return — it must return immediately, not wait on the wire write", elapsed)
	}

	// Past grace the link must be torn down (the only safe way to unblock a
	// carrier write that will not drain — see cancelForwardWriteGrace docstring).
	select {
	case <-block.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("stuck cancel-forward write never forced the link closed — grace timer did not fire")
	}

	// And the blocked write call itself must actually return (proving the
	// goroutine carrying the write exits — no leak past grace).
	select {
	case <-block.writeUnblocked:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked write never returned after link teardown — write goroutine leaked")
	}
}
