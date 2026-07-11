package link

import (
	"errors"
	"sync"
	"testing"
	"time"

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
// cancelForwardWriteGrace only that actor substream is closed, which unblocks
// the write without killing healthy sibling streams on the same session.
func TestSendCancelRequest_StuckWriteDoesNotBlockCaller(t *testing.T) {
	origGrace := cancelForwardWriteGrace
	cancelForwardWriteGrace = 50 * time.Millisecond
	defer func() { cancelForwardWriteGrace = origGrace }()

	// The blocked RWC stands in for one yamux actor substream. A distinct sibling
	// proves the timeout does not escalate into session-wide teardown.
	block := newBlockingRWC()
	sibling := newBlockingRWC()

	const id = actor.ActorID("actor:stuck")
	codec := ipc.NewCodec(block, block)
	as := &actorStream{id: id, stream: block, codec: codec}

	d := testDialer()
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

	// Past grace only the wedged actor stream is torn down.
	select {
	case <-block.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("stuck cancel-forward write never forced the actor stream closed")
	}
	select {
	case <-sibling.closed:
		t.Fatal("stuck actor stream killed a healthy sibling")
	default:
	}

	// And the blocked write call itself must actually return (proving the
	// goroutine carrying the write exits — no leak past grace).
	select {
	case <-block.writeUnblocked:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked write never returned after link teardown — write goroutine leaked")
	}
}
