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

// blockingWireConn is a wireConn whose WriteMessage never returns until Close
// is called — the stand-in for a stuck/half-dead peer (TCP black hole, or a
// peer that simply never drains its receive buffer). writeUnblocked receives
// once each blocked WriteMessage call actually returns, so a test can prove a
// write goroutine was released (not merely that some other path moved on).
type blockingWireConn struct {
	closeOnce      sync.Once
	closed         chan struct{}
	writeUnblocked chan struct{}
}

func newBlockingWireConn() *blockingWireConn {
	return &blockingWireConn{
		closed:         make(chan struct{}),
		writeUnblocked: make(chan struct{}, 8),
	}
}

func (c *blockingWireConn) ReadMessage() ([]byte, error) {
	<-c.closed
	return nil, errors.New("link: closed (test)")
}

func (c *blockingWireConn) WriteMessage([]byte) error {
	<-c.closed
	select {
	case c.writeUnblocked <- struct{}{}:
	default:
	}
	return errors.New("link: closed (test)")
}

func (c *blockingWireConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

var _ wireConn = (*blockingWireConn)(nil)

// TestSendCancelRequest_StuckWriteDoesNotBlockCaller is the review-finding
// regression: a cancel-forward write onto a wedged link (peer not reading,
// TCP black hole) must never leave the caller — the actor worker/ledger
// goroutine abandoning its own request — hostage to a write that will not
// drain (violates the fire-and-forget best-effort contract SendCancelRequest
// itself documents). It must also not leak the goroutine carrying the write:
// past cancelForwardWriteGrace the link is torn down, which force-unblocks
// the stuck conn write from underneath it.
func TestSendCancelRequest_StuckWriteDoesNotBlockCaller(t *testing.T) {
	origGrace := cancelForwardWriteGrace
	cancelForwardWriteGrace = 50 * time.Millisecond
	defer func() { cancelForwardWriteGrace = origGrace }()

	conn := newBlockingWireConn()
	lc := newLinkConn(conn, nil, nil)

	// Wire one actor stream directly into the mux table (bypassing openStream,
	// whose own OpOpen write would otherwise block forever on this fake conn
	// before the test even reaches SendCancelRequest).
	const id = actor.ActorID("actor:stuck")
	s := newStream(1, lc.writeFrame, func() { lc.dropStream(1) })
	lc.mu.Lock()
	lc.streams[1] = s
	lc.mu.Unlock()
	codec := ipc.NewCodec(s, s)
	as := &actorStream{id: id, stream: s, codec: codec}

	d := testDialer()
	d.lc = lc
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
	// conn write that will not drain — see cancelForwardWriteGrace docstring).
	select {
	case <-conn.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("stuck cancel-forward write never forced the link closed — grace timer did not fire")
	}

	// And the blocked WriteMessage call itself must actually return (proving
	// the goroutine carrying the write exits — no leak past grace).
	select {
	case <-conn.writeUnblocked:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked conn write never returned after link teardown — write goroutine leaked")
	}
}
