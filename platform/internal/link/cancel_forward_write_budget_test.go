package link

// CANCEL FORWARD on a wedged actor stream.
//
// The daemon forwards "the caller abandoned request R" up to the home as a
// one-way frame: ActorStreamResource.CancelRequest (= RemoteWriter.sendCancel,
// dial.go) → relayCore.writeOneWay → ipc.Codec.Write → the actor substream.
// The caller is a cell/ledger goroutine giving up on its own request; it must
// never become hostage to a peer that stopped reading.
//
// The CURRENT contract is the SHARED transport budget, not a cancel-specific
// grace: linksession.go wraps every actor substream in a boundedConn whose
// per-write budget is streamWriteBudget, and a write that misses it kills THAT
// substream only — "a peer that stops reading kills only its own stream; the
// failure is local and never session evidence" (linksession.go dispatch). This
// is deliberately WIDER than the retired cancel-only grace, so what these tests
// pin is the new shape, verbatim:
//
//	(1) the actor substream — the one a cancel forward writes onto — carries the
//	    shared bounded budget, and the lane substream deliberately does not;
//	(2) a cancel forward onto a wedged substream returns to its caller when that
//	    budget expires, with an honest timeout error and no fabricated success;
//	(3) the verdict closes the wedged substream and NOTHING else: the carrier
//	    lives, no session evidence is raised, and a sibling actor substream on
//	    the same carrier still forwards a cancel end to end;
//	(4) nothing stays parked on the dead write — the caller's own goroutine
//	    carries it (there is no detached write goroutine racing a grace timer
//	    any more), and the arm's write lock is free the instant it returns.
//
// Budget injection uses boundedConn.budget, the field production documents as
// the test seam; test (1) is what ties that injected number back to the real
// streamWriteBudget the production wrap installs.

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// cancelForwardTimeout bounds events that MUST happen. It is far wider than
// any budget under test so a loaded machine cannot flake these.
const cancelForwardTimeout = 20 * time.Second

// cancelForwardBudget is the injected stand-in for streamWriteBudget. Test (1)
// pins the production value it stands for.
const cancelForwardBudget = 300 * time.Millisecond

// TestActorStreamCarriesSharedWriteBudget pins the wrap decision the whole
// cancel-forward bound rests on. The cancel forward writes onto an actor
// substream, so that substream must arrive bounded by the shared budget
// (budget==0 → streamWriteBudget).
func TestActorStreamCarriesSharedWriteBudget(t *testing.T) {
	rig := newCancelForwardRig(t)

	actorConn, err := rig.session.openTagged(context.Background(), streamActor)
	if err != nil {
		t.Fatalf("open actor stream: %v", err)
	}
	bounded, ok := actorConn.(*boundedConn)
	if !ok {
		t.Fatalf("actor substream is %T, not write-budgeted — a cancel forward onto it is unbounded", actorConn)
	}
	if bounded.budget != 0 {
		t.Fatalf("actor substream budget=%v want 0 (inherit the shared streamWriteBudget)", bounded.budget)
	}
	if streamWriteBudget <= 0 || streamWriteBudget > 10*time.Second {
		t.Fatalf("streamWriteBudget=%v: the shared per-write budget must stay bounded by the declared 10s", streamWriteBudget)
	}
}

// TestCancelForwardOnWedgedStreamIsBoundedAndLocal drives the real forward path
// onto a substream whose peer has stopped reading, and holds it to (2)+(3)+(4).
func TestCancelForwardOnWedgedStreamIsBoundedAndLocal(t *testing.T) {
	rig := newCancelForwardRig(t)

	wedged := rig.openActorStream(t, cancelForwardBudget)
	healthy := rig.openActorStream(t, 0)

	// The peer of `wedged` is accepted and then never read from; `healthy`'s
	// peer is drained. Fill the wedged substream's send window so the next
	// write has nowhere to go — the transport shape of a stuck peer.
	_ = rig.acceptPeer(t) // the wedged substream's peer: accepted, never read
	healthyPeer := rig.acceptPeer(t)
	rig.readStreamHeader(t, healthyPeer)
	rig.wedgeSendWindow(t, wedged.Conn)

	// (2) The forward is issued exactly as OpenExactActorStream wires it.
	forward := ActorStreamResource{CancelRequest: NewRemoteWriter(ipc.NewCodec(wedged, wedged)).sendCancel}
	type forwardResult struct {
		err     error
		elapsed time.Duration
	}
	returned := make(chan forwardResult, 1)
	go func() {
		start := time.Now()
		err := forward.CancelRequest("req-wedged")
		returned <- forwardResult{err: err, elapsed: time.Since(start)}
	}()

	var got forwardResult
	select {
	case got = <-returned:
	case <-time.After(cancelForwardTimeout):
		t.Fatal("cancel forward never returned: the caller is hostage to a peer that stopped reading")
	}
	if got.err == nil {
		t.Fatal("a cancel forward that never reached the wire reported success")
	}
	if !isConnectionWriteTimeout(got.err) {
		t.Fatalf("cancel forward error=%v; want the transport write-budget verdict", got.err)
	}
	// The caller's OWN goroutine carried the write to its verdict: it returns
	// at the budget, not immediately (which would mean a detached goroutine
	// still holds the write) and not later (which would mean unbounded).
	if got.elapsed < cancelForwardBudget*9/10 {
		t.Fatalf("cancel forward returned after %v, before its %v budget — the write was handed to something else",
			got.elapsed, cancelForwardBudget)
	}

	// (3) The verdict closed the wedged substream…
	if _, err := wedged.Conn.Write([]byte("x")); err == nil {
		t.Fatal("the wedged substream survived its own write-budget verdict")
	}
	// …and nothing else. The carrier is untouched and raised no evidence: a
	// stuck peer on one actor substream is a local failure, never a session
	// death.
	select {
	case <-rig.session.closed():
		t.Fatal("one wedged actor substream killed the whole carrier")
	default:
	}
	if reasons := rig.evidenceReasons(); len(reasons) != 0 {
		t.Fatalf("a local substream write failure escalated to session evidence: %v", reasons)
	}

	// A sibling substream on the same carrier still forwards end to end.
	const liveRequest = message.ID("req-healthy")
	sibling := ActorStreamResource{CancelRequest: NewRemoteWriter(ipc.NewCodec(healthy, healthy)).sendCancel}
	if err := sibling.CancelRequest(liveRequest); err != nil {
		t.Fatalf("sibling substream cancel forward failed after the wedged one died: %v", err)
	}
	frame := rig.readFrame(t, healthyPeer)
	if frame.Kind != ipc.KindCancelRequest {
		t.Fatalf("sibling frame kind=%q want %q", frame.Kind, ipc.KindCancelRequest)
	}
	var payload ipc.CancelPayload
	if err := json.Unmarshal(frame.Payload, &payload); err != nil {
		t.Fatalf("decode sibling cancel payload: %v", err)
	}
	if payload.RequestID != liveRequest {
		t.Fatalf("sibling cancel request=%q want %q", payload.RequestID, liveRequest)
	}

	// (4) Nothing is parked on the dead write: the same arm answers again at
	// once instead of queueing behind a write lock held by a stuck goroutine.
	second := make(chan error, 1)
	go func() { second <- forward.CancelRequest("req-wedged-again") }()
	select {
	case err := <-second:
		if err == nil {
			t.Fatal("a forward onto the closed substream reported success")
		}
	case <-time.After(cancelForwardTimeout):
		t.Fatal("the arm's write path stayed locked by the abandoned write")
	}
}

// --- rig -------------------------------------------------------------------

// cancelForwardRig is one live carrier with a production linkSession on the
// dialing end and a bare yamux peer on the other, so "only that substream
// died" is an observable statement about a real multiplexed carrier.
type cancelForwardRig struct {
	session *linkSession
	peer    *yamux.Session

	evidence chan SessionEndReason
}

func newCancelForwardRig(t *testing.T) *cancelForwardRig {
	t.Helper()
	carrierA, carrierB := net.Pipe()
	client, err := yamux.Client(carrierA, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	peer, err := yamux.Server(carrierB, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	rig := &cancelForwardRig{peer: peer, evidence: make(chan SessionEndReason, 8)}
	rig.session = newLinkSession(client, nil, nil, nil,
		func(reason SessionEndReason, _ string, _ error) {
			select {
			case rig.evidence <- reason:
			default:
			}
		}, nil)
	t.Cleanup(func() {
		_ = rig.session.closeCarrier()
		_ = peer.Close()
		if !rig.session.waitWorkers(cancelForwardTimeout) {
			t.Error("link session workers never joined after the carrier was collected")
		}
	})
	return rig
}

// openActorStream opens one actor substream through the PRODUCTION wrap and,
// when budget is non-zero, retunes only its per-write budget (the field
// production documents as the test seam).
func (r *cancelForwardRig) openActorStream(t *testing.T, budget time.Duration) *boundedConn {
	t.Helper()
	conn, err := r.session.openTagged(context.Background(), streamActor)
	if err != nil {
		t.Fatalf("open actor stream: %v", err)
	}
	bounded, ok := conn.(*boundedConn)
	if !ok {
		t.Fatalf("actor substream is %T, not write-budgeted", conn)
	}
	if budget != 0 {
		bounded.budget = budget
	}
	return bounded
}

func (r *cancelForwardRig) acceptPeer(t *testing.T) net.Conn {
	t.Helper()
	type accepted struct {
		conn net.Conn
		err  error
	}
	out := make(chan accepted, 1)
	go func() {
		conn, err := r.peer.Accept()
		out <- accepted{conn: conn, err: err}
	}()
	select {
	case got := <-out:
		if got.err != nil {
			t.Fatalf("accept substream: %v", got.err)
		}
		t.Cleanup(func() { _ = got.conn.Close() })
		return got.conn
	case <-time.After(cancelForwardTimeout):
		t.Fatal("substream never reached the peer")
		return nil
	}
}

// readStreamHeader consumes the tagging header openTagged wrote, leaving the
// substream positioned at the first ipc frame.
func (r *cancelForwardRig) readStreamHeader(t *testing.T, conn net.Conn) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		var header streamHeader
		err := readStreamJSON(conn, &header)
		if err == nil && header.Kind != streamActor {
			t.Errorf("substream header kind=%q want %q", header.Kind, streamActor)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read substream header: %v", err)
		}
	case <-time.After(cancelForwardTimeout):
		t.Fatal("substream header never arrived")
	}
}

func (r *cancelForwardRig) readFrame(t *testing.T, conn net.Conn) ipc.Frame {
	t.Helper()
	type readResult struct {
		frame ipc.Frame
		err   error
	}
	out := make(chan readResult, 1)
	go func() {
		frame, err := ipc.NewCodec(conn, conn).Read()
		out <- readResult{frame: frame, err: err}
	}()
	select {
	case got := <-out:
		if got.err != nil {
			t.Fatalf("read frame: %v", got.err)
		}
		return got.frame
	case <-time.After(cancelForwardTimeout):
		t.Fatal("no frame reached the peer end of the substream")
		return ipc.Frame{}
	}
}

// cancelForwardFillStall is how long a fill write may stall before the window
// is judged exhausted. It is orders of magnitude longer than an in-memory copy
// so a loaded machine cannot mistake slowness for a full window; the loop only
// pays it once, on the write that actually stalls.
const cancelForwardFillStall = time.Second

// wedgeSendWindow fills the substream's flow-control window so the next write
// has nowhere to go — the transport shape of a peer that stopped reading. A
// stalled fill write is only believed once a single further BYTE also has
// nowhere to go; if the window reopened instead, filling resumes. The deadline
// is cleared on the way out so the object under test installs its own.
func (r *cancelForwardRig) wedgeSendWindow(t *testing.T, conn net.Conn) {
	t.Helper()
	chunk := make([]byte, 32*1024)
	deadline := time.Now().Add(cancelForwardTimeout)
	for time.Now().Before(deadline) {
		_ = conn.SetWriteDeadline(time.Now().Add(cancelForwardFillStall))
		if _, err := conn.Write(chunk); err != nil {
			if !isConnectionWriteTimeout(err) {
				t.Fatalf("filling the substream window failed with %v, not a stall", err)
			}
			_ = conn.SetWriteDeadline(time.Now().Add(cancelForwardFillStall))
			if _, err := conn.Write([]byte{0}); err != nil {
				if !isConnectionWriteTimeout(err) {
					t.Fatalf("confirming the exhausted window failed with %v, not a stall", err)
				}
				_ = conn.SetWriteDeadline(time.Time{})
				return
			}
		}
	}
	t.Fatal("the substream window never filled: the peer is still draining it")
}

func (r *cancelForwardRig) evidenceReasons() []SessionEndReason {
	var reasons []SessionEndReason
	for {
		select {
		case reason := <-r.evidence:
			reasons = append(reasons, reason)
		default:
			return reasons
		}
	}
}
