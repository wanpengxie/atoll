package link_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

func attachPortForTest(r *actorrt.Runtime, ctx context.Context, conn io.ReadWriteCloser, sinks actorrt.Sinks, resolve actorrt.ResolveFunc, kindOf actorrt.KindOf, cancel func(actor.ActorID, message.ID)) (actorrt.Incarnation, error) {
	prepared, err := r.PrepareHandshake(ctx, conn, sinks, resolve, kindOf, cancel, func(actorrt.Incarnation) {})
	if err != nil {
		return actorrt.Incarnation{}, err
	}
	return prepared.Commit(func() bool { return true })
}

// recordPen is a minimal raw harness.Pen: it counts the writes that reach it and
// always accepts. Wrapping it in a livePen lets a test assert which writes the
// incarnation gate let THROUGH to the raw pen versus fenced before it.
type recordPen struct {
	mu sync.Mutex
	n  int
}

func (p *recordPen) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	p.mu.Lock()
	p.n++
	seq := p.n
	p.mu.Unlock()
	return harness.WriteResult{MessageID: env.ID, Seq: int64(seq)}, nil
}

func (p *recordPen) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

// noopLiveActor is a trivial cell — it exists only to be hosted (and despawned).
type noopLiveActor struct{}

func (noopLiveActor) Receive(context.Context, *message.Envelope) error { return nil }

// TestLivePenFencesPostDeathWrite covers the death-after-write fence on the cell
// path: a pen welded to an incarnation writes while the incarnation is live, is
// structurally rejected during construction (before go-live), and is rejected
// with ErrWriterNotLive after the incarnation is despawned — proving a capability
// that outlives its incarnation cannot author truth on its behalf.
func TestLivePenFencesPostDeathWrite(t *testing.T) {
	t.Parallel()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	defer rt.StopAll()

	raw := &recordPen{}
	var pen harness.Pen
	var ctorErr error

	// The build closure runs inside Spawn, BEFORE go-live: IsLive(inc)==false, so a
	// write attempted during construction is fenced (the "factory must not write"
	// rule is structural, not a soft convention).
	inc, _, _ := rt.SpawnIfAbsent("w", actor.KindTool, func(i actorrt.Incarnation) actorrt.Actor {
		pen = link.NewLivePen(raw, i, rt)
		_, ctorErr = pen.Write(context.Background(), &message.Envelope{ID: "during-ctor"})
		return noopLiveActor{}
	})

	if !errors.Is(ctorErr, link.ErrWriterNotLive) {
		t.Fatalf("construction-time write err = %v, want ErrWriterNotLive (pre-go-live)", ctorErr)
	}
	if raw.count() != 0 {
		t.Fatalf("raw pen saw %d writes during construction, want 0 (fenced before go-live)", raw.count())
	}

	// Now live: the write passes the gate and reaches the raw pen.
	if _, err := pen.Write(context.Background(), &message.Envelope{ID: "while-live"}); err != nil {
		t.Fatalf("live write err = %v, want nil", err)
	}
	if raw.count() != 1 {
		t.Fatalf("raw pen saw %d writes while live, want 1", raw.count())
	}

	// Despawn the incarnation; the welded pen must now fence every write.
	rt.Despawn(inc)
	_, err := pen.Write(context.Background(), &message.Envelope{ID: "after-death"})
	if !errors.Is(err, link.ErrWriterNotLive) {
		t.Fatalf("post-death write err = %v, want ErrWriterNotLive", err)
	}
	if raw.count() != 1 {
		t.Fatalf("raw pen saw %d writes total, want 1 (post-death write fenced before raw)", raw.count())
	}
}

// attachTestPort connects a port INTO rt over an in-memory net.Pipe (running the
// remote handshake concurrently with Attach) and returns the bound Incarnation.
// It is the port-path analogue of Spawn in the cell test above — the only
// difference is the embodiment is an out-of-process `port`, not an in-process
// cell.
func attachTestPort(t *testing.T, rt *actorrt.Runtime, id actor.ActorID) (actorrt.Incarnation, net.Conn) {
	t.Helper()
	hostConn, remoteConn := net.Pipe()
	codec := ipc.NewCodec(remoteConn, remoteConn)
	hsErr := make(chan error, 1)
	go func() {
		payload, _ := json.Marshal(ipc.HandshakePayload{LeaseID: string(id)})
		if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload}); err != nil {
			hsErr <- err
			return
		}
		ack, err := codec.Read()
		if err != nil {
			hsErr <- err
			return
		}
		if ack.Kind != ipc.KindHandshakeAck {
			hsErr <- io.ErrUnexpectedEOF
			return
		}
		hsErr <- nil
	}()
	inc, err := attachPortForTest(rt, context.Background(),
		hostConn,
		actorrt.Sinks{Emit: func(context.Context, actorrt.Incarnation, *message.Envelope) (ipc.EmitResult, error) {
			return ipc.EmitResult{}, nil
		}},
		func(ipc.HandshakePayload) (actor.ActorID, error) { return id, nil },
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if e := <-hsErr; e != nil {
		t.Fatalf("remote handshake: %v", e)
	}
	return inc, remoteConn
}

// TestLivePenFencesPostDeathWrite_PortPath covers the death-after-write fence on
// the PORT path: a livePen welded to an out-of-process port's Incarnation
// writes while the port is the live embodiment, then — once a same-id reattach
// REPLACES that port (the predecessor incarnation is stopped) — fences every
// write with ErrWriterNotLive. It is the message-plane parity of the cell test:
// the same membrane, gated by pointer identity (ABA-safe), on the wire transport.
func TestLivePenFencesPostDeathWrite_PortPath(t *testing.T) {
	t.Parallel()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	defer rt.StopAll()

	const id = actor.ActorID("remote-1")
	inc1, remote1 := attachTestPort(t, rt, id)
	defer remote1.Close()

	raw := &recordPen{}
	pen := link.NewLivePen(raw, inc1, rt)

	// While inc1 is the live embodiment, the welded pen writes through to raw.
	if _, err := pen.Write(context.Background(), &message.Envelope{ID: "while-live"}); err != nil {
		t.Fatalf("live write err = %v, want nil", err)
	}
	if raw.count() != 1 {
		t.Fatalf("raw pen saw %d writes while live, want 1", raw.count())
	}

	// A second attach for the SAME id replaces the port: Attach stops the
	// predecessor (inc1) before returning, so a pen welded to inc1 is no longer
	// the live incarnation and must fence every emit — an in-flight cross-wire
	// emit from the replaced port cannot author truth on the successor's id.
	_, remote2 := attachTestPort(t, rt, id)
	defer remote2.Close()

	_, err := pen.Write(context.Background(), &message.Envelope{ID: "after-replace"})
	if !errors.Is(err, link.ErrWriterNotLive) {
		t.Fatalf("post-replace write err = %v, want ErrWriterNotLive", err)
	}
	if raw.count() != 1 {
		t.Fatalf("raw pen saw %d writes total, want 1 (post-replace write fenced before raw)", raw.count())
	}
}

// attachGatedPort attaches a port whose emitSink is the caller-supplied one, and
// stands up a REAL RemoteWriter on the daemon end over net.Pipe (handshake +
// EmitAck read loop). Unlike attachTestPort's no-op sink, this drives the FULL
// wire chain: remote RemoteWriter.Write → KindEmit → port.readLoop → emitSink →
// KindEmitAck → RemoteWriter.DeliverAck. Returns the bound Incarnation, the
// daemon-side writer, and the raw remote conn (for cleanup).
func attachGatedPort(t *testing.T, rt *actorrt.Runtime, id actor.ActorID, emit actorrt.EmitSink) (actorrt.Incarnation, *link.RemoteWriter, net.Conn) {
	t.Helper()
	hostConn, remoteConn := net.Pipe()
	codec := ipc.NewCodec(remoteConn, remoteConn)
	rwCh := make(chan *link.RemoteWriter, 1)
	hsErr := make(chan error, 1)
	go func() {
		payload, _ := json.Marshal(ipc.HandshakePayload{LeaseID: string(id)})
		if err := codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload}); err != nil {
			hsErr <- err
			return
		}
		ack, err := codec.Read()
		if err != nil {
			hsErr <- err
			return
		}
		if ack.Kind != ipc.KindHandshakeAck {
			hsErr <- io.ErrUnexpectedEOF
			return
		}
		rw := link.NewRemoteWriter(codec)
		rwCh <- rw
		hsErr <- nil
		// EmitAck read loop: route each ack into the FIFO waiter; any conn error
		// (the host closed the port on replace) fails all pending writers.
		for {
			f, rerr := codec.Read()
			if rerr != nil {
				rw.Close()
				return
			}
			if f.Kind == ipc.KindEmitAck {
				var ap ipc.EmitAckPayload
				if json.Unmarshal(f.Payload, &ap) == nil {
					rw.DeliverAck(ap)
				}
			}
		}
	}()
	inc, err := attachPortForTest(rt, context.Background(), hostConn, actorrt.Sinks{Emit: emit}, func(ipc.HandshakePayload) (actor.ActorID, error) { return id, nil }, nil, nil)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if e := <-hsErr; e != nil {
		t.Fatalf("remote handshake: %v", e)
	}
	return inc, <-rwCh, remoteConn
}

// TestLivePenFencesInFlightEmit_PortWireChain is the FULL-wire-chain behaviour
// proof of the replacement-live-flip invariant on the port death-write fence: an
// emit that is ALREADY IN FLIGHT inside the host emitSink when a same-id reattach
// REPLACES the port must be fenced with ErrWriterNotLive and never reach the raw
// pen — because Attach flips the predecessor dead in the SAME critical section as
// the map swap, so by the time the parked emit resumes and consults IsLive, the
// replaced incarnation already reads not-live.
//
// Determinism: the gate releases ONLY after CurrentIncarnation reports a different
// pointer for id, which can only happen once the successor is installed — and the
// predecessor's markDead precedes that install in the one critical section. So the
// IsLive verdict the parked emit sees is deterministically false. (Without the F1
// fix the flip would trail the map swap by the lock-release window, making the
// verdict a race — this test would then be flaky-red rather than stably green, so
// it is written to assert the FIXED, deterministic outcome.)
func TestLivePenFencesInFlightEmit_PortWireChain(t *testing.T) {
	t.Parallel()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	defer rt.StopAll()

	const id = actor.ActorID("remote-wire")
	raw := &recordPen{}

	emitEntered := make(chan struct{})
	release := make(chan struct{})
	sinkVerdict := make(chan error, 1)

	// The host emitSink mirrors Acceptor.emitSink's core (mint → NewLivePen → Write)
	// but parks BEFORE the livePen.Write so the test can interleave the replace. The
	// raw pen here stands in for the freshly-minted harness pen.
	emit := func(ctx context.Context, inc actorrt.Incarnation, env *message.Envelope) (ipc.EmitResult, error) {
		close(emitEntered)
		<-release
		res, werr := link.NewLivePen(raw, inc, rt).Write(ctx, env)
		sinkVerdict <- werr
		return ipc.EmitResult{
			MessageID:    res.MessageID,
			Seq:          res.Seq,
			RejectReason: string(res.RejectReason),
			RejectDetail: res.RejectDetail,
		}, werr
	}

	inc1, rw1, remote1 := attachGatedPort(t, rt, id, emit)
	defer remote1.Close()

	// The daemon emits over the real wire; the write blocks on the host's EmitAck.
	emitDone := make(chan error, 1)
	go func() {
		_, werr := rw1.Write(context.Background(), &message.Envelope{ID: "in-flight"})
		emitDone <- werr
	}()

	// Wait until the emit has traversed the wire and parked in the host sink.
	<-emitEntered

	// Releaser: once the successor is installed (pointer differs), the predecessor's
	// in-lock markDead has already run — release the parked emit then.
	go func() {
		for {
			if cur, ok := rt.CurrentIncarnation(id); ok && cur != inc1 {
				break
			}
			time.Sleep(200 * time.Microsecond) // gentle poll (not a hot spin) — avoids CPU contention under parallel test load
		}
		close(release)
	}()

	// Replace: a second attach for the SAME id stops the predecessor (Attach's
	// old.stop() joins inc1's readLoop, which the releaser unparks above).
	_, remote2 := attachTestPort(t, rt, id)
	defer remote2.Close()

	// The in-flight emit is fenced end-to-end: the daemon write returns an error
	// (its port was torn down), the sink verdict is ErrWriterNotLive, and the raw
	// pen never saw the write.
	//
	// Why the daemon-side error is asserted as "non-nil" and NOT as an ack
	// carrying ErrWriterNotLive: on the port path the incarnation IS the wire
	// — the replacement that killed the old incarnation closed its conn
	// in the same event, so the old readLoop's ack write hits a dead conn and
	// the daemon observes transport death, never the verdict payload. An ack
	// carrying ErrWriterNotLive would require "port alive but incarnation dead",
	// which cannot exist here. The authoritative fence assertions are therefore
	// host-side: the sink verdict below and the untouched raw pen.
	if werr := <-emitDone; werr == nil {
		t.Fatalf("daemon emit err = nil, want non-nil (port torn down under it)")
	}
	if verr := <-sinkVerdict; !errors.Is(verr, link.ErrWriterNotLive) {
		t.Fatalf("host sink verdict = %v, want ErrWriterNotLive", verr)
	}
	if raw.count() != 0 {
		t.Fatalf("raw pen saw %d writes, want 0 (in-flight emit fenced post-replace)", raw.count())
	}
}
