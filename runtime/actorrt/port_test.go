package actorrt

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// remoteEnd is the out-of-process actor's side of a port connection: a Codec
// over one half of a net.Pipe, used by tests to drive the wire protocol
// (present the handshake, read the ack, emit upward, signal down, etc.).
type remoteEnd struct {
	conn  net.Conn
	codec *ipc.Codec
}

// dialPort builds an attached port over an in-memory net.Pipe. It runs the
// remote-side handshake (presenting leaseID) concurrently with Attach (which
// reads the handshake and replies with the ack), then returns the bound id,
// the runtime-side conn (closed by the port), and the remote end the test
// drives. emit/resolve are injected so each test can observe relays / control
// identity binding; kindOf is injected so a test can assert the port's welded
// Kind attribute (nil in tests that do not care, weld the zero value).
func dialPort(t *testing.T, r *Runtime, leaseID string, emit EmitSink, resolve ResolveFunc, kindOf KindOf) (actor.ActorID, *remoteEnd) {
	t.Helper()
	hostConn, remoteConn := net.Pipe()
	remote := &remoteEnd{conn: remoteConn, codec: ipc.NewCodec(remoteConn, remoteConn)}

	// The remote presents its handshake and consumes the ack on its own
	// goroutine; Attach (below) blocks reading the handshake + writing the ack,
	// so both must run concurrently over the synchronous pipe.
	hsErr := make(chan error, 1)
	go func() {
		payload, _ := json.Marshal(ipc.HandshakePayload{LeaseID: leaseID})
		if err := remote.codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload}); err != nil {
			hsErr <- err
			return
		}
		ack, err := remote.codec.Read()
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

	inc, err := attachTest(r, context.Background(), hostConn, Sinks{Emit: emit}, resolve, kindOf, nil)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if e := <-hsErr; e != nil {
		t.Fatalf("remote handshake: %v", e)
	}
	return inc.ID(), remote
}

// staticResolve binds every lease to the same id.
func staticResolve(id actor.ActorID) ResolveFunc {
	return func(ipc.HandshakePayload) (actor.ActorID, error) { return id, nil }
}

func nopEmit(context.Context, Incarnation, *message.Envelope) (ipc.EmitResult, error) {
	return ipc.EmitResult{}, nil
}

// TestPortParentDespawnCascadesForkedChildBeforeEscort is the port-parent
// counterpart of TestFork_CascadeOnParentDeath. The remote deliberately stops
// reading after the handshake, so the parent's despawn-frame write remains
// parked until grace. Child cancellation must still happen first: ownership is
// taken and children are signalled before the parent escort starts.
func TestPortParentDespawnCascadesForkedChildBeforeEscort(t *testing.T) {
	rt, _ := New(Config{Parent: context.Background(), ZombieGrace: 500 * time.Millisecond})
	t.Cleanup(func() { rt.StopAll() })

	parentID := actor.ActorID("tool:remote-parent")
	_, remote := dialPort(t, rt, "lease", nopEmit, staticResolve(parentID), nil)
	t.Cleanup(func() { _ = remote.conn.Close() })
	// No goroutine reads remote.codec after dialPort consumes the handshake ACK.
	// A KindDespawn write therefore cannot complete cooperatively.
	parent, ok := rt.CurrentIncarnation(parentID)
	if !ok {
		t.Fatal("attached port parent is not live")
	}
	childActor := newRecordActor()
	child, err := rt.Fork(parent, "tool:remote-parent/child", actor.KindTool, static(childActor))
	if err != nil {
		t.Fatalf("Fork from port parent: %v", err)
	}
	if !rt.IsLive(child) {
		t.Fatal("forked child is not live")
	}
	select {
	case <-childActor.startedCh:
	case <-time.After(time.Second):
		t.Fatal("forked child did not start")
	}
	rt.mu.RLock()
	owned := append([]embodiment(nil), rt.owned[parent.p]...)
	rt.mu.RUnlock()
	if len(owned) != 1 || owned[0] != child.p {
		t.Fatalf("owned port children = %v, want exactly the forked child", owned)
	}

	start := time.Now()
	rt.Despawn(parent)
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("Despawn(port parent) blocked %v on non-reading peer", elapsed)
	}
	if rt.IsLive(child) {
		t.Fatal("forked child remained live after port parent was judged dead")
	}
	rt.mu.RLock()
	_, ownsChildren := rt.owned[parent.p]
	_, parentEscorting := rt.zombies[parent.p]
	rt.mu.RUnlock()
	if ownsChildren {
		t.Fatal("port parent's owned bookkeeping survived parent despawn")
	}
	if !parentEscorting {
		t.Fatal("non-cooperative port parent completed before its child cascade could be compared")
	}

	select {
	case <-childActor.stoppedCh:
		// The peer still is not reading and the parent remains in its escort below:
		// child Stop therefore precedes parent grace/escort completion.
	case <-time.After(250 * time.Millisecond):
		t.Fatal("forked child did not stop before the port parent's grace bound")
	}
	rt.mu.RLock()
	_, parentEscorting = rt.zombies[parent.p]
	rt.mu.RUnlock()
	if !parentEscorting {
		t.Fatal("port parent escort completed before child Stop")
	}

	// Repeating both parent and child termination is a no-op: no ownership edge
	// is recreated and the child's Stop hook runs at most once.
	rt.Despawn(parent)
	rt.Despawn(child)
	select {
	case <-childActor.stoppedCh:
		t.Fatal("repeated termination invoked child Stop more than once")
	case <-time.After(20 * time.Millisecond):
	}
	rt.mu.RLock()
	_, ownsChildren = rt.owned[parent.p]
	rt.mu.RUnlock()
	if ownsChildren {
		t.Fatal("repeated termination recreated port ownership bookkeeping")
	}
}

// TestPortHandshakeBindsResolvedID: Attach resolves the presented lease to an
// ActorID, sends the ack, and registers the embodiment under that id (A1 — the
// connection IS the actor, addressable by the resolved id).
func TestPortHandshakeBindsResolvedID(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	gotLease := make(chan string, 1)
	resolve := func(hp ipc.HandshakePayload) (actor.ActorID, error) {
		gotLease <- hp.LeaseID
		return "remote-1", nil
	}
	id, remote := dialPort(t, rt, "lease-xyz", nopEmit, resolve, nil)
	defer remote.conn.Close()

	if id != "remote-1" {
		t.Fatalf("Attach bound id = %q, want remote-1", id)
	}
	if got := <-gotLease; got != "lease-xyz" {
		t.Fatalf("resolve got lease %q, want lease-xyz", got)
	}
	if _, ok := rt.Stat("remote-1"); !ok {
		t.Fatal("port not addressable after Attach")
	}
}

// TestPortHandshakeRejects covers the closed set of handshake failures: a
// non-handshake first frame, a resolve error, and an empty resolved id are all
// rejected (Attach returns an error and registers no embodiment).
func TestPortHandshakeRejects(t *testing.T) {
	t.Parallel()

	t.Run("wrong first frame kind", func(t *testing.T) {
		t.Parallel()
		rt, _ := New(Config{Parent: context.Background()})
		hostConn, remoteConn := net.Pipe()
		go func() {
			c := ipc.NewCodec(remoteConn, remoteConn)
			// An EMIT frame where a handshake is required — protocol violation.
			_ = c.Write(ipc.Frame{Kind: ipc.KindEmit})
		}()
		if _, err := attachTest(rt, context.Background(), hostConn, Sinks{Emit: nopEmit}, staticResolve("x"), nil, nil); err == nil {
			t.Fatal("Attach accepted a non-handshake first frame")
		}
	})

	t.Run("resolve error", func(t *testing.T) {
		t.Parallel()
		rt, _ := New(Config{Parent: context.Background()})
		hostConn, remoteConn := net.Pipe()
		go func() {
			c := ipc.NewCodec(remoteConn, remoteConn)
			p, _ := json.Marshal(ipc.HandshakePayload{LeaseID: "bad"})
			_ = c.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: p})
		}()
		resolve := func(ipc.HandshakePayload) (actor.ActorID, error) { return "", io.ErrUnexpectedEOF }
		if _, err := attachTest(rt, context.Background(), hostConn, Sinks{Emit: nopEmit}, resolve, nil, nil); err == nil {
			t.Fatal("Attach accepted a connection whose lease failed to resolve")
		}
	})

	t.Run("empty resolved id", func(t *testing.T) {
		t.Parallel()
		rt, _ := New(Config{Parent: context.Background()})
		hostConn, remoteConn := net.Pipe()
		go func() {
			c := ipc.NewCodec(remoteConn, remoteConn)
			p, _ := json.Marshal(ipc.HandshakePayload{LeaseID: "anon"})
			_ = c.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: p})
		}()
		if _, err := attachTest(rt, context.Background(), hostConn, Sinks{Emit: nopEmit}, staticResolve(""), nil, nil); err == nil {
			t.Fatal("Attach accepted an empty resolved actor id")
		}
	})
}

// TestAttachHandshakeBounded (F8): a peer that connects but NEVER sends a
// handshake must not pin the Attach goroutine forever — the substrate self-
// guards the handshake time bound via hsCtx. With a short-deadline hsCtx, Attach
// returns an error promptly instead of blocking on the parked read.
func TestAttachHandshakeBounded(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	hostConn, remoteConn := net.Pipe()
	// remoteConn is held open but silent — it never writes a handshake frame.
	defer func() { _ = remoteConn.Close() }()

	hsCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := attachTest(rt, hsCtx, hostConn, Sinks{Emit: nopEmit}, staticResolve("x"), nil, nil)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Attach accepted a silent peer that never handshook")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Attach hung on a silent peer — handshake bound not enforced")
	}
}

// TestPortRequiresSinks: a port cannot be constructed without an EmitSink or a
// ResolveFunc (the relay seam and the auth seam are both mandatory).
func TestPortRequiresSinks(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	hostConn, _ := net.Pipe()
	if _, err := attachTest(rt, context.Background(), hostConn, Sinks{}, staticResolve("x"), nil, nil); err == nil {
		t.Fatal("Attach accepted a nil EmitSink")
	}
	hostConn2, _ := net.Pipe()
	if _, err := attachTest(rt, context.Background(), hostConn2, Sinks{Emit: nopEmit}, nil, nil, nil); err == nil {
		t.Fatal("Attach accepted a nil ResolveFunc")
	}
}

// TestPortDeliverReachesWire: Deliver enqueues an envelope that writeLoop emits
// to the remote as a KindDeliver frame carrying the exact envelope (the host
// side of A1: a delivered message reaches the bound remote actor verbatim).
func TestPortDeliverReachesWire(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)
	defer remote.conn.Close()

	res, err := rt.deliver([]actor.ActorID{id}, env("hello"))
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if got := res.Per[id]; got != Delivered {
		t.Fatalf("outcome = %v, want Delivered", got)
	}

	remote.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame, err := remote.codec.Read()
	if err != nil {
		t.Fatalf("remote read deliver frame: %v", err)
	}
	if frame.Kind != ipc.KindDeliver {
		t.Fatalf("wire frame kind = %s, want deliver", frame.Kind)
	}
	var dp ipc.DeliverPayload
	if err := json.Unmarshal(frame.Payload, &dp); err != nil {
		t.Fatalf("decode deliver payload: %v", err)
	}
	if dp.Envelope.ID != message.ID("hello") {
		t.Fatalf("delivered envelope ID = %q, want hello", dp.Envelope.ID)
	}
}

// TestPortDeliverRejectsNilEnvelope: a mailbox carries only envelopes — Deliver
// rejects nil rather than enqueueing it (and never reports it as Delivered).
func TestPortDeliverRejectsNilEnvelope(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)
	defer remote.conn.Close()

	// Reach the port embodiment directly to exercise its nil guard (Runtime.Deliver
	// guards nil before fan-out, so the per-embodiment guard needs a direct call).
	rt.mu.RLock()
	p := rt.embodiments[id]
	rt.mu.RUnlock()
	if err := p.Deliver(nil); err == nil {
		t.Fatal("port.Deliver accepted a nil envelope")
	}
}

// TestPortEmitRelayed: an EMIT frame the remote writes is relayed verbatim to
// the injected EmitSink (the port is the upward seam from out-of-process actor
// into the channel log), and the sink's verdict is acked back to the remote as
// a KindEmitAck — the writer contract is not downgraded across the wire.
func TestPortEmitRelayed(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	type relayed struct {
		id  actor.ActorID
		env *message.Envelope
	}
	got := make(chan relayed, 1)
	emit := func(_ context.Context, inc Incarnation, e *message.Envelope) (ipc.EmitResult, error) {
		got <- relayed{id: inc.ID(), env: e}
		return ipc.EmitResult{MessageID: "m-up-1", RejectReason: "harness_kind_invalid"}, nil
	}
	_, remote := dialPort(t, rt, "l", emit, staticResolve("remote-1"), nil)
	defer remote.conn.Close()

	// The remote self-reports a DIFFERENT sender on the wire; the substrate must
	// hand the sink the AUTHENTICATED bound id, never the wire's self-report.
	payload, _ := json.Marshal(ipc.EmitPayload{Envelope: message.Envelope{ID: "up-1", Sender: message.Sender{ID: "user:alice"}}})
	if err := remote.codec.Write(ipc.Frame{Kind: ipc.KindEmit, Payload: payload}); err != nil {
		t.Fatalf("remote emit write: %v", err)
	}
	select {
	case r := <-got:
		if r.env.ID != message.ID("up-1") {
			t.Fatalf("relayed envelope ID = %q, want up-1", r.env.ID)
		}
		if r.id != actor.ActorID("remote-1") {
			t.Fatalf("relayed bound id = %q, want remote-1 (the authenticated id, not the wire self-report)", r.id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("EmitSink never received the remote emit")
	}
	// The host acks the emit back in receipt order (FIFO, no id): the remote
	// must see the sink's authoritative verdict on the same connection.
	ack, err := remote.codec.Read()
	if err != nil {
		t.Fatalf("remote read ack: %v", err)
	}
	if ack.Kind != ipc.KindEmitAck {
		t.Fatalf("ack kind = %q, want emit_ack", ack.Kind)
	}
	var ap ipc.EmitAckPayload
	if err := json.Unmarshal(ack.Payload, &ap); err != nil {
		t.Fatalf("ack decode: %v", err)
	}
	if ap.MessageID != message.ID("m-up-1") || ap.RejectReason != "harness_kind_invalid" {
		t.Fatalf("ack verdict = %+v, want {m-up-1, harness_kind_invalid}", ap)
	}
}

// TestPortDownPublishesDown: a DOWN frame from the remote raises exactly
// one down edge addressed by the bound id and self-evicts the embodiment (A3 —
// the substrate materialises receiver_unavailable from positively-observed
// death; the dead port is unaddressable WITHOUT the runtime self-evicting it).
func TestPortDownPublishesDown(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)

	dp, _ := json.Marshal(ipc.DownPayload{Reason: "actor exited"})
	if err := remote.codec.Write(ipc.Frame{Kind: ipc.KindDown, Payload: dp}); err != nil {
		t.Fatalf("remote down write: %v", err)
	}
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no down edge after remote DOWN")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.downs) != 1 || w.downs[0] != id {
		t.Fatalf("deaths = %+v, want one for %s", w.downs, id)
	}
	if _, ok := rt.Stat(id); ok {
		t.Fatal("dead port still addressable — did not self-evict")
	}
}

// TestPortEOFPublishesDown: connection EOF (remote closed the conn) is the
// terminal-equivalent of DOWN — it publishes an down edge and self-evicts.
func TestPortEOFPublishesDown(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)

	remote.conn.Close()
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no down edge after connection EOF")
	}
	if _, ok := rt.Stat(id); ok {
		t.Fatal("port still addressable after EOF")
	}
}

// TestPortUnknownFrameKindFailsClosed: the wire is a CLOSED set — an unknown
// frame kind is a protocol violation that kills the port (never silently
// ignored), publishing an down edge.
func TestPortUnknownFrameKindFailsClosed(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)
	defer remote.conn.Close()

	if err := remote.codec.Write(ipc.Frame{Kind: ipc.Kind("garbage")}); err != nil {
		t.Fatalf("remote garbage write: %v", err)
	}
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("unknown frame kind was silently ignored — wire is not fail-closed")
	}
	if _, ok := rt.Stat(id); ok {
		t.Fatal("port still addressable after protocol violation")
	}
}

// TestPortParentCancelQuietTeardown: cancelling the cancellable parent ctx is
// CHANNEL TEARDOWN, not an observed death — the port must release its
// resources (conn closes, the remote sees EOF) and retract from addressing
// (Stat reports absent), but publish NO down edge: an edge here would
// materialise receiver_unavailable terminals mid-teardown for port-hosted
// actors while cell-hosted actors stay silent (same event, two truth
// outcomes by transport). Closure correctness belongs to the level-scan
// reconciler on the next open. Cell symmetry: the cell's ctx arm returns
// with deathCause=nil and publishes nothing either.
func TestPortParentCancelQuietTeardown(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	rt, _ := New(Config{Parent: ctx})
	rt.WatchDown(w)
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)
	defer remote.conn.Close()

	// The remote observes EOF once the port closes its conn on teardown.
	eof := make(chan struct{})
	go func() {
		remote.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _ = remote.codec.Read()
		close(eof)
	}()

	cancel() // collapse the channel scope

	select {
	case <-eof:
	case <-time.After(2 * time.Second):
		t.Fatal("conn not closed after parent cancel — quiet teardown must still closeConn")
	}
	// Eviction (die→onExit) runs on the port goroutine just after closeConn, so it
	// is async relative to the remote's EOF observation above — poll for it.
	evicted := time.Now().Add(2 * time.Second)
	for {
		if _, ok := rt.Stat(id); !ok {
			break
		}
		if time.Now().After(evicted) {
			t.Fatal("port still addressable after parent cancel — did not retract from embodiment")
		}
		time.Sleep(2 * time.Millisecond)
	}
	// NO down edge: teardown is not death. (Checked AFTER eof+Stat confirm the
	// teardown completed, so a would-be edge has had every chance to fire.)
	select {
	case <-w.notify:
		t.Fatal("parent-ctx teardown must NOT publish a down edge (teardown ≠ observed death)")
	default:
	}
}

// TestPortStopIsNotDeath: a by-name Despawn tears the port down WITHOUT publishing
// a down edge — death is for positively-observed remote failure, not for an orderly
// host-initiated teardown. Despawn does send the remote a best-effort KindDespawn
// frame (host→remote "end your arm"), so the remote drains the wire here; the frame
// is observation, not a local death edge.
func TestPortStopIsNotDeath(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 4)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)
	defer remote.conn.Close()

	// Drain the remote end: Despawn writes a KindDespawn frame down the wire before
	// closing, and the codec write is synchronous over the net.Pipe — without a
	// reader it would block. The remote reads the KindDespawn, then sees EOF on the
	// close.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			if _, err := remote.codec.Read(); err != nil {
				return
			}
		}
	}()

	rt.mu.Lock()
	p := rt.embodiments[id]
	rt.mu.Unlock()
	rt.Despawn(Incarnation{id: id, p: p})
	<-drained
	if _, ok := rt.Stat(id); ok {
		t.Fatal("port still addressable after Despawn")
	}
	// Give any erroneous death path a window to fire.
	select {
	case <-w.notify:
		t.Fatal("clean Despawn published an down edge — stop must be silent")
	case <-time.After(200 * time.Millisecond):
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.downs) != 0 {
		t.Fatalf("deaths = %+v, want none after clean stop", w.downs)
	}
}

// TestPortDeliverAfterStop: once a port is torn down, Deliver reports Stopped
// (a hosted-but-tearing-down embodiment is not Delivered, not NotHosted).
func TestPortDeliverAfterStop(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)
	defer remote.conn.Close()

	rt.mu.RLock()
	p := rt.embodiments[id]
	rt.mu.RUnlock()
	p.initiateStop()
	<-p.doneCh()

	if err := p.Deliver(env("x")); err != ErrCellStopped {
		t.Fatalf("Deliver after stop = %v, want ErrCellStopped", err)
	}
}

// TestPortMailboxFull: a port mirrors a cell's bounded mailbox — once the send
// queue saturates (the remote never drains the wire) Deliver reports
// MailboxFull rather than blocking.
func TestPortMailboxFull(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	// The remote NEVER reads, so the wire buffer + writeLoop's one in-flight
	// frame + the bounded sendq all back up; past saturation Deliver is full.
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)
	defer remote.conn.Close()

	var sawFull bool
	deadline := time.After(3 * time.Second)
	for !sawFull {
		select {
		case <-deadline:
			t.Fatal("port sendq never reported MailboxFull under a non-draining remote")
		default:
		}
		res, err := rt.deliver([]actor.ActorID{id}, env("x"))
		if err != nil {
			t.Fatalf("deliver: %v", err)
		}
		switch res.Per[id] {
		case MailboxFull:
			sawFull = true
		case Delivered:
			// keep filling
		default:
			t.Fatalf("unexpected outcome %v while saturating", res.Per[id])
		}
	}
}

// TestPortAttachReplaceStopsOld: a second connect-in for the same resolved id
// replaces the first (one actor, one owner) — the old port is stopped and the
// new one is the live embodiment. (connect-in REPLACE, the zombie-reconnect path.)
func TestPortAttachReplaceStopsOld(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	id1, remote1 := dialPort(t, rt, "l", nopEmit, staticResolve("dup"), nil)
	// Reading on the old conn should observe EOF once it is stopped+closed by the
	// replace, proving the old port was torn down.
	closed := make(chan struct{})
	go func() {
		remote1.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _ = remote1.codec.Read()
		close(closed)
	}()

	id2, remote2 := dialPort(t, rt, "l", nopEmit, staticResolve("dup"), nil)
	defer remote2.conn.Close()
	if id1 != id2 {
		t.Fatalf("ids differ: %q vs %q", id1, id2)
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("old port was not torn down on replace")
	}
	if _, ok := rt.Stat("dup"); !ok {
		t.Fatal("no live embodiment after replace")
	}
}

// dialPortCancel is dialPort's twin that also threads an onCancelRequest handler
// into Attach — the injection seam the platform link layer uses to relay a
// caller-side KindCancelRequest upstream. Same handshake choreography.
func dialPortCancel(t *testing.T, r *Runtime, leaseID string, resolve ResolveFunc, onCancel func(actor.ActorID, message.ID)) (actor.ActorID, *remoteEnd) {
	t.Helper()
	hostConn, remoteConn := net.Pipe()
	remote := &remoteEnd{conn: remoteConn, codec: ipc.NewCodec(remoteConn, remoteConn)}
	hsErr := make(chan error, 1)
	go func() {
		payload, _ := json.Marshal(ipc.HandshakePayload{LeaseID: leaseID})
		if err := remote.codec.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: payload}); err != nil {
			hsErr <- err
			return
		}
		if _, err := remote.codec.Read(); err != nil {
			hsErr <- err
			return
		}
		hsErr <- nil
	}()
	inc, err := attachTest(r, context.Background(), hostConn, Sinks{Emit: nopEmit}, resolve, nil, onCancel)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if e := <-hsErr; e != nil {
		t.Fatalf("remote handshake: %v", e)
	}
	return inc.ID(), remote
}

// TestPortCancelRequestRelayed: the remote actor writes a KindCancelRequest frame
// (the caller-side upstream cancel) and the port's readLoop relays it to the
// injected onCancelRequest with THIS port's authenticated bound id (the wire never
// self-reports it) + the named request id. The port stays alive (best-effort,
// no ack — same posture as obs), so a well-formed frame just relays and the loop
// continues.
func TestPortCancelRequestRelayed(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	type got struct {
		id  actor.ActorID
		req message.ID
	}
	relayed := make(chan got, 1)
	id, remote := dialPortCancel(t, rt, "l", staticResolve("caller-1"), func(boundID actor.ActorID, reqID message.ID) {
		relayed <- got{id: boundID, req: reqID}
	})
	defer remote.conn.Close()

	payload, _ := json.Marshal(ipc.CancelPayload{RequestID: message.ID("req-9")})
	if err := remote.codec.Write(ipc.Frame{Kind: ipc.KindCancelRequest, Payload: payload}); err != nil {
		t.Fatalf("remote write cancel_request: %v", err)
	}
	select {
	case g := <-relayed:
		if g.id != id {
			t.Fatalf("relayed bound id = %q, want %q", g.id, id)
		}
		if g.req != message.ID("req-9") {
			t.Fatalf("relayed request id = %q, want req-9", g.req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel_request was not relayed to onCancelRequest")
	}
	// The port survives a well-formed upstream cancel (non-fatal, no ack).
	if _, ok := rt.Stat(id); !ok {
		t.Fatal("port died after a well-formed cancel_request (should be non-fatal)")
	}
}

// TestPortCancelRequestNilHandlerDropped: with no onCancelRequest wired, a
// KindCancelRequest frame is silently dropped (no consumer) and the port stays
// alive — the nil-handler degrade, same as a nil obs consumer.
func TestPortCancelRequestNilHandlerDropped(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	id, remote := dialPortCancel(t, rt, "l", staticResolve("caller-2"), nil)
	defer remote.conn.Close()

	payload, _ := json.Marshal(ipc.CancelPayload{RequestID: message.ID("req-x")})
	if err := remote.codec.Write(ipc.Frame{Kind: ipc.KindCancelRequest, Payload: payload}); err != nil {
		t.Fatalf("remote write cancel_request: %v", err)
	}
	// Give the read loop a moment to process; the port must remain addressable.
	time.Sleep(50 * time.Millisecond)
	if _, ok := rt.Stat(id); !ok {
		t.Fatal("port died after a dropped cancel_request (should be non-fatal)")
	}
}
