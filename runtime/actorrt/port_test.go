package actorrt

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/ipc"
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
// identity binding.
func dialPort(t *testing.T, r *Runtime, leaseID string, emit EmitSink, resolve ResolveFunc) (actor.ActorID, *remoteEnd) {
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

	inc, err := r.Attach(context.Background(), hostConn, emit, resolve)
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
	return func(string) (actor.ActorID, error) { return id, nil }
}

func nopEmit(context.Context, Incarnation, *message.Envelope) (ipc.EmitResult, error) {
	return ipc.EmitResult{}, nil
}

// TestPortHandshakeBindsResolvedID: Attach resolves the presented lease to an
// ActorID, sends the ack, and registers the embodiment under that id (A1 — the
// connection IS the actor, addressable by the resolved id).
func TestPortHandshakeBindsResolvedID(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	gotLease := make(chan string, 1)
	resolve := func(lease string) (actor.ActorID, error) {
		gotLease <- lease
		return "remote-1", nil
	}
	id, remote := dialPort(t, rt, "lease-xyz", nopEmit, resolve)
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
		if _, err := rt.Attach(context.Background(), hostConn, nopEmit, staticResolve("x")); err == nil {
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
		resolve := func(string) (actor.ActorID, error) { return "", io.ErrUnexpectedEOF }
		if _, err := rt.Attach(context.Background(), hostConn, nopEmit, resolve); err == nil {
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
		if _, err := rt.Attach(context.Background(), hostConn, nopEmit, staticResolve("")); err == nil {
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
		_, err := rt.Attach(hsCtx, hostConn, nopEmit, staticResolve("x"))
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
	if _, err := rt.Attach(context.Background(), hostConn, nil, staticResolve("x")); err == nil {
		t.Fatal("Attach accepted a nil EmitSink")
	}
	hostConn2, _ := net.Pipe()
	if _, err := rt.Attach(context.Background(), hostConn2, nopEmit, nil); err == nil {
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
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
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
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
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
	_, remote := dialPort(t, rt, "l", emit, staticResolve("remote-1"))
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
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))

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
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))

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
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
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
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
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
	if _, ok := rt.Stat(id); ok {
		t.Fatal("port still addressable after parent cancel — did not retract from embodiment")
	}
	// NO down edge: teardown is not death. (Checked AFTER eof+Stat confirm the
	// teardown completed, so a would-be edge has had every chance to fire.)
	select {
	case <-w.notify:
		t.Fatal("parent-ctx teardown must NOT publish a down edge (teardown ≠ observed death)")
	default:
	}
}

// TestPortStopIsNotDeath: an external stop() (clean despawn) tears the port down
// WITHOUT publishing an down edge — death is for positively-observed remote
// failure, not for an orderly host-initiated teardown.
func TestPortStopIsNotDeath(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 4)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
	defer remote.conn.Close()

	rt.mu.Lock()
	p := rt.embodiments[id]
	rt.mu.Unlock()
	rt.Despawn(Incarnation{id: id, p: p})
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
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
	defer remote.conn.Close()

	rt.mu.RLock()
	p := rt.embodiments[id]
	rt.mu.RUnlock()
	p.stop()

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
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
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

	id1, remote1 := dialPort(t, rt, "l", nopEmit, staticResolve("dup"))
	// Reading on the old conn should observe EOF once it is stopped+closed by the
	// replace, proving the old port was torn down.
	closed := make(chan struct{})
	go func() {
		remote1.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _ = remote1.codec.Read()
		close(closed)
	}()

	id2, remote2 := dialPort(t, rt, "l", nopEmit, staticResolve("dup"))
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
