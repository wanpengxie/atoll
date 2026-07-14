package actorrt

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// newCell is a test shim over the two-phase allocShell: it allocates the cell
// shell and fills impl in one step, so the direct-construction edge tests below
// keep their single-call shape (production fills c.impl from the Spawn build
// closure instead). It does NOT go-live (live stays false) — these tests drive
// start()/stop() directly and do not assert IsLive.
func newCell(parent context.Context, id actor.ActorID, impl Actor, mailbox int, onDown func(actor.ActorID, embodiment, error), onObs func(actor.ActorID, embodiment, ObsKind, ObsValue), onExit func(actor.ActorID, embodiment), started time.Time, logger *slog.Logger) *cell {
	c := allocShell(parent, id, actor.KindAgent, mailbox, onDown, onObs, onExit, nil, started, logger)
	c.impl = impl
	return c
}

// --- cell.go direct-construction edge tests -------------------------------

// TestNewCellDefaults: newCell applies its own defaults — a non-positive
// mailbox becomes 64, a nil logger becomes the discard logger. (cell.go
// defaults branch, exercised by constructing a cell directly, since Runtime.New
// pre-clamps the mailbox before handing it down.)
func TestNewCellDefaults(t *testing.T) {
	t.Parallel()
	c := newCell(context.Background(), "a", newRecordActor(), 0, nil, nil, nil, time.Now(), nil)
	if cap(c.inbox) != 64 {
		t.Fatalf("newCell mailbox<=0 default cap = %d, want 64", cap(c.inbox))
	}
	if c.logger == nil {
		t.Fatal("newCell nil logger was not defaulted")
	}
	// A negative mailbox also clamps.
	c2 := newCell(context.Background(), "b", newRecordActor(), -5, nil, nil, nil, time.Now(), nil)
	if cap(c2.inbox) != 64 {
		t.Fatalf("newCell negative mailbox cap = %d, want 64", cap(c2.inbox))
	}
}

// TestCellDeliverAfterStopGuarded: work enqueue (Deliver) refuses a torn-down
// cell with ErrCellStopped.
func TestCellDeliverAfterStopGuarded(t *testing.T) {
	t.Parallel()
	c := newCell(context.Background(), "a", newRecordActor(), 4, nil, nil, nil, time.Now(), nil)
	c.start()
	c.initiateStop()
	<-c.done
	if err := c.Deliver(env("x")); err != ErrCellStopped {
		t.Fatalf("Deliver after stop = %v, want ErrCellStopped", err)
	}
}

// startErrActor returns a (non-panic) error from Start — an abnormal death that
// publishes the down edge WITHOUT a panic (the deathCause branch of
// start()'s Starter arm).
type startErrActor struct{}

func (startErrActor) Receive(context.Context, *message.Envelope) error { return nil }
func (startErrActor) Start(context.Context, ActorContext) error {
	return errors.New("start refused")
}

// TestCellStartErrorPublishesDown: Start returning an error (not a panic) is a
// death — it self-evicts and publishes the down edge.
func TestCellStartErrorPublishesDown(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	_, _, _ = rt.SpawnIfAbsent("a", actor.KindAgent, static(startErrActor{}))
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no down edge after Start returned an error")
	}
	if _, ok := rt.Stat("a"); ok {
		t.Fatal("cell still addressable after Start error")
	}
}

// errReceiveActor returns an error from Receive — NOT a death (closure belongs
// to the sender); the cell swallows it for observability and keeps running.
type errReceiveActor struct {
	got chan struct{}
}

func (a errReceiveActor) Receive(context.Context, *message.Envelope) error {
	select {
	case a.got <- struct{}{}:
	default:
	}
	return errors.New("handler error, not a death")
}

// TestSafeReceiveSwallowsError: a Receive error is recorded for observability
// but does NOT terminate the cell (the substrate never synthesises a terminal
// from a handler error).
func TestSafeReceiveSwallowsError(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	a := errReceiveActor{got: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	defer rt.StopAll()
	_, _, _ = rt.SpawnIfAbsent("a", actor.KindAgent, static(a))
	mustDeliver(t, rt, "a", env("x"))
	select {
	case <-a.got:
	case <-time.After(2 * time.Second):
		t.Fatal("Receive never ran")
	}
	// A handler error is NOT a death.
	select {
	case <-w.notify:
		t.Fatal("Receive error wrongly published an down edge")
	case <-time.After(150 * time.Millisecond):
	}
	if _, ok := rt.Stat("a"); !ok {
		t.Fatal("cell wrongly torn down by a Receive error")
	}
}

// --- runtime.go edge tests ------------------------------------------------

// TestNewDefaultsParent: New with a nil Parent defaults it to context.Background
// (the runtime is usable and can host a cell).
func TestNewDefaultsParent(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{}) // nil Parent, nil Clock, nil Logger, zero Mailbox
	defer rt.StopAll()
	if rt.parent == nil {
		t.Fatal("New did not default a nil Parent")
	}
	_, _, _ = rt.SpawnIfAbsent("a", actor.KindAgent, static(newRecordActor()))
	if _, ok := rt.Stat("a"); !ok {
		t.Fatal("runtime built from a zero Config cannot host a cell")
	}
}

// TestWatchersIgnoreNil: WatchDown(nil) and WatchObsAll(nil) are no-ops — a nil
// watcher must never be enrolled (it would panic on the closure-critical path).
func TestWatchersIgnoreNil(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.WatchDown(nil)
	rt.WatchObsAll(nil)
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	if len(rt.watchers) != 0 {
		t.Fatalf("nil DownWatcher was enrolled: %d watchers", len(rt.watchers))
	}
	if len(rt.obsWatchers) != 0 {
		t.Fatalf("nil ObsWatcher was enrolled: %d", len(rt.obsWatchers))
	}
}

// panickyDownWatcher panics in OnDown — the runtime must guard it (the
// closure-critical death path cannot crash the process) and log it.
type panickyDownWatcher struct{ notify chan struct{} }

func (w panickyDownWatcher) OnDown(context.Context, actor.ActorID, Incarnation, error) {
	if w.notify != nil {
		w.notify <- struct{}{}
	}
	panic("watcher boom")
}

// TestPublishDownWatcherPanicGuarded: a watcher that panics on OnDown does not
// escape/crash the dying goroutine — publishDown recovers it. A second sane
// watcher still runs (guard is per-watcher).
func TestPublishDownWatcherPanicGuarded(t *testing.T) {
	t.Parallel()
	bad := panickyDownWatcher{notify: make(chan struct{}, 1)}
	good := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(bad)
	rt.WatchDown(good)
	_, _, _ = rt.SpawnIfAbsent("a", actor.KindAgent, static(panicActor{}))
	mustDeliver(t, rt, "a", env("x"))
	select {
	case <-bad.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("panicking watcher never ran")
	}
	select {
	case <-good.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("sane watcher did not run after a prior watcher panicked")
	}
}

// panickyObsWatcher panics in OnObs — publishObs must guard it.
type panickyObsWatcher struct{ notify chan struct{} }

func (w panickyObsWatcher) OnObs(context.Context, actor.ActorID, Incarnation, ObsKind, ObsValue) {
	if w.notify != nil {
		w.notify <- struct{}{}
	}
	panic("obs watcher boom")
}

// TestPublishObsWatcherPanicGuarded: a watcher that panics on OnObs does not
// escape the publishing cell's goroutine — publishObs recovers it; a second
// sane watcher still runs.
func TestPublishObsWatcherPanicGuarded(t *testing.T) {
	t.Parallel()
	bad := panickyObsWatcher{notify: make(chan struct{}, 1)}
	good := &obsCollector{notify: make(chan struct{}, 1)}
	rt, del := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.WatchObsAll(bad)
	rt.WatchObsAll(good)
	_, _, _ = rt.SpawnIfAbsent("a", actor.KindAgent, static(&observerActor{}))
	if _, err := del.Deliver([]actor.ActorID{"a"}, env("trigger")); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	select {
	case <-bad.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("panicking obs watcher never ran")
	}
	select {
	case <-good.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("sane obs watcher did not run after a prior watcher panicked")
	}
}

// TestDeliverStoppedAndDefaultOutcome: deliver maps a hosted-but-stopped
// embodiment to Stopped (ErrCellStopped arm). Exercised by stopping a cell out of
// band while it remains in the addressing map, then delivering to it directly
// through the runtime's deliver path.
func TestDeliverStoppedOutcome(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	// Insert a cell, start it, then mark it closed via stop() WITHOUT removing it
	// from the map, so deliver() finds it and gets ErrCellStopped.
	c := newCell(rt.parent, "a", newRecordActor(), 4, rt.publishDown, rt.publishObs, rt.removeIf, rt.clock(), rt.logger)
	c.start()
	rt.mu.Lock()
	rt.embodiments["a"] = c
	rt.mu.Unlock()
	c.initiateStop()
	<-c.done
	// removeIf deleted the map entry on exit; re-insert the stopped cell so the
	// deliver path reaches its ErrCellStopped guard.
	rt.mu.Lock()
	rt.embodiments["a"] = c
	rt.mu.Unlock()

	res, err := rt.deliver([]actor.ActorID{"a"}, env("x"))
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if got := res.Per["a"]; got != Stopped {
		t.Fatalf("outcome for stopped cell = %v, want Stopped", got)
	}
}

// fakeStoppedEmbodiment is an embodiment whose Deliver returns an arbitrary
// (non-sentinel) error, exercising deliver()'s default arm (mapped to Stopped —
// the substrate refuses to report Delivered for any non-nil enqueue error).
type fakeErrEmbodiment struct{ started time.Time }

func (fakeErrEmbodiment) Deliver(*message.Envelope) error { return errors.New("weird enqueue error") }
func (p fakeErrEmbodiment) startedAt() time.Time          { return p.started }
func (fakeErrEmbodiment) cancelRequest(message.ID)        {}
func (fakeErrEmbodiment) initiateStop()                   {}
func (fakeErrEmbodiment) beginTeardown()                  {}
func (fakeErrEmbodiment) signalDespawn(context.Context)   {}
func (fakeErrEmbodiment) doneCh() <-chan struct{}         { return nil }
func (fakeErrEmbodiment) isLive() bool                    { return false }
func (fakeErrEmbodiment) markDead()                       {}
func (fakeErrEmbodiment) kind() actor.Kind                { return "" }

// TestDeliverDefaultArmMapsToStopped: an enqueue error that is neither
// ErrMailboxFull nor ErrCellStopped maps to Stopped (deliver's default switch
// arm — the substrate never reports Delivered for a failed enqueue).
func TestDeliverDefaultArmMapsToStopped(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	rt.mu.Lock()
	rt.embodiments["a"] = fakeErrEmbodiment{started: time.Now()}
	rt.mu.Unlock()
	res, err := rt.deliver([]actor.ActorID{"a"}, env("x"))
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if got := res.Per["a"]; got != Stopped {
		t.Fatalf("default-arm outcome = %v, want Stopped", got)
	}
}

// --- port.go edge tests ---------------------------------------------------

// TestPortEmitDecodeErrorIsDeath: a KindEmit frame whose payload fails to decode
// is a protocol violation that kills the port (emit decode error arm of
// readLoop), publishing an down edge.
func TestPortEmitDecodeErrorIsDeath(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)
	defer remote.conn.Close()

	// A KindEmit frame carrying a payload that is not a valid EmitPayload object.
	if err := remote.codec.Write(ipc.Frame{Kind: ipc.KindEmit, Payload: json.RawMessage(`"not an object"`)}); err != nil {
		t.Fatalf("remote emit write: %v", err)
	}
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("malformed emit frame did not kill the port")
	}
	if _, ok := rt.Stat(id); ok {
		t.Fatal("port still addressable after malformed emit")
	}
}

// TestPortDownEmptyReasonDefaults: a DOWN frame with no reason gets the default
// "remote down" reason (the empty-reason branch of readLoop's KindDown arm).
func TestPortDownEmptyReasonDefaults(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var cause error
	got := make(chan struct{}, 1)
	w := downCaptureWatcher{onDown: func(_ actor.ActorID, c error) {
		mu.Lock()
		cause = c
		mu.Unlock()
		got <- struct{}{}
	}}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	_, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)
	defer remote.conn.Close()

	// DOWN with empty reason payload.
	dp, _ := json.Marshal(ipc.DownPayload{Reason: ""})
	if err := remote.codec.Write(ipc.Frame{Kind: ipc.KindDown, Payload: dp}); err != nil {
		t.Fatalf("remote down write: %v", err)
	}
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("no down edge for empty-reason DOWN")
	}
	mu.Lock()
	defer mu.Unlock()
	if cause == nil || !contains(cause.Error(), "remote down") {
		t.Fatalf("down cause = %v, want default 'remote down' reason", cause)
	}
}

// downCaptureWatcher captures the cause passed to OnDown.
type downCaptureWatcher struct{ onDown func(actor.ActorID, error) }

func (w downCaptureWatcher) OnDown(_ context.Context, id actor.ActorID, _ Incarnation, cause error) {
	w.onDown(id, cause)
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestPortDeliverWriteFailsIsDeath: when the wire write of a delivered envelope
// fails (the conn is closed under the writeLoop), the port dies (the deliver
// write-error arm of writeLoop), publishing an down edge.
func TestPortDeliverWriteFailsIsDeath(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)

	// Close the remote end so the host-side write fails. net.Pipe writes block
	// until read; closing the remote makes the write return an error.
	remote.conn.Close()
	// Deliver many envelopes; once the writeLoop tries the wire it fails and dies.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-w.notify:
			if _, ok := rt.Stat(id); ok {
				t.Fatal("port still addressable after deliver write failure")
			}
			return
		case <-deadline:
			t.Fatal("deliver write failure did not kill the port")
		default:
		}
		_, _ = rt.deliver([]actor.ActorID{id}, env("x"))
		time.Sleep(2 * time.Millisecond)
	}
}

// TestPortDeliverMalformedEnvelopeDropped: an envelope that fails to marshal
// (invalid json.RawMessage payload) is DROPPED by writeLoop (not a transport
// death — the log is truth, closure belongs to the sender). The port stays
// alive and a subsequent well-formed deliver still reaches the wire.
func TestPortDeliverMalformedEnvelopeDropped(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _ := New(Config{Parent: context.Background()})
	rt.WatchDown(w)
	defer rt.StopAll()
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"), nil)
	defer remote.conn.Close()

	// A drainer so the wire never backs up.
	drained := make(chan ipc.Frame, 4)
	go func() {
		for {
			f, err := remote.codec.Read()
			if err != nil {
				return
			}
			drained <- f
		}
	}()

	// Malformed: invalid RawMessage payload makes json.Marshal of DeliverPayload fail.
	bad := &message.Envelope{ID: "bad", Payload: json.RawMessage("this is not json")}
	if _, err := rt.deliver([]actor.ActorID{id}, bad); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	// Then a well-formed envelope — it must still arrive (the malformed one was
	// dropped, not fatal).
	if _, err := rt.deliver([]actor.ActorID{id}, env("good")); err != nil {
		t.Fatalf("deliver good: %v", err)
	}
	select {
	case f := <-drained:
		var dp ipc.DeliverPayload
		if err := json.Unmarshal(f.Payload, &dp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if dp.Envelope.ID != message.ID("good") {
			t.Fatalf("got envelope %q, want good (malformed should have been dropped)", dp.Envelope.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("well-formed envelope never reached the wire after a malformed drop")
	}
	// The port must NOT have died from the malformed envelope.
	select {
	case <-w.notify:
		t.Fatal("malformed envelope wrongly killed the port")
	default:
	}
}

// TestNewPortHandshakeReadError: a connection that closes before presenting any
// handshake fails newPort at the handshake read (Attach returns an error).
func TestNewPortHandshakeReadError(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	hostConn, remoteConn := net.Pipe()
	remoteConn.Close() // no handshake — read fails immediately
	if _, err := rt.Attach(context.Background(), hostConn, Sinks{Emit: nopEmit}, staticResolve("x"), nil, nil); err == nil {
		t.Fatal("Attach accepted a connection that never sent a handshake")
	}
}

// TestNewPortHandshakeDecodeError: a KindHandshake frame whose payload is not a
// valid HandshakePayload fails newPort at the decode step.
func TestNewPortHandshakeDecodeError(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	hostConn, remoteConn := net.Pipe()
	go func() {
		c := ipc.NewCodec(remoteConn, remoteConn)
		// Handshake kind but a non-object payload that fails to unmarshal into
		// HandshakePayload.
		_ = c.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: json.RawMessage(`12345`)})
	}()
	if _, err := rt.Attach(context.Background(), hostConn, Sinks{Emit: nopEmit}, staticResolve("x"), nil, nil); err == nil {
		t.Fatal("Attach accepted a handshake with an undecodable payload")
	}
}

// TestNewPortHandshakeAckWriteError: the ack write fails when the remote closes
// the conn right after sending the handshake (before the host can write the
// ack). Attach returns the ack-write error.
func TestNewPortHandshakeAckWriteError(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	hostConn, remoteConn := net.Pipe()
	go func() {
		c := ipc.NewCodec(remoteConn, remoteConn)
		p, _ := json.Marshal(ipc.HandshakePayload{LeaseID: "l"})
		// This Write returns only once the host has consumed the handshake
		// (net.Pipe is synchronous). Immediately after, close the conn so the
		// host's subsequent ack write fails on a broken pipe.
		_ = c.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: p})
		remoteConn.Close()
	}()
	if _, err := rt.Attach(context.Background(), hostConn, Sinks{Emit: nopEmit}, staticResolve("remote-1"), nil, nil); err == nil {
		t.Fatal("Attach succeeded despite a failed ack write")
	}
	if ids := rt.LiveIDs(); len(ids) != 0 {
		t.Fatalf("failed ACK left live IDs: %v", ids)
	}
	if _, ok := rt.Stat("remote-1"); ok {
		t.Fatal("failed ACK left a visible port")
	}
	waitZombiesZero(t, rt, time.Second)
}

// TestNewPortNilLoggerDefaulted: newPort defaults a nil logger to discard (the
// port is constructed and usable). Driven directly so the logger arg is nil.
func TestNewPortNilLoggerDefaulted(t *testing.T) {
	t.Parallel()
	hostConn, remoteConn := net.Pipe()
	defer hostConn.Close()
	defer remoteConn.Close()
	go func() {
		c := ipc.NewCodec(remoteConn, remoteConn)
		p, _ := json.Marshal(ipc.HandshakePayload{LeaseID: "l"})
		_ = c.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: p})
		_, _ = c.Read() // consume ack
	}()
	p, err := newPort(context.Background(), context.Background(), hostConn, Sinks{Emit: nopEmit}, staticResolve("remote-1"), nil, nil, nil, nil, nil, nil, time.Now(), nil)
	if err != nil {
		t.Fatalf("newPort: %v", err)
	}
	if p.logger == nil {
		t.Fatal("newPort nil logger was not defaulted")
	}
	if p.id != actor.ActorID("remote-1") {
		t.Fatalf("port id = %q, want remote-1", p.id)
	}
}

var _ io.ReadWriteCloser = (net.Conn)(nil)
