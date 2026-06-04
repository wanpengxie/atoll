package actorrt

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/ipc"
)

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

// TestCellObserveAfterStopGuarded: a cell that has begun teardown refuses an
// obs PULL with ErrCellStopped (the lifecycle guard mirrors Deliver/signal — an
// obs hook must not run on state Stop may be releasing).
func TestCellObserveAfterStopGuarded(t *testing.T) {
	t.Parallel()
	c := newCell(context.Background(), "a", newRecordActor(), 4, nil, nil, nil, time.Now(), nil)
	c.start()
	c.stop()
	if _, err := c.observe(context.Background(), "anything"); err != ErrCellStopped {
		t.Fatalf("observe after stop = %v, want ErrCellStopped", err)
	}
}

// TestCellDeliverSignalAfterStopGuarded: both work enqueue (Deliver) and
// control enqueue (signal) refuse a torn-down cell with ErrCellStopped.
func TestCellDeliverSignalAfterStopGuarded(t *testing.T) {
	t.Parallel()
	c := newCell(context.Background(), "a", newRecordActor(), 4, nil, nil, nil, time.Now(), nil)
	c.start()
	c.stop()
	if err := c.Deliver(env("x")); err != ErrCellStopped {
		t.Fatalf("Deliver after stop = %v, want ErrCellStopped", err)
	}
	if err := c.signal(Signal{Kind: SignalReload}); err != ErrCellStopped {
		t.Fatalf("signal after stop = %v, want ErrCellStopped", err)
	}
}

// TestCellSignalControlLaneFull: a saturated control lane returns ErrMailboxFull
// (non-blocking, exactly like the work mailbox). Constructed directly so the
// lane can be filled without the cell goroutine draining it.
func TestCellSignalControlLaneFull(t *testing.T) {
	t.Parallel()
	// Mailbox 1 => control lane cap 1. Do NOT start the cell, so nothing drains.
	c := newCell(context.Background(), "a", newRecordActor(), 1, nil, nil, nil, time.Now(), nil)
	if err := c.signal(Signal{Kind: SignalReload}); err != nil {
		t.Fatalf("first signal = %v, want nil", err)
	}
	if err := c.signal(Signal{Kind: SignalQuota}); err != ErrMailboxFull {
		t.Fatalf("signal into full lane = %v, want ErrMailboxFull", err)
	}
}

// startErrActor returns a (non-panic) error from Start — an abnormal death that
// publishes the presence-down edge WITHOUT a panic (the deathCause branch of
// start()'s Starter arm).
type startErrActor struct{}

func (startErrActor) Receive(context.Context, *message.Envelope) error { return nil }
func (startErrActor) Start(context.Context, ActorContext) error {
	return errors.New("start refused")
}

// TestCellStartErrorPublishesDown: Start returning an error (not a panic) is a
// death — it self-evicts and publishes the presence-down edge.
func TestCellStartErrorPublishesDown(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _, _ := New(Config{Parent: context.Background()})
	rt.WatchPresence(w)
	rt.Spawn("a", startErrActor{})
	select {
	case <-w.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("no presence-down edge after Start returned an error")
	}
	if _, ok := rt.Stat("a"); ok {
		t.Fatal("cell still addressable after Start error")
	}
}

// idleControllableActor is Controllable but does no work — so the cell sits in
// the BLOCKING (second) select with an empty inbox, where a control signal
// arrives via the non-prioritized control case.
type idleControllableActor struct {
	notify chan SignalKind
}

func (idleControllableActor) Receive(context.Context, *message.Envelope) error { return nil }
func (a idleControllableActor) OnControl(_ context.Context, sig Signal) {
	a.notify <- sig.Kind
}

// TestCellControlInBlockingSelect: with an empty inbox the cell blocks in the
// second select; a control signal raised then is delivered via that select's
// control case (the non-prioritized arm), not the priority drain.
func TestCellControlInBlockingSelect(t *testing.T) {
	t.Parallel()
	a := idleControllableActor{notify: make(chan SignalKind, 1)}
	rt, _, ctrl := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", a)
	// Give the goroutine time to settle into the blocking select (empty inbox).
	time.Sleep(20 * time.Millisecond)
	if err := ctrl.Raise("a", Signal{Kind: SignalReload}); err != nil {
		t.Fatalf("Raise: %v", err)
	}
	select {
	case k := <-a.notify:
		if k != SignalReload {
			t.Fatalf("OnControl kind = %v, want reload", k)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("control signal never reached the blocking-select control arm")
	}
}

// TestHandleControlDefaultIgnore: a non-Controllable actor receiving a non-Stop
// signal gets the runtime default disposition: ignored (Unix default action for
// an uncaught non-terminal signal). The cell stays alive and addressable.
func TestHandleControlDefaultIgnore(t *testing.T) {
	t.Parallel()
	rt, _, ctrl := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.Spawn("a", newRecordActor()) // not Controllable
	if err := ctrl.Raise("a", Signal{Kind: SignalReload}); err != nil {
		t.Fatalf("Raise reload: %v", err)
	}
	// The signal is ignored — the cell must remain addressable. Poll briefly to
	// let the control lane drain.
	time.Sleep(50 * time.Millisecond)
	if _, ok := rt.Stat("a"); !ok {
		t.Fatal("default-ignore wrongly tore down the cell")
	}
	// And it still receives work (proves the goroutine is alive and looping).
	mustDeliver(t, rt, "a", env("x"))
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
	rt, _, _ := New(Config{Parent: context.Background()})
	rt.WatchPresence(w)
	defer rt.StopAll()
	rt.Spawn("a", a)
	mustDeliver(t, rt, "a", env("x"))
	select {
	case <-a.got:
	case <-time.After(2 * time.Second):
		t.Fatal("Receive never ran")
	}
	// A handler error is NOT a death.
	select {
	case <-w.notify:
		t.Fatal("Receive error wrongly published a presence-down edge")
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
	rt, _, _ := New(Config{}) // nil Parent, nil Clock, nil Logger, zero Mailbox
	defer rt.StopAll()
	if rt.parent == nil {
		t.Fatal("New did not default a nil Parent")
	}
	rt.Spawn("a", newRecordActor())
	if _, ok := rt.Stat("a"); !ok {
		t.Fatal("runtime built from a zero Config cannot host a cell")
	}
}

// TestWatchersIgnoreNil: WatchPresence(nil) and WatchObs(nil) are no-ops — a nil
// watcher must never be enrolled (it would panic on the closure-critical path).
func TestWatchersIgnoreNil(t *testing.T) {
	t.Parallel()
	rt, _, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.WatchPresence(nil)
	rt.WatchObs("a", nil)
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	if len(rt.watchers) != 0 {
		t.Fatalf("nil PresenceWatcher was enrolled: %d watchers", len(rt.watchers))
	}
	if len(rt.obsWatch["a"]) != 0 {
		t.Fatalf("nil ObsWatcher was enrolled: %d", len(rt.obsWatch["a"]))
	}
}

// panickyPresenceWatcher panics in OnDown — the runtime must guard it (the
// closure-critical death path cannot crash the process) and log it.
type panickyPresenceWatcher struct{ notify chan struct{} }

func (w panickyPresenceWatcher) OnDown(context.Context, actor.ActorID, error) {
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
	bad := panickyPresenceWatcher{notify: make(chan struct{}, 1)}
	good := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _, _ := New(Config{Parent: context.Background()})
	rt.WatchPresence(bad)
	rt.WatchPresence(good)
	rt.Spawn("a", panicActor{})
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

func (w panickyObsWatcher) OnObs(context.Context, actor.ActorID, ObsKind, ObsValue) {
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
	rt, del, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	rt.WatchObs("a", bad)
	rt.WatchObs("a", good)
	rt.Spawn("a", &observerActor{})
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

// TestRaiseDroppedOnFullLane: raise reports the per-condition error when the
// presence's control lane is full (ErrMailboxFull) — the substrate surfaces the
// drop, never silently swallows it.
func TestRaiseDroppedOnFullLane(t *testing.T) {
	t.Parallel()
	// A Controllable actor that blocks forever inside OnControl, so the control
	// lane backs up and saturates while it is wedged on the first signal.
	block := make(chan struct{})
	a := &blockingControllable{enter: make(chan struct{}, 1), block: block}
	rt, _, ctrl := New(Config{Parent: context.Background(), Mailbox: 1})
	defer func() { close(block); rt.StopAll() }()
	rt.Spawn("a", a)

	var dropped bool
	deadline := time.After(3 * time.Second)
	for !dropped {
		select {
		case <-deadline:
			t.Fatal("control lane never saturated to report a drop")
		default:
		}
		err := ctrl.Raise("a", Signal{Kind: SignalReload})
		switch err {
		case nil:
			// keep filling
		case ErrMailboxFull:
			dropped = true
		default:
			t.Fatalf("Raise err = %v, want nil or ErrMailboxFull", err)
		}
	}
}

// blockingControllable wedges inside OnControl on the first signal, so the
// control lane fills behind it.
type blockingControllable struct {
	enter chan struct{}
	block chan struct{}
}

func (blockingControllable) Receive(context.Context, *message.Envelope) error { return nil }
func (a *blockingControllable) OnControl(context.Context, Signal) {
	select {
	case a.enter <- struct{}{}:
	default:
	}
	<-a.block
}

// TestDeliverStoppedAndDefaultOutcome: deliver maps a hosted-but-stopped
// presence to Stopped (ErrCellStopped arm). Exercised by stopping a cell out of
// band while it remains in the addressing map, then delivering to it directly
// through the runtime's deliver path.
func TestDeliverStoppedOutcome(t *testing.T) {
	t.Parallel()
	rt, _, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	// Insert a cell, start it, then mark it closed via stop() WITHOUT removing it
	// from the map, so deliver() finds it and gets ErrCellStopped.
	c := newCell(rt.parent, "a", newRecordActor(), 4, rt.publishDown, rt.publishObs, rt.removeIf, rt.clock(), rt.logger)
	c.start()
	rt.mu.Lock()
	rt.presences["a"] = c
	rt.mu.Unlock()
	c.stop()
	// removeIf deleted the map entry on exit; re-insert the stopped cell so the
	// deliver path reaches its ErrCellStopped guard.
	rt.mu.Lock()
	rt.presences["a"] = c
	rt.mu.Unlock()

	res, err := rt.deliver([]actor.ActorID{"a"}, env("x"))
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if got := res.Per["a"]; got != Stopped {
		t.Fatalf("outcome for stopped cell = %v, want Stopped", got)
	}
}

// fakeStoppedPresence is a presence whose Deliver returns an arbitrary
// (non-sentinel) error, exercising deliver()'s default arm (mapped to Stopped —
// the substrate refuses to report Delivered for any non-nil enqueue error).
type fakeErrPresence struct{ started time.Time }

func (fakeErrPresence) Deliver(*message.Envelope) error              { return errors.New("weird enqueue error") }
func (fakeErrPresence) signal(Signal) error                          { return nil }
func (fakeErrPresence) observe(context.Context, ObsKind) (ObsValue, error) {
	return nil, ErrObsUnsupported
}
func (p fakeErrPresence) startedAt() time.Time { return p.started }
func (fakeErrPresence) stop()                  {}

// TestDeliverDefaultArmMapsToStopped: an enqueue error that is neither
// ErrMailboxFull nor ErrCellStopped maps to Stopped (deliver's default switch
// arm — the substrate never reports Delivered for a failed enqueue).
func TestDeliverDefaultArmMapsToStopped(t *testing.T) {
	t.Parallel()
	rt, _, _ := New(Config{Parent: context.Background()})
	rt.mu.Lock()
	rt.presences["a"] = fakeErrPresence{started: time.Now()}
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

// TestPortObserveUnsupported: a port's obs PULL is not yet wired over the wire —
// it reports ErrObsUnsupported (the wire arm is a no-op until a real consumer
// drives it).
func TestPortObserveUnsupported(t *testing.T) {
	t.Parallel()
	rt, _, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
	defer remote.conn.Close()
	if _, err := rt.Observe(context.Background(), id, "anything"); err != ErrObsUnsupported {
		t.Fatalf("port Observe = %v, want ErrObsUnsupported", err)
	}
}

// TestPortSignalReachesWireAsControlFrame: a control Signal raised at a port is
// drained by writeLoop onto the wire as a KindControl frame (prioritized ahead
// of work), carrying the opaque Kind + Payload verbatim.
func TestPortSignalReachesWireAsControlFrame(t *testing.T) {
	t.Parallel()
	rt, _, ctrl := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
	defer remote.conn.Close()

	if err := ctrl.Raise(id, Signal{Kind: SignalQuota, Payload: []byte("budget")}); err != nil {
		t.Fatalf("Raise: %v", err)
	}
	remote.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame, err := remote.codec.Read()
	if err != nil {
		t.Fatalf("remote read control frame: %v", err)
	}
	if frame.Kind != ipc.KindControl {
		t.Fatalf("wire frame kind = %s, want control", frame.Kind)
	}
	var cp ipc.ControlPayload
	if err := json.Unmarshal(frame.Payload, &cp); err != nil {
		t.Fatalf("decode control payload: %v", err)
	}
	if cp.Kind != string(SignalQuota) || string(cp.Payload) != "budget" {
		t.Fatalf("control payload = %+v, want quota/budget", cp)
	}
}

// TestPortSignalInBlockingWriteSelect: when writeLoop is parked in its BLOCKING
// (second) select with an empty send queue, a control signal raised then is
// drained via that select's control arm (not the priority drain). The sleep
// ensures writeLoop has passed the priority check and is blocked before the
// signal arrives.
func TestPortSignalInBlockingWriteSelect(t *testing.T) {
	t.Parallel()
	rt, _, ctrl := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
	defer remote.conn.Close()

	// A drainer so a delivered control frame is consumed off the wire.
	gotKind := make(chan ipc.Kind, 1)
	go func() {
		f, err := remote.codec.Read()
		if err != nil {
			return
		}
		gotKind <- f.Kind
	}()

	// Let writeLoop settle into the blocking second select (empty controlq at the
	// priority check, empty sendq), so the raised signal arrives via that arm.
	time.Sleep(50 * time.Millisecond)
	if err := ctrl.Raise(id, Signal{Kind: SignalReload}); err != nil {
		t.Fatalf("Raise: %v", err)
	}
	select {
	case k := <-gotKind:
		if k != ipc.KindControl {
			t.Fatalf("wire frame kind = %s, want control", k)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("control signal never reached the wire via the blocking select")
	}
}

// TestPortSignalAfterStop: signal into a torn-down port returns ErrCellStopped
// (the closed guard, mirroring Deliver).
func TestPortSignalAfterStop(t *testing.T) {
	t.Parallel()
	rt, _, _ := New(Config{Parent: context.Background()})
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
	defer remote.conn.Close()
	rt.mu.RLock()
	p := rt.presences[id]
	rt.mu.RUnlock()
	p.stop()
	if err := p.signal(Signal{Kind: SignalReload}); err != ErrCellStopped {
		t.Fatalf("port signal after stop = %v, want ErrCellStopped", err)
	}
}

// TestPortSignalLaneFull: a saturated control lane returns ErrMailboxFull. The
// remote never reads, so the wire + writeLoop's one in-flight + the bounded
// controlq all back up.
func TestPortSignalLaneFull(t *testing.T) {
	t.Parallel()
	rt, _, ctrl := New(Config{Parent: context.Background()})
	defer rt.StopAll()
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
	defer remote.conn.Close()

	var sawFull bool
	deadline := time.After(3 * time.Second)
	for !sawFull {
		select {
		case <-deadline:
			t.Fatal("port control lane never reported MailboxFull")
		default:
		}
		err := ctrl.Raise(id, Signal{Kind: SignalReload})
		switch err {
		case nil:
			// keep filling
		case ErrMailboxFull:
			sawFull = true
		default:
			t.Fatalf("Raise err = %v, want nil or ErrMailboxFull", err)
		}
	}
}

// TestPortEmitDecodeErrorIsDeath: a KindEmit frame whose payload fails to decode
// is a protocol violation that kills the port (emit decode error arm of
// readLoop), publishing a presence-down edge.
func TestPortEmitDecodeErrorIsDeath(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _, _ := New(Config{Parent: context.Background()})
	rt.WatchPresence(w)
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
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
	rt, _, _ := New(Config{Parent: context.Background()})
	rt.WatchPresence(w)
	_, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
	defer remote.conn.Close()

	// DOWN with empty reason payload.
	dp, _ := json.Marshal(ipc.DownPayload{Reason: ""})
	if err := remote.codec.Write(ipc.Frame{Kind: ipc.KindDown, Payload: dp}); err != nil {
		t.Fatalf("remote down write: %v", err)
	}
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("no presence-down edge for empty-reason DOWN")
	}
	mu.Lock()
	defer mu.Unlock()
	if cause == nil || !contains(cause.Error(), "remote down") {
		t.Fatalf("down cause = %v, want default 'remote down' reason", cause)
	}
}

// downCaptureWatcher captures the cause passed to OnDown.
type downCaptureWatcher struct{ onDown func(actor.ActorID, error) }

func (w downCaptureWatcher) OnDown(_ context.Context, id actor.ActorID, cause error) {
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
// write-error arm of writeLoop), publishing a presence-down edge.
func TestPortDeliverWriteFailsIsDeath(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _, _ := New(Config{Parent: context.Background()})
	rt.WatchPresence(w)
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))

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

// TestPortControlWriteFailsIsDeath: a control-frame write failure kills the port
// via writeControl's die path (returns false → writeLoop returns).
func TestPortControlWriteFailsIsDeath(t *testing.T) {
	t.Parallel()
	w := &recordingWatcher{notify: make(chan struct{}, 1)}
	rt, _, ctrl := New(Config{Parent: context.Background()})
	rt.WatchPresence(w)
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))

	remote.conn.Close()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-w.notify:
			if _, ok := rt.Stat(id); ok {
				t.Fatal("port still addressable after control write failure")
			}
			return
		case <-deadline:
			t.Fatal("control write failure did not kill the port")
		default:
		}
		_ = ctrl.Raise(id, Signal{Kind: SignalReload})
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
	rt, _, _ := New(Config{Parent: context.Background()})
	rt.WatchPresence(w)
	defer rt.StopAll()
	id, remote := dialPort(t, rt, "l", nopEmit, staticResolve("remote-1"))
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
	rt, _, _ := New(Config{Parent: context.Background()})
	hostConn, remoteConn := net.Pipe()
	remoteConn.Close() // no handshake — read fails immediately
	if _, err := rt.Attach(hostConn, nopEmit, staticResolve("x")); err == nil {
		t.Fatal("Attach accepted a connection that never sent a handshake")
	}
}

// TestNewPortHandshakeDecodeError: a KindHandshake frame whose payload is not a
// valid HandshakePayload fails newPort at the decode step.
func TestNewPortHandshakeDecodeError(t *testing.T) {
	t.Parallel()
	rt, _, _ := New(Config{Parent: context.Background()})
	hostConn, remoteConn := net.Pipe()
	go func() {
		c := ipc.NewCodec(remoteConn, remoteConn)
		// Handshake kind but a non-object payload that fails to unmarshal into
		// HandshakePayload.
		_ = c.Write(ipc.Frame{Kind: ipc.KindHandshake, Payload: json.RawMessage(`12345`)})
	}()
	if _, err := rt.Attach(hostConn, nopEmit, staticResolve("x")); err == nil {
		t.Fatal("Attach accepted a handshake with an undecodable payload")
	}
}

// TestNewPortHandshakeAckWriteError: the ack write fails when the remote closes
// the conn right after sending the handshake (before the host can write the
// ack). Attach returns the ack-write error.
func TestNewPortHandshakeAckWriteError(t *testing.T) {
	t.Parallel()
	rt, _, _ := New(Config{Parent: context.Background()})
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
	if _, err := rt.Attach(hostConn, nopEmit, staticResolve("remote-1")); err == nil {
		t.Fatal("Attach succeeded despite a failed ack write")
	}
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
	p, err := newPort(context.Background(), hostConn, nopEmit, staticResolve("remote-1"), nil, nil, time.Now(), nil)
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
