package schedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/timerspec"
)

// baseDeps builds a fully-wired Deps (every field non-nil) so a fail-fast
// table test can zero exactly one field per case.
func baseDeps() Deps {
	return Deps{
		Store:  newFakeStore(),
		Fire:   &fakeFireSink{},
		Host:   newTestRuntimeUnstarted(),
		Revive: &fakeReviver{},
		Clock:  newFakeClock(time.UnixMilli(1_000_000)),
	}
}

// newTestRuntimeUnstarted builds a bare *actorrt.Runtime for use as a
// LivenessProbe in tests that never exercise liveness (fail-fast table only
// needs SOME non-nil value satisfying the interface).
func newTestRuntimeUnstarted() *actorrt.Runtime {
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	return rt
}

func TestNewFailFast(t *testing.T) {
	cases := []struct {
		name string
		zero func(*Deps)
	}{
		{"Store", func(d *Deps) { d.Store = nil }},
		{"Fire", func(d *Deps) { d.Fire = nil }},
		{"Host", func(d *Deps) { d.Host = nil }},
		{"Revive", func(d *Deps) { d.Revive = nil }},
		{"Clock", func(d *Deps) { d.Clock = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deps := baseDeps()
			tc.zero(&deps)
			minter, engine, err := New(deps)
			if err == nil {
				t.Fatalf("New with nil %s: got nil error, want fail-fast", tc.name)
			}
			if minter != nil || engine != nil {
				t.Fatalf("New with nil %s: got non-nil (minter=%v engine=%v), want both nil", tc.name, minter, engine)
			}
		})
	}
}

func TestNewSucceedsWithNilLogger(t *testing.T) {
	deps := baseDeps()
	deps.Logger = nil // nil -> discard, must NOT fail-fast
	minter, engine, err := New(deps)
	if err != nil {
		t.Fatalf("New with nil Logger: %v", err)
	}
	if minter == nil || engine == nil {
		t.Fatal("New with nil Logger: got nil minter/engine on success")
	}
}

// ---------------------------------------------------------------------
// Schedule validation matrix (§3.2 钉5 / 切片9).
// ---------------------------------------------------------------------

func TestScheduleValidationMatrix(t *testing.T) {
	rt := newTestRuntime(t)
	rt.Spawn("author-1", func(actorrt.Incarnation) actorrt.Actor { return stubActor{} })

	minter, engine, err := New(Deps{
		Store: newFakeStore(), Fire: &fakeFireSink{}, Host: rt,
		Revive: &fakeReviver{}, Clock: newFakeClock(time.UnixMilli(1_000_000)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Start()
	defer engine.Close()
	handle := minter.Mint("author-1")

	cases := []struct {
		name    string
		req     ScheduleReq
		wantErr bool
	}{
		{"bind outside closed set", ScheduleReq{Bind: "bogus", FireAt: 2_000_000, Type: "t"}, true},
		{"FireAt zero", ScheduleReq{Bind: BindIdentity, FireAt: 0, Type: "t"}, true},
		{"FireAt negative", ScheduleReq{Bind: BindIdentity, FireAt: -1, Type: "t"}, true},
		{"Type empty", ScheduleReq{Bind: BindIdentity, FireAt: 2_000_000, Type: ""}, true},
		{"Type reserved prefix", ScheduleReq{Bind: BindIdentity, FireAt: 2_000_000, Type: "system.internal"}, true},
		{"past FireAt is legal", ScheduleReq{Bind: BindIdentity, FireAt: 1, Type: "t"}, false},
		{"future identity is legal", ScheduleReq{Bind: BindIdentity, FireAt: 2_000_000, Type: "t"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := handle.Schedule(context.Background(), tc.req)
			if tc.wantErr && !errors.Is(err, ErrBadSchedule) {
				t.Fatalf("Schedule(%+v): err=%v, want ErrBadSchedule", tc.req, err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Schedule(%+v): unexpected err=%v", tc.req, err)
			}
		})
	}
}

// TestScheduleIncarnationNoLiveEmbodiment: the attach seam's structural
// bottom — an author with no live embodiment has nothing to weld an
// incarnation-bind entry to (拍点 8.4).
func TestScheduleIncarnationNoLiveEmbodiment(t *testing.T) {
	rt := newTestRuntime(t)
	minter, engine, err := New(Deps{
		Store: newFakeStore(), Fire: &fakeFireSink{}, Host: rt,
		Revive: &fakeReviver{}, Clock: newFakeClock(time.UnixMilli(1_000_000)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Start()
	defer engine.Close()

	handle := minter.Mint("ghost")
	_, err = handle.Schedule(context.Background(), ScheduleReq{Bind: BindIncarnation, FireAt: 2_000_000, Type: "t"})
	if !errors.Is(err, ErrBadSchedule) {
		t.Fatalf("Schedule(incarnation, no live embodiment): err=%v, want ErrBadSchedule", err)
	}
}

// ---------------------------------------------------------------------
// Two-family routing (§1.3 两个家) + basic fire field-table assertions.
// ---------------------------------------------------------------------

func TestBindIdentityRoutesToStoreAndFires(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	clock := newFakeClock(time.UnixMilli(1_000_000))
	rt := newTestRuntime(t)

	minter, engine, err := New(Deps{Store: store, Fire: sink, Host: rt, Revive: &fakeReviver{}, Clock: clock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Start()
	defer engine.Close()

	handle := minter.Mint("author-1")
	fireAt := clock.Now().Add(time.Hour).UnixMilli()
	id, err := handle.Schedule(context.Background(), ScheduleReq{
		Bind: BindIdentity, FireAt: fireAt, Type: "demo.tick", CorrelationID: "corr-1",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// Routed to the durable store, NOT the in-memory family.
	if !store.hasRow(id) {
		t.Fatal("bind=identity did not create a durable timers row")
	}

	waitForArmedAtLeast(t, clock, 1)
	clock.Advance(time.Hour)

	waitFor(t, 2*time.Second, func() bool { return sink.callCount() == 1 })
	call := sink.lastCall()

	if call.author != "author-1" {
		t.Fatalf("fire author = %q, want author-1 (self-targeted)", call.author)
	}
	wantID := message.ID("timer:" + string(id))
	if call.env.ID != wantID {
		t.Fatalf("fire env.ID = %q, want %q", call.env.ID, wantID)
	}
	if call.env.Kind != message.KindEvent {
		t.Fatalf("fire env.Kind = %q, want event (拍点 8.3)", call.env.Kind)
	}
	if len(call.env.Audience) != 1 || call.env.Audience[0] != "author-1" {
		t.Fatalf("fire env.Audience = %v, want [author-1] (self-targeted)", call.env.Audience)
	}
	if call.env.Visibility != "" {
		t.Fatalf("fire env.Visibility = %q, want empty (StepNormalize default)", call.env.Visibility)
	}
	if call.env.Sender.ID != "" || call.env.ChannelID != "" {
		t.Fatalf("fire env.Sender/ChannelID pre-filled by the engine: %+v / %q, want empty (pen welds them)", call.env.Sender, call.env.ChannelID)
	}
	if string(call.env.CorrelationID) != "corr-1" {
		t.Fatalf("fire env.CorrelationID = %q, want corr-1 (§1.4 继承)", call.env.CorrelationID)
	}
	if call.env.Type != "demo.tick" {
		t.Fatalf("fire env.Type = %q, want demo.tick", call.env.Type)
	}
	if string(call.env.Payload) != "{}" {
		t.Fatalf("fire env.Payload = %q, want {} (empty payload normalised)", call.env.Payload)
	}
	if call.env.TS == 0 {
		t.Fatal("fire env.TS is zero, want engine-clock-stamped")
	}

	waitFor(t, time.Second, func() bool { return !store.hasRow(id) })
}

func TestBindIncarnationRoutesToMemoryOnly(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	clock := newFakeClock(time.UnixMilli(1_000_000))
	rt := newTestRuntime(t)
	rt.Spawn("author-1", func(actorrt.Incarnation) actorrt.Actor { return stubActor{} })

	minter, engine, err := New(Deps{Store: store, Fire: sink, Host: rt, Revive: &fakeReviver{}, Clock: clock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Start()
	defer engine.Close()

	handle := minter.Mint("author-1")
	fireAt := clock.Now().Add(time.Hour).UnixMilli()
	id, err := handle.Schedule(context.Background(), ScheduleReq{Bind: BindIncarnation, FireAt: fireAt, Type: "demo.retry"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// Never a durable row — the whole point of v1.1's 历史校准 (structure IS
	// the bind: incarnation-bind timers are engine memory, full stop).
	if store.rowCount() != 0 {
		t.Fatalf("bind=incarnation created %d durable rows, want 0 (never persisted)", store.rowCount())
	}

	waitForArmedAtLeast(t, clock, 1)
	clock.Advance(time.Hour)
	waitFor(t, 2*time.Second, func() bool { return sink.callCount() == 1 })
	if sink.lastCall().author != "author-1" {
		t.Fatalf("fire author = %q, want author-1", sink.lastCall().author)
	}
	_ = id
}

// ---------------------------------------------------------------------
// Incarnation death drop — pointer-level ABA guard (§5.3, 切片3).
// ---------------------------------------------------------------------

func TestIncarnationBindDropsOnDeathEvenWithLiveSuccessor(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	clock := newFakeClock(time.UnixMilli(1_000_000))
	rt := newTestRuntime(t)
	rt.Spawn("author-1", func(actorrt.Incarnation) actorrt.Actor { return stubActor{} })

	minter, engine, err := New(Deps{Store: store, Fire: sink, Host: rt, Revive: &fakeReviver{}, Clock: clock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Start()
	defer engine.Close()

	handle := minter.Mint("author-1")
	fireAt := clock.Now().Add(time.Hour).UnixMilli()
	if _, err := handle.Schedule(context.Background(), ScheduleReq{Bind: BindIncarnation, FireAt: fireAt, Type: "demo.retry"}); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// Predecessor dies, a SAME-ID successor takes over (respawn) — the
	// successor being live must NOT rescue the predecessor's timer (pointer
	// identity, not id identity, is the drop check).
	rt.DespawnID("author-1")
	rt.Spawn("author-1", func(actorrt.Incarnation) actorrt.Actor { return stubActor{} })

	waitForArmedAtLeast(t, clock, 1)
	clock.Advance(time.Hour)

	// Give the run loop a bounded real-time window to process the
	// (non-)fire, then assert nothing was ever appended.
	time.Sleep(20 * time.Millisecond)
	if sink.callCount() != 0 {
		t.Fatalf("fire.Append called %d times for a dead-embodiment incarnation-bind timer, want 0", sink.callCount())
	}
	if store.rowCount() != 0 {
		t.Fatalf("dead incarnation-bind timer leaked into the durable store: %d rows", store.rowCount())
	}
}

// ---------------------------------------------------------------------
// FireSink tri-state contract (§3.2 钉2, 切片5).
// ---------------------------------------------------------------------

func TestFireTriStateOutcomes(t *testing.T) {
	t.Run("nil deletes the row", func(t *testing.T) {
		store := newFakeStore()
		sink := &fakeFireSink{}
		clock := newFakeClock(time.UnixMilli(1_000_000))
		engine := mustNewEngine(t, store, sink, &fakeReviver{}, clock)
		engine.Start()
		defer engine.Close()

		id := scheduleIdentityDue(t, engine, "author-1", clock)
		waitFor(t, 2*time.Second, func() bool { return sink.callCount() == 1 })
		waitFor(t, time.Second, func() bool { return !store.hasRow(id) })
	})

	t.Run("ErrDuplicateFire deletes the row (crash replay)", func(t *testing.T) {
		store := newFakeStore()
		sink := &fakeFireSink{respond: func(actor.ActorID, *message.Envelope) error { return ErrDuplicateFire }}
		clock := newFakeClock(time.UnixMilli(1_000_000))
		engine := mustNewEngine(t, store, sink, &fakeReviver{}, clock)
		engine.Start()
		defer engine.Close()

		id := scheduleIdentityDue(t, engine, "author-1", clock)
		waitFor(t, 2*time.Second, func() bool { return sink.callCount() == 1 })
		waitFor(t, time.Second, func() bool { return !store.hasRow(id) })
	})

	t.Run("FireRejected deletes the row and logs loudly", func(t *testing.T) {
		store := newFakeStore()
		sink := &fakeFireSink{respond: func(actor.ActorID, *message.Envelope) error {
			return FireRejected{Reason: "harness_reserved_type_unauthorized_sender", Detail: "boom"}
		}}
		clock := newFakeClock(time.UnixMilli(1_000_000))
		engine := mustNewEngine(t, store, sink, &fakeReviver{}, clock)
		engine.Start()
		defer engine.Close()

		id := scheduleIdentityDue(t, engine, "author-1", clock)
		waitFor(t, 2*time.Second, func() bool { return sink.callCount() == 1 })
		// Poison row disposed: deleted, never left to retry forever.
		waitFor(t, time.Second, func() bool { return !store.hasRow(id) })
	})

	t.Run("transient error leaves the row for retry", func(t *testing.T) {
		store := newFakeStore()
		sink := &fakeFireSink{respond: func(actor.ActorID, *message.Envelope) error { return errors.New("store unavailable") }}
		clock := newFakeClock(time.UnixMilli(1_000_000))
		engine := mustNewEngine(t, store, sink, &fakeReviver{}, clock)
		engine.Start()
		defer engine.Close()

		id := scheduleIdentityDue(t, engine, "author-1", clock)
		waitFor(t, 2*time.Second, func() bool { return sink.callCount() >= 1 })
		if !store.hasRow(id) {
			t.Fatal("transient fire failure deleted the row, want at-least-once retention")
		}
	})
}

// mustNewEngine builds an Engine with an unstarted-real-actorrt Host — enough
// for identity-family tests, which never consult liveness.
func mustNewEngine(t *testing.T, store timerspec.TimerStore, sink FireSink, revive Reviver, clock Clock) *Engine {
	t.Helper()
	_, engine, err := New(Deps{Store: store, Fire: sink, Host: newTestRuntime(t), Revive: revive, Clock: clock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return engine
}

// scheduleIdentityDue schedules an identity-bind timer already due (past
// FireAt is legal, §3.2 钉5) directly through the engine (bypassing a Minter
// — these tri-state tests only care about the fire path).
func scheduleIdentityDue(t *testing.T, engine *Engine, author actor.ActorID, clock *fakeClock) TimerID {
	t.Helper()
	id, err := engine.schedule(context.Background(), author, ScheduleReq{
		Bind: BindIdentity, FireAt: clock.Now().UnixMilli() - 1, Type: "demo.due",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	return id
}

// ---------------------------------------------------------------------
// Revive seam (拍点 8.2, 切片8).
// ---------------------------------------------------------------------

func TestReviveGatesIdentityFire(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	revive := &fakeReviver{err: errors.New("no builder registered yet")}
	clock := newFakeClock(time.UnixMilli(1_000_000))
	rt := newTestRuntime(t)

	minter, engine, err := New(Deps{Store: store, Fire: sink, Host: rt, Revive: revive, Clock: clock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Start()
	defer engine.Close()

	handle := minter.Mint("author-1")
	id, err := handle.Schedule(context.Background(), ScheduleReq{Bind: BindIdentity, FireAt: clock.Now().UnixMilli() - 1, Type: "demo.wake"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// EnsureLive fails: fire never attempted, row stays (顺序焊死 EnsureLive→append).
	waitFor(t, 2*time.Second, func() bool { return revive.callCount() >= 1 })
	time.Sleep(20 * time.Millisecond)
	if sink.callCount() != 0 {
		t.Fatalf("Append called %d times before EnsureLive succeeded, want 0 (revive gates append)", sink.callCount())
	}
	if !store.hasRow(id) {
		t.Fatal("row deleted despite a failing Revive, want retained (at-least-once)")
	}

	// EnsureLive recovers: the very next tick fires exactly once.
	revive.mu.Lock()
	revive.err = nil
	revive.mu.Unlock()
	clock.Advance(backoffDuration)

	waitFor(t, 2*time.Second, func() bool { return sink.callCount() == 1 })
	waitFor(t, time.Second, func() bool { return !store.hasRow(id) })
}

// ---------------------------------------------------------------------
// Cancel (§3.2 钉6, 切片6).
// ---------------------------------------------------------------------

func TestCancelTriState(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	clock := newFakeClock(time.UnixMilli(1_000_000))
	rt := newTestRuntime(t)
	rt.Spawn("author-1", func(actorrt.Incarnation) actorrt.Actor { return stubActor{} })
	rt.Spawn("author-2", func(actorrt.Incarnation) actorrt.Actor { return stubActor{} })

	minter, engine, err := New(Deps{Store: store, Fire: sink, Host: rt, Revive: &fakeReviver{}, Clock: clock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Start()
	defer engine.Close()

	h1 := minter.Mint("author-1")
	h2 := minter.Mint("author-2")

	// Pending identity timer: cancel prevents the fire.
	idIdentity, err := h1.Schedule(context.Background(), ScheduleReq{Bind: BindIdentity, FireAt: clock.Now().Add(time.Hour).UnixMilli(), Type: "t"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := h1.Cancel(context.Background(), idIdentity); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if store.hasRow(idIdentity) {
		t.Fatal("Cancel left the identity row in place")
	}

	// Pending incarnation timer: cancel prevents the fire too.
	idInc, err := h1.Schedule(context.Background(), ScheduleReq{Bind: BindIncarnation, FireAt: clock.Now().Add(time.Hour).UnixMilli(), Type: "t"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := h1.Cancel(context.Background(), idInc); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// Non-owner cancel: existed=false, silent, no leak — h2 cannot cancel h1's timer.
	idForeign, err := h1.Schedule(context.Background(), ScheduleReq{Bind: BindIdentity, FireAt: clock.Now().Add(time.Hour).UnixMilli(), Type: "t"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := h2.Cancel(context.Background(), idForeign); err != nil {
		t.Fatalf("Cancel (non-owner): %v", err)
	}
	if !store.hasRow(idForeign) {
		t.Fatal("a non-owner Cancel deleted someone else's timer")
	}
	if err := h1.Cancel(context.Background(), idForeign); err != nil { // cleanup
		t.Fatalf("Cancel (owner cleanup): %v", err)
	}

	// Already-fired: Cancel is a silent no-op (fired truth is not retractable).
	idFired, err := h1.Schedule(context.Background(), ScheduleReq{Bind: BindIdentity, FireAt: clock.Now().UnixMilli() - 1, Type: "t"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return sink.callCount() == 1 })
	waitFor(t, time.Second, func() bool { return !store.hasRow(idFired) })
	if err := h1.Cancel(context.Background(), idFired); err != nil {
		t.Fatalf("Cancel (already fired): %v", err)
	}

	// Nothing left pending should ever fire from the cancelled entries.
	clock.Advance(2 * time.Hour)
	time.Sleep(20 * time.Millisecond)
	if sink.callCount() != 1 {
		t.Fatalf("sink.callCount() = %d after cancels, want 1 (only idFired)", sink.callCount())
	}
}

// ---------------------------------------------------------------------
// TimerID never reused (v1.2 blocker, 切片12).
// ---------------------------------------------------------------------

func TestTimerIDNeverReused(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	clock := newFakeClock(time.UnixMilli(1_000_000))
	rt := newTestRuntime(t)

	minter, engine, err := New(Deps{Store: store, Fire: sink, Host: rt, Revive: &fakeReviver{}, Clock: clock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Start()
	defer engine.Close()
	handle := minter.Mint("author-1")

	req := ScheduleReq{Bind: BindIdentity, FireAt: clock.Now().Add(time.Hour).UnixMilli(), Type: "t"}
	id1, err := handle.Schedule(context.Background(), req)
	if err != nil {
		t.Fatalf("Schedule 1: %v", err)
	}
	if err := handle.Cancel(context.Background(), id1); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	id2, err := handle.Schedule(context.Background(), req)
	if err != nil {
		t.Fatalf("Schedule 2: %v", err)
	}
	if id1 == id2 {
		t.Fatalf("re-Schedule after Cancel reused TimerID %q", id1)
	}

	// id1 and id2 target the SAME instant (req reused verbatim): a stale
	// alarm entry left over from id1's already-superseded schedule can
	// legitimately race a single big Advance (advanceUntil doc) — nudge
	// forward repeatedly until the (unique) surviving row actually fires.
	advanceUntil(t, clock, time.Minute, func() bool { return sink.callCount() == 1 })
	call := sink.lastCall()
	wantID := message.ID("timer:" + string(id2))
	if call.env.ID != wantID {
		t.Fatalf("fire env.ID = %q, want %q (the NEW id, not the cancelled one)", call.env.ID, wantID)
	}
}

// ---------------------------------------------------------------------
// Backoff, not a busy loop (v1.2 opus-major, 切片13).
// ---------------------------------------------------------------------

func TestBackoffNotBusyLoop(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{respond: func(actor.ActorID, *message.Envelope) error { return errors.New("transient store fault") }}
	clock := newFakeClock(time.UnixMilli(1_000_000))
	engine := mustNewEngine(t, store, sink, &fakeReviver{}, clock)
	engine.Start()
	defer engine.Close()

	_, err := engine.schedule(context.Background(), "author-1", ScheduleReq{
		Bind: BindIdentity, FireAt: clock.Now().UnixMilli() - 1, Type: "demo.due",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// A freshly-Scheduled already-due item races its OWN wake token against
	// the run loop's discovery of it: the coalesced wake channel (§3.2 钉块)
	// may still hold that one token by the time the first failed attempt
	// arms its backoff alarm, letting it slip through once more before the
	// backoff genuinely holds — a bounded, single-token artifact, never an
	// unbounded spin (a wake token is consumed at most once). Settle past
	// that race (wait for callCount to stop moving) before asserting the
	// no-busy-loop invariant on what follows.
	settled := waitStable(t, func() int { return sink.callCount() }, 100*time.Millisecond)
	if settled < 1 {
		t.Fatalf("sink.callCount() settled at %d, want >= 1", settled)
	}
	if got := clock.lastArmedDuration(); got != backoffDuration {
		t.Fatalf("armed backoff duration = %v, want %v (real retry pacing, not a busy loop)", got, backoffDuration)
	}

	// No wake source exists anymore (settled means the race above is over) —
	// a bounded real-time window here MUST see zero further Append calls if
	// the loop is correctly blocked on the alarm rather than spinning.
	time.Sleep(30 * time.Millisecond)
	if sink.callCount() != settled {
		t.Fatalf("sink.callCount() = %d after settling at %d, want unchanged (loop must not busy-spin)", sink.callCount(), settled)
	}

	clock.Advance(backoffDuration)
	waitFor(t, 2*time.Second, func() bool { return sink.callCount() == settled+1 })
	if store.rowCount() != 1 {
		t.Fatalf("row count = %d after transient failures, want 1 (at-least-once, still pending)", store.rowCount())
	}
}

// ---------------------------------------------------------------------
// -race: concurrent Schedule/Cancel against a ticking run loop (切片11).
// ---------------------------------------------------------------------

func TestConcurrentScheduleCancelRace(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	clock := newFakeClock(time.UnixMilli(1_000_000))
	rt := newTestRuntime(t)

	minter, engine, err := New(Deps{Store: store, Fire: sink, Host: rt, Revive: &fakeReviver{}, Clock: clock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Start()
	defer engine.Close()
	handle := minter.Mint("author-1")

	const n = 50
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			id, err := handle.Schedule(context.Background(), ScheduleReq{
				Bind: BindIdentity, FireAt: clock.Now().Add(time.Duration(i+1) * time.Minute).UnixMilli(), Type: "t",
			})
			if err != nil {
				t.Errorf("Schedule: %v", err)
				return
			}
			_ = handle.Cancel(context.Background(), id)
		}
	}()

	// Concurrently advance the clock so the run loop is actively re-arming
	// and re-evaluating its due set while Schedule/Cancel race it.
	for i := 0; i < n; i++ {
		clock.Advance(30 * time.Second)
		time.Sleep(time.Millisecond)
	}
	<-done
}

// TestNextFireAtStoreFaultDegradesToBackoffRetry (code review 收口, engine.go
// nextFireAt): a transient NextFireAt store fault must degrade to a
// backoff-PACED retry, never a bare wake-only wait — with an empty mem family
// the old fold-into-"nothing due" behaviour would park every durable fire on
// a quiet channel until somebody happened to Schedule again.
func TestNextFireAtStoreFaultDegradesToBackoffRetry(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	clock := newFakeClock(time.UnixMilli(1_000_000))
	deps := Deps{Store: store, Fire: sink, Host: newTestRuntimeUnstarted(), Revive: &fakeReviver{}, Clock: clock}

	// A durable row ALREADY DUE, and a store whose NextFireAt is faulting from
	// the engine's very first tick (set before Start — no race).
	store.mu.Lock()
	store.rows["t-fault"] = timerspec.TimerRow{
		ID: "t-fault", AuthorID: "author-1", FireAt: clock.Now().UnixMilli() - 1, Type: "x",
	}
	store.nextErr = errors.New("injected transient NextFireAt fault")
	store.mu.Unlock()

	_, engine, err := New(deps)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Start()
	defer engine.Close()

	// The faulting tick must ARM (backoff pacing), not fall into the bare
	// wake-only wait of the "both families empty" branch.
	waitForArmedAtLeast(t, clock, 1)
	if got := sink.callCount(); got != 0 {
		t.Fatalf("fire calls while store faulting = %d, want 0", got)
	}

	// Heal the store; the armed backoff alarm elapsing must retry the query
	// and fire the overdue row — with NO Schedule/Cancel wake ever arriving.
	store.mu.Lock()
	store.nextErr = nil
	store.mu.Unlock()
	clock.Advance(backoffDuration + time.Second)

	waitFor(t, 2*time.Second, func() bool { return sink.callCount() >= 1 })
	waitFor(t, 2*time.Second, func() bool { return !store.hasRow("t-fault") })
}

// ---------------------------------------------------------------------
// Reviver two-class error contract (FireSink tri-state 的镜像).
// ---------------------------------------------------------------------

// ReviveRejected = permanently unrevivable author → the row is a poison row,
// disposed per 拍点 8.8 (deleted, never fired, loud log) — left in place it
// would retry hot forever and starve later-due rows once such rows fill a
// due page. A plain error stays transient: the row survives for the next tick.
func TestReviveTwoClassOutcomes(t *testing.T) {
	t.Run("ReviveRejected disposes the row without firing", func(t *testing.T) {
		store := newFakeStore()
		sink := &fakeFireSink{}
		clock := newFakeClock(time.UnixMilli(1_000_000))
		reviver := &fakeReviver{err: ReviveRejected{Reason: "builder_gone", Detail: "class removed during sleep"}}
		engine := mustNewEngine(t, store, sink, reviver, clock)
		engine.Start()
		defer engine.Close()

		id := scheduleIdentityDue(t, engine, "author-1", clock)
		waitFor(t, 2*time.Second, func() bool { return !store.hasRow(id) })
		if n := sink.callCount(); n != 0 {
			t.Fatalf("fire sink called %d times, want 0 (unrevivable row must never fire)", n)
		}
	})

	t.Run("plain error is transient: the row survives", func(t *testing.T) {
		store := newFakeStore()
		sink := &fakeFireSink{}
		clock := newFakeClock(time.UnixMilli(1_000_000))
		reviver := &fakeReviver{err: errors.New("host busy")}
		engine := mustNewEngine(t, store, sink, reviver, clock)
		engine.Start()
		defer engine.Close()

		id := scheduleIdentityDue(t, engine, "author-1", clock)
		waitFor(t, 2*time.Second, func() bool { return reviver.callCount() >= 2 }) // retried across ticks
		if !store.hasRow(id) {
			t.Fatal("transient revive failure must leave the row for retry")
		}
		if n := sink.callCount(); n != 0 {
			t.Fatalf("fire sink called %d times, want 0 (never revived)", n)
		}
	})
}
