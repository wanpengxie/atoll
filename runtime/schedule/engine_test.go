package schedule

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

func testDeps(store timerspec.TimerStore, sink FireSink, clock Clock) Deps {
	return Deps{
		Store: store, Fire: sink,
		DurableFire: fakeDurableFire{store: store, sink: sink},
		Clock:       clock, Authority: allowScheduleAuthority{},
	}
}

func newTestEngine(t *testing.T, store *fakeStore, sink *fakeFireSink, clock *fakeClock) (Minter, *Engine) {
	t.Helper()
	minter, engine, err := New(testDeps(store, sink, clock))
	if err != nil {
		t.Fatal(err)
	}
	engine.Start()
	t.Cleanup(engine.Close)
	return minter, engine
}

func waitSchedule(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("schedule condition did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNewFailFast(t *testing.T) {
	base := testDeps(newFakeStore(), &fakeFireSink{}, newFakeClock(time.UnixMilli(1)))
	cases := []struct {
		name string
		edit func(*Deps)
	}{
		{"store", func(d *Deps) { d.Store = nil }},
		{"fire", func(d *Deps) { d.Fire = nil }},
		{"durable fire", func(d *Deps) { d.DurableFire = nil }},
		{"clock", func(d *Deps) { d.Clock = nil }},
		{"authority", func(d *Deps) { d.Authority = nil }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			deps := base
			test.edit(&deps)
			if _, _, err := New(deps); err == nil {
				t.Fatal("missing dependency accepted")
			}
		})
	}
}

func TestScheduleValidationDoesNotCoupleTimerHomeToActorKind(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	clock := newFakeClock(time.UnixMilli(1_000))
	minter, engine, err := New(Deps{
		Store: store, Fire: sink,
		DurableFire: fakeDurableFire{store: store, sink: sink},
		Clock:       clock, Authority: allowScheduleAuthority{},
	})
	if err != nil {
		t.Fatal(err)
	}
	engine.Start()
	t.Cleanup(engine.Close)
	handle := minter.Mint(testStamp("agent:a"))
	for _, request := range []ScheduleReq{
		{},
		{Home: "unknown", FireAt: 2_000, Type: "ok"},
		{Home: TimerHomeMemory, FireAt: 2_000},
		{Home: TimerHomeMemory, FireAt: 2_000, Type: message.ReservedTypePrefix + "bad"},
	} {
		if _, err := handle.Schedule(context.Background(), request); !errors.Is(err, ErrBadSchedule) {
			t.Fatalf("request %+v err=%v, want ErrBadSchedule", request, err)
		}
	}
	if _, err := handle.Schedule(context.Background(), ScheduleReq{
		Home: TimerHomeDurable, FireAt: 2_000, Type: "durable",
	}); err != nil {
		t.Fatalf("durable schedule err=%v", err)
	}
}

func TestIdentityTimerCommitsOneDeterministicFire(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	clock := newFakeClock(time.UnixMilli(1_000))
	minter, _ := newTestEngine(t, store, sink, clock)
	handle := minter.Mint(testStamp("agent:a"))
	id, err := handle.Schedule(context.Background(), ScheduleReq{
		Home: TimerHomeDurable, FireAt: 2_000, Type: "timer.tick", Payload: []byte(`{"x":1}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	waitSchedule(t, func() bool { return sink.callCount() == 1 })
	call := sink.lastCall()
	if call.author != "agent:a" || call.env.ID != fireMessageID(id) ||
		call.env.Kind != message.KindEvent || len(call.env.Audience) != 1 ||
		call.env.Audience[0] != "agent:a" {
		t.Fatalf("fire=%+v author=%s", call.env, call.author)
	}
	if store.rowCount() != 0 {
		t.Fatal("committed identity timer remained pending")
	}
}

func TestMemoryTimerBelongsToSchedulerHomeNotActorIncarnation(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	clock := newFakeClock(time.UnixMilli(1_000))
	minter, _ := newTestEngine(t, store, sink, clock)
	handle := minter.Mint(testStamp("agent:a"))
	if _, err := handle.Schedule(context.Background(), ScheduleReq{
		Home: TimerHomeMemory, FireAt: 2_000, Type: "local.tick",
	}); err != nil {
		t.Fatal(err)
	}
	if store.rowCount() != 0 {
		t.Fatal("memory timer entered durable store")
	}
	// There is deliberately no actor-current predicate to flip here. A body
	// replacement does not change ownership of the Scheduler alarm.
	clock.Advance(time.Second)
	waitSchedule(t, func() bool { return sink.callCount() == 1 })
}

func TestFireFailureClasses(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		pending bool
	}{
		{"duplicate completes", ErrDuplicateFire, false},
		{"deterministic reject disposes", FireRejected{Reason: "bad", Detail: "shape"}, false},
		{"transient retries", errors.New("temporary"), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newFakeStore()
			sink := &fakeFireSink{respond: func(_ actor.ActorID, _ *message.Envelope) error {
				return test.err
			}}
			clock := newFakeClock(time.UnixMilli(1_000))
			minter, _ := newTestEngine(t, store, sink, clock)
			id, err := minter.Mint(testStamp("agent:a")).Schedule(context.Background(), ScheduleReq{
				Home: TimerHomeDurable, FireAt: 2_000, Type: "timer.tick",
			})
			if err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			waitSchedule(t, func() bool { return sink.callCount() > 0 })
			if got := store.hasRow(id); got != test.pending {
				t.Fatalf("pending=%v want %v", got, test.pending)
			}
		})
	}
}

func TestCancelAndQuota(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	clock := newFakeClock(time.UnixMilli(1_000))
	minter, engine := newTestEngine(t, store, sink, clock)
	handle := minter.Mint(testStamp("agent:a"))
	id, err := handle.Schedule(context.Background(), ScheduleReq{
		Home: TimerHomeMemory, FireAt: 2_000, Type: "timer.tick",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Cancel(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	time.Sleep(10 * time.Millisecond)
	if sink.callCount() != 0 {
		t.Fatal("cancelled timer fired")
	}

	for i := 0; i < maxMemTimersPerAuthor; i++ {
		if _, err := engine.schedule(context.Background(), "agent:q", ScheduleReq{
			Home: TimerHomeMemory, FireAt: 100_000, Type: "quota",
		}); err != nil {
			t.Fatalf("timer %d: %v", i, err)
		}
	}
	if _, err := engine.schedule(context.Background(), "agent:q", ScheduleReq{
		Home: TimerHomeMemory, FireAt: 100_000, Type: "quota",
	}); !errors.Is(err, ErrScheduleQuota) {
		t.Fatalf("quota err=%v", err)
	}
}

func TestStoreFaultUsesBackoffInsteadOfBusyLoop(t *testing.T) {
	store := newFakeStore()
	store.nextErr = errors.New("store unavailable")
	clock := newFakeClock(time.UnixMilli(1_000))
	_, engine, err := New(testDeps(store, &fakeFireSink{}, clock))
	if err != nil {
		t.Fatal(err)
	}
	engine.Start()
	t.Cleanup(engine.Close)
	waitSchedule(t, func() bool { return clock.armedCount() > 0 })
	if duration := clock.lastArmedDuration(); duration != backoffDuration {
		t.Fatalf("backoff=%s want %s", duration, backoffDuration)
	}
}

func TestErrorStringsStayActionable(t *testing.T) {
	if !strings.Contains(FireRejected{Reason: "bad", Detail: "shape"}.Error(), "bad") {
		t.Fatal("rejection error lost reason")
	}
}
