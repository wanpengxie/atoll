package schedule

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var errScheduleIdentityInactive = errors.New("test: schedule identity inactive")

type scheduleIdentityAuthority struct {
	id      actor.ActorID
	allowed atomic.Bool
	calls   atomic.Int64
}

func (a *scheduleIdentityAuthority) ActorID() actor.ActorID { return a.id }
func (a *scheduleIdentityAuthority) Admit() error {
	a.calls.Add(1)
	if !a.allowed.Load() {
		return errScheduleIdentityInactive
	}
	return nil
}

type scheduleBackingAuthority struct {
	allowScheduleAuthority
	checkCalls atomic.Int64
}

func (a *scheduleBackingAuthority) CheckAuthor(
	_ context.Context,
	_ storespec.AuthorStamp,
) (storespec.AuthorVerdict, error) {
	a.checkCalls.Add(1)
	return storespec.AuthorNotMember, nil
}

type blockingNowClock struct {
	base    *fakeClock
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingNowClock) Now() time.Time {
	block := false
	c.once.Do(func() {
		block = true
		close(c.entered)
	})
	if block {
		<-c.release
	}
	return c.base.Now()
}

func (c *blockingNowClock) NewAlarm(deadline time.Time) Timer {
	return c.base.NewAlarm(deadline)
}

func TestAuthorityScheduleAdmitsOnceAndLetsAcceptedScheduleFinish(t *testing.T) {
	store := newFakeStore()
	sink := &fakeFireSink{}
	clock := &blockingNowClock{
		base:    newFakeClock(time.UnixMilli(1_000)),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	backing := &scheduleBackingAuthority{
		allowScheduleAuthority: allowScheduleAuthority{world: storespec.WorldDurable},
	}
	minted, _, err := New(Deps{
		Store:       store,
		Fire:        sink,
		DurableFire: fakeDurableFire{store: store, sink: sink},
		Clock:       clock,
		Authority:   backing,
	})
	if err != nil {
		t.Fatal(err)
	}
	authority := &scheduleIdentityAuthority{id: "agent:schedule-authority"}
	authority.allowed.Store(true)
	handle := minted.(*minter).MintAuthority(authority)

	done := make(chan error, 1)
	go func() {
		_, err := handle.Schedule(t.Context(), ScheduleReq{
			Home: TimerHomeDurable, FireAt: 2_000, Type: "authority.timer",
		})
		done <- err
	}()
	<-clock.entered

	// Model replacement/end immediately after the identity admission. The
	// accepted invocation must not be re-authorized by the old AuthorStamp
	// path and may finish against its Scheduler home.
	authority.allowed.Store(false)
	close(clock.release)
	if err := <-done; err != nil {
		t.Fatalf("accepted Schedule was re-authorized: %v", err)
	}
	if got := authority.calls.Load(); got != 1 {
		t.Fatalf("authority calls=%d, want one", got)
	}
	if got := backing.checkCalls.Load(); got != 0 {
		t.Fatalf("legacy CheckAuthor calls=%d, want zero", got)
	}

	if _, err := handle.Schedule(t.Context(), ScheduleReq{
		Home: TimerHomeMemory, FireAt: 3_000, Type: "authority.stale",
	}); !errors.Is(err, errScheduleIdentityInactive) {
		t.Fatalf("next inactive Schedule err=%v", err)
	}
	if got := authority.calls.Load(); got != 2 {
		t.Fatalf("authority calls=%d, want one per invocation", got)
	}
}
