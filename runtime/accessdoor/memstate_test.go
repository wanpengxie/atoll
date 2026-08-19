package accessdoor

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

type stateTestAuthority struct {
	id  actor.ActorID
	err error
}

func (a stateTestAuthority) ActorID() actor.ActorID { return a.id }
func (a stateTestAuthority) Admit() error           { return a.err }

type liveAuthority struct{ id actor.ActorID }

func (a liveAuthority) ActorID() actor.ActorID { return a.id }
func (a liveAuthority) Admit() error           { return nil }

func newStateResolver(t *testing.T) (StateHandleResolver, *fakeStateStore) {
	t.Helper()
	durable := &fakeStateStore{}
	minter, err := New(Deps{
		Registry: &fakeRegistry{}, Drivers: DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Authority: &fakeMembership{}, State: durable,
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewStateHandleResolver(minter)
	if err != nil {
		t.Fatal(err)
	}
	return resolver, durable
}

func TestStateResolverUsesDurableBackingForEveryLiveMember(t *testing.T) {
	ctx := context.Background()
	resolver, durable := newStateResolver(t)
	for _, id := range []actor.ActorID{"agent:first:1", "tool:second:2"} {
		handle, err := resolver.ResolveAuthority(ctx, stateTestAuthority{id: id})
		if err != nil {
			t.Fatal(err)
		}
		out, err := handle.Invoke(ctx, access.OpCreate, "k", []byte("v"))
		if err != nil || out.RejectReason != "" {
			t.Fatalf("%s create=%+v err=%v", id, out, err)
		}
	}
	if len(durable.createCalls) != 2 {
		t.Fatalf("durable creates=%+v", durable.createCalls)
	}
}

func TestStateResolverRejectsInactiveAndUnknownAuthority(t *testing.T) {
	resolver, _ := newStateResolver(t)
	ctx := context.Background()
	for _, authority := range []stateTestAuthority{
		{id: "agent:dead:1", err: errors.New("inactive")},
		{},
	} {
		if _, err := resolver.ResolveAuthority(ctx, authority); !errors.Is(err, ErrStateHandleUnavailable) {
			t.Fatalf("authority=%+v err=%v", authority, err)
		}
	}
}

func TestForgetActorsLeavesDurableStateIntact(t *testing.T) {
	ctx := context.Background()
	resolver, durable := newStateResolver(t)
	authority := stateTestAuthority{id: "agent:kept:1"}
	handle, err := resolver.ResolveAuthority(ctx, authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Invoke(ctx, access.OpCreate, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	writes := len(durable.createCalls) + len(durable.writeCalls) + len(durable.deleteCalls)
	resolver.ForgetActors([]actor.ActorID{authority.id})
	if got := len(durable.createCalls) + len(durable.writeCalls) + len(durable.deleteCalls); got != writes {
		t.Fatalf("forget changed durable calls: got=%d want=%d", got, writes)
	}
}

func TestStateIngressAdmitsBeforeUsingDurableBacking(t *testing.T) {
	ctx := context.Background()
	resolver, durable := newStateResolver(t)
	out, err := resolver.StateIngress(ctx, stateTestAuthority{id: "agent:dead:1", err: errors.New("inactive")}, StateOp{
		Operation: access.OpCreate, Resource: "k", Args: []byte("v"),
	})
	if err != nil || out.RejectReason != access.OwnerInactive || len(durable.createCalls) != 0 {
		t.Fatalf("out=%+v err=%v creates=%+v", out, err, durable.createCalls)
	}
}
