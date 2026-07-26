package accessdoor

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// fakeEntries is the closed classification seam under test: the set of ids that
// live in the process entry table, plus the set that exists at all.
type fakeEntries struct {
	entry map[actor.ActorID]bool
	calls int
}

func (f *fakeEntries) IsEntry(
	_ context.Context,
	id actor.ActorID,
) (bool, bool, error) {
	f.calls++
	isEntry, found := f.entry[id]
	return isEntry, found, nil
}

// liveAuthority is an always-admitting identity authority (the A-level verdict
// the state arm asks for).
type liveAuthority struct{ id actor.ActorID }

func (a liveAuthority) ActorID() actor.ActorID { return a.id }
func (a liveAuthority) Admit() error           { return nil }

func newStateResolver(t *testing.T, entries *fakeEntries) (StateHandleResolver, *fakeStateStore) {
	t.Helper()
	durable := &fakeStateStore{}
	minter, err := New(Deps{
		Registry:  &fakeRegistry{},
		Drivers:   DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Authority: &fakeMembership{},
		State:     durable,
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewStateHandleResolver(entries, minter)
	if err != nil {
		t.Fatal(err)
	}
	return resolver, durable
}

// Backing selection is the ONE consumer of the classification fact and it lives
// entirely inside the state organ: an entry record gets the process locus, a
// durable record gets the durable locus, and the actor-facing API is byte-for-
// byte the same on both — same verb, same arguments, same outcome shape.
func TestStateBackingSelectionIsTheOnlyClassificationConsumer(t *testing.T) {
	ctx := context.Background()
	entries := &fakeEntries{entry: map[actor.ActorID]bool{
		"agent:parent/worker-1": true,
		"agent:declared":        false,
	}}
	resolver, durable := newStateResolver(t, entries)

	entryHandle, err := resolver.ResolveAuthority(ctx, liveAuthority{id: "agent:parent/worker-1"})
	if err != nil {
		t.Fatal(err)
	}
	durableHandle, err := resolver.ResolveAuthority(ctx, liveAuthority{id: "agent:declared"})
	if err != nil {
		t.Fatal(err)
	}

	for name, handle := range map[string]AccessHandle{
		"entry": entryHandle, "durable": durableHandle,
	} {
		out, err := handle.Invoke(ctx, access.OpCreate, "k", []byte("v"), nil)
		if err != nil || out.RejectReason != "" {
			t.Fatalf("%s create: out=%+v err=%v", name, out, err)
		}
	}
	// The entry record's bytes never reached the durable store; only the
	// declared record's did.
	if len(durable.createCalls) != 1 || durable.createCalls[0].owner != "agent:declared" {
		t.Fatalf("durable store saw %+v, want exactly the declared record", durable.createCalls)
	}

	// A record that exists nowhere has no backing to select.
	if _, err := resolver.ResolveAuthority(ctx, liveAuthority{id: "agent:nobody"}); !errors.Is(err, ErrStateHandleUnavailable) {
		t.Fatalf("unknown id err=%v, want ErrStateHandleUnavailable", err)
	}
}

// The classification read happens ONCE per local mint: the handle it hands back
// is welded to the chosen backing, so nothing re-routes on later calls.
func TestLocalMintReadsClassificationOnceAndWeldsTheBacking(t *testing.T) {
	ctx := context.Background()
	entries := &fakeEntries{entry: map[actor.ActorID]bool{"agent:parent/worker-1": true}}
	resolver, _ := newStateResolver(t, entries)

	handle, err := resolver.ResolveAuthority(ctx, liveAuthority{id: "agent:parent/worker-1"})
	if err != nil {
		t.Fatal(err)
	}
	reads := entries.calls
	for i := 0; i < 3; i++ {
		if _, err := handle.Invoke(ctx, access.OpRead, "k", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if entries.calls != reads {
		t.Fatalf("classification read %d extra times after the mint", entries.calls-reads)
	}
}

// ForgetActors is the narrow process-memory release port: it drops the dead
// ids' in-memory rows and nothing else. It is blind (an id with no memory locus
// is a no-op), idempotent, leaves no tombstone, and never touches the durable
// store. A released id resolving again gets a clean locus — the same thing a
// process restart gives for free.
func TestForgetActorsReleasesTheProcessLocusOnly(t *testing.T) {
	ctx := context.Background()
	entries := &fakeEntries{entry: map[actor.ActorID]bool{
		"agent:parent/worker-1": true,
		"agent:declared":        false,
	}}
	resolver, durable := newStateResolver(t, entries)

	entryHandle, err := resolver.ResolveAuthority(ctx, liveAuthority{id: "agent:parent/worker-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entryHandle.Invoke(ctx, access.OpCreate, "k", []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	durableHandle, err := resolver.ResolveAuthority(ctx, liveAuthority{id: "agent:declared"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durableHandle.Invoke(ctx, access.OpCreate, "k", []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	durableWrites := len(durable.createCalls) + len(durable.writeCalls) + len(durable.deleteCalls)

	resolver.ForgetActors([]actor.ActorID{"agent:parent/worker-1", "agent:declared", "agent:ghost"})
	resolver.ForgetActors([]actor.ActorID{"agent:parent/worker-1"})

	if got := len(durable.createCalls) + len(durable.writeCalls) + len(durable.deleteCalls); got != durableWrites {
		t.Fatalf("release touched the durable store: %d ops, want %d", got, durableWrites)
	}

	// The released id's process locus is gone: a fresh resolve reads empty.
	fresh, err := resolver.ResolveAuthority(ctx, liveAuthority{id: "agent:parent/worker-1"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := fresh.Invoke(ctx, access.OpRead, "k", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.Found {
		t.Fatalf("released process locus still holds data: %+v", out)
	}
}

// deadAuthority refuses. It is the A-level verdict a dead or unknown identity
// gets at the door.
type deadAuthority struct{ id actor.ActorID }

func (a deadAuthority) ActorID() actor.ActorID { return a.id }
func (a deadAuthority) Admit() error           { return errors.New("author inactive") }

// endingAuthority admits once and, in the act of admitting, ends the actor:
// the classification it would route by afterwards is the OTHER one. It is the
// End-lands-mid-call race, made deterministic.
type endingAuthority struct {
	id      actor.ActorID
	entries *fakeEntries
}

func (a endingAuthority) ActorID() actor.ActorID { return a.id }

func (a endingAuthority) Admit() error {
	a.entries.entry[a.id] = false
	return nil
}

// The remote body has no welded backing — it holds no classification and the
// classification never crosses the wire — so its state calls route inside the
// organ, once per call, through the SAME function the local mint uses.
func TestStateIngressRoutesInsideTheOrganOnEveryCall(t *testing.T) {
	ctx := context.Background()
	entries := &fakeEntries{entry: map[actor.ActorID]bool{
		"agent:parent/worker-1": true,
		"agent:declared":        false,
	}}
	resolver, durable := newStateResolver(t, entries)

	for i := 0; i < 3; i++ {
		before := entries.calls
		if _, err := resolver.StateIngress(
			ctx,
			liveAuthority{id: "agent:parent/worker-1"},
			StateOp{Operation: access.OpCreate, Resource: "k", Args: []byte("v")},
		); err != nil {
			t.Fatal(err)
		}
		if entries.calls != before+1 {
			t.Fatalf("call %d routed %d times, want exactly one", i, entries.calls-before)
		}
	}
	if len(durable.createCalls) != 0 {
		t.Fatalf("entry record's state reached the durable store: %+v", durable.createCalls)
	}

	if _, err := resolver.StateIngress(
		ctx,
		liveAuthority{id: "agent:declared"},
		StateOp{Operation: access.OpCreate, Resource: "k", Args: []byte("v")},
	); err != nil {
		t.Fatal(err)
	}
	if len(durable.createCalls) != 1 || durable.createCalls[0].owner != "agent:declared" {
		t.Fatalf("durable store saw %+v, want exactly the declared record", durable.createCalls)
	}

	// A record that exists nowhere has no backing to select — the ingress
	// cannot invent a default one.
	if _, err := resolver.StateIngress(
		ctx, liveAuthority{id: "agent:nobody"},
		StateOp{Operation: access.OpRead, Resource: "k"},
	); !errors.Is(err, ErrStateHandleUnavailable) {
		t.Fatalf("unknown id err=%v, want ErrStateHandleUnavailable", err)
	}
}

// The three steps are pinned in one order: select the backing, admit the
// ActorID, execute on the backing already selected. A refused identity never
// reaches the store; an End that lands after the admission neither redirects
// the accepted call nor overturns it.
func TestStateIngressSelectsThenAdmitsThenExecutesOnTheSelectedBacking(t *testing.T) {
	ctx := context.Background()
	entries := &fakeEntries{entry: map[actor.ActorID]bool{
		"agent:parent/worker-1": true,
		"agent:declared":        false,
	}}
	resolver, durable := newStateResolver(t, entries)

	// Refused identity: routing happened (step 1), execution did not.
	before := entries.calls
	out, err := resolver.StateIngress(
		ctx, deadAuthority{id: "agent:declared"},
		StateOp{Operation: access.OpCreate, Resource: "k", Args: []byte("v")},
	)
	if err != nil {
		t.Fatal(err)
	}
	if out.RejectReason != access.OwnerInactive {
		t.Fatalf("dead author outcome = %+v, want owner_inactive", out)
	}
	if entries.calls != before+1 {
		t.Fatal("the door was reached without selecting a backing first")
	}
	if len(durable.createCalls) != 0 {
		t.Fatalf("a refused call still wrote: %+v", durable.createCalls)
	}

	// An End landing inside the admission flips the classification. The call is
	// already accepted, so it completes on the backing chosen before — it is
	// not redirected to the durable store and not judged a second time.
	if _, err := resolver.StateIngress(
		ctx,
		endingAuthority{id: "agent:parent/worker-1", entries: entries},
		StateOp{Operation: access.OpCreate, Resource: "k", Args: []byte("v")},
	); err != nil {
		t.Fatal(err)
	}
	if len(durable.createCalls) != 0 {
		t.Fatalf("an accepted call was redirected mid-flight: %+v", durable.createCalls)
	}
}
