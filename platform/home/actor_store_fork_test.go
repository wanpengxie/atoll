package home

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func TestLookupForkReturnsReceiptWithoutRestoringRunWorldChild(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cs, err := runtime.OpenChannel(
		ctx,
		"fork-receipt",
		filepath.Join(t.TempDir(), "channel.sqlite"),
		runtime.OpenChannelOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	caller := actor.ActorID("agent:caller")
	child := actor.ActorID("agent:child")
	request := message.ID("fork-request")
	_, err = cs.SysOps.ForkActor(ctx, storespec.ForkTx{
		SysOpMeta: storespec.SysOpMeta{
			Anchor:        forkAnchor(caller, request),
			RequestDigest: "receipt-only",
			Source:        storespec.SysOpSourceMember,
			Sender:        caller,
		},
		Child: storespec.ActorControlRow{
			ID: child, Kind: actor.KindAgent, Class: "test",
			CurrentDeclVersion: 1,
			Placement:          storespec.NewServerPlacement(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	store := newHomeActorStore(channel.ID("fork-receipt"), cs, nil, nil)
	got, found, err := store.LookupFork(ctx, caller, request)
	if err != nil || !found || got != child {
		t.Fatalf("LookupFork = (%q,%v,%v), want (%q,true,nil)", got, found, err, child)
	}
	if len(store.runRows) != 0 {
		t.Fatalf("durable receipt restored run-world rows: %#v", store.runRows)
	}
	if _, active, err := store.LookupActive(ctx, child); err != nil || active {
		t.Fatalf("receipt child active after restart read-back: active=%v err=%v", active, err)
	}
}

func TestCommitForkDoesNotCreateSponsorLifecycleEdge(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cs, err := runtime.OpenChannel(
		ctx,
		"fork-independent",
		filepath.Join(t.TempDir(), "channel.sqlite"),
		runtime.OpenChannelOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	store := newHomeActorStore(channel.ID("fork-independent"), cs, nil, nil)
	parent := actor.ActorID("agent:parent")
	child := actor.ActorID("agent:child")
	commit, err := store.CommitFork(ctx, actorctl.ForkCommitRequest{
		CallerActorID: parent,
		RequestID:     message.ID("fork-independent-request"),
		ChildActorID:  child,
		Spec: actorcaps.ForkSpec{
			Kind: actor.KindAgent, Class: "test",
		},
		Placement: storespec.NewServerPlacement(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := commit.Actor.Row.Sponsor; got != "" {
		t.Fatalf("fork child sponsor=%q, want empty", got)
	}

	rows := []storespec.ActorControlRow{
		{ID: parent, Kind: actor.KindAgent},
		commit.Actor.Row,
	}
	parentEnd, err := store.ResolveTerminal(ctx, actorctl.TerminalCommand{
		Kind: actorctl.TerminalEnd,
		End:  actorctl.EndRequest{CallerActorID: parent, Target: parent},
	}, rows)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(parentEnd.IDs, []actor.ActorID{parent}) {
		t.Fatalf("ending parent resolved IDs=%v, want only parent", parentEnd.IDs)
	}
	if _, err := store.ResolveTerminal(ctx, actorctl.TerminalCommand{
		Kind: actorctl.TerminalEnd,
		End:  actorctl.EndRequest{CallerActorID: parent, Target: child},
	}, rows); !errors.Is(err, ErrEndNotSponsor) {
		t.Fatalf("fork caller ended independent child: %v", err)
	}
}

func TestExplicitSponsorLifecycleEdgeStillApplies(t *testing.T) {
	t.Parallel()
	store := &homeActorStore{}
	parent := actor.ActorID("agent:parent")
	child := actor.ActorID("agent:explicit-child")
	rows := []storespec.ActorControlRow{
		{ID: parent, Kind: actor.KindAgent},
		{ID: child, Kind: actor.KindAgent, Sponsor: parent},
	}
	plan, err := store.ResolveTerminal(context.Background(), actorctl.TerminalCommand{
		Kind: actorctl.TerminalEnd,
		End:  actorctl.EndRequest{CallerActorID: parent, Target: parent},
	}, rows)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(plan.IDs, []actor.ActorID{child, parent}) {
		t.Fatalf("explicit sponsor closure IDs=%v, want parent and child", plan.IDs)
	}
	if _, err := store.ResolveTerminal(context.Background(), actorctl.TerminalCommand{
		Kind: actorctl.TerminalEnd,
		End:  actorctl.EndRequest{CallerActorID: parent, Target: child},
	}, rows); err != nil {
		t.Fatalf("explicit sponsor could not end sponsored child: %v", err)
	}
}
