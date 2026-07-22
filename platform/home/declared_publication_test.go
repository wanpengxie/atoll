package home

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type publicationAwareRouting struct {
	storespec.ChannelRouting
	home      *Home
	published bool
	err       error
}

func (r *publicationAwareRouting) SetDefaultAgent(ctx context.Context, id actor.ActorID) error {
	_, live := r.home.liveness.stateForTest(id)
	_, active, err := r.home.controlIndex.LookupActive(ctx, id)
	r.err = err
	r.published = live && active && err == nil
	return r.ChannelRouting.SetDefaultAgent(ctx, id)
}

func TestPublishDeclaredActorAssemblyOrder(t *testing.T) {
	source, err := os.ReadFile("declared_publication.go")
	if err != nil {
		t.Fatal(err)
	}
	ordered := []string{"LookupDeclaredActive", "AdmitIdentity", "UpsertBatch", "ensureSubjectSlot", "pokeReconcile"}
	last := -1
	for _, token := range ordered {
		next := strings.Index(string(source), token)
		if next <= last {
			t.Fatalf("publication assembly order violated at %s", token)
		}
		last = next
	}
}

func TestDeclarePublishesIdentityBeforeDefaultRouting(t *testing.T) {
	h := openWhiteboxHome(t)
	probe := &publicationAwareRouting{ChannelRouting: h.cs.Routing, home: h}
	h.cs.Routing = probe
	result, err := h.declare(context.Background(), DeclareRequest{
		SourceDeclID: "decl:default-order", Kind: actor.KindAgent, Class: "default-order-agent",
		Placement: storespec.NewServerPlacement(), MakeDefault: true, CreatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if probe.err != nil || !probe.published {
		t.Fatalf("default routing observed unpublished actor %s: published=%v err=%v", result.Row.ID, probe.published, probe.err)
	}
}

func TestDescendantClosureIsOrderIndependent(t *testing.T) {
	rows := []storespec.ActorControlRow{
		{ID: actor.ActorID("grandchild"), Sponsor: actor.ActorID("child")},
		{ID: actor.ActorID("unrelated"), Sponsor: actor.ActorID("other")},
		{ID: actor.ActorID("child"), Sponsor: actor.ActorID("root")},
	}
	got := descendantClosure(rows, actor.ActorID("root"))
	for _, id := range []actor.ActorID{"root", "child", "grandchild"} {
		if !got[id] {
			t.Fatalf("closure missing %q: %#v", id, got)
		}
	}
	if got["unrelated"] {
		t.Fatalf("closure included unrelated actor: %#v", got)
	}
}
