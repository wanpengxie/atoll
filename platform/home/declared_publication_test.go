package home

import (
	"os"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

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
