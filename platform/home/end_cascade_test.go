package home

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func TestMixedDurableRunCascadeClearsRoutingAndPublishesOneClosedWorld(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	parent, err := h.Admit(ctx, actor.KindHuman, "cascade-parent")
	if err != nil {
		t.Fatal(err)
	}
	child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "worker"}, "cascade-child")
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := h.forkAdmission(ctx, child, 1, actorrt.ForkSpec{Kind: actor.KindTool, Class: "tool"}, "cascade-grandchild")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.SetDefaultAgent(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if err := h.EndIdentity(ctx, storespec.AuthorStamp{ID: actor.SystemActorID, BirthVersion: 1}, parent, "cascade"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []actor.ActorID{parent, child, grandchild} {
		if _, ok, err := h.controlIndex.LookupActive(ctx, id); err != nil || ok {
			t.Fatalf("active after cascade %s: ok=%v err=%v", id, ok, err)
		}
	}
	if id, ok, err := h.DefaultAgent(ctx); err != nil || ok || id != "" {
		t.Fatalf("default after cascade=(%q,%v,%v)", id, ok, err)
	}
	if durable, err := h.cs.DurableHistory.ExistsEver(ctx, parent); err != nil || !durable {
		t.Fatalf("durable parent history=(%v,%v)", durable, err)
	}
	for _, id := range []actor.ActorID{child, grandchild} {
		if durable, err := h.cs.DurableHistory.ExistsEver(ctx, id); err != nil || durable {
			t.Fatalf("run child durable history %s=(%v,%v)", id, durable, err)
		}
		if _, err := h.stateHandles.Resolve(ctx, id); !errors.Is(err, accessdoor.ErrStateHandleUnavailable) {
			t.Fatalf("run state after cascade %s: %v", id, err)
		}
	}
}

func TestEndCascadeContainsConcurrentForkOrRejectsIt(t *testing.T) {
	for n := 0; n < 25; n++ {
		h := openWhiteboxHome(t)
		ctx := context.Background()
		parent, err := h.Admit(ctx, actor.KindHuman, "cascade-race-"+string(rune('a'+n)))
		if err != nil {
			t.Fatal(err)
		}
		child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "worker"}, "seed")
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		var injected actor.ActorID
		var forkErr, endErr error
		go func() {
			defer wg.Done()
			injected, forkErr = h.forkAdmission(ctx, child, 1, actorrt.ForkSpec{Kind: actor.KindTool, Class: "tool"}, "racing")
		}()
		go func() {
			defer wg.Done()
			endErr = h.EndIdentity(ctx, storespec.AuthorStamp{ID: actor.SystemActorID, BirthVersion: 1}, parent, "race")
		}()
		wg.Wait()
		if endErr != nil {
			t.Fatalf("iteration %d end: %v", n, endErr)
		}
		if forkErr == nil {
			if _, ok, _ := h.controlIndex.LookupActive(ctx, injected); ok {
				t.Fatalf("iteration %d concurrent child escaped cascade: %s", n, injected)
			}
		} else if !errors.Is(forkErr, ErrForkParentGone) && !errors.Is(forkErr, ErrEndNotMember) {
			t.Fatalf("iteration %d fork error: %v", n, forkErr)
		}
		_ = h.Close()
	}
}
