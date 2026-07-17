package home

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
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
	if err := h.systemEndHandle().End(ctx, parent, "cascade"); err != nil {
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
	rows, err := h.cs.Query.ReadAfterSeq(ctx, 0, 1024)
	var ended *message.Envelope
	for _, row := range rows {
		if row.Envelope.ID == message.ID("actor-ended:"+string(parent)) {
			env := row.Envelope
			ended = &env
			break
		}
	}
	if err != nil || ended == nil {
		t.Fatalf("ended event found=%v err=%v", ended != nil, err)
	}
	var payload struct {
		EndedBy actor.ActorID `json:"ended_by"`
	}
	if err := json.Unmarshal(ended.Payload, &payload); err != nil || payload.EndedBy != actor.SystemActorID {
		t.Fatalf("ended payload=%s decoded=%+v err=%v", ended.Payload, payload, err)
	}
	for _, id := range []actor.ActorID{child, grandchild} {
		if durable, err := h.cs.DurableHistory.ExistsEver(ctx, id); err != nil || durable {
			t.Fatalf("run child durable history %s=(%v,%v)", id, durable, err)
		}
		if _, err := h.stateHandles.Resolve(ctx, storespec.AuthorStamp{ID: id, BirthVersion: 1}); !errors.Is(err, accessdoor.ErrStateHandleUnavailable) {
			t.Fatalf("run state after cascade %s: %v", id, err)
		}
	}
}

// readEndedPayload locates the deterministic actor-ended:<target> event and
// decodes its ended_by field — the shared helper for the two directional
// assertions below (self and parent) mirroring the system-direction proof
// already pinned by TestMixedDurableRunCascadeClearsRoutingAndPublishesOneClosedWorld.
func readEndedPayload(t *testing.T, h *Home, target actor.ActorID) actor.ActorID {
	t.Helper()
	ctx := context.Background()
	rows, err := h.cs.Query.ReadAfterSeq(ctx, 0, 1024)
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	var ended *message.Envelope
	for _, row := range rows {
		if row.Envelope.ID == message.ID("actor-ended:"+string(target)) {
			env := row.Envelope
			ended = &env
			break
		}
	}
	if ended == nil {
		t.Fatalf("no actor-ended event found for %s", target)
	}
	var payload struct {
		EndedBy actor.ActorID `json:"ended_by"`
	}
	if err := json.Unmarshal(ended.Payload, &payload); err != nil {
		t.Fatalf("decode ended payload %s: %v", ended.Payload, err)
	}
	return payload.EndedBy
}

// TestEndedBySelfWhenActorEndsItself pins the self-End direction of the S9
// payload contract (spec §S9 "End 事件署名 = payload 口径"): when an actor ends
// itself via Sys.End (spawnHandle.EndSelf), the cascade envelope's ended_by
// equals the actor's own id — the existing end_cascade_test.go coverage only
// exercised the system/管理 direction (h.systemEndHandle()), never self.
func TestEndedBySelfWhenActorEndsItself(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	parent, err := h.Admit(ctx, actor.KindHuman, "self-end-parent")
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(parent)
		return live
	})
	inc, _ := h.channel.Cells().CurrentIncarnation(parent)
	handle := newSpawnHandle(h, inc, 1, h.channel.Cells())
	if err := handle.EndSelf(ctx); err != nil {
		t.Fatalf("EndSelf: %v", err)
	}
	if _, ok, err := h.controlIndex.LookupActive(ctx, parent); err != nil || ok {
		t.Fatalf("active after self-End: ok=%v err=%v", ok, err)
	}
	if endedBy := readEndedPayload(t, h, parent); endedBy != parent {
		t.Fatalf("ended_by=%q, want self=%q", endedBy, parent)
	}
}

// TestEndedByParentWhenParentDespawnsChild pins the parent-DespawnChild
// direction: the cascade envelope's ended_by equals the PARENT's id, not the
// child's own id and not system — the third leg of the S9 payload contract
// (self / parent / system), previously only system was asserted.
func TestEndedByParentWhenParentDespawnsChild(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	parent, err := h.Admit(ctx, actor.KindHuman, "despawn-child-parent")
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(parent)
		return live
	})
	inc, _ := h.channel.Cells().CurrentIncarnation(parent)
	child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "worker"}, "despawn-target")
	if err != nil {
		t.Fatal(err)
	}
	handle := newSpawnHandle(h, inc, 1, h.channel.Cells())
	if err := handle.DespawnChild(ctx, child, "parent_despawn"); err != nil {
		t.Fatalf("DespawnChild: %v", err)
	}
	if _, ok, err := h.controlIndex.LookupActive(ctx, child); err != nil || ok {
		t.Fatalf("active after DespawnChild: ok=%v err=%v", ok, err)
	}
	if endedBy := readEndedPayload(t, h, child); endedBy != parent {
		t.Fatalf("ended_by=%q, want parent=%q", endedBy, parent)
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
			endErr = h.systemEndHandle().End(ctx, parent, "race")
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
