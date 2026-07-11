package store_test

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// --- requestLookupStore.FindByID (the value→pointer adapter) ------------------

// FindByID returns a pointer to the persisted envelope on hit, and ok=false on
// miss. Both arms exercise the RequestLookup wrapper's success/miss paths.
func TestRequestLookup_FindByID(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	// Miss: nil envelope, ok=false, no error.
	env, ok, err := cs.Requests.FindByID(ctx, "ghost")
	if err != nil || ok || env != nil {
		t.Fatalf("miss: env=%v ok=%v err=%v", env, ok, err)
	}

	// Hit: insert a request, then look it up through the RequestLookup port.
	in := newEnv("req-1", message.KindRequest, message.Audience{"tool:xhs"},
		withSender(actor.KindAgent, "planner"), withType("xhs.publish"),
		withPayload(`{"k":"v"}`))
	if _, err := cs.Log.Append(ctx, in, false); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, ok, err := cs.Requests.FindByID(ctx, "req-1")
	if err != nil || !ok {
		t.Fatalf("hit: ok=%v err=%v", ok, err)
	}
	if got == nil || got.ID != "req-1" || got.Type != "xhs.publish" {
		t.Fatalf("hit envelope mismatch: %+v", got)
	}
	if string(got.Payload) != `{"k":"v"}` {
		t.Errorf("payload=%s", got.Payload)
	}
}

// Admit is an ensure operation: repeated calls converge on one active id.
func TestAdmit_RepeatedPrincipalConverges(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	first, err := cs.Membership.Admit(ctx, actor.KindAgent, "dup", 1)
	if err != nil {
		t.Fatalf("first Admit: %v", err)
	}
	second, err := cs.Membership.Admit(ctx, actor.KindAgent, "dup", 2)
	if err != nil || second != first {
		t.Fatalf("second Admit = %q, %v; want %q", second, err, first)
	}
}

func TestAdmit_PrincipalPersists(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	id, err := cs.Membership.Admit(ctx, actor.KindTool, "b", 1)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	got, ok, err := cs.Registry.Lookup(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Lookup ok=%v err=%v", ok, err)
	}
	if got.Principal != "b" {
		t.Errorf("principal=%q want b", got.Principal)
	}
}

// --- ApplyMemberTransitions inner skip / closed-set gate arms -----------------

// The membership WRITE path gates the actor.Kind / actor.Binding closed sets
// (the control-plane twin of stepSenderConsistent's write-side gate): a poison
// kind must fail LOUD at write time — the read path (ListActive) fails on the
// first bad row, so one admitted poison row would brick the whole channel's
// member enumeration. Empty kind is a caller bug, not a human default (the
// former ""→KindHuman fallback silently registered kind-less adds as humans).
func TestApplyMemberTransitions_ClosedSetGate(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	// Empty ID entries are skipped, not errors (idempotent batch hygiene).
	if err := cs.Membership.ApplyMemberTransitions(ctx,
		[]storespec.MemberActorAdd{{ID: "", Kind: actor.KindAgent, At: 1}},
		[]storespec.MemberActorRemove{{ID: "", At: 1}},
	); err != nil {
		t.Fatalf("empty-ID entries must be skipped: %v", err)
	}

	for name, add := range map[string]storespec.MemberActorAdd{
		"empty kind":  {ID: "a1", Kind: "", At: 2000},
		"poison kind": {ID: "a2", Kind: "wizard", At: 2000},
		"poison bind": {ID: "a3", Kind: actor.KindAgent, Binding: "teleport", At: 2000},
	} {
		if err := cs.Membership.ApplyMemberTransitions(ctx,
			[]storespec.MemberActorAdd{add}, nil,
		); err == nil {
			t.Errorf("%s must be rejected at the write gate", name)
		}
		if _, ok, _ := cs.Registry.Lookup(ctx, add.ID); ok {
			t.Errorf("%s: poison row must not land", name)
		}
	}

	// ListActive still enumerates cleanly — no poison row was admitted.
	if _, err := cs.Registry.ListActive(ctx); err != nil {
		t.Fatalf("ListActive after rejected poisons: %v", err)
	}

	// Admit takes the same gate.
	if _, err := cs.Membership.Admit(ctx, "wizard", "b1", 1); err == nil {
		t.Error("Admit must gate the kind closed set")
	}
}

// Mirror ids are random uuids, so a same-millisecond membership bounce
// (add → remove → re-add, identical At) MUST succeed — under the former
// deterministic <type>:<actor>:<at> id the re-add's mirror collided with the
// first registration's on messages.id UNIQUE and rolled the whole re-add
// back, which made any deterministic-At replayer (reconcile re-laying a
// member plan, a seeded boot) fail permanently. Idempotency does not need
// the deterministic id: it lives in the registry-state guard (changed=false
// appends nothing) + the tx atomicity.
func TestApplyMemberTransitions_SameMillisecondBounce(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	const at = int64(5000)
	oldID, err := cs.Membership.Admit(ctx, actor.KindAgent, "bounce", at)
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.Membership.ApplyMemberTransitions(ctx, nil,
		[]storespec.MemberActorRemove{{ID: oldID, At: at}}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	newID, err := cs.Membership.Admit(ctx, actor.KindAgent, "bounce", at)
	if err != nil {
		t.Fatal(err)
	}
	if newID == oldID {
		t.Fatal("same-ms re-admission reused actor id")
	}

	rec, ok, _ := cs.Registry.Lookup(ctx, newID)
	if !ok || !rec.IsActive() {
		t.Fatalf("bounced actor must be active, ok=%v", ok)
	}
	if n := len(mirrorEventsOf(t, cs.Query, "system.actor.registered", string(newID))); n != 1 {
		t.Errorf("new-id registered mirrors=%d want 1", n)
	}
	if n := len(mirrorEventsOf(t, cs.Query, "system.actor.deregistered", string(oldID))); n != 1 {
		t.Errorf("deregistered mirrors = %d, want 1", n)
	}
}

// A mid-batch failure rolls the WHOLE transition tx back — no partial state,
// no partial mirrors. (The former trigger, squatting a deterministic mirror
// id, no longer exists — mirror ids are uuids — so the failure is injected
// via the closed-set write gate: a poison entry after a good one.)
func TestApplyMemberTransitions_MidBatchFailureRollsBackAll(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	adds := []storespec.MemberActorAdd{
		{ID: "good", Kind: actor.KindAgent, At: 1000},
		{ID: "poison", Kind: "wizard", At: 1000}, // rejected by validateMemberIdentity
	}
	if err := cs.Membership.ApplyMemberTransitions(ctx, adds, nil); err == nil {
		t.Fatal("poison entry must fail the batch")
	}
	// Rolled back: the good entry's registry row AND its mirror must not exist.
	if _, ok, _ := cs.Registry.Lookup(ctx, "good"); ok {
		t.Error("mid-batch failure must roll back the earlier registry row")
	}
	if n := len(mirrorEventsOf(t, cs.Query, "system.actor.registered", "good")); n != 0 {
		t.Errorf("mid-batch failure must roll back the earlier mirror, got %d", n)
	}
}
