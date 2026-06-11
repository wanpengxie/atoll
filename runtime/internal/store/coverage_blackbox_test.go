package store_test

import (
	"context"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
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

// --- Insert: UNIQUE conflict (actor_id PRIMARY KEY) → exec error arm ----------

// A second Insert of the same actor_id violates the actor_registry PRIMARY KEY
// and must surface as an error from the INSERT exec (line: actor insert %q).
func TestInsert_DuplicateIDConflicts(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	rec := storespec.Record{ID: "dup", Kind: actor.KindAgent, CreatedAt: 1}
	if err := cs.Membership.Insert(ctx, rec); err != nil {
		t.Fatalf("first Insert: %v", err)
	}
	if err := cs.Membership.Insert(ctx, rec); err == nil {
		t.Error("second Insert of same actor_id must conflict on PRIMARY KEY")
	}
}

// Insert with a non-empty binding takes the binding!=nil branch and commits.
func TestInsert_WithBindingCommits(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	rec := storespec.Record{ID: "b", Kind: actor.KindTool, Binding: actor.BindingEmbedded, CreatedAt: 1}
	if err := cs.Membership.Insert(ctx, rec); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	got, ok, err := cs.Registry.Lookup(ctx, "b")
	if err != nil || !ok {
		t.Fatalf("Lookup ok=%v err=%v", ok, err)
	}
	if got.Binding != actor.BindingEmbedded {
		t.Errorf("binding=%q want embedded", got.Binding)
	}
}

// --- ApplyMemberTransitions inner skip / default arms ------------------------

// An add/remove with an empty ID is skipped (continue) — no row, no mirror.
// An add with empty Kind defaults to KindHuman.
func TestApplyMemberTransitions_EmptyIDSkippedAndKindDefaulted(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	adds := []storespec.MemberActorAdd{
		{ID: "", Kind: actor.KindAgent, At: 1}, // skipped: empty ID
		{ID: "defaulted", Kind: "", At: 2000},  // Kind defaults to human
	}
	removes := []storespec.MemberActorRemove{
		{ID: "", At: 1}, // skipped: empty ID
	}
	if err := cs.Membership.ApplyMemberTransitions(ctx, adds, removes); err != nil {
		t.Fatalf("ApplyMemberTransitions: %v", err)
	}
	rec, ok, err := cs.Registry.Lookup(ctx, "defaulted")
	if err != nil || !ok {
		t.Fatalf("defaulted actor ok=%v err=%v", ok, err)
	}
	if rec.Kind != actor.KindHuman {
		t.Errorf("empty kind must default to human, got %q", rec.Kind)
	}
}

// The add-mirror appendTx fails when a message row already occupies the mirror
// event's deterministic id (system.actor.registered:<id>:<at>). The whole tx
// must roll back: neither the registry row nor any partial state survives.
func TestApplyMemberTransitions_AddMirrorAppendConflict(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	mirrorID := "system.actor.registered:collide:5000"
	// Pre-occupy the mirror event id with an unrelated message row.
	squat := newEnv(mirrorID, message.KindEvent, message.Audience{"system"})
	if _, err := cs.Log.Append(ctx, squat, false); err != nil {
		t.Fatalf("seed squat row: %v", err)
	}
	add := storespec.MemberActorAdd{ID: "collide", Kind: actor.KindAgent, At: 5000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{add}, nil); err == nil {
		t.Fatal("add mirror append must fail on the id collision")
	}
	// Rolled back: the registry row must NOT exist.
	if _, ok, _ := cs.Registry.Lookup(ctx, "collide"); ok {
		t.Error("failed transition must roll back the actor_registry row")
	}
}

// The remove-mirror appendTx fails the same way: pre-occupy the deregistered
// mirror id, then a real remove must fail and roll back (the actor stays active).
func TestApplyMemberTransitions_RemoveMirrorAppendConflict(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	add := storespec.MemberActorAdd{ID: "rc", Kind: actor.KindAgent, At: 1000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{add}, nil); err != nil {
		t.Fatalf("add: %v", err)
	}
	mirrorID := "system.actor.deregistered:rc:6000"
	squat := newEnv(mirrorID, message.KindEvent, message.Audience{"system"})
	if _, err := cs.Log.Append(ctx, squat, false); err != nil {
		t.Fatalf("seed squat row: %v", err)
	}
	rm := storespec.MemberActorRemove{ID: "rc", At: 6000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{rm}); err == nil {
		t.Fatal("remove mirror append must fail on the id collision")
	}
	// Rolled back: the actor must still be active.
	rec, ok, _ := cs.Registry.Lookup(ctx, "rc")
	if !ok || !rec.IsActive() {
		t.Error("failed remove transition must roll back; actor must stay active")
	}
}
