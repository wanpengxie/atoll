package store_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// --- Insert / Lookup / Exists / Deregister -----------------------------------

func TestRegistry_InsertLookup(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	in := storespec.Record{
		ID: "tool:xhs", Kind: actor.KindTool,
		Binding: actor.BindingRuntimeInboundViaRelay, CreatedAt: 1000,
	}
	if err := cs.Membership.Insert(ctx, in); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	rec, ok, err := cs.Registry.Lookup(ctx, "tool:xhs")
	if err != nil || !ok {
		t.Fatalf("Lookup ok=%v err=%v", ok, err)
	}
	if rec.ID != in.ID || rec.Kind != in.Kind || rec.Binding != in.Binding || rec.CreatedAt != 1000 {
		t.Errorf("rec=%+v want %+v", rec, in)
	}
	if !rec.IsActive() {
		t.Errorf("freshly inserted actor must be active")
	}
}

func TestRegistry_InsertRejectsEmptyID(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	if err := cs.Membership.Insert(ctx, storespec.Record{Kind: actor.KindAgent, CreatedAt: 1}); err == nil {
		t.Fatal("Insert with empty ID must error")
	}
}

func TestRegistry_LookupMissing(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	_, ok, err := cs.Registry.Lookup(ctx, "ghost")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if ok {
		t.Error("missing actor ok=true want false")
	}
}

func TestRegistry_Exists(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	if ex, err := cs.Registry.Exists(ctx, "tool:xhs"); err != nil || ex {
		t.Fatalf("pre-insert Exists=%v err=%v", ex, err)
	}
	if err := cs.Membership.Insert(ctx, storespec.Record{ID: "tool:xhs", Kind: actor.KindTool, CreatedAt: 1}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if ex, err := cs.Registry.Exists(ctx, "tool:xhs"); err != nil || !ex {
		t.Fatalf("post-insert Exists=%v err=%v", ex, err)
	}
	// Exists is true even after soft-deregister (it is presence-of-row, not active).
	if err := cs.Membership.Deregister(ctx, "tool:xhs", 2000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if ex, err := cs.Registry.Exists(ctx, "tool:xhs"); err != nil || !ex {
		t.Fatalf("post-deregister Exists=%v err=%v want true", ex, err)
	}
}

// Deregister soft-removes: Lookup still returns the row but IsActive()==false;
// ListActive no longer includes it.
func TestRegistry_DeregisterSoftRemoves(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	if err := cs.Membership.Insert(ctx, storespec.Record{ID: "tool:xhs", Kind: actor.KindTool, CreatedAt: 1000}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := cs.Membership.Deregister(ctx, "tool:xhs", 5000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	rec, ok, err := cs.Registry.Lookup(ctx, "tool:xhs")
	if err != nil || !ok {
		t.Fatalf("Lookup after deregister ok=%v err=%v (soft-remove keeps the row)", ok, err)
	}
	if rec.IsActive() {
		t.Errorf("deregistered actor IsActive()=true; DeregisteredAt=%d", rec.DeregisteredAt)
	}
	active, err := cs.Registry.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("ListActive=%v want empty after deregister", active)
	}
}

// Deregister of a missing or already-deregistered actor is a no-op (no error).
func TestRegistry_DeregisterIdempotent(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	if err := cs.Membership.Deregister(ctx, "ghost", 1); err != nil {
		t.Fatalf("Deregister missing must be no-op, got: %v", err)
	}
	if err := cs.Membership.Insert(ctx, storespec.Record{ID: "a", Kind: actor.KindAgent, CreatedAt: 1}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := cs.Membership.Deregister(ctx, "a", 2); err != nil {
		t.Fatalf("Deregister 1: %v", err)
	}
	if err := cs.Membership.Deregister(ctx, "a", 3); err != nil {
		t.Fatalf("Deregister again must be no-op, got: %v", err)
	}
}

// ListActive returns only active actors, sorted by actor_id.
func TestRegistry_ListActiveSortedAndFiltered(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	for _, id := range []actor.ActorID{"zeta", "alpha", "mike"} {
		if err := cs.Membership.Insert(ctx, storespec.Record{ID: id, Kind: actor.KindAgent, CreatedAt: 1}); err != nil {
			t.Fatalf("Insert %s: %v", id, err)
		}
	}
	if err := cs.Membership.Deregister(ctx, "mike", 9); err != nil {
		t.Fatalf("Deregister mike: %v", err)
	}
	active, err := cs.Registry.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	got := []actor.ActorID{}
	for _, r := range active {
		got = append(got, r.ID)
	}
	want := []actor.ActorID{"alpha", "zeta"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("ListActive ids=%v want %v (sorted, mike filtered)", got, want)
	}
}

// --- Membership control plane: transitions + mirror events -------------------

// ApplyMemberTransitions registers the actor AND emits a system.actor.registered
// mirror event in one tx — addressed to the system actor (A1), kind=event,
// never terminal (A3 events are not closures).
func TestApplyMemberTransitions_AddEmitsMirror(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	adds := []storespec.MemberActorAdd{{
		ID: "tool:xhs", Kind: actor.KindTool,
		Binding: actor.BindingRuntimeInboundViaRelay, At: 7000,
	}}
	if err := cs.Membership.ApplyMemberTransitions(ctx, adds, nil); err != nil {
		t.Fatalf("ApplyMemberTransitions: %v", err)
	}

	// Registry projection updated.
	rec, ok, err := cs.Registry.Lookup(ctx, "tool:xhs")
	if err != nil || !ok || !rec.IsActive() {
		t.Fatalf("after add: ok=%v active=%v err=%v", ok, rec.IsActive(), err)
	}

	// Mirror event present in the log, system-addressed, event, non-terminal.
	mirrorID := message.ID("system.actor.registered:tool:xhs:7000")
	row, ok, err := cs.Log.FindByID(ctx, mirrorID)
	if err != nil || !ok {
		t.Fatalf("mirror event ok=%v err=%v", ok, err)
	}
	e := row.Envelope
	// The mirror event's channel scope is the BINDING (testChannelID, fixed at
	// OpenChannel), never a per-call arg — F3.
	if e.ChannelID != testChannelID {
		t.Errorf("mirror channel_id=%q want bound %q", e.ChannelID, testChannelID)
	}
	if e.Kind != message.KindEvent {
		t.Errorf("mirror kind=%q want event", e.Kind)
	}
	if e.Type != "system.actor.registered" {
		t.Errorf("mirror type=%q", e.Type)
	}
	if e.Sender.Kind != actor.KindSystem || e.Sender.ID != actor.SystemActorID {
		t.Errorf("mirror sender=%+v want system", e.Sender)
	}
	if len(e.Audience) != 1 || e.Audience[0] != actor.SystemActorID {
		t.Errorf("mirror audience=%v want [system]", e.Audience)
	}
	if row.IsTerminal {
		t.Errorf("mirror event must not be terminal")
	}
}

// The commit signal belongs to the log append chokepoint, not to one writer:
// BOTH the request-path Append AND the control-plane membership mirror must fire
// OnCommit (③ — the membership path used to bypass the platform writer wrapper
// and so never woke the tap). A no-op transition (which appends nothing) must
// NOT fire — the signal tracks truth advancing, not call attempts.
func TestOnCommit_BothWritePathsFire_NoopSilent(t *testing.T) {
	ctx := context.Background()
	var fires atomic.Int64
	cs := openTestChannelOnCommit(t, func() { fires.Add(1) })

	// Control-plane path: a real member add appends a mirror row → fires once.
	add := storespec.MemberActorAdd{ID: "tool:xhs", Kind: actor.KindTool, Binding: actor.BindingEmbedded, At: 7000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{add}, nil); err != nil {
		t.Fatalf("ApplyMemberTransitions add: %v", err)
	}
	if got := fires.Load(); got != 1 {
		t.Fatalf("control-plane add: OnCommit fires=%d want 1 (membership path must signal)", got)
	}

	// No-op path: re-adding an already-active actor appends nothing → no wake.
	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{add}, nil); err != nil {
		t.Fatalf("ApplyMemberTransitions duplicate add: %v", err)
	}
	if got := fires.Load(); got != 1 {
		t.Fatalf("no-op transition: OnCommit fires=%d want still 1 (no spurious wake)", got)
	}

	// Request path: a plain Append commits one row → fires once more.
	if _, err := cs.Log.Append(ctx, newEnv("m1", message.KindEvent, message.Audience{"tool:xhs"}), false); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if got := fires.Load(); got != 2 {
		t.Fatalf("request-path append: OnCommit fires=%d want 2 (harness path must signal)", got)
	}
}

// A duplicate add of an already-active actor is an idempotent no-op: no second
// mirror event is emitted.
func TestApplyMemberTransitions_DuplicateAddIdempotent(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	add := storespec.MemberActorAdd{ID: "tool:xhs", Kind: actor.KindTool, Binding: actor.BindingEmbedded, At: 7000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{add}, nil); err != nil {
		t.Fatalf("first add: %v", err)
	}
	// Re-apply with a different At — already active, so it must NOT emit a
	// second registered mirror (idempotent; substrate identity carries no
	// per-actor declaration to diff).
	add2 := add
	add2.At = 8000
	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{add2}, nil); err != nil {
		t.Fatalf("duplicate add: %v", err)
	}
	if _, ok, _ := cs.Log.FindByID(ctx, "system.actor.registered:tool:xhs:8000"); ok {
		t.Errorf("duplicate add emitted a second mirror event")
	}
}

// Remove deregisters AND emits a system.actor.deregistered mirror event.
func TestApplyMemberTransitions_RemoveEmitsMirror(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	add := storespec.MemberActorAdd{ID: "tool:xhs", Kind: actor.KindTool, Binding: actor.BindingEmbedded, At: 7000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{add}, nil); err != nil {
		t.Fatalf("add: %v", err)
	}
	rm := storespec.MemberActorRemove{ID: "tool:xhs", At: 9000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{rm}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	rec, ok, _ := cs.Registry.Lookup(ctx, "tool:xhs")
	if !ok || rec.IsActive() {
		t.Fatalf("after remove: ok=%v active=%v want inactive", ok, rec.IsActive())
	}
	if _, ok, err := cs.Log.FindByID(ctx, "system.actor.deregistered:tool:xhs:9000"); err != nil || !ok {
		t.Errorf("deregistered mirror ok=%v err=%v", ok, err)
	}
}

// A remove of an already-deregistered actor is an idempotent no-op: no second
// deregistered mirror event.
func TestApplyMemberTransitions_DuplicateRemoveIdempotent(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	add := storespec.MemberActorAdd{ID: "a", Kind: actor.KindAgent, At: 1000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{add}, nil); err != nil {
		t.Fatalf("add: %v", err)
	}
	rm := storespec.MemberActorRemove{ID: "a", At: 2000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{rm}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	rm2 := storespec.MemberActorRemove{ID: "a", At: 3000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{rm2}); err != nil {
		t.Fatalf("duplicate remove: %v", err)
	}
	if _, ok, _ := cs.Log.FindByID(ctx, "system.actor.deregistered:a:3000"); ok {
		t.Error("duplicate remove emitted a second mirror event")
	}
}

// Missing timestamps are rejected (no half-applied state). The channel scope is
// the binding now (fixed at OpenChannel), so there is no per-call empty-channel
// arg to guard — a normally-opened store is always bound.
func TestApplyMemberTransitions_GuardsInputs(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{{ID: "a", Kind: actor.KindAgent, At: 0}}, nil); err == nil {
		t.Error("add with zero timestamp must error")
	}
	if err := cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{{ID: "a", At: 0}}); err == nil {
		t.Error("remove with zero timestamp must error")
	}
}

// Reactivation: a deregistered actor re-added becomes active again and emits a
// fresh registered mirror.
func TestApplyMemberTransitions_Reactivation(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	add := storespec.MemberActorAdd{ID: "a", Kind: actor.KindAgent, At: 1000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{add}, nil); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{{ID: "a", At: 2000}}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	readd := storespec.MemberActorAdd{ID: "a", Kind: actor.KindAgent, Binding: actor.BindingEmbedded, At: 3000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{readd}, nil); err != nil {
		t.Fatalf("re-add: %v", err)
	}
	rec, ok, _ := cs.Registry.Lookup(ctx, "a")
	if !ok || !rec.IsActive() {
		t.Fatalf("reactivated actor active=%v", rec.IsActive())
	}
	if _, ok, err := cs.Log.FindByID(ctx, "system.actor.registered:a:3000"); err != nil || !ok {
		t.Errorf("reactivation mirror ok=%v err=%v", ok, err)
	}
}
