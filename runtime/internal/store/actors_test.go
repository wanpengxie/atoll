package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// mirrorEventsOf scans the log for mirror events of typ whose payload names
// actorID. Mirror ids are random uuids (no deterministic id to FindByID), so
// tests locate them by content, the same way a real consumer would.
func mirrorEventsOf(t *testing.T, q storespec.MessageQuery, typ string, actorID string) []storespec.StoredRow {
	t.Helper()
	rows, err := q.ReadAfterSeq(context.Background(), 0, 1000)
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	var out []storespec.StoredRow
	for _, r := range rows {
		if r.Envelope.Type != typ {
			continue
		}
		var p struct {
			ActorID string `json:"actor_id"`
		}
		if err := json.Unmarshal(r.Envelope.Payload, &p); err != nil || p.ActorID != actorID {
			continue
		}
		out = append(out, r)
	}
	return out
}

// --- Admit / Lookup / Exists / Deregister ------------------------------------

func TestRegistry_AdmitLookup(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	id, err := cs.Membership.Admit(ctx, actor.KindTool, "xhs", 1000)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	rec, ok, err := cs.Registry.Lookup(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Lookup ok=%v err=%v", ok, err)
	}
	if rec.ID != id || rec.Kind != actor.KindTool || rec.Principal != "xhs" || rec.CreatedAt != 1000 {
		t.Errorf("rec=%+v", rec)
	}
	if !rec.IsActive() {
		t.Errorf("freshly inserted actor must be active")
	}
}

func TestRegistry_AdmitRejectsInvalidPrincipal(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	for _, principal := range []string{"", "bad:principal"} {
		if _, err := cs.Membership.Admit(ctx, actor.KindAgent, principal, 1); err == nil {
			t.Fatalf("Admit principal %q must error", principal)
		}
	}
}

func TestRegistry_ConcurrentAdmitConvergesOnOneIdentity(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	start := make(chan struct{})
	type result struct {
		id  actor.ActorID
		err error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			id, err := cs.Membership.Admit(ctx, actor.KindAgent, "same-principal", 1000)
			results <- result{id: id, err: err}
		}()
	}
	close(start)
	a, b := <-results, <-results
	if a.err != nil || b.err != nil {
		t.Fatalf("concurrent Admit errors: %v, %v", a.err, b.err)
	}
	if a.id == "" || a.id != b.id {
		t.Fatalf("concurrent Admit ids=(%q,%q), want same non-empty id", a.id, b.id)
	}
	active, err := cs.Registry.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	var matches int
	for _, rec := range active {
		if rec.Kind == actor.KindAgent && rec.Principal == "same-principal" {
			matches++
		}
	}
	if matches != 1 {
		t.Fatalf("active matching identities=%d want 1", matches)
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
	id, err := cs.Membership.Admit(ctx, actor.KindTool, "xhs", 1)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if ex, err := cs.Registry.Exists(ctx, id); err != nil || !ex {
		t.Fatalf("post-insert Exists=%v err=%v", ex, err)
	}
	// Exists is true even after soft-deregister (it is existence-of-row, not active).
	if err := cs.Membership.Deregister(ctx, id, 2000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if ex, err := cs.Registry.Exists(ctx, id); err != nil || !ex {
		t.Fatalf("post-deregister Exists=%v err=%v want true", ex, err)
	}
}

// Deregister soft-removes: Lookup still returns the row but IsActive()==false;
// ListActive no longer includes it.
func TestRegistry_DeregisterSoftRemoves(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	id, err := cs.Membership.Admit(ctx, actor.KindTool, "xhs", 1000)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if err := cs.Membership.Deregister(ctx, id, 5000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	rec, ok, err := cs.Registry.Lookup(ctx, id)
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
	id, err := cs.Membership.Admit(ctx, actor.KindAgent, "a", 1)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if err := cs.Membership.Deregister(ctx, id, 2); err != nil {
		t.Fatalf("Deregister 1: %v", err)
	}
	if err := cs.Membership.Deregister(ctx, id, 3); err != nil {
		t.Fatalf("Deregister again must be no-op, got: %v", err)
	}
}

// ListActive returns only active actors, sorted by actor_id.
func TestRegistry_ListActiveSortedAndFiltered(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	ids := map[string]actor.ActorID{}
	for _, principal := range []string{"zeta", "alpha", "mike"} {
		id, err := cs.Membership.Admit(ctx, actor.KindAgent, principal, 1)
		if err != nil {
			t.Fatalf("Admit %s: %v", principal, err)
		}
		ids[principal] = id
	}
	if err := cs.Membership.Deregister(ctx, ids["mike"], 9); err != nil {
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
	want := []actor.ActorID{ids["alpha"], ids["zeta"]}
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

	id, err := cs.Membership.Admit(ctx, actor.KindTool, "xhs", 7000)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}

	// Registry projection updated.
	rec, ok, err := cs.Registry.Lookup(ctx, id)
	if err != nil || !ok || !rec.IsActive() {
		t.Fatalf("after add: ok=%v active=%v err=%v", ok, rec.IsActive(), err)
	}

	// Mirror event present in the log, system-addressed, event, non-terminal.
	mirrors := mirrorEventsOf(t, cs.Query, "system.actor.registered", string(id))
	if len(mirrors) != 1 {
		t.Fatalf("registered mirrors = %d, want 1", len(mirrors))
	}
	row := mirrors[0]
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
	id, err := cs.Membership.Admit(ctx, actor.KindTool, "xhs", 7000)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if got := fires.Load(); got != 1 {
		t.Fatalf("control-plane add: OnCommit fires=%d want 1 (membership path must signal)", got)
	}

	// No-op path: re-adding an already-active actor appends nothing → no wake.
	if got, err := cs.Membership.Admit(ctx, actor.KindTool, "xhs", 8000); err != nil || got != id {
		t.Fatalf("idempotent Admit=%q err=%v", got, err)
	}
	if got := fires.Load(); got != 1 {
		t.Fatalf("no-op transition: OnCommit fires=%d want still 1 (no spurious wake)", got)
	}

	// Request path: a plain Append commits one row → fires once more.
	if _, err := cs.Log.Append(ctx, newEnv("m1", message.KindEvent, message.Audience{id}), false); err != nil {
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

	id, err := cs.Membership.Admit(ctx, actor.KindTool, "xhs", 7000)
	if err != nil {
		t.Fatalf("first Admit: %v", err)
	}
	// Re-apply with a different At — already active, so it must NOT emit a
	// second registered mirror (idempotent; substrate identity carries no
	// per-actor declaration to diff).
	if got, err := cs.Membership.Admit(ctx, actor.KindTool, "xhs", 8000); err != nil || got != id {
		t.Fatalf("duplicate Admit=%q err=%v", got, err)
	}
	if n := len(mirrorEventsOf(t, cs.Query, "system.actor.registered", string(id))); n != 1 {
		t.Errorf("registered mirrors = %d after duplicate add, want 1 (idempotent no-op emits nothing)", n)
	}
}

// Remove deregisters AND emits a system.actor.deregistered mirror event.
func TestApplyMemberTransitions_RemoveEmitsMirror(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	id, err := cs.Membership.Admit(ctx, actor.KindTool, "xhs", 7000)
	if err != nil {
		t.Fatal(err)
	}
	rm := storespec.MemberActorRemove{ID: id, At: 9000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{rm}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	rec, ok, _ := cs.Registry.Lookup(ctx, id)
	if !ok || rec.IsActive() {
		t.Fatalf("after remove: ok=%v active=%v want inactive", ok, rec.IsActive())
	}
	if n := len(mirrorEventsOf(t, cs.Query, "system.actor.deregistered", string(id))); n != 1 {
		t.Errorf("deregistered mirrors = %d, want 1", n)
	}
}

// A remove of an already-deregistered actor is an idempotent no-op: no second
// deregistered mirror event.
func TestApplyMemberTransitions_DuplicateRemoveIdempotent(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	id, err := cs.Membership.Admit(ctx, actor.KindAgent, "a", 1000)
	if err != nil {
		t.Fatal(err)
	}
	rm := storespec.MemberActorRemove{ID: id, At: 2000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{rm}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	rm2 := storespec.MemberActorRemove{ID: id, At: 3000}
	if err := cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{rm2}); err != nil {
		t.Fatalf("duplicate remove: %v", err)
	}
	if n := len(mirrorEventsOf(t, cs.Query, "system.actor.deregistered", string(id))); n != 1 {
		t.Errorf("deregistered mirrors = %d after duplicate remove, want 1 (idempotent no-op emits nothing)", n)
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

// Admission after removal mints a fresh instance id; the old id stays retired.
func TestApplyMemberTransitions_OldIdentityStaysRetired(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	oldID, err := cs.Membership.Admit(ctx, actor.KindAgent, "a", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{{ID: oldID, At: 2000}}); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{{ID: oldID, Kind: actor.KindAgent, At: 3000}}, nil); !errors.Is(err, storespec.ErrMemberInactive) {
		t.Fatalf("old-id add err=%v", err)
	}
	newID, err := cs.Membership.Admit(ctx, actor.KindAgent, "a", 3000)
	if err != nil {
		t.Fatal(err)
	}
	if newID == oldID {
		t.Fatal("re-admission reused old actor id")
	}
	rec, ok, _ := cs.Registry.Lookup(ctx, newID)
	if !ok || !rec.IsActive() {
		t.Fatalf("fresh identity active=%v", rec.IsActive())
	}
	if n := len(mirrorEventsOf(t, cs.Query, "system.actor.registered", string(newID))); n != 1 {
		t.Errorf("new-id registered mirrors=%d want 1", n)
	}
}
