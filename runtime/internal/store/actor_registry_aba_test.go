package store

// ABA safety of the actor record store: an identity that ended, followed by a
// NEW identity that takes over the same semantic key (same source declaration,
// or same login principal). The dead identity's lagging writes must land on
// nothing — never on its successor, never on its own tombstone.
//
// The mechanism under test is the `AND deregistered_at IS NULL` predicate that
// every dedup lookup and every mutation carries, plus Insert's tombstone
// avoidance while minting. Both are load-bearing and were previously asserted
// by no test.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func humanDraft(principal, class string, createdAt int64) storespec.ActorDraft {
	return storespec.ActorDraft{
		Kind:       actor.KindHuman,
		Principal:  principal,
		CreatedAt:  createdAt,
		Definition: storespec.ActorDefinition{Class: class},
		Placement:  storespec.NewServerPlacement(),
	}
}

// A declaration whose actor ended and is declared again gets a FRESH identity —
// the dead id is never resurrected, even when the mint seed (kind + key +
// timestamp) is byte-identical, which is exactly the tombstone-avoidance case.
func TestActorRegistryABA_DeclarationReuseMintsFreshIdentity(t *testing.T) {
	ctx := context.Background()
	rig := newActorRegRig(t)

	old := rig.mustInsert(agentDraft("decl:aba", "v1", 1000))
	if err := rig.reg.Deregister(ctx, []actor.ActorID{old.ID}, 2000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	// Same declaration, same mint seed: only the tombstone check can separate
	// the two identities.
	fresh := rig.mustInsert(agentDraft("decl:aba", "v2", 1000))
	if fresh.ID == old.ID {
		t.Fatalf("reused the dead identity %q; a re-declared actor must be a new identity", old.ID)
	}
	if fresh.Definition.Class != "v2" {
		t.Fatalf("fresh identity class = %q, want v2", fresh.Definition.Class)
	}
	if n := rig.rawRowCount(); n != 2 {
		t.Fatalf("actor_registry rows = %d, want 2 (tombstone + successor)", n)
	}

	if _, found, err := rig.reg.LookupActive(ctx, old.ID); err != nil || found {
		t.Fatalf("dead identity %q still active (found=%v err=%v)", old.ID, found, err)
	}
	active, err := rig.reg.ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	if len(active) != 1 || active[0].ID != fresh.ID {
		t.Fatalf("ListActive = %+v, want only the successor %q", active, fresh.ID)
	}
}

// The dead identity's lagging definition write must hit nothing: not the
// successor that now holds the declaration, and not its own tombstoned row.
func TestActorRegistryABA_LateWriteFromDeadIdentityHitsNothing(t *testing.T) {
	ctx := context.Background()
	rig := newActorRegRig(t)

	old := rig.mustInsert(agentDraft("decl:aba", "v1", 1000))
	if err := rig.reg.Deregister(ctx, []actor.ActorID{old.ID}, 2000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	fresh := rig.mustInsert(agentDraft("decl:aba", "v2", 1000))
	before := rig.commits

	stale := storespec.ActorDefinition{Class: "stale", Config: json.RawMessage(`{"stale":true}`)}
	if _, err := rig.reg.UpdateDefinition(ctx, old.ID, stale); err == nil {
		t.Fatal("a definition write aimed at an ended identity must be refused")
	} else if !errors.Is(err, storespec.ErrActorNotFound) {
		t.Fatalf("refusal must be ErrActorNotFound, got %v", err)
	}
	if rig.commits != before {
		t.Fatalf("refused write fired the commit signal %d times", rig.commits-before)
	}

	// The successor is untouched.
	got, found, err := rig.reg.LookupActive(ctx, fresh.ID)
	if err != nil || !found {
		t.Fatalf("successor lookup: found=%v err=%v", found, err)
	}
	if got.Definition.Class != "v2" {
		t.Fatalf("successor class = %q, want v2 — the dead identity's write bled through",
			got.Definition.Class)
	}
	// The tombstone is untouched too: a dead row is history, not a target.
	class, dereg, ok := rig.rawRow(old.ID)
	if !ok {
		t.Fatalf("tombstone row %q vanished", old.ID)
	}
	if class != "v1" {
		t.Fatalf("tombstone class = %q, want the frozen v1", class)
	}
	if !dereg.Valid || dereg.Int64 != 2000 {
		t.Fatalf("tombstone deregistered_at = %+v, want 2000", dereg)
	}
}

// A lagging termination from the dead identity's era is a no-op: it must not
// re-stamp the tombstone and must not reach the successor holding the same key.
func TestActorRegistryABA_LateDeregisterDoesNotReachSuccessor(t *testing.T) {
	ctx := context.Background()
	rig := newActorRegRig(t)

	old := rig.mustInsert(agentDraft("decl:aba", "v1", 1000))
	if err := rig.reg.Deregister(ctx, []actor.ActorID{old.ID}, 2000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	fresh := rig.mustInsert(agentDraft("decl:aba", "v2", 1000))

	if err := rig.reg.Deregister(ctx, []actor.ActorID{old.ID}, 5000); err != nil {
		t.Fatalf("replayed Deregister must be a silent no-op, got %v", err)
	}
	if _, dereg, ok := rig.rawRow(old.ID); !ok || !dereg.Valid || dereg.Int64 != 2000 {
		t.Fatalf("tombstone stamp moved: ok=%v dereg=%+v, want the first 2000", ok, dereg)
	}
	if _, found, err := rig.reg.LookupActive(ctx, fresh.ID); err != nil || !found {
		t.Fatalf("successor %q was killed by the dead identity's termination (found=%v err=%v)",
			fresh.ID, found, err)
	}
}

// The principal half of the same law: a human whose identity ended and who logs
// in again is admitted as a NEW identity, and the principal lookup answers with
// the successor only.
func TestActorRegistryABA_PrincipalReuseAfterEndMintsFreshIdentity(t *testing.T) {
	ctx := context.Background()
	rig := newActorRegRig(t)

	old := rig.mustInsert(humanDraft("alice", "human", 1000))
	if err := rig.reg.Deregister(ctx, []actor.ActorID{old.ID}, 2000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	fresh := rig.mustInsert(humanDraft("alice", "human", 1000))
	if fresh.ID == old.ID {
		t.Fatalf("re-admission reused the dead identity %q", old.ID)
	}

	rec, found, err := rig.reg.LookupActivePrincipal(ctx, actor.KindHuman, "alice")
	if err != nil || !found {
		t.Fatalf("LookupActivePrincipal: found=%v err=%v", found, err)
	}
	if rec.ID != fresh.ID {
		t.Fatalf("principal lookup = %q, want the successor %q", rec.ID, fresh.ID)
	}
	if n := rig.rawRowCount(); n != 2 {
		t.Fatalf("actor_registry rows = %d, want 2 (tombstone + successor)", n)
	}
}

// An explicitly-named id that a tombstone already holds is refused outright:
// the id-in-use check deliberately does NOT filter on deregistered_at, because a
// dead identity keeps its name forever.
func TestActorRegistryABA_ExplicitIDCannotReuseATombstone(t *testing.T) {
	ctx := context.Background()
	rig := newActorRegRig(t)

	old := rig.mustInsert(agentDraft("decl:aba", "v1", 1000))
	if err := rig.reg.Deregister(ctx, []actor.ActorID{old.ID}, 2000); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	revival := agentDraft("decl:other", "v2", 3000)
	revival.ID = old.ID
	if _, err := rig.reg.Insert(ctx, revival); err == nil {
		t.Fatalf("insert reusing the tombstoned id %q must be refused", old.ID)
	}
	if n := rig.rawRowCount(); n != 1 {
		t.Fatalf("actor_registry rows = %d, want 1 (the refused insert wrote nothing)", n)
	}
	if class, dereg, ok := rig.rawRow(old.ID); !ok || class != "v1" || !dereg.Valid {
		t.Fatalf("tombstone was overwritten: class=%q dereg=%+v ok=%v", class, dereg, ok)
	}
}

// While the predecessor is still ACTIVE, the same declaration deduplicates onto
// it — no second identity. This is the negative pole of the ABA law: only death
// releases the key.
func TestActorRegistryABA_ActivePredecessorBlocksSecondIdentity(t *testing.T) {
	rig := newActorRegRig(t)

	first := rig.mustInsert(agentDraft("decl:live", "v1", 1000))
	second := rig.mustInsert(agentDraft("decl:live", "v2", 4000))
	if second.ID != first.ID {
		t.Fatalf("live declaration minted a second identity %q next to %q", second.ID, first.ID)
	}
	if n := rig.rawRowCount(); n != 1 {
		t.Fatalf("actor_registry rows = %d, want 1", n)
	}
}
