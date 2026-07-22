package home

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// The control index is a bounded-staleness cache of actor_registry, never a
// correctness precondition: whatever caused a post-commit refresh to be missed,
// the projection sync arm re-aligns the durable half within one interval —
// missing rows come back, rows whose registry row is gone are swept (with an
// end-of-life re-read guarding against racing admits).
func TestProjectionSyncRepairsMissedRefreshBothWays(t *testing.T) {
	h, err := Open(Config{
		ChannelID:            "projection-sync",
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  &compositionActivationResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	declared, err := h.declare(ctx, DeclareRequest{
		SourceDeclID: "decl:sync", Class: "probe",
		Placement: storespec.NewServerPlacement(), Kind: actor.KindAgent, CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	id := declared.Row.ID

	// Simulate a missed post-commit refresh: the registry row exists but the
	// index entry is gone (the exact state an expired caller context leaves).
	h.controlIndex.DeleteBatch([]actor.ActorID{id})
	if _, found, _ := h.controlIndex.LookupActive(ctx, id); found {
		t.Fatal("fixture: index entry still present")
	}
	// A ghost durable entry whose registry row never existed (the reverse
	// direction: index ahead of the account).
	ghost := declared.Row
	ghost.ID = "agent:ghost:1"
	ghost.Role = storespec.RoleNone
	if !h.controlIndex.UpsertBatch([]controlEntry{{Row: ghost, World: storespec.WorldDurable}}) {
		t.Fatal("fixture: ghost upsert rejected")
	}

	h.lastProjectionSyncNs.Store(0)
	h.syncDeclaredProjection(ctx)

	if _, found, err := h.controlIndex.LookupActive(ctx, id); err != nil || !found {
		t.Fatalf("sync arm did not restore the missing durable entry (found=%v err=%v)", found, err)
	}
	if _, found, _ := h.controlIndex.LookupActive(ctx, "agent:ghost:1"); found {
		t.Fatal("sync arm kept a durable entry with no registry row")
	}
}
