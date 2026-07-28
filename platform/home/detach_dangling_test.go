package home

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Detach is a wiring-domain action and nothing else: it removes ONE binding row.
// The actors placed on the detached daemon stay members — their desired dangles,
// execution simply has nowhere to run, messages still arrive, and re-attaching
// the same daemon id makes the whole thing whole again with no repair step.
// Cleaning up such actors is a separate, explicit End/Remove with a target list.
func TestDetachLeavesActorsDanglingButFullyMembers(t *testing.T) {
	ctx := context.Background()
	const daemonID = "d1"

	placement, err := storespec.NewDaemonPlacement(daemonID)
	if err != nil {
		t.Fatal(err)
	}
	h, err := Open(Config{
		ChannelID:            "detach-home",
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  emptyCompositionResolver{},
		IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: "remote", Kind: actor.KindAgent, Class: "remote-worker",
			Placement: placement, CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })

	declared, err := h.View().DeclaredInstances(ctx, "remote")
	if err != nil || len(declared) != 1 {
		t.Fatalf("declared instance: instances=%v err=%v", declared, err)
	}
	target := declared[0]
	sender, err := admitThroughSysOp(h, ctx, actor.KindHuman, "alice")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := h.opEntry.AttachDaemon(ctx, channel.DaemonRequest{DaemonID: daemonID}); err != nil {
		t.Fatal(err)
	}
	planBefore, err := h.planForDaemon(ctx, daemonID)
	if err != nil || len(planBefore) != 1 || planBefore[0].ActorID != target {
		t.Fatalf("plan before detach = %+v err=%v", planBefore, err)
	}

	if _, err := h.opEntry.DetachDaemon(ctx, channel.DaemonRequest{DaemonID: daemonID}); err != nil {
		t.Fatal(err)
	}

	// Only the binding row moved.
	if bound, err := h.bindings.IsBound(ctx, storespec.DaemonID(daemonID)); err != nil || bound {
		t.Fatalf("binding survived detach: bound=%v err=%v", bound, err)
	}
	if active, err := h.actors.IsActive(ctx, target); err != nil || !active {
		t.Fatalf("detach killed the actor: active=%v err=%v", active, err)
	}
	// The record is untouched: the actor is still placed on the detached
	// daemon, which is exactly why the desired below still dangles.
	if plan, planErr := h.planForDaemon(ctx, daemonID); planErr != nil || len(plan) != 1 {
		t.Fatalf("detach rewrote the record: plan=%+v err=%v", plan, planErr)
	}

	// The desired dangles rather than disappearing: the plan still names the
	// actor, there is just no bound daemon to serve it.
	planAfter, err := h.planForDaemon(ctx, daemonID)
	if err != nil || len(planAfter) != 1 || planAfter[0].ActorID != target {
		t.Fatalf("plan after detach = %+v err=%v", planAfter, err)
	}

	// Collaboration is untouched: a message to a dangling member is accepted.
	result, err := h.minter.WriteAdmitted(
		ctx,
		storespec.IdentityAdmission{ID: sender, Kind: actor.KindHuman},
		&message.Envelope{
			ID: "detach-probe", TS: time.Now().UnixMilli(), Kind: message.KindRequest,
			Type: "detach.probe", Visibility: message.VisibilityPublic,
			Audience: message.Audience{target},
		},
	)
	if err != nil || !result.Accepted() {
		t.Fatalf("message to a dangling member: result=%+v err=%v", result, err)
	}

	// Re-attaching the same daemon id needs no repair action.
	if _, err := h.opEntry.AttachDaemon(ctx, channel.DaemonRequest{DaemonID: daemonID}); err != nil {
		t.Fatal(err)
	}
	planBack, err := h.planForDaemon(ctx, daemonID)
	if err != nil || len(planBack) != 1 || planBack[0].ActorID != target ||
		planBack[0].AttemptKey != planBefore[0].AttemptKey {
		t.Fatalf("plan after re-attach = %+v err=%v", planBack, err)
	}
}
