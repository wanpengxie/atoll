package home

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Restarting a channel gives its WORKING members a fresh term and leaves
// everything else alone. The exclusion policy is one rule — agent and tool
// only — because everything that must be spared falls outside those two kinds
// by construction: a human cell holds no running work, system and the
// registrar are the kernel's own residents, and the service door and channel
// peers are platform plumbing (a peer especially: it is another channel's
// door, and recycling it would interrupt calls that have nothing to do with
// the channel being rescued).
func TestChannelRestartRecyclesOnlyTheMembersThatDoWork(t *testing.T) {
	const owner = "restart-owner"
	h, err := Open(completeHomeTestConfig(Config{
		ChannelID:            "restart-channel",
		DBPath:               t.TempDir() + "/channel.sqlite",
		CompositionResolver:  routingResolver{},
		IntroductionResolver: routingResolver{},
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		Genesis: &storespec.ChannelGenesis{
			ChannelID: "restart-channel", Type: "channel",
			OwnerPrincipal: owner, CreatedAt: time.Now().UnixMilli(),
		},
		BootstrapHumanPrincipals: []string{owner},
		BootstrapDeclarations: []DeclareRequest{
			routingDeclaration("worker-a", "routing-live"),
			routingDeclaration("worker-b", "routing-live"),
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	before, err := h.controller.ActiveIdentities()
	if err != nil {
		t.Fatal(err)
	}
	value, err := h.opEntry.Execute(ctx, sysactor.TypeMemberRestartAll, sysactor.OperateRequest{
		ChannelID: h.channelID,
		Initiator: routingAgent(t, h, "worker-a"),
		Caller:    harness.Caller{Channel: h.channelID, Actor: routingAgent(t, h, "worker-a")},
		Anchor:    "restart-request",
		Cause:     message.Root(),
		Payload:   json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, _ := value.(map[string]any)

	restarted, _ := result["restarted"].([]actor.ActorID)
	recycled := map[actor.ActorID]bool{}
	for _, id := range restarted {
		recycled[id] = true
	}
	for _, identity := range before {
		working := identity.Kind == actor.KindAgent || identity.Kind == actor.KindTool
		if working && !recycled[identity.ID] {
			t.Fatalf("%s (%s) does work but was not restarted", identity.ID, identity.Kind)
		}
		if !working && recycled[identity.ID] {
			t.Fatalf("%s (%s) was restarted; only agent and tool should be", identity.ID, identity.Kind)
		}
	}
	if len(restarted) == 0 {
		t.Fatal("nothing was restarted at all")
	}

	// Whoever was spared is named and given a reason, so the answer is a full
	// account of the roster rather than a silent subset.
	skipped, _ := result["skipped"].([]map[string]any)
	if len(skipped)+len(restarted) != len(before) {
		t.Fatalf("restarted=%d skipped=%d, want them to cover the whole roster of %d",
			len(restarted), len(skipped), len(before))
	}
	for _, row := range skipped {
		if row["reason"] == "" || row["member"] == nil {
			t.Fatalf("skipped row is not self-explaining: %v", row)
		}
	}

	// Restarting is in-place: it is a new TERM, not a new member. Ids, the
	// roster and the registry rows all survive — which is the whole difference
	// between this and deleting and re-creating the channel.
	after, err := h.controller.ActiveIdentities()
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("roster changed size: before=%d after=%d", len(before), len(after))
	}
	stillThere := map[actor.ActorID]bool{}
	for _, identity := range after {
		stillThere[identity.ID] = true
	}
	for _, identity := range before {
		if !stillThere[identity.ID] {
			t.Fatalf("%s lost its identity across the restart", identity.ID)
		}
	}
}

// The button is pressed when a channel is already wedged, so one member that
// refuses to come back must not keep the rest stuck. The walk finishes and the
// caller is told exactly who failed and why.
func TestChannelRestartFinishesTheWalkWhenAMemberFails(t *testing.T) {
	h := openRoutingHome(t, "restart-partial",
		routingDeclaration("worker-a", "routing-live"),
		routingDeclaration("worker-b", "routing-live"))
	ctx := context.Background()

	// A member that is gone by the time the walk reaches it is the ordinary
	// shape of "this one cannot come back": the controller refuses it, the
	// walk records it, and everybody else still restarts.
	victim := routingAgent(t, h, "worker-b")
	if _, err := h.opEntry.remove(ctx, removeRequest{Target: victim, InitiatorActorID: victim}); err != nil {
		t.Fatal(err)
	}

	value, err := h.opEntry.Execute(ctx, sysactor.TypeMemberRestartAll, sysactor.OperateRequest{
		ChannelID: h.channelID,
		Initiator: routingAgent(t, h, "worker-a"),
		Anchor:    "restart-request",
		Cause:     message.Root(),
		Payload:   json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("a failing member aborted the whole restart: %v", err)
	}
	result, _ := value.(map[string]any)
	restarted, _ := result["restarted"].([]actor.ActorID)
	if len(restarted) == 0 {
		t.Fatal("the surviving member was not restarted")
	}
	for _, id := range restarted {
		if id == victim {
			t.Fatal("a removed member was reported as restarted")
		}
	}
}

// A control word with no parameters means exactly that: a payload carrying
// anything else is a caller mistake, not a silently ignored field.
func TestChannelRestartRefusesAPayload(t *testing.T) {
	h := openRoutingHome(t, "restart-payload", routingDeclaration("worker-a", "routing-live"))
	_, err := h.opEntry.Execute(context.Background(), sysactor.TypeMemberRestartAll, sysactor.OperateRequest{
		ChannelID: h.channelID,
		Initiator: routingAgent(t, h, "worker-a"),
		Cause:     message.Root(),
		Payload:   json.RawMessage(`{"member":"agent:someone:1"}`),
	})
	var opErr *sysactor.OperateError
	if err == nil || !errors.As(err, &opErr) {
		t.Fatalf("err=%v, want a typed refusal", err)
	}
	if opErr.Code != string(channelspec.ErrCodeBadPayload) {
		t.Fatalf("code=%q, want bad_payload", opErr.Code)
	}
}
