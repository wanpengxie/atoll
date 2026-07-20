package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

const planValidClass = "test-plan-valid-class"

func init() { registry.Register(planValidClass, registry.ClassDecl{Kind: actor.KindAgent}) }

func TestAppPlanProvider_UsesHomeIntentAndRejectsMissingSource(t *testing.T) {
	ctx := context.Background()
	chID := channel.ID("plan-channel")
	a := newBareAppForTest(t)
	snapshot, err := (channel.RenderedSnapshot{Class: planValidClass, Config: json.RawMessage(`{}`), Placement: channel.Placement{Kind: channel.PlacementDaemon, DesiredHost: "daemon-1"}, RenderSeq: 1}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	h := openTestChannelForTest(t, a, chID, []channelhost.GenesisDeclaration{{DeclID: "missing", Principal: "principal", Kind: actor.KindAgent, Rendered: snapshot}})
	declared, err := h.DeclaredBySource(ctx, "missing")
	if err != nil || len(declared) != 1 {
		t.Fatalf("declared=%v err=%v", declared, err)
	}
	// Always-on daemon declarations become an intent on reconcile; plan
	// construction must still resolve the source configuration atomically.
	_, _ = h.RestartInstanceDirect(ctx, declared[0].ID) // harmless poke if already starting
	deadline := time.Now().Add(time.Second)
	for {
		plans, planErr := (appPlanProvider{app: a}).Plan(ctx, chID, "daemon-1")
		if planErr != nil {
			break
		}
		if len(plans) != 0 {
			t.Fatal("missing declaration source produced a successful non-empty plan")
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon attachment intent was not established")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
