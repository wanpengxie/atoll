package app

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const planValidClass = "test-plan-valid-class"

func init() { registry.Register(planValidClass, registry.ClassDecl{Kind: actor.KindAgent}) }

func TestAppPlanProvider_UsesHomeIntentAndRejectsMissingSource(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := openTestAppDB(t, filepath.Join(dir, "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	chID := channel.ID("plan-channel")
	a := &App{db: db, homes: map[channel.ID]*home.Home{}}
	h, err := home.Open(home.Config{
		ChannelID: chID, DBPath: filepath.Join(dir, "channel.sqlite"),
		CompositionResolver: compositionResolver{app: a}, DaemonAuthority: appDaemonAuthority{app: a},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	placement, _ := storespec.NewDaemonPlacement("daemon-1")
	result, err := h.Declare(ctx, home.DeclareRequest{
		SourceDeclID: "missing", Principal: "principal", Class: planValidClass,
		Placement: placement, Kind: actor.KindAgent, CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Always-on daemon declarations become an intent on reconcile; plan
	// construction must still resolve the source configuration atomically.
	_, _ = h.RestartInstanceDirect(ctx, result.Row.ID) // harmless poke if already starting
	a.homes[chID] = h
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
