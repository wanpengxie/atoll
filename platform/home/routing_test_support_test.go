package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type routingResolver struct{}

func (routingResolver) BuildClass(_ channel.ID, _ actor.ActorID, class string, _ json.RawMessage) (platform.ActorFactory, bool) {
	if class != "routing-live" {
		return platform.ActorFactory{}, false
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error { <-sys.Life().Done(); return nil }, nil
	}}}, true
}

func (routingResolver) ResolveDeclaration(_ context.Context, _ channel.ID, source string) (channelspec.DeclarationFacts, error) {
	if source == "" {
		return channelspec.DeclarationFacts{}, channelspec.ErrDeclarationNotFound
	}
	return channelspec.DeclarationFacts{Name: source, Class: "routing-live", Config: json.RawMessage(`{}`), Visibility: "public"}, nil
}
func (routingResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return actor.KindAgent, true, nil
}
func (routingResolver) ClassPlacement(context.Context, string) (channelspec.PlacementKind, bool, error) {
	return channelspec.PlacementServer, true, nil
}
func (routingResolver) AdmitIntroduction(context.Context, channel.ID, channelspec.DeclarationFacts) error {
	return nil
}

func routingDeclaration(source, class string) DeclareRequest {
	return DeclareRequest{SourceDeclID: source, Seed: source, Kind: actor.KindAgent, Class: class,
		Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli()}
}

func openRoutingHomeAt(t *testing.T, name, dbPath string, bootstrap bool, declarations ...DeclareRequest) *Home {
	t.Helper()
	h, err := Open(completeHomeTestConfig(Config{
		ChannelID: channel.ID(name), DBPath: dbPath,
		CompositionResolver: routingResolver{}, IntroductionResolver: routingResolver{}, ReconcileInterval: time.Hour,
		Bootstrap: bootstrap, MustExistDB: !bootstrap, BootstrapDeclarations: declarations,
	}))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func openRoutingHome(t *testing.T, name string, declarations ...DeclareRequest) *Home {
	t.Helper()
	h := openRoutingHomeAt(t, name, filepath.Join(t.TempDir(), "channel.sqlite"), true, declarations...)
	t.Cleanup(func() { _ = h.closeInternal("test") })
	return h
}

func routingAgent(t *testing.T, h *Home, source string) actor.ActorID {
	t.Helper()
	ids, err := rosterMembersForSource(context.Background(), h.View(), source)
	if err != nil || len(ids) != 1 {
		t.Fatalf("declaration %q: instances=%v err=%v", source, ids, err)
	}
	return ids[0]
}

func rosterMembersForSource(ctx context.Context, view View, source string) ([]actor.ActorID, error) {
	roster, err := view.Roster(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]actor.ActorID, 0)
	for _, row := range roster {
		if row.DeclID == source {
			out = append(out, row.ID)
		}
	}
	return out, nil
}

func activeMembersForSource(controller *actorctl.Controller, source string) ([]actor.ActorID, error) {
	rows, err := controller.DeclaredReconcileList()
	if err != nil {
		return nil, err
	}
	out := make([]actor.ActorID, 0)
	for _, row := range rows {
		if row.SourceDeclID == source {
			out = append(out, row.ID)
		}
	}
	return out, nil
}
