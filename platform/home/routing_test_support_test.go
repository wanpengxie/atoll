package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
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

func routingDeclaration(source, class string) DeclareRequest {
	return DeclareRequest{SourceDeclID: source, Kind: actor.KindAgent, Class: class,
		Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli()}
}

func openRoutingHomeAt(t *testing.T, name, dbPath string, bootstrap bool, declarations ...DeclareRequest) *Home {
	t.Helper()
	h, err := Open(completeHomeTestConfig(Config{
		ChannelID: channel.ID(name), DBPath: dbPath,
		CompositionResolver: routingResolver{}, ReconcileInterval: time.Hour,
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
	ids, err := h.View().DeclaredInstances(context.Background(), source)
	if err != nil || len(ids) != 1 {
		t.Fatalf("declaration %q: instances=%v err=%v", source, ids, err)
	}
	return ids[0]
}
