package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/registry"
)

const realmFetchSeederClass = "test-realm-fetch-seeder"

func init() {
	registry.Register(realmFetchSeederClass, registry.ClassDecl{Kind: actor.KindAgent, New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
		return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent, Factory: platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error {
				_, _ = sys.Resource().Create("kv:agent-source", []byte("agent-visible-artifact"))
				for {
					if _, err := sys.Recv(); err != nil {
						return err
					}
				}
			}, nil
		}}}}, nil
	}})
}

func TestRealmOpsAgentCannotWriteDeclarationRegistry(t *testing.T) {
	a := newBareAppForTest(t)
	snapshot, err := (channel.RenderedSnapshot{
		Class: "dormant-agent", Placement: channel.Placement{Kind: channel.PlacementServer}, RenderSeq: 1,
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	bundle := openTestChannelForTest(t, a, "agent-realm-ops", []channelhost.GenesisDeclaration{{
		DeclID: "requester-decl", Kind: actor.KindAgent, Rendered: snapshot,
	}})
	agent, found, err := bundle.View().DeclaredBySourceOne(context.Background(), "requester-decl")
	if err != nil || !found {
		t.Fatalf("resolve requester=(%s,%v,%v)", agent.ID, found, err)
	}
	_, err = (realmOps{app: a}).CreateDeclaration(context.Background(), channel.Requester{
		ActorID: agent.ID, ChannelID: "agent-realm-ops", RequestID: "agent-create-request",
	}, channel.DeclSpec{Name: "must not persist", Class: "go-kimi", Visibility: "public", Config: json.RawMessage(`{}`)})
	var realmErr *channel.RealmError
	if !errors.As(err, &realmErr) || realmErr.Code != channel.RealmForbidden {
		t.Fatalf("agent create err=%v", err)
	}
	var count int
	if err := a.db.QueryRow(`SELECT COUNT(*) FROM actor_decls WHERE name='must not persist'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("agent declaration writes=%d", count)
	}
}

func TestRealmToolDerivedRefIsChannelScoped(t *testing.T) {
	one := channel.DerivedRealmToolRef("channel-a", "same-request")
	two := channel.DerivedRealmToolRef("channel-b", "same-request")
	if one == two || one == "" || two == "" {
		t.Fatalf("derived refs collide: %q %q", one, two)
	}
	if one != channel.DerivedRealmToolRef("channel-a", "same-request") {
		t.Fatal("derived ref is not deterministic")
	}
}

func TestObserverResourceStreamStopsAtChunkBoundaryAfterRealmToolRemoval(t *testing.T) {
	a := newBareAppForTest(t)
	snapshot, err := (channel.RenderedSnapshot{
		Class: realmToolClass, Placement: channel.Placement{Kind: channel.PlacementServer}, RenderSeq: 1,
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	bundle := openTestChannelForTest(t, a, "stream-source", []channelhost.GenesisDeclaration{{
		DeclID: realmToolDeclID, Kind: actor.KindTool, Rendered: snapshot,
	}})
	if _, err := a.db.Exec(`INSERT INTO channels(id,name,type,created_at,parent_id) VALUES ('stream-source','stream-source','group',?,NULL)`, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	body := &observerResourceBody{
		ctx: context.Background(), app: a, source: "stream-source", principal: "outside-principal",
		body: io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("x"), 64*1024))),
	}
	defer body.Close()
	buffer := make([]byte, 64*1024)
	n, err := body.Read(buffer)
	if err != nil || n != 32*1024 {
		t.Fatalf("first chunk n=%d err=%v", n, err)
	}
	if _, err := bundle.SysOp().RevokeDeclTargets(context.Background(), channel.RevokeDeclRequest{
		Ref: "stream-revoke", DeclID: realmToolDeclID,
	}); err != nil {
		t.Fatal(err)
	}
	n, err = body.Read(buffer)
	var realmErr *channel.RealmError
	if n != 0 || !errors.As(err, &realmErr) || realmErr.Code != channel.RealmCapabilityUnavailable {
		t.Fatalf("post-revoke chunk n=%d err=%v", n, err)
	}
}

func TestRealmOpsFetchAllowsAgentWithZeroSourceMembership(t *testing.T) {
	a := newBareAppForTest(t)
	seal := func(class string) channel.RenderedSnapshot {
		t.Helper()
		snapshot, err := (channel.RenderedSnapshot{
			Class: class, Placement: channel.Placement{Kind: channel.PlacementServer}, RenderSeq: 1,
		}).Seal()
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	source := openTestChannelForTest(t, a, "agent-fetch-source", []channelhost.GenesisDeclaration{
		{DeclID: realmToolDeclID, Kind: actor.KindTool, Rendered: seal(realmToolClass)},
		{DeclID: "fetch-seeder", Kind: actor.KindAgent, Rendered: seal(realmFetchSeederClass)},
		{DeclID: "fetch-requester", Kind: actor.KindAgent, Rendered: seal("dormant-fetch-requester")},
	})
	for _, row := range []struct{ id, name string }{{"agent-fetch-source", "agent-fetch-source"}} {
		if _, err := a.db.Exec(`INSERT INTO channels(id,name,type,created_at,parent_id) VALUES (?,?, 'group',?,NULL)`, row.id, row.name, time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	sourceOwner, found, err := source.View().ResolvePrincipal(context.Background(), actor.KindHuman, "owner")
	if err != nil || !found {
		t.Fatalf("source owner=(%s,%v,%v)", sourceOwner, found, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err = source.View().Resources().Stat(context.Background(), channel.Reader{
			ActorID: sourceOwner, Mode: channel.ReaderMember,
		}, "kv:agent-source")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("seed resource did not appear: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	requesterRow, found, err := source.View().DeclaredBySourceOne(context.Background(), "fetch-requester")
	if err != nil || !found {
		t.Fatalf("source requester=(%s,%v,%v)", requesterRow.ID, found, err)
	}
	fetched, err := (realmOps{app: a}).FetchResource(context.Background(), channel.Requester{
		ActorID: requesterRow.ID, ChannelID: "agent-fetch-source", RequestID: "fetch-as-agent",
	}, "agent-fetch-source", resource.ResourceID("kv:agent-source"))
	if err != nil {
		t.Fatal(err)
	}
	defer fetched.Body.Close()
	body, err := io.ReadAll(fetched.Body)
	if err != nil || string(body) != "agent-visible-artifact" {
		t.Fatalf("agent fetch body=%q err=%v", body, err)
	}
}
