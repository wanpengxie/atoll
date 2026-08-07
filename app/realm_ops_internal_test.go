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
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/realmtool"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/registry"
)

const realmFetchSeederClass = "test-realm-fetch-seeder"
const realmCrossKindToolClass = "test-realm-cross-kind-tool"

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
	registry.Register(realmCrossKindToolClass, registry.ClassDecl{Kind: actor.KindTool, New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
		return platform.ActorDecl{ID: spec.ID, Kind: actor.KindTool}, nil
	}})
}

func TestRealmOpsEditDeclarationRejectsCrossKind(t *testing.T) {
	a := newBareAppForTest(t)
	bundle := openTestChannelForTest(t, a, "realm-edit-kind", nil)
	owner, found, err := bundle.View().ResolvePrincipal(context.Background(), "owner")
	if err != nil || !found {
		t.Fatalf("owner=(%s,%v,%v)", owner, found, err)
	}
	now := time.Now().UnixMilli()
	if _, err := a.db.Exec(`INSERT INTO actor_decls(id,name,owner,default_class,config_json,created_at,updated_at,visibility) VALUES ('kind-pinned','kind-pinned','owner',?,'{}',?,?,'private')`, realmFetchSeederClass, now, now); err != nil {
		t.Fatal(err)
	}
	_, err = (realmOps{app: a}).EditDeclaration(context.Background(), realmtool.Requester{
		ActorID: owner, ChannelID: "realm-edit-kind", RequestID: "cross-kind-edit",
	}, "kind-pinned", realmtool.DeclSpec{Name: "kind-pinned", Class: realmCrossKindToolClass, Visibility: "private", Config: json.RawMessage(`{}`)})
	var realmErr *channelspec.RealmError
	if !errors.As(err, &realmErr) || realmErr.Code != channelspec.RealmInvalidRequest {
		t.Fatalf("cross-kind edit err=%v", err)
	}
	var class string
	if err := a.db.QueryRow(`SELECT default_class FROM actor_decls WHERE id='kind-pinned'`).Scan(&class); err != nil || class != realmFetchSeederClass {
		t.Fatalf("cross-kind edit persisted class=%q err=%v", class, err)
	}
}

func TestRealmOpsAgentCannotWriteDeclarationRegistry(t *testing.T) {
	a := newBareAppForTest(t)
	snapshot, err := (channelspec.RenderedSnapshot{
		Class: "dormant-agent", Placement: channel.Placement{Kind: channel.PlacementServer},
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	bundle := openTestChannelForTest(t, a, "agent-realm-ops", []channelhost.GenesisDeclaration{{
		DeclID: "requester-decl", Kind: actor.KindAgent, Rendered: snapshot,
	}})
	agent, found, err := declaredInstanceOneForTest(context.Background(), bundle.View(), "requester-decl")
	if err != nil || !found {
		t.Fatalf("resolve requester=(%s,%v,%v)", agent, found, err)
	}
	_, err = (realmOps{app: a}).CreateDeclaration(context.Background(), realmtool.Requester{
		ActorID: agent, ChannelID: "agent-realm-ops", RequestID: "agent-create-request",
	}, realmtool.DeclSpec{Name: "must not persist", Class: "go-kimi", Visibility: "public", Config: json.RawMessage(`{}`)})
	var realmErr *channelspec.RealmError
	if !errors.As(err, &realmErr) || realmErr.Code != channelspec.RealmForbidden {
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

func TestRealmOpsAgentWithEmptyPrincipalOwnsIntroduceByActorCoordinate(t *testing.T) {
	a := newBareAppForTest(t)
	requesterSnapshot, err := (channelspec.RenderedSnapshot{
		Class: "dormant-requester", Placement: channel.Placement{Kind: channel.PlacementServer},
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	bundle := openTestChannelForTest(t, a, "agent-owned-operation", []channelhost.GenesisDeclaration{{
		DeclID: "requester-decl", Kind: actor.KindAgent, Rendered: requesterSnapshot,
	}})
	requester, found, err := declaredInstanceOneForTest(context.Background(), bundle.View(), "requester-decl")
	if err != nil || !found {
		t.Fatalf("requester=%+v found=%v err=%v", requester, found, err)
	}
	if facts, ok, factsErr := bundle.View().ActorFacts(context.Background(), requester); factsErr != nil ||
		!ok || facts.Principal != "" {
		t.Fatalf("requester facts=%+v ok=%v err=%v", facts, ok, factsErr)
	}
	if _, err := bundle.SysOp().AttachDaemon(context.Background(), channelspec.DaemonRequest{Ref: "attach-agent-owner", DaemonID: "daemon-a"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if _, err := a.db.Exec(`INSERT INTO channels(
		id,name,type,status,owner_principal,spec_json,created_at,parent_id)
		VALUES ('agent-owned-operation','agent-owned-operation','group','present','owner','{}',?,NULL)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := a.db.Exec(`INSERT INTO actor_decls(id,name,owner,default_class,created_at,updated_at,visibility) VALUES ('public-target','public-target','owner',?,?,?,'public')`, realmFetchSeederClass, now, now); err != nil {
		t.Fatal(err)
	}
	result, err := (realmOps{app: a}).Introduce(context.Background(), realmtool.Requester{
		ActorID: requester, ChannelID: "agent-owned-operation", RequestID: "agent-introduce",
	}, "public-target", realmtool.IntroduceOpts{})
	if err != nil || !result.Created || result.ActorID == "" {
		t.Fatalf("agent introduce=(%+v,%v)", result, err)
	}
	replayed, err := (realmOps{app: a}).Introduce(context.Background(), realmtool.Requester{
		ActorID: requester, ChannelID: "agent-owned-operation", RequestID: "agent-introduce-replay",
	}, "public-target", realmtool.IntroduceOpts{})
	if err != nil || replayed.Created || replayed.ActorID != result.ActorID {
		t.Fatalf("agent introduce replay=(%+v,%v)", replayed, err)
	}
}

func TestRealmToolDerivedRefIsChannelScoped(t *testing.T) {
	one := realmtool.DerivedRealmToolRef("channel-a", "same-request")
	two := realmtool.DerivedRealmToolRef("channel-b", "same-request")
	if one == two || one == "" || two == "" {
		t.Fatalf("derived refs collide: %q %q", one, two)
	}
	if one != realmtool.DerivedRealmToolRef("channel-a", "same-request") {
		t.Fatal("derived ref is not deterministic")
	}
}

func TestObserverResourceStreamStopsAtChunkBoundaryAfterRealmToolRemoval(t *testing.T) {
	a := newBareAppForTest(t)
	snapshot, err := (channelspec.RenderedSnapshot{
		Class: realmToolClass, Placement: channel.Placement{Kind: channel.PlacementServer},
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	bundle := openTestChannelForTest(t, a, "stream-source", []channelhost.GenesisDeclaration{{
		DeclID: realmToolDeclID, Kind: actor.KindTool, Rendered: snapshot,
	}})
	if _, err := a.db.Exec(`INSERT INTO channels(
		id,name,type,status,owner_principal,spec_json,created_at,parent_id)
		VALUES ('stream-source','stream-source','group','present','owner','{}',?,NULL)`, time.Now().UnixMilli()); err != nil {
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
	target, found, err := declaredInstanceOneForTest(context.Background(), bundle.View(), realmToolDeclID)
	if err != nil || !found {
		t.Fatalf("resolve realm tool target=(%+v,%v,%v)", target, found, err)
	}
	initiator, found, err := bundle.View().ResolvePrincipal(context.Background(), "owner")
	if err != nil || !found {
		t.Fatalf("resolve owner=(%s,%v,%v)", initiator, found, err)
	}
	if _, err := bundle.SysOp().Remove(context.Background(), channelspec.RemoveRequest{
		Ref: "stream-remove", Target: target, InitiatorActorID: initiator,
	}); err != nil {
		t.Fatal(err)
	}
	n, err = body.Read(buffer)
	var realmErr *channelspec.RealmError
	if n != 0 || !errors.As(err, &realmErr) || realmErr.Code != channelspec.RealmCapabilityUnavailable {
		t.Fatalf("post-revoke chunk n=%d err=%v", n, err)
	}
}

func TestRealmOpsFetchAllowsAgentWithZeroSourceMembership(t *testing.T) {
	a := newBareAppForTest(t)
	seal := func(class string) channelspec.RenderedSnapshot {
		t.Helper()
		snapshot, err := (channelspec.RenderedSnapshot{
			Class: class, Placement: channel.Placement{Kind: channel.PlacementServer},
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
		if _, err := a.db.Exec(`INSERT INTO channels(
			id,name,type,status,owner_principal,spec_json,created_at,parent_id)
			VALUES (?,?,'group','present','owner','{}',?,NULL)`, row.id, row.name, time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	sourceOwner, found, err := source.View().ResolvePrincipal(context.Background(), "owner")
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
	requesterRow, found, err := declaredInstanceOneForTest(context.Background(), source.View(), "fetch-requester")
	if err != nil || !found {
		t.Fatalf("source requester=(%s,%v,%v)", requesterRow, found, err)
	}
	fetched, err := (realmOps{app: a}).FetchResource(context.Background(), realmtool.Requester{
		ActorID: requesterRow, ChannelID: "agent-fetch-source", RequestID: "fetch-as-agent",
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

func TestRealmCopyLimitIsEnforcedByRealmStream(t *testing.T) {
	body := newRealmCopyPolicyBody(io.NopCloser(bytes.NewReader([]byte("12345"))), 4)
	defer body.Close()
	_, err := io.ReadAll(body)
	var realmErr *channelspec.RealmError
	if !errors.As(err, &realmErr) || realmErr.Code != channelspec.RealmInvalidRequest {
		t.Fatalf("oversized realm stream err=%v, want invalid_request", err)
	}

	exact := newRealmCopyPolicyBody(io.NopCloser(bytes.NewReader([]byte("1234"))), 4)
	defer exact.Close()
	got, err := io.ReadAll(exact)
	if err != nil || string(got) != "1234" {
		t.Fatalf("exact-limit realm stream=%q err=%v", got, err)
	}
}
