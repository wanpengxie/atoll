package channelhost

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type testResolver struct {
	declaration     channelspec.DeclarationFacts
	declarationLive bool
}

type testBindings struct{}

func (testBindings) IsBound(context.Context, channel.ID, string) (bool, error) { return false, nil }
func (testBindings) ListBoundDeviceIDs(context.Context, channel.ID) ([]string, error) {
	return nil, nil
}
func (testBindings) ListChannels(context.Context) ([]regspec.ChannelRow, error) { return nil, nil }
func (testBindings) GetChannelDesired(context.Context, channel.ID) (regspec.ChannelRow, bool, error) {
	return regspec.ChannelRow{}, false, nil
}

func (testResolver) BuildClass(channel.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}

func (r testResolver) ResolveDeclaration(context.Context, channel.ID, string) (channelspec.DeclarationFacts, error) {
	if !r.declarationLive {
		return channelspec.DeclarationFacts{}, channelspec.ErrDeclarationNotFound
	}
	return r.declaration, nil
}

func TestOpenFirstSweepPullsLatestDeclaration(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	liveResolver := testResolver{declarationLive: true, declaration: channelspec.DeclarationFacts{Class: "test-agent", Config: json.RawMessage(`{"value":"a"}`)}}
	host, err := New(root, testBindings{}, HomeDeps{CompositionResolver: liveResolver, IntroductionResolver: liveResolver, RegistryBindings: testBindings{}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := (channelspec.RenderedSnapshot{
		Class: "test-agent", Config: json.RawMessage(`{"value":"a"}`), Placement: channel.Placement{Kind: channel.PlacementServer},
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	spec := genesisSpec("offline-declaration")
	spec.Declarations = []lagoon.GenesisDeclaration{{DeclID: "decl-a", Kind: actor.KindAgent, Rendered: snapshot}}
	if err := host.provisionGenesis(ctx, spec, "c0.test"); err != nil {
		t.Fatal(err)
	}
	if err := host.Open(ctx, OpenSpec{ChannelID: spec.ChannelID, ChannelName: "c0.test", ExpectedType: spec.Type}); err != nil {
		t.Fatal(err)
	}
	initial, ok := host.Acquire(spec.ChannelID)
	if !ok {
		t.Fatal("provisioned channel not serving")
	}
	initialRows, err := initial.View().DeclaredInstances(ctx, "decl-a")
	if err != nil || len(initialRows) != 1 {
		t.Fatalf("equal first sweep double-wrote genesis: instances=%+v err=%v", initialRows, err)
	}
	if err := host.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	latestResolver := testResolver{declarationLive: true, declaration: channelspec.DeclarationFacts{Class: "test-agent", Config: json.RawMessage(`{"value":"b"}`)}}
	reopened, err := New(root, testBindings{}, HomeDeps{CompositionResolver: latestResolver, IntroductionResolver: latestResolver, RegistryBindings: testBindings{}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	if err := reopened.Open(ctx, OpenSpec{ChannelID: spec.ChannelID, ChannelName: "c0.test", ExpectedType: spec.Type}); err != nil {
		t.Fatal(err)
	}
	bundle, ok := reopened.Acquire(spec.ChannelID)
	if !ok {
		t.Fatal("reopened channel not serving")
	}
	// The reopened channel keeps exactly one instance of the declaration: the
	// first sweep re-applies the latest definition onto the same record instead
	// of introducing a second one. What the definition now says is the
	// Controller's own reconcile projection, not a business-membrane fact.
	rows, err := bundle.View().DeclaredInstances(ctx, "decl-a")
	if err != nil || len(rows) != 1 || rows[0] != initialRows[0] {
		t.Fatalf("first-sweep declaration=(%+v,%v)", rows, err)
	}
}
func (testResolver) ClassKind(_ context.Context, class string) (actor.Kind, bool, error) {
	if class == "test-agent" {
		return actor.KindAgent, true, nil
	}
	return "", false, nil
}
func newTestHost(t *testing.T) *ChannelHost {
	t.Helper()
	host, err := New(t.TempDir(), testBindings{}, HomeDeps{CompositionResolver: testResolver{}, IntroductionResolver: testResolver{}, RegistryBindings: testBindings{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close(context.Background()) })
	return host
}

func TestViewAdapterForwardsObservationRoster(t *testing.T) {
	ctx := context.Background()
	host := newTestHost(t)
	id := channel.ID("obs-roster")
	if err := host.provisionGenesis(ctx, genesisSpec(id), "c0.obs-roster"); err != nil {
		t.Fatal(err)
	}
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ChannelName: "c0.obs-roster", ExpectedType: "group"}); err != nil {
		t.Fatal(err)
	}
	bundle, serving := host.Acquire(id)
	if !serving {
		t.Fatal("opened channel is not serving")
	}
	rows, err := bundle.View().Roster(ctx)
	if err != nil {
		t.Fatal(err)
	}
	foundSystem := false
	for _, row := range rows {
		if row.ID == actor.SystemActorID && row.Kind == actor.KindSystem {
			foundSystem = true
		}
	}
	if !foundSystem {
		t.Fatalf("forwarded roster omitted system actor: %+v", rows)
	}
}

func TestMembraneUnregisterPrecedesHomeQuiesce(t *testing.T) {
	ctx := context.Background()
	var opened Bundle
	var host *ChannelHost
	callback := make(chan error, 1)
	host, err := New(t.TempDir(), testBindings{}, HomeDeps{
		CompositionResolver: testResolver{}, IntroductionResolver: testResolver{},
		RegistryBindings: testBindings{},
		OnMembraneClose: func(id channel.ID, _ uint64) {
			if _, visible := host.Acquire(id); visible {
				callback <- errors.New("closing membrane remained published")
				return
			}
			_, _, err := opened.View().OwnerPrincipal(ctx)
			callback <- err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close(context.Background())
	id := channel.ID("staged-close")
	if err := host.provisionGenesis(ctx, genesisSpec(id), "c0.test"); err != nil {
		t.Fatal(err)
	}
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ChannelName: "c0.test", ExpectedType: "group"}); err != nil {
		t.Fatal(err)
	}
	opened, _ = host.Acquire(id)
	if err := host.Destroy(ctx, id); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-callback:
		if err != nil {
			t.Fatalf("membrane close ordering: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("membrane close callback did not run")
	}
}

func genesisSpec(id channel.ID) lagoon.GenesisSpec {
	return lagoon.GenesisSpec{ChannelID: id, Type: "group", OwnerPrincipal: "owner", CreatedAt: time.Now().UnixMilli()}
}

func TestLifecycleTombstoneAndCensus(t *testing.T) {
	ctx := context.Background()
	host := newTestHost(t)
	id := channel.ID("opaque/频道?id=1")
	if err := host.provisionGenesis(ctx, genesisSpec(id), "c0.test"); err != nil {
		t.Fatal(err)
	}
	assertCensus(t, host, id, CensusPresent)
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ChannelName: "c0.test", ExpectedType: "group"}); err != nil {
		t.Fatal(err)
	}
	bundle, ok := host.Acquire(id)
	if !ok || bundle.Generation() == 0 {
		t.Fatal("open channel was not acquirable with a generation")
	}
	owner, found, err := bundle.View().OwnerPrincipal(ctx)
	if err != nil || !found || owner != "owner" {
		t.Fatalf("owner=(%q,%v,%v)", owner, found, err)
	}
	assertCensus(t, host, id, CensusOpen)
	if err := host.Destroy(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, ok := host.Acquire(id); ok {
		t.Fatal("destroyed channel remained acquirable")
	}
	if entries, err := host.Census(ctx); err != nil || len(entries) != 0 {
		t.Fatalf("post-destroy census=%v err=%v", entries, err)
	}
	main, tombstone, _ := host.paths(id)
	if _, err := os.Stat(tombstone); err != nil {
		t.Fatalf("tombstone absent: %v", err)
	}
	if _, err := os.Stat(main); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("live db survived tombstone: %v", err)
	}
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ChannelName: "c0.test", ExpectedType: "group"}); !errors.Is(err, ErrChannelNotFound) {
		t.Fatalf("Open retired=%v", err)
	}
	if err := host.Destroy(ctx, id); err != nil {
		t.Fatalf("repeated destroy: %v", err)
	}
	if err := host.provisionGenesis(ctx, genesisSpec(id), "c0.test"); !errors.Is(err, ErrChannelRetired) {
		t.Fatalf("retired id reprovision=%v", err)
	}
}

func TestCensusDoesNotSeeSiblingRegistryDatabase(t *testing.T) {
	parent := t.TempDir()
	channelRoot := filepath.Join(parent, "channels")
	host, err := New(channelRoot, testBindings{}, HomeDeps{CompositionResolver: testResolver{}, IntroductionResolver: testResolver{}, RegistryBindings: testBindings{}})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close(context.Background())
	registryPath := filepath.Join(parent, "registry.db")
	const registryBytes = "registry-sentinel"
	if err := os.WriteFile(registryPath, []byte(registryBytes), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := host.Census(context.Background())
	if err != nil || len(entries) != 0 {
		t.Fatalf("census included sibling registry: entries=%v err=%v", entries, err)
	}
	got, err := os.ReadFile(registryPath)
	if err != nil || string(got) != registryBytes {
		t.Fatalf("census touched sibling registry: bytes=%q err=%v", got, err)
	}
}

func TestOpenRejectsTypeOwnerAndCopiedIdentity(t *testing.T) {
	ctx := context.Background()
	host := newTestHost(t)
	id := channel.ID("source")
	if err := host.provisionGenesis(ctx, genesisSpec(id), "c0.test"); err != nil {
		t.Fatal(err)
	}
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ChannelName: "c0.test", ExpectedType: "other"}); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("wrong type err=%v", err)
	}
	main, _, _ := host.paths(id)
	db, err := sql.Open("sqlite", main)
	if err != nil {
		t.Fatal(err)
	}
	// Owner lives only on the genesis pointer, so the open check degenerates to
	// "the pointer is present". Emptying it is the one failure left.
	if _, err := db.Exec(`UPDATE channel_genesis SET owner_principal=''`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ChannelName: "c0.test", ExpectedType: "group"}); !errors.Is(err, ErrOwnerInvariant) {
		t.Fatalf("missing owner err=%v", err)
	}

	copyID := channel.ID("copy")
	copyMain, _, _ := host.paths(copyID)
	raw, err := os.ReadFile(main)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(copyMain, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := host.Open(ctx, OpenSpec{ChannelID: copyID, ChannelName: "c0.test", ExpectedType: "group"}); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("copied identity err=%v", err)
	}
}

func TestDestroyNoReplacePreservesExistingTombstone(t *testing.T) {
	ctx := context.Background()
	host := newTestHost(t)
	id := channel.ID("no-replace")
	if err := host.provisionGenesis(ctx, genesisSpec(id), "c0.test"); err != nil {
		t.Fatal(err)
	}
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ChannelName: "c0.test", ExpectedType: "group"}); err != nil {
		t.Fatal(err)
	}
	_, tombstone, _ := host.paths(id)
	const sentinel = "do-not-overwrite"
	if err := os.WriteFile(tombstone, []byte(sentinel), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := host.Destroy(ctx, id); !errors.Is(err, ErrTombstoneExists) {
		t.Fatalf("Destroy err=%v", err)
	}
	got, err := os.ReadFile(tombstone)
	if err != nil || string(got) != sentinel {
		t.Fatalf("existing tombstone overwritten: %q err=%v", got, err)
	}
	if _, ok := host.Acquire(id); ok {
		t.Fatal("failed file stage reopened sealed channel")
	}
}

func TestGenesisOriginAndRenderedDeclaration(t *testing.T) {
	ctx := context.Background()
	host := newTestHost(t)
	snapshot, err := (channelspec.RenderedSnapshot{
		Class: "test-agent", Config: json.RawMessage(`{"v":1}`),
		Placement: channel.Placement{Kind: channel.PlacementServer},
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	spec := genesisSpec("child")
	spec.ParentID = "parent"
	spec.InitiatorPrincipal = "owner"
	spec.Declarations = []lagoon.GenesisDeclaration{{DeclID: "decl", Kind: actor.KindAgent, Rendered: snapshot}}
	if err := host.provisionGenesis(ctx, spec, "c0.test"); err != nil {
		t.Fatal(err)
	}
	main, _, _ := host.paths("child")
	db, err := sql.Open("sqlite", main)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var parent, initiator string
	if err := db.QueryRow(`SELECT parent_channel_id,initiator_principal FROM channel_genesis`).Scan(&parent, &initiator); err != nil {
		t.Fatal(err)
	}
	if parent != "parent" || initiator != "owner" {
		t.Fatalf("origin=(%q,%q)", parent, initiator)
	}
}

func TestGenesisDeclarationWireShapeHasNoPrincipal(t *testing.T) {
	raw, err := json.Marshal(lagoon.GenesisDeclaration{DeclID: "decl", Kind: actor.KindAgent})
	if err != nil {
		t.Fatal(err)
	}
	var shape map[string]json.RawMessage
	if err := json.Unmarshal(raw, &shape); err != nil {
		t.Fatal(err)
	}
	if _, exists := shape["principal"]; exists {
		t.Fatalf("declaration provenance leaked a login principal: %s", raw)
	}
}

func assertCensus(t *testing.T, host *ChannelHost, id channel.ID, state CensusState) {
	t.Helper()
	entries, err := host.Census(context.Background())
	if err != nil || len(entries) != 1 || entries[0].ChannelID != id || entries[0].State != state {
		t.Fatalf("census=%v err=%v want %s/%s", entries, err, id, state)
	}
}
