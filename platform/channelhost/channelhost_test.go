package channelhost

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type testResolver struct{ daemonDeleted bool }

func (testResolver) BuildClass(channel.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}

func (testResolver) ResolveDeclaration(context.Context, channel.ID, string) (channel.DeclarationFacts, error) {
	return channel.DeclarationFacts{}, channel.ErrDeclarationNotFound
}
func (testResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return "", false, nil
}
func (r testResolver) DaemonFacts(context.Context, string) (channel.DaemonFacts, error) {
	return channel.DaemonFacts{Deleted: r.daemonDeleted}, nil
}

func TestOpenFirstSweepDetachesPersistedTombstonedDaemon(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	liveResolver := testResolver{}
	host, err := New(root, HomeDeps{CompositionResolver: liveResolver, IntroductionResolver: liveResolver})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := (channel.RenderedSnapshot{
		Class: "test-agent", Placement: channel.Placement{Kind: channel.PlacementDaemon, DesiredHost: "daemon-a"},
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	spec := provisionSpec("offline-daemon")
	spec.GenesisDeclarations = []GenesisDeclaration{{DeclID: "decl-a", Kind: actor.KindAgent, Rendered: snapshot}}
	if _, err := host.Provision(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if err := host.Open(ctx, OpenSpec{ChannelID: spec.ChannelID, ExpectedType: spec.Type}); err != nil {
		t.Fatal(err)
	}
	bundle, ok := host.Acquire(spec.ChannelID)
	if !ok {
		t.Fatal("initial channel not serving")
	}
	if _, err := bundle.SysOp().AttachDaemon(ctx, channel.DaemonRequest{Ref: "offline:attach", DaemonID: "daemon-a"}); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	deletedResolver := testResolver{daemonDeleted: true}
	reopened, err := New(root, HomeDeps{CompositionResolver: deletedResolver, IntroductionResolver: deletedResolver})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Open(ctx, OpenSpec{ChannelID: spec.ChannelID, ExpectedType: spec.Type}); err != nil {
		t.Fatal(err)
	}
	bundle, ok = reopened.Acquire(spec.ChannelID)
	if !ok {
		t.Fatal("reopened channel not serving")
	}
	if bound, err := bundle.View().IsBound(ctx, "daemon-a"); err != nil || bound {
		t.Fatalf("first sweep binding=(%v,%v), want detached", bound, err)
	}
	if _, found, err := bundle.View().DeclaredBySourceOne(ctx, "decl-a"); err != nil || found {
		t.Fatalf("daemon-placed actor survived first sweep: found=%v err=%v", found, err)
	}
}

func newTestHost(t *testing.T) *ChannelHost {
	t.Helper()
	host, err := New(t.TempDir(), HomeDeps{CompositionResolver: testResolver{}, IntroductionResolver: testResolver{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close() })
	return host
}

func provisionSpec(id channel.ID) ProvisionSpec {
	return ProvisionSpec{ChannelID: id, Type: "group", OwnerPrincipal: "owner", CreatedAt: time.Now().UnixMilli()}
}

func TestLifecycleTombstoneAndCensus(t *testing.T) {
	ctx := context.Background()
	host := newTestHost(t)
	id := channel.ID("opaque/频道?id=1")
	if _, err := host.Provision(ctx, provisionSpec(id)); err != nil {
		t.Fatal(err)
	}
	assertCensus(t, host, id, CensusPresent)
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ExpectedType: "group"}); err != nil {
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
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ExpectedType: "group"}); !errors.Is(err, ErrChannelNotFound) {
		t.Fatalf("Open retired=%v", err)
	}
	if err := host.Destroy(ctx, id); err != nil {
		t.Fatalf("repeated destroy: %v", err)
	}
	if _, err := host.Provision(ctx, provisionSpec(id)); !errors.Is(err, ErrChannelRetired) {
		t.Fatalf("retired id reprovision=%v", err)
	}
}

func TestOpenRejectsTypeOwnerAndCopiedIdentity(t *testing.T) {
	ctx := context.Background()
	host := newTestHost(t)
	id := channel.ID("source")
	if _, err := host.Provision(ctx, provisionSpec(id)); err != nil {
		t.Fatal(err)
	}
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ExpectedType: "other"}); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("wrong type err=%v", err)
	}
	main, _, _ := host.paths(id)
	db, err := sql.Open("sqlite", main)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE channel_genesis SET owner_principal='forged'`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ExpectedType: "group"}); !errors.Is(err, ErrOwnerInvariant) {
		t.Fatalf("owner mismatch err=%v", err)
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
	if err := host.Open(ctx, OpenSpec{ChannelID: copyID, ExpectedType: "group"}); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("copied identity err=%v", err)
	}
}

func TestDestroyNoReplacePreservesExistingTombstone(t *testing.T) {
	ctx := context.Background()
	host := newTestHost(t)
	id := channel.ID("no-replace")
	if _, err := host.Provision(ctx, provisionSpec(id)); err != nil {
		t.Fatal(err)
	}
	if err := host.Open(ctx, OpenSpec{ChannelID: id, ExpectedType: "group"}); err != nil {
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
	snapshot, err := (channel.RenderedSnapshot{
		Class: "test-agent", Config: json.RawMessage(`{"v":1}`),
		Placement: channel.Placement{Kind: channel.PlacementServer},
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	spec := provisionSpec("child")
	spec.Origin = &Origin{ParentChannelID: "parent", InitiatorPrincipal: "owner"}
	spec.GenesisDeclarations = []GenesisDeclaration{{DeclID: "decl", Kind: actor.KindAgent, Rendered: snapshot}}
	if _, err := host.Provision(ctx, spec); err != nil {
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
	raw, err := json.Marshal(GenesisDeclaration{DeclID: "decl", Kind: actor.KindAgent})
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
