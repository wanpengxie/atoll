package actorstore_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/actorstore"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func openStore(t *testing.T) (*actorstore.Store, *runtime.ChannelStores) {
	t.Helper()
	ctx := context.Background()
	cs, err := runtime.OpenChannel(ctx, "C-actorstore",
		filepath.Join(t.TempDir(), "channel.sqlite"), runtime.OpenChannelOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	store, err := actorstore.New(cs.Actors, func() int64 { return 1000 })
	if err != nil {
		t.Fatal(err)
	}
	return store, cs
}

func declaredDraft(decl string) storespec.ActorDraft {
	return storespec.ActorDraft{
		Kind: actor.KindAgent, SourceDeclID: decl, CreatedAt: 1,
		Definition: storespec.ActorDefinition{Class: "agent", Config: []byte(`{"n":1}`)},
		Placement:  storespec.NewServerPlacement(),
	}
}

// The declaration birth is one transaction bed: a retry recovers the same id by
// semantic key instead of minting a second record.
func TestInsertReplaysBySemanticKey(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)

	first, err := store.Insert(ctx, declaredDraft("decl:a"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Insert(ctx, declaredDraft("decl:a"))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("replayed birth minted a second id: %q vs %q", first.ID, second.ID)
	}
}

// Records handed out are always deep copies: a caller cannot reach into the
// store through a retained Config slice.
func TestRecordHandoffIsAlwaysACopy(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)

	record, err := store.Insert(ctx, declaredDraft("decl:a"))
	if err != nil {
		t.Fatal(err)
	}
	record.Definition.Config[2] = 'X'
	again, ok, err := store.Lookup(ctx, record.ID)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if string(again.Definition.Config) != `{"n":1}` {
		t.Fatalf("store config was mutated through a handed-out slice: %s", again.Definition.Config)
	}
}

// The entry table is the whole classification fact: IsEntry is true only for
// records installed into it, and the restore path never rebuilds it.
func TestEntryTableIsTheWholeClassificationFact(t *testing.T) {
	ctx := context.Background()
	store, cs := openStore(t)

	durable, err := store.Insert(ctx, declaredDraft("decl:a"))
	if err != nil {
		t.Fatal(err)
	}
	entry := storespec.ActorRecord{
		ID: "agent:parent/worker-1", Kind: actor.KindAgent, CreatedAt: 1,
		Definition: storespec.ActorDefinition{Class: "worker"},
		Placement:  storespec.NewServerPlacement(),
	}
	store.InstallEntry(entry)

	if isEntry, found, err := store.IsEntry(ctx, durable.ID); err != nil || !found || isEntry {
		t.Fatalf("durable record: entry=%v found=%v err=%v", isEntry, found, err)
	}
	if isEntry, found, err := store.IsEntry(ctx, entry.ID); err != nil || !found || !isEntry {
		t.Fatalf("entry record: entry=%v found=%v err=%v", isEntry, found, err)
	}
	if _, found, err := store.IsEntry(ctx, "agent:nobody"); err != nil || found {
		t.Fatalf("unknown id: found=%v err=%v", found, err)
	}

	// A fresh process (fresh store over the same durable registry) restores the
	// durable side only.
	next, err := actorstore.New(cs.Actors, func() int64 { return 1000 })
	if err != nil {
		t.Fatal(err)
	}
	restored, err := next.RestoreActive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored) != 1 || restored[0].ID != durable.ID {
		t.Fatalf("restore=%+v want only the durable record", restored)
	}
}

// A record verb touches the registry and nothing else; the durable half of a
// terminal must commit before any entry is dropped.
func TestDeregisterIsIdempotentAndEntryScoped(t *testing.T) {
	ctx := context.Background()
	store, cs := openStore(t)

	durable, err := store.Insert(ctx, declaredDraft("decl:a"))
	if err != nil {
		t.Fatal(err)
	}
	entry := storespec.ActorRecord{
		ID: "agent:parent/worker-1", Kind: actor.KindAgent, CreatedAt: 1,
		Definition: storespec.ActorDefinition{Class: "worker"},
		Placement:  storespec.NewServerPlacement(),
	}
	store.InstallEntry(entry)

	// A mixed set is one verdict and one transaction; ids with no durable row
	// are naturally no-ops.
	if err := store.Deregister(ctx, []actor.ActorID{durable.ID, entry.ID, "agent:ghost"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := cs.Actors.LookupActive(ctx, durable.ID); err != nil || ok {
		t.Fatalf("durable row still active: ok=%v err=%v", ok, err)
	}
	if _, found, err := store.IsEntry(ctx, entry.ID); err != nil || found {
		t.Fatalf("entry survived terminal: found=%v err=%v", found, err)
	}
	// Repeating the whole thing is a no-op.
	if err := store.Deregister(ctx, []actor.ActorID{durable.ID, entry.ID}); err != nil {
		t.Fatalf("repeat deregister: %v", err)
	}
}

// failingRegistry is a durable half whose termination transaction always fails.
// It exists to pin the ONE ordering rule of the two-step terminal: the entry
// deletions run only after the durable transaction has committed.
type failingRegistry struct {
	storespec.ActorRegistryStore
	err error
}

func (r failingRegistry) Deregister(context.Context, []actor.ActorID, int64) error {
	return r.err
}

// The durable transaction goes first and the entry table follows it: when the
// transaction fails, NOTHING moves — the entries are still installed, and the
// error is returned as-is.
func TestDurableTerminalFailureLeavesEntriesUntouched(t *testing.T) {
	ctx := context.Background()
	_, cs := openStore(t)

	boom := errors.New("durable terminal refused")
	store, err := actorstore.New(
		failingRegistry{ActorRegistryStore: cs.Actors, err: boom},
		func() int64 { return 1000 })
	if err != nil {
		t.Fatal(err)
	}
	durable, err := store.Insert(ctx, declaredDraft("decl:a"))
	if err != nil {
		t.Fatal(err)
	}
	entry := storespec.ActorRecord{
		ID: "agent:parent/worker-1", Kind: actor.KindAgent, CreatedAt: 1,
		Definition: storespec.ActorDefinition{Class: "worker"},
		Placement:  storespec.NewServerPlacement(),
	}
	store.InstallEntry(entry)

	if err := store.Deregister(ctx, []actor.ActorID{durable.ID, entry.ID}); !errors.Is(err, boom) {
		t.Fatalf("Deregister err=%v, want the durable failure verbatim", err)
	}
	if isEntry, found, err := store.IsEntry(ctx, entry.ID); err != nil || !found || !isEntry {
		t.Fatalf("entry moved on a failed durable transaction: entry=%v found=%v err=%v",
			isEntry, found, err)
	}
	if _, ok, err := cs.Actors.LookupActive(ctx, durable.ID); err != nil || !ok {
		t.Fatalf("durable row moved on a failed terminal: ok=%v err=%v", ok, err)
	}
}

// A record with no durable declaration cannot take a definition change; that is
// an operation verdict, not a species branch.
func TestUpdateDefinitionRefusesAnEntryRecord(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)

	entry := storespec.ActorRecord{
		ID: "agent:parent/worker-1", Kind: actor.KindAgent, CreatedAt: 1,
		Definition: storespec.ActorDefinition{Class: "worker"},
		Placement:  storespec.NewServerPlacement(),
	}
	store.InstallEntry(entry)
	if _, err := store.UpdateDefinition(ctx, entry.ID,
		storespec.ActorDefinition{Class: "other"}); err == nil {
		t.Fatal("an entry record has no declaration to change")
	}

	durable, err := store.Insert(ctx, declaredDraft("decl:a"))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := store.UpdateDefinition(ctx, durable.ID,
		storespec.ActorDefinition{Class: "agent-v2", Config: []byte(`{"n":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Definition.Class != "agent-v2" || string(updated.Definition.Config) != `{"n":2}` {
		t.Fatalf("update returned %+v", updated.Definition)
	}
	// The returned value is authoritative — no read-back needed, but it must
	// match what the registry now holds.
	stored, ok, err := store.Lookup(ctx, durable.ID)
	if err != nil || !ok || !stored.Definition.Equal(updated.Definition) {
		t.Fatalf("stored=%+v updated=%+v ok=%v err=%v", stored, updated, ok, err)
	}
}

// The kernel is a constant, not a member: the registry refuses to give it a
// record at all.
func TestKernelCannotBeInserted(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)

	if _, err := store.Insert(ctx, storespec.ActorDraft{
		ID: actor.SystemActorID, Kind: actor.KindSystem, CreatedAt: 1,
		Definition: storespec.ActorDefinition{Class: "system"},
		Placement:  storespec.NewServerPlacement(),
	}); err == nil {
		t.Fatal("the kernel must never receive an actor record")
	}
}
