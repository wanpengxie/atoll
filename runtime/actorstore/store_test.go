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
	cs, err := runtime.OpenChannel(ctx, "C-actorstore", filepath.Join(t.TempDir(), "channel.sqlite"), runtime.OpenChannelOptions{})
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
		Kind: actor.KindAgent, SourceDeclID: decl, Seed: decl, CreatedAt: 1,
		Definition: storespec.ActorDefinition{Class: "agent", Config: []byte(`{"n":1}`)},
		Placement:  storespec.NewServerPlacement(),
	}
}

func TestInsertAllowsMultipleInstancesUnlessSingleton(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)
	first, err := store.Insert(ctx, declaredDraft("a"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Insert(ctx, declaredDraft("a"))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatalf("non-singleton births reused id %q", first.ID)
	}
	if first.SourceDeclID != "a" || second.SourceDeclID != "a" {
		t.Fatalf("source declaration was not retained: %+v %+v", first, second)
	}
}

func TestDurableRecordHandoffIsAlwaysACopy(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)
	record, err := store.Insert(ctx, declaredDraft("copy"))
	if err != nil {
		t.Fatal(err)
	}
	record.Definition.Config[2] = 'X'
	again, ok, err := store.Lookup(ctx, record.ID)
	if err != nil || !ok {
		t.Fatalf("lookup ok=%v err=%v", ok, err)
	}
	if string(again.Definition.Config) != `{"n":1}` {
		t.Fatalf("record handoff aliased config: %s", again.Definition.Config)
	}
}

func TestRestoreAndDeregisterUseOnlyTheDurableRegistry(t *testing.T) {
	ctx := context.Background()
	store, cs := openStore(t)
	record, err := store.Insert(ctx, declaredDraft("restore"))
	if err != nil {
		t.Fatal(err)
	}
	next, err := actorstore.New(cs.Actors, func() int64 { return 1000 })
	if err != nil {
		t.Fatal(err)
	}
	restored, err := next.RestoreActive(ctx)
	if err != nil || len(restored) != 1 || restored[0].ID != record.ID {
		t.Fatalf("restore=%+v err=%v", restored, err)
	}
	if err := store.Deregister(ctx, []actor.ActorID{record.ID, "agent:ghost:1"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Deregister(ctx, []actor.ActorID{record.ID}); err != nil {
		t.Fatalf("repeat deregister: %v", err)
	}
}

type failingRegistry struct {
	storespec.ActorRegistryStore
	err error
}

func (r failingRegistry) Deregister(context.Context, []actor.ActorID, int64) error { return r.err }

func TestDeregisterReturnsTheDurableFailure(t *testing.T) {
	ctx := context.Background()
	_, cs := openStore(t)
	boom := errors.New("durable terminal refused")
	store, err := actorstore.New(failingRegistry{ActorRegistryStore: cs.Actors, err: boom}, func() int64 { return 1000 })
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.Insert(ctx, declaredDraft("failure"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Deregister(ctx, []actor.ActorID{record.ID}); !errors.Is(err, boom) {
		t.Fatalf("Deregister err=%v", err)
	}
	if _, ok, err := cs.Actors.LookupActive(ctx, record.ID); err != nil || !ok {
		t.Fatalf("failed transaction moved row: ok=%v err=%v", ok, err)
	}
}

func TestUpdateDefinitionReturnsTheCommittedValue(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)
	record, err := store.Insert(ctx, declaredDraft("update"))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := store.UpdateDefinition(ctx, record.ID, storespec.ActorDefinition{Class: "agent-v2", Config: []byte(`{"n":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Definition.Class != "agent-v2" || string(updated.Definition.Config) != `{"n":2}` {
		t.Fatalf("updated=%+v", updated)
	}
}

func TestUpdateDefinitionRefusesAHumanRecord(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)
	human, err := store.Insert(ctx, storespec.ActorDraft{
		Kind: actor.KindHuman, Principal: "alice@example.com", CreatedAt: 1,
		Definition: storespec.ActorDefinition{Class: "human"}, Placement: storespec.NewServerPlacement(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdateDefinition(ctx, human.ID, storespec.ActorDefinition{Class: "human-v2"}); !errors.Is(err, storespec.ErrNoDeclaration) {
		t.Fatalf("err=%v, want ErrNoDeclaration", err)
	}
}

func TestKernelCannotBeInserted(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)
	if _, err := store.Insert(ctx, storespec.ActorDraft{
		Kind: actor.KindSystem, CreatedAt: 1,
		Definition: storespec.ActorDefinition{Class: "system"}, Placement: storespec.NewServerPlacement(),
	}); err == nil {
		t.Fatal("the kernel must never receive an actor record")
	}
}
