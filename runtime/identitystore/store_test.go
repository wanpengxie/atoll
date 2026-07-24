package identitystore

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type durableFake struct {
	rows map[actor.ActorID]storespec.ActorControlRow
}

func (f durableFake) LookupDeclaredActive(
	_ context.Context,
	id actor.ActorID,
) (storespec.ActorControlRow, bool, error) {
	row, ok := f.rows[id]
	return row, ok, nil
}

func (f durableFake) ListDeclaredActive(context.Context) ([]storespec.ActorControlRow, error) {
	out := make([]storespec.ActorControlRow, 0, len(f.rows))
	for _, row := range f.rows {
		out = append(out, row)
	}
	return out, nil
}

func identityRow(id actor.ActorID) storespec.ActorControlRow {
	return storespec.ActorControlRow{
		ID: id, Kind: actor.KindAgent, Class: "test",
		CurrentDeclVersion: 1, Placement: storespec.NewServerPlacement(),
	}
}

func TestStoreConfinesStorageHome(t *testing.T) {
	ctx := context.Background()
	durable := identityRow("actor:durable")
	memory := identityRow("actor:memory")
	store, err := New(durableFake{
		rows: map[actor.ActorID]storespec.ActorControlRow{durable.ID: durable},
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareMemory(memory)
	if err != nil {
		t.Fatal(err)
	}
	store.PublishMemory(prepared)

	for _, id := range []actor.ActorID{durable.ID, memory.ID} {
		row, found, err := store.LookupActive(ctx, id)
		if err != nil || !found || row.ID != id {
			t.Fatalf("LookupActive(%q)=(%+v,%v,%v)", id, row, found, err)
		}
	}
	restored, err := store.RestoreActive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored) != 1 || restored[0].ID != durable.ID {
		t.Fatalf("RestoreActive=%+v, want durable identity only", restored)
	}
	durableIDs, memoryIDs, err := store.Partition(
		ctx, []actor.ActorID{memory.ID, durable.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(durableIDs) != 1 || durableIDs[0] != durable.ID ||
		len(memoryIDs) != 1 || memoryIDs[0] != memory.ID {
		t.Fatalf("Partition durable=%v memory=%v", durableIDs, memoryIDs)
	}
}

func TestPrepareMemoryRejectsInvalidIdentityBeforeCommit(t *testing.T) {
	if _, err := PrepareMemory(storespec.ActorControlRow{}); err != ErrInvalidIdentity {
		t.Fatalf("PrepareMemory invalid err=%v", err)
	}
}
