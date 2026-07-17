package home

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func testControlEntry(id actor.ActorID, world storespec.ActorWorld, config []byte) controlEntry {
	return controlEntry{Row: storespec.ActorControlRow{
		ID: id, Kind: actor.KindAgent, CurrentDeclVersion: 1,
		Config: config, Placement: storespec.NewServerPlacement(),
	}, World: world}
}

func TestActorControlIndexPublishesWorldWithImmutableRows(t *testing.T) {
	ctx := context.Background()
	idx := newActorControlIndex()
	config := []byte(`{"v":1}`)
	if !idx.ReplaceAll([]controlEntry{testControlEntry("actor:a", storespec.WorldDurable, config)}) {
		t.Fatal("ReplaceAll rejected valid boot image")
	}
	config[5] = '9'

	row, ok, err := idx.LookupActive(ctx, "actor:a")
	if err != nil || !ok || string(row.Config) != `{"v":1}` {
		t.Fatalf("lookup = (%+v,%v,%v)", row, ok, err)
	}
	row.Config[5] = '8'
	row2, _, _ := idx.LookupActive(ctx, "actor:a")
	if string(row2.Config) != `{"v":1}` {
		t.Fatalf("caller mutated authority config: %s", row2.Config)
	}
	world, ok, err := idx.WorldOf(ctx, "actor:a")
	if err != nil || !ok || world != storespec.WorldDurable {
		t.Fatalf("world = (%v,%v,%v)", world, ok, err)
	}
	if verdict, _ := idx.CheckAuthor(ctx, storespec.AuthorStamp{ID: "actor:a", BirthVersion: 2}); verdict != storespec.AuthorVersionStale {
		t.Fatalf("stale verdict = %v", verdict)
	}
}

func TestActorControlIndexBatchPublicationAndValidation(t *testing.T) {
	ctx := context.Background()
	idx := newActorControlIndex()
	if idx.ReplaceAll([]controlEntry{{Row: storespec.ActorControlRow{ID: "bad"}, World: storespec.WorldDurable}}) {
		t.Fatal("accepted invalid boot row")
	}
	if !idx.UpsertBatch([]controlEntry{
		testControlEntry("actor:a", storespec.WorldDurable, nil),
		testControlEntry("actor:a/child-x", storespec.WorldRun, nil),
	}) {
		t.Fatal("UpsertBatch rejected valid rows")
	}
	rows, err := idx.ListActive(ctx)
	if err != nil || len(rows) != 2 {
		t.Fatalf("ListActive len=%d err=%v", len(rows), err)
	}
	idx.DeleteBatch([]actor.ActorID{"actor:a", "actor:a/child-x"})
	if rows, _ := idx.ListActive(ctx); len(rows) != 0 {
		t.Fatalf("DeleteBatch left %d rows", len(rows))
	}
}
