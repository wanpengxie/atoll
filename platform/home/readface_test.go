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

func TestActorControlIndexRejectsInvalidAndDuplicateOwnersAtomically(t *testing.T) {
	owner := func(id actor.ActorID) controlEntry {
		entry := testControlEntry(id, storespec.WorldDurable, nil)
		entry.Row.Kind = actor.KindHuman
		entry.Row.Sponsor = actor.SystemActorID
		entry.Row.Role = storespec.RoleOwner
		return entry
	}
	invalid := []controlEntry{
		func() controlEntry { e := owner("run-owner"); e.World = storespec.WorldRun; return e }(),
		func() controlEntry { e := owner("agent-owner"); e.Row.Kind = actor.KindAgent; return e }(),
		func() controlEntry { e := owner("foreign-owner"); e.Row.Sponsor = "human:sponsor"; return e }(),
	}
	for _, entry := range invalid {
		idx := newActorControlIndex()
		if idx.ReplaceAll([]controlEntry{entry}) || idx.UpsertBatch([]controlEntry{entry}) {
			t.Fatalf("accepted invalid owner %+v", entry)
		}
	}

	idx := newActorControlIndex()
	first := owner("owner-a")
	if !idx.ReplaceAll([]controlEntry{first}) {
		t.Fatal("first owner rejected")
	}
	if idx.UpsertBatch([]controlEntry{owner("owner-b")}) {
		t.Fatal("second owner accepted")
	}
	if _, ok, _ := idx.LookupActive(context.Background(), first.Row.ID); !ok {
		t.Fatal("failed upsert mutated the old image")
	}
	if _, ok, _ := idx.LookupActive(context.Background(), "owner-b"); ok {
		t.Fatal("failed upsert partially published second owner")
	}
	downgrade := first
	downgrade.Row.Role = storespec.RoleNone
	if idx.UpsertBatch([]controlEntry{downgrade}) {
		t.Fatal("owner role downgrade accepted")
	}
}
