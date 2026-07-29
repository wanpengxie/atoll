package relation

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func testStore(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:relation-test-"+t.Name()+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	for _, ddl := range []string{
		`CREATE TABLE principal_channels (
			principal TEXT NOT NULL, channel_id TEXT NOT NULL, actor_id TEXT NOT NULL,
			updated_at INTEGER NOT NULL, PRIMARY KEY(principal,channel_id))`,
		`CREATE TABLE channel_decl_instances (
			channel_id TEXT NOT NULL, decl_id TEXT NOT NULL, actor_id TEXT NOT NULL,
			updated_at INTEGER NOT NULL, PRIMARY KEY(channel_id,actor_id))`,
		`CREATE TABLE daemon_channels (
			channel_id TEXT NOT NULL, daemon_id TEXT NOT NULL,
			updated_at INTEGER NOT NULL, PRIMARY KEY(channel_id,daemon_id))`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = db.Close() })
	return New(db), db
}

func TestApplyRelationEventClosedSet(t *testing.T) {
	store, db := testStore(t)
	ctx := context.Background()
	chID := channel.ID("c")
	if err := store.Apply(ctx, chID, []channelspec.RelationDelta{
		{Kind: channelspec.RelationJoined, ChannelID: chID, Principal: "alice", ActorID: "human:a"},
		{Kind: channelspec.RelationIntroduced, ChannelID: chID, DeclID: "decl:a", ActorID: "agent:a"},
		{Kind: channelspec.RelationBound, ChannelID: chID, DaemonID: "daemon:a"},
	}); err != nil {
		t.Fatal(err)
	}
	assertCount(t, db, "principal_channels", 1)
	assertCount(t, db, "channel_decl_instances", 1)
	assertCount(t, db, "daemon_channels", 1)

	if err := store.Apply(ctx, chID, []channelspec.RelationDelta{
		{Kind: channelspec.RelationLeft, ChannelID: chID, Principal: "alice", ActorID: "human:a"},
		{Kind: channelspec.RelationInstanceRemoved, ChannelID: chID, DeclID: "decl:a", ActorID: "agent:a"},
		{Kind: channelspec.RelationUnbound, ChannelID: chID, DaemonID: "daemon:a"},
	}); err != nil {
		t.Fatal(err)
	}
	assertCount(t, db, "principal_channels", 0)
	assertCount(t, db, "channel_decl_instances", 0)
	assertCount(t, db, "daemon_channels", 0)

	if err := store.Apply(ctx, chID, []channelspec.RelationDelta{
		{Kind: channelspec.RelationJoined, ChannelID: chID, Principal: "alice", ActorID: "human:new"},
		{Kind: channelspec.RelationGone, ChannelID: chID},
	}); err != nil {
		t.Fatal(err)
	}
	assertCount(t, db, "principal_channels", 0)
}

func TestSnapshotResetAlignsOneChannelOnly(t *testing.T) {
	store, db := testStore(t)
	ctx := context.Background()
	for _, chID := range []channel.ID{"target", "other"} {
		if err := store.Apply(ctx, chID, []channelspec.RelationDelta{
			{Kind: channelspec.RelationJoined, ChannelID: chID, Principal: "stale-" + string(chID), ActorID: "old"},
			{Kind: channelspec.RelationIntroduced, ChannelID: chID, DeclID: "old", ActorID: "old"},
			{Kind: channelspec.RelationBound, ChannelID: chID, DaemonID: "old"},
		}); err != nil {
			t.Fatal(err)
		}
	}
	target := channel.ID("target")
	if err := store.Apply(ctx, target, []channelspec.RelationDelta{
		{ChannelID: target, Reset: true},
		{Kind: channelspec.RelationJoined, ChannelID: target, Principal: "fresh", ActorID: "human:fresh"},
		{Kind: channelspec.RelationIntroduced, ChannelID: target, DeclID: "decl:fresh", ActorID: "agent:fresh"},
		{Kind: channelspec.RelationBound, ChannelID: target, DaemonID: "daemon:fresh"},
	}); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"principal_channels", "channel_decl_instances", "daemon_channels"} {
		var targetRows, otherRows int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table + ` WHERE channel_id='target'`).Scan(&targetRows); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table + ` WHERE channel_id='other'`).Scan(&otherRows); err != nil {
			t.Fatal(err)
		}
		if targetRows != 1 || otherRows != 1 {
			t.Fatalf("%s target=%d other=%d", table, targetRows, otherRows)
		}
	}
}

func TestStaleRelationIsOverGrantUntilDirectedRepair(t *testing.T) {
	store, db := testStore(t)
	ctx := context.Background()
	chID := channel.ID("c")
	if err := store.Apply(ctx, chID, []channelspec.RelationDelta{{
		Kind: channelspec.RelationJoined, ChannelID: chID,
		Principal: "alice", ActorID: "old",
	}}); err != nil {
		t.Fatal(err)
	}
	// A dropped Left event can only leave an extra candidate row. Directed
	// membrane verification removes it; it can never manufacture membership.
	if err := store.ReconcilePrincipal(ctx, chID, "alice", "old", false); err != nil {
		t.Fatal(err)
	}
	assertCount(t, db, "principal_channels", 0)
}

func TestStaleLeftRepairCannotDeleteRenewedActor(t *testing.T) {
	store, db := testStore(t)
	ctx := context.Background()
	chID := channel.ID("c")
	if err := store.Apply(ctx, chID, []channelspec.RelationDelta{
		{Kind: channelspec.RelationJoined, ChannelID: chID, Principal: "alice", ActorID: "old"},
		{Kind: channelspec.RelationJoined, ChannelID: chID, Principal: "alice", ActorID: "new"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcilePrincipal(ctx, chID, "alice", "old", false); err != nil {
		t.Fatal(err)
	}
	var actorID string
	if err := db.QueryRow(`SELECT actor_id FROM principal_channels
		WHERE principal='alice' AND channel_id='c'`).Scan(&actorID); err != nil {
		t.Fatal(err)
	}
	if actorID != "new" {
		t.Fatalf("renewed actor=%q", actorID)
	}
}

func assertCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s rows=%d want %d", table, got, want)
	}
}
