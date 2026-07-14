package app

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

func TestCompositionStopWorldMigrationRetiresAppSourceAndReopens(t *testing.T) {
	registry.Register("migration-test-agent", registry.ClassDecl{Kind: actor.KindAgent, New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
		return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent}, nil
	}})
	dir := t.TempDir()
	appPath := filepath.Join(dir, "app.sqlite")
	channelPath := filepath.Join(dir, "channel.sqlite")
	db, err := OpenDB(appPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`ALTER TABLE channels ADD COLUMN default_agent TEXT`,
		`CREATE TABLE channel_actors(channel_id TEXT NOT NULL,instance_id TEXT NOT NULL,principal TEXT NOT NULL DEFAULT '',class TEXT NOT NULL,config_json TEXT,placement TEXT NOT NULL DEFAULT 'server',desired_host TEXT NOT NULL DEFAULT '',restart_epoch INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(channel_id,instance_id))`,
		`INSERT INTO users(id,email,password,created_at) VALUES ('u','u@x','x',1)`,
		`INSERT INTO workspaces(id,owner_id,name,created_at) VALUES ('w','u','w',1)`,
		`INSERT INTO channels(id,workspace_id,name,type,db_path,default_agent,created_at) VALUES ('c','w','c','group','` + channelPath + `','legacy-agent',1)`,
		`INSERT INTO actor_decls(id,name,owner,default_class,created_at,updated_at) VALUES ('decl','d','u','migration-test-agent',1,1)`,
		`INSERT INTO channel_actors(channel_id,instance_id,principal,class,placement) VALUES ('c','legacy-agent','decl','migration-test-agent','server')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}
	a, err := New(Config{DB: db, ChannelDBDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	h := a.getHome(channel.ID("c"))
	rows, err := h.Composition(context.Background())
	if err != nil || len(rows) != 1 || rows[0].DeclID != "decl" || !rows[0].IsDefault {
		t.Fatalf("migrated composition = %+v err=%v", rows, err)
	}
	var sourceTables int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='channel_actors'`).Scan(&sourceTables); err != nil || sourceTables != 0 {
		t.Fatalf("retired source tables=%d err=%v", sourceTables, err)
	}
	pragma, err := db.Query(`PRAGMA table_info(channels)`)
	if err != nil {
		t.Fatal(err)
	}
	defer pragma.Close()
	for pragma.Next() {
		var cid, notnull, pk int
		var name, typ string
		var dflt any
		if err := pragma.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatal(err)
		}
		if name == "default_agent" {
			t.Fatal("retired default_agent column survived migration")
		}
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	db, err = OpenDB(appPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a, err = New(Config{DB: db, ChannelDBDir: dir})
	if err != nil {
		t.Fatalf("idempotent reopen: %v", err)
	}
	defer a.Close()
	rows, err = a.getHome("c").Composition(context.Background())
	if err != nil || len(rows) != 1 {
		t.Fatalf("reopen composition = %+v err=%v", rows, err)
	}
}
