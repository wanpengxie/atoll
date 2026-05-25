package kimi

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
)

func TestChannelStoreSnapshotMergesDeclarationCatalog(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() { _ = db.Close() }()

	const ddl = `
CREATE TABLE actor_registry (
  actor_id TEXT PRIMARY KEY,
  actor_kind TEXT NOT NULL,
  actor_binding TEXT,
  display_name TEXT,
  created_at INTEGER NOT NULL,
  deregistered_at INTEGER,
  ready_state TEXT NOT NULL DEFAULT 'unknown',
  ready_reason TEXT NOT NULL DEFAULT 'unknown',
  ready_detail TEXT NOT NULL DEFAULT '{}',
  last_ready_at INTEGER NOT NULL DEFAULT 0,
  last_state_change_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE type_registry (
  type TEXT PRIMARY KEY,
  allowed_kinds TEXT NOT NULL,
  handler_binding TEXT NOT NULL,
  terminal_convention TEXT NOT NULL DEFAULT 'payload_status',
  max_pending_ms INTEGER,
  handler_actor_id TEXT,
  install_status TEXT NOT NULL DEFAULT 'installed'
);
CREATE TABLE adapter_state (
  key TEXT PRIMARY KEY,
  value BLOB NOT NULL,
  updated_at INTEGER NOT NULL
);`
	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("ddl: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO actor_registry
		(actor_id, actor_kind, actor_binding, display_name, created_at,
		 ready_state, ready_reason, ready_detail, last_ready_at, last_state_change_at)
		VALUES ('tool:xhs-adapter', 'tool', 'embedded', 'xhs', 1,
		 'not_ready', 'device_offline', '{"device_state":"offline"}', 1000, 2000)`); err != nil {
		t.Fatalf("insert actor: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO type_registry
		(type, allowed_kinds, handler_binding, max_pending_ms, handler_actor_id, install_status)
		VALUES ('xhs.publish', '["request","response"]', 'embedded', 300000, 'tool:xhs-adapter', 'installed')`); err != nil {
		t.Fatalf("insert type: %v", err)
	}
	catalog := adapter.DeclarationCatalog{
		Name:        "xhs",
		ActorID:     actor.ActorID("tool:xhs-adapter"),
		Description: "XHS automation",
		SkillDoc:    "Publish and inspect notes.",
		Types: map[string]adapter.TypeConventionDoc{
			"xhs.publish": {
				Description:    "Publish a note",
				PayloadExample: json.RawMessage(`{"title":"hello"}`),
				PayloadFields: []adapter.FieldDoc{{
					Name:     "title",
					Required: true,
				}},
				ErrorCodes: []adapter.ErrorDoc{{
					Code:     "publish_timeout",
					Recovery: "Retry later",
				}},
				Notes: "Requires logged-in browser.",
			},
		},
	}
	raw, err := json.Marshal(catalog)
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO adapter_state (key, value, updated_at) VALUES (?, ?, 1)`,
		"adapter:xhs:"+adapter.DeclarationConventionStateKey, raw); err != nil {
		t.Fatalf("insert catalog: %v", err)
	}

	store := &ChannelStore{db: db}
	snapshot, err := store.Snapshot(context.Background(), "ch-test", "xhs-creator")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snapshot.Actors) != 1 || snapshot.Actors[0].Description != "XHS automation" || snapshot.Actors[0].SkillDoc == "" {
		t.Fatalf("actor metadata not merged: %+v", snapshot.Actors)
	}
	if snapshot.Actors[0].Ready || snapshot.Actors[0].ReadyReason != "device_offline" ||
		snapshot.Actors[0].LastReadyAt != 1000 || snapshot.Actors[0].LastStateChangeAt != 2000 {
		t.Fatalf("actor readiness not loaded: %+v", snapshot.Actors[0])
	}
	if len(snapshot.Types) != 1 {
		t.Fatalf("types len=%d want 1", len(snapshot.Types))
	}
	got := snapshot.Types[0]
	if got.Description != "Publish a note" || string(got.PayloadExample) != `{"title":"hello"}` || got.Notes == "" {
		t.Fatalf("type metadata not merged: %+v", got)
	}
	if len(got.PayloadFields) != 1 || got.PayloadFields[0].Name != "title" {
		t.Fatalf("payload fields=%+v", got.PayloadFields)
	}
	if len(got.ErrorCodes) != 1 || got.ErrorCodes[0].Code != "publish_timeout" {
		t.Fatalf("error codes=%+v", got.ErrorCodes)
	}
}
