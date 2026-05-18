package cli

// dedupe_test.go covers M1.6-T5 phase-4 publish duplicate-publish
// guard. The matrix:
//
//	1. COAGENT_CHANNEL_DB unset → skip path (return false, no error)
//	2. note_id empty             → skip path
//	3. db missing                → soft pass, stderr warning
//	4. no matching row           → no duplicate
//	5. matching row              → duplicate detected
//	6. matching row but status="failed"        → not a duplicate
//	7. matching row but kind="request"         → not a duplicate
//	8. matching row but is_terminal=0          → not a duplicate
//
// We seed a tiny in-memory-style sqlite (real on-disk temp file
// because modernc.org/sqlite doesn't ship a true in-memory mode that
// survives multiple connections) so the query is exercised end-to-end.

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// minimalMessagesDDL is a stripped subset of runtime/store/schema.go
// covering ONLY the columns this dedupe query reads. Pulling in the
// full ChannelLocalDDL would introduce a hard dependency back on
// runtime/store; the dedupe layer doesn't need it.
const minimalMessagesDDL = `
CREATE TABLE IF NOT EXISTS messages (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT,
  id           TEXT NOT NULL UNIQUE,
  ts           INTEGER NOT NULL,
  ts_received  INTEGER NOT NULL,
  channel_id   TEXT NOT NULL,
  sender_kind  TEXT NOT NULL,
  sender_id    TEXT NOT NULL,
  kind         TEXT NOT NULL,
  type         TEXT NOT NULL,
  payload      TEXT NOT NULL,
  parent_id    TEXT,
  correlation_id TEXT,
  visibility   TEXT NOT NULL,
  audience     TEXT NOT NULL,
  is_terminal  INTEGER NOT NULL DEFAULT 0
);`

// seedChannelDB writes a sqlite file at <dir>/channel.sqlite with the
// supplied rows. Returns the abs path.
func seedChannelDB(t *testing.T, rows []map[string]any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "channel.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(t.Context(), minimalMessagesDDL); err != nil {
		t.Fatalf("DDL: %v", err)
	}
	for i, r := range rows {
		_, err := db.ExecContext(t.Context(), `
INSERT INTO messages (id, ts, ts_received, channel_id, sender_kind, sender_id,
                     kind, type, payload, parent_id, correlation_id,
                     visibility, audience, is_terminal)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			r["id"], 0, 0, "ch-1", "tool", "tool:xhs-adapter",
			r["kind"], r["type"], r["payload"],
			r["parent_id"], r["correlation_id"], "public", "[]", r["is_terminal"],
		)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
	return path
}

func TestDedupePublish_SkipWhenEnvUnset(t *testing.T) {
	t.Setenv(EnvChannelDB, "")
	matched, err := dedupePublish(t.Context(), "n-1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if matched {
		t.Errorf("expected skip (matched=false) when env unset")
	}
}

func TestDedupePublish_SkipWhenNoteIDEmpty(t *testing.T) {
	path := seedChannelDB(t, nil)
	t.Setenv(EnvChannelDB, path)
	matched, err := dedupePublish(t.Context(), "")
	if err != nil || matched {
		t.Errorf("matched=%v err=%v want false/nil", matched, err)
	}
}

func TestDedupePublish_SoftSkipWhenDBMissing(t *testing.T) {
	t.Setenv(EnvChannelDB, "/nope/does/not/exist.sqlite")
	matched, err := dedupePublish(t.Context(), "n-1")
	if err != nil {
		t.Errorf("expected soft skip nil err, got %v", err)
	}
	if matched {
		t.Errorf("missing db must not report duplicate")
	}
}

func TestDedupePublish_NoMatch(t *testing.T) {
	path := seedChannelDB(t, []map[string]any{
		{
			"id": "m-1", "kind": "response", "type": "xhs.publish",
			"payload":     `{"status":"completed","note_id":"OTHER"}`,
			"is_terminal": 1, "parent_id": "req-1", "correlation_id": "req-1",
		},
	})
	t.Setenv(EnvChannelDB, path)
	matched, err := dedupePublish(t.Context(), "n-1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if matched {
		t.Errorf("matched=true want false")
	}
}

func TestDedupePublish_MatchesTerminalCompleted(t *testing.T) {
	path := seedChannelDB(t, []map[string]any{
		{
			"id": "m-1", "kind": "response", "type": "xhs.publish",
			"payload":     `{"status":"completed","note_id":"n-1","url":"https://x"}`,
			"is_terminal": 1, "parent_id": "req-1", "correlation_id": "req-1",
		},
	})
	t.Setenv(EnvChannelDB, path)
	matched, err := dedupePublish(t.Context(), "n-1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !matched {
		t.Errorf("matched=false want true (terminal completed publish for n-1 exists)")
	}
}

func TestDedupePublish_FailedStatusNotDuplicate(t *testing.T) {
	path := seedChannelDB(t, []map[string]any{
		{
			"id": "m-1", "kind": "response", "type": "xhs.publish",
			"payload":     `{"status":"failed","note_id":"n-1"}`,
			"is_terminal": 1, "parent_id": "req-1", "correlation_id": "req-1",
		},
	})
	t.Setenv(EnvChannelDB, path)
	matched, _ := dedupePublish(t.Context(), "n-1")
	if matched {
		t.Errorf("failed publish must NOT count as duplicate")
	}
}

func TestDedupePublish_RequestKindNotDuplicate(t *testing.T) {
	// A pending request for n-1 (kind=request, no terminal response yet)
	// must not be treated as a duplicate — the dedupe surface only
	// rejects after a TERMINAL completed response exists.
	path := seedChannelDB(t, []map[string]any{
		{
			"id": "m-1", "kind": "request", "type": "xhs.publish",
			"payload":     `{"note_id":"n-1","title":"t"}`,
			"is_terminal": 0, "parent_id": nil, "correlation_id": "m-1",
		},
	})
	t.Setenv(EnvChannelDB, path)
	matched, _ := dedupePublish(t.Context(), "n-1")
	if matched {
		t.Errorf("kind=request must NOT count as duplicate")
	}
}

func TestDedupePublish_NonTerminalNotDuplicate(t *testing.T) {
	path := seedChannelDB(t, []map[string]any{
		{
			"id": "m-1", "kind": "response", "type": "xhs.publish",
			"payload":     `{"status":"completed","note_id":"n-1"}`,
			"is_terminal": 0, "parent_id": "req-1", "correlation_id": "req-1",
		},
	})
	t.Setenv(EnvChannelDB, path)
	matched, _ := dedupePublish(t.Context(), "n-1")
	if matched {
		t.Errorf("is_terminal=0 must NOT count as duplicate")
	}
}

// Compile-time sanity that the unused context import doesn't drift.
var _ = context.Background
