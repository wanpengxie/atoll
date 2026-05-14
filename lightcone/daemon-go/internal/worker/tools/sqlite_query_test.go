package tools

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/store"
)

// TestSQLiteQueryTool_SelectHappyPath verifies a real SELECT lands rows
// with the right shape.
func TestSQLiteQueryTool_SelectHappyPath(t *testing.T) {
	t.Parallel()
	db, err := store.OpenChannel(context.Background(),
		filepath.Join(t.TempDir(), "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Seed two rows so the query has something interesting to scan.
	ctx := context.Background()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('alice', 'agent', 'in_worker_bus', 1700000000, NULL),
		        ('bob',   'agent', 'in_worker_bus', 1700000001, NULL)`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tool := NewSQLiteQueryTool(db)
	res, err := tool.Execute(ctx, json.RawMessage(`{"sql":"SELECT actor_id, actor_kind FROM actor_registry ORDER BY actor_id"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.IsError {
		t.Fatalf("unexpected IsError=true, value=%v", res.Value.Value)
	}
	payload, ok := res.Value.Value.(map[string]any)
	if !ok {
		t.Fatalf("value type = %T, want map", res.Value.Value)
	}
	cols, _ := payload["columns"].([]string)
	if len(cols) != 2 || cols[0] != "actor_id" || cols[1] != "actor_kind" {
		t.Fatalf("columns = %v, want [actor_id actor_kind]", cols)
	}
	rows, _ := payload["rows"].([][]any)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0][0] != "alice" {
		t.Fatalf("row 0 actor_id = %v, want alice", rows[0][0])
	}
}

// TestSQLiteQueryTool_RejectsNonSelect verifies non-SELECT statements
// land as IsError with the documented reason.
func TestSQLiteQueryTool_RejectsNonSelect(t *testing.T) {
	t.Parallel()
	db, err := store.OpenChannel(context.Background(),
		filepath.Join(t.TempDir(), "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tool := NewSQLiteQueryTool(db)
	cases := []struct {
		name  string
		sqlIn string
	}{
		{"insert", `INSERT INTO actor_registry VALUES (...)`},
		{"update", `UPDATE actor_registry SET actor_kind='foo'`},
		{"delete", `DELETE FROM actor_registry`},
		{"drop", `DROP TABLE actor_registry`},
		{"pragma", `PRAGMA journal_mode=WAL`},
		{"attach", `ATTACH DATABASE 'evil.db' AS evil`},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			body, _ := json.Marshal(map[string]any{"sql": tc.sqlIn})
			res, err := tool.Execute(context.Background(), body)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !res.IsError {
				t.Fatalf("expected IsError=true for %s", tc.name)
			}
			val := res.Value.Value.(map[string]any)
			if !strings.HasPrefix(val["reason"].(string), "sqlite_query_not_select") {
				t.Fatalf("reason = %v, want sqlite_query_not_select", val["reason"])
			}
		})
	}
}

// TestSQLiteQueryTool_EmptySQL is a separate case because the reason
// code differs from "not select".
func TestSQLiteQueryTool_EmptySQL(t *testing.T) {
	t.Parallel()
	db, err := store.OpenChannel(context.Background(),
		filepath.Join(t.TempDir(), "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tool := NewSQLiteQueryTool(db)
	res, err := tool.Execute(context.Background(), json.RawMessage(`{"sql":""}`))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected IsError=true")
	}
	val := res.Value.Value.(map[string]any)
	if val["reason"] != "sqlite_query_invalid_args" && val["reason"] != "sqlite_query_empty" {
		t.Fatalf("reason = %v, want sqlite_query_invalid_args|sqlite_query_empty", val["reason"])
	}
}

// TestSQLiteQueryTool_WithSelect verifies CTE-style WITH ... SELECT
// is accepted (common read-only construct).
func TestSQLiteQueryTool_WithSelect(t *testing.T) {
	t.Parallel()
	db, err := store.OpenChannel(context.Background(),
		filepath.Join(t.TempDir(), "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tool := NewSQLiteQueryTool(db)
	res, err := tool.Execute(context.Background(), json.RawMessage(`{"sql":"WITH x AS (SELECT 1 AS n) SELECT n FROM x"}`))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.IsError {
		t.Fatalf("unexpected error: %v", res.Value.Value)
	}
}

// TestSQLiteQueryTool_Params verifies positional parameters work.
func TestSQLiteQueryTool_Params(t *testing.T) {
	t.Parallel()
	db, err := store.OpenChannel(context.Background(),
		filepath.Join(t.TempDir(), "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('alice', 'agent', 'in_worker_bus', 1700000000, NULL),
		        ('bob',   'agent', 'in_worker_bus', 1700000001, NULL)`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tool := NewSQLiteQueryTool(db)
	body, _ := json.Marshal(map[string]any{
		"sql":    "SELECT actor_id FROM actor_registry WHERE actor_id = ?",
		"params": []any{"alice"},
	})
	res, err := tool.Execute(context.Background(), body)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.IsError {
		t.Fatalf("unexpected error: %v", res.Value.Value)
	}
	rows := res.Value.Value.(map[string]any)["rows"].([][]any)
	if len(rows) != 1 || rows[0][0] != "alice" {
		t.Fatalf("unexpected rows: %v", rows)
	}
}

// TestSQLiteQueryTool_NilDB returns IsError+ infra error.
func TestSQLiteQueryTool_NilDB(t *testing.T) {
	t.Parallel()
	tool := NewSQLiteQueryTool(nil)
	res, err := tool.Execute(context.Background(), json.RawMessage(`{"sql":"SELECT 1"}`))
	if err == nil {
		t.Fatalf("expected infra error for nil db")
	}
	if !res.IsError {
		t.Fatalf("expected IsError=true")
	}
}
