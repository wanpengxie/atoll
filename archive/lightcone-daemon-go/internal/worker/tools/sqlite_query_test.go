package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coagent-ai/daemon-go/internal/store"
)

// openChannelRWandRO opens a fresh channel sqlite at a tmp path and
// returns both a writable and a sibling read-only `*sql.DB` handle.
// Tests use the writable handle to seed and the ro handle to drive
// the tool — mirroring the production runtime.go wiring put in place
// for R2-FIX-7 (t113).
func openChannelRWandRO(t *testing.T) (rw, ro *sql.DB, path string) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "messages.sqlite")
	ctx := context.Background()
	var err error
	rw, err = store.OpenChannel(ctx, path, store.OpenOptions{})
	if err != nil {
		t.Fatalf("open channel rw: %v", err)
	}
	ro, err = store.OpenChannel(ctx, path, store.OpenOptions{ReadOnly: true, SkipDDL: true})
	if err != nil {
		_ = rw.Close()
		t.Fatalf("open channel ro: %v", err)
	}
	t.Cleanup(func() {
		_ = ro.Close()
		_ = rw.Close()
	})
	return rw, ro, path
}

// TestSQLiteQueryTool_SelectHappyPath verifies a real SELECT lands rows
// with the right shape.
func TestSQLiteQueryTool_SelectHappyPath(t *testing.T) {
	t.Parallel()
	rw, ro, _ := openChannelRWandRO(t)

	// Seed two rows so the query has something interesting to scan.
	ctx := context.Background()
	if _, err := rw.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('alice', 'agent', 'in_worker_bus', 1700000000, NULL),
		        ('bob',   'agent', 'in_worker_bus', 1700000001, NULL)`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tool := NewSQLiteQueryTool(ro)
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
	_, ro, _ := openChannelRWandRO(t)

	tool := NewSQLiteQueryTool(ro)
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

// TestSQLiteQueryTool_RejectsDMLCTE closes R2-FIX-7 (t113): the old
// validator accepted any WITH-prefixed statement that mentioned SELECT
// somewhere, which let SQLite ≥3.35 DML CTEs (`WITH x AS (DELETE ...
// RETURNING) SELECT ...`) slip through. The new tokeniser must reject
// at the validator layer (the ro `*sql.DB` is the second-line defence
// in production — exercised separately by
// TestSQLiteQueryTool_ReadOnlyHandleBlocksWrites below).
func TestSQLiteQueryTool_RejectsDMLCTE(t *testing.T) {
	t.Parallel()
	_, ro, _ := openChannelRWandRO(t)

	tool := NewSQLiteQueryTool(ro)
	cases := []struct {
		name  string
		sqlIn string
	}{
		{"delete_cte", `WITH x AS (DELETE FROM actor_registry RETURNING *) SELECT * FROM x`},
		{"update_cte", `WITH x AS (UPDATE actor_registry SET actor_kind='evil' RETURNING actor_id) SELECT * FROM x`},
		{"insert_cte", `WITH x AS (INSERT INTO actor_registry (actor_id) VALUES ('z') RETURNING *) SELECT * FROM x`},
		{"replace_cte", `WITH x AS (REPLACE INTO actor_registry (actor_id) VALUES ('z') RETURNING *) SELECT * FROM x`},
		{"drop_cte", `WITH x AS (DROP TABLE actor_registry) SELECT 1`},
		{"alter_cte", `WITH x AS (ALTER TABLE actor_registry RENAME TO evil) SELECT 1`},
		{"create_cte", `WITH x AS (CREATE TABLE evil(a)) SELECT 1`},
		{"truncate_nested", `WITH x AS (SELECT 1) SELECT * FROM (TRUNCATE actor_registry)`},
		// block-comment / line-comment evasion attempts also fail.
		{"line_comment_evasion", "SELECT * FROM x;\n-- harmless\nDELETE FROM actor_registry"},
		{"block_comment_evasion", "SELECT 1 /* still */ DELETE FROM actor_registry"},
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
				t.Fatalf("expected IsError=true for %s; got %+v", tc.name, res.Value.Value)
			}
			val := res.Value.Value.(map[string]any)
			reason, _ := val["reason"].(string)
			if !strings.HasPrefix(reason, "sqlite_query_not_select") {
				t.Fatalf("reason = %v, want sqlite_query_not_select", reason)
			}
		})
	}
}

// TestSQLiteQueryTool_AllowsKeywordsInsideStringLiterals confirms the
// tokeniser does NOT mistake DML keywords that appear inside SQL
// string literals or quoted identifiers for actual DML statements.
// A regression here would break legitimate read-only queries that
// happen to mention DML names as values or column aliases.
func TestSQLiteQueryTool_AllowsKeywordsInsideStringLiterals(t *testing.T) {
	t.Parallel()
	_, ro, _ := openChannelRWandRO(t)

	tool := NewSQLiteQueryTool(ro)
	cases := []string{
		`SELECT 'DELETE FROM x' AS s`,
		`SELECT 'embedded ''DELETE''' AS s`,
		`SELECT "DELETE" AS col`, // quoted identifier
		`SELECT 1 AS n -- DELETE FROM actor_registry`,
		`SELECT 1 AS n /* DELETE FROM actor_registry */`,
	}
	for _, sqlIn := range cases {
		sqlIn := sqlIn
		t.Run(sqlIn, func(t *testing.T) {
			body, _ := json.Marshal(map[string]any{"sql": sqlIn})
			res, err := tool.Execute(context.Background(), body)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if res.IsError {
				t.Fatalf("expected pass-through for %q, got reject: %+v", sqlIn, res.Value.Value)
			}
		})
	}
}

// TestSQLiteQueryTool_ReadOnlyHandleBlocksWrites is the driver-level
// belt-and-braces guarantee. Even if the validator were defeated by a
// novel evasion, the underlying `*sql.DB` handle is opened with
// `mode=ro`, so SQLite itself refuses any write. We sidestep the
// validator here by calling the ro handle directly with a DML
// statement — production code never does this; the test exists to pin
// the driver-level contract.
func TestSQLiteQueryTool_ReadOnlyHandleBlocksWrites(t *testing.T) {
	t.Parallel()
	rw, ro, _ := openChannelRWandRO(t)

	// Sanity: writable handle accepts a benign INSERT.
	if _, err := rw.ExecContext(context.Background(),
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('seed', 'agent', 'in_worker_bus', 1700000000, NULL)`); err != nil {
		t.Fatalf("seed via rw failed: %v", err)
	}
	// Now drive the SAME statement through the ro handle. The
	// modernc.org/sqlite driver must surface a readonly-database
	// error (SQLite SQLITE_READONLY = 8).
	_, err := ro.ExecContext(context.Background(),
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('blocked', 'agent', 'in_worker_bus', 1700000001, NULL)`)
	if err == nil {
		t.Fatalf("expected DML on ro handle to fail at driver level")
	}
	lower := strings.ToLower(err.Error())
	if !strings.Contains(lower, "read") && !strings.Contains(lower, "readonly") {
		t.Fatalf("ro driver error %q does not mention read-only", err)
	}
}

// TestSQLiteQueryTool_EmptySQL is a separate case because the reason
// code differs from "not select".
func TestSQLiteQueryTool_EmptySQL(t *testing.T) {
	t.Parallel()
	_, ro, _ := openChannelRWandRO(t)

	tool := NewSQLiteQueryTool(ro)
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
	_, ro, _ := openChannelRWandRO(t)

	tool := NewSQLiteQueryTool(ro)
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
	rw, ro, _ := openChannelRWandRO(t)
	if _, err := rw.ExecContext(context.Background(),
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('alice', 'agent', 'in_worker_bus', 1700000000, NULL),
		        ('bob',   'agent', 'in_worker_bus', 1700000001, NULL)`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tool := NewSQLiteQueryTool(ro)
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

// TestSQLiteQueryTool_PerQueryTimeout asserts FIX-3 R1 / FIX D
// (T103 / claude 96-2 major): a runaway SELECT MUST surface as
// a structured IsError result whose reason maps to the deadline
// path, not block the worker until ctx cancellation. We pin the
// tool's per-query timeout to 1ms and drive a deterministically
// expensive WITH RECURSIVE that produces millions of rows so the
// driver definitely observes the deadline mid-iteration.
func TestSQLiteQueryTool_PerQueryTimeout(t *testing.T) {
	t.Parallel()
	_, ro, _ := openChannelRWandRO(t)

	tool := &SQLiteQueryTool{db: ro, timeout: 1 * time.Millisecond}
	// Recursive CTE that walks 5M rows — orders of magnitude over the
	// 1ms budget so the driver/iterator must observe the deadline.
	heavy := `WITH RECURSIVE r(n) AS (
	  SELECT 1
	  UNION ALL
	  SELECT n+1 FROM r WHERE n < 5000000
	)
	SELECT count(*) FROM r`
	body, _ := json.Marshal(map[string]any{"sql": heavy})

	start := time.Now()
	res, err := tool.Execute(context.Background(), body)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Execute should NOT return infra error (it surfaces via IsError): %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected IsError=true on per-query timeout, got %+v", res.Value.Value)
	}
	val := res.Value.Value.(map[string]any)
	reason, _ := val["reason"].(string)
	if reason != "sqlite_query_failed" && reason != "sqlite_query_iter_failed" {
		t.Fatalf("reason = %q, want sqlite_query_failed or sqlite_query_iter_failed", reason)
	}
	msg, _ := val["message"].(string)
	if !strings.Contains(strings.ToLower(msg), "context") && !strings.Contains(strings.ToLower(msg), "deadline") && !strings.Contains(strings.ToLower(msg), "interrupt") {
		t.Fatalf("message %q does not look like a deadline/cancel surface", msg)
	}
	// Must respect the 1ms budget — give a generous 2s upper bound for
	// CI jitter. A regression that drops the per-query timeout would
	// stall on the 5M-row recursion (seconds → minutes).
	if elapsed > 2*time.Second {
		t.Fatalf("Execute took %s, well beyond the 1ms budget — per-query timeout did not fire", elapsed)
	}
}

// TestSQLiteQueryTool_DefaultTimeoutSet confirms NewSQLiteQueryTool
// initialises the timeout field to the documented default (so
// production callers do not accidentally get a zero-deadline tool
// whose ctx.WithTimeout(0) fails immediately).
func TestSQLiteQueryTool_DefaultTimeoutSet(t *testing.T) {
	t.Parallel()
	tool := NewSQLiteQueryTool(nil)
	if tool.timeout != defaultSQLiteQueryTimeout {
		t.Fatalf("default timeout = %s, want %s", tool.timeout, defaultSQLiteQueryTimeout)
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
