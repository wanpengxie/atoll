package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// openTestDB opens a fresh channel-local sqlite under t.TempDir(),
// applies the channel DDL, and seeds two actors (alice agent + bob
// agent + system + tool:xhs) so harness tests can write without
// boilerplate.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := store.OpenChannel(context.Background(), filepath.Join(dir, "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	// Seed actor_registry rows directly (bypass registry.Register's
	// audit-event side effect — keep the row count predictable for
	// concurrent terminal_uniqueness tests).
	now := int64(1700000000)
	seed := func(id string, kind, binding string) {
		var bindArg any
		if binding != "" {
			bindArg = binding
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
			 VALUES (?, ?, ?, ?, NULL)`,
			id, kind, bindArg, now,
		); err != nil {
			t.Fatalf("seed actor %s: %v", id, err)
		}
	}
	seed("alice", "agent", "in_worker_bus")
	seed("bob", "agent", "in_worker_bus")
	seed("system", "system", "")
	seed("tool:xhs", "tool", "daemon_rpc")
	return db
}

func validSqliteCallerCtx() pkgharness.CallerCtx {
	return pkgharness.CallerCtx{
		Authenticated: true,
		ActorID:       "alice",
	}
}

func newSqliteEnv(id string) *v4types.Envelope {
	return &v4types.Envelope{
		ID:         id,
		TS:         1700000000_000,
		ChannelID:  "ch-1",
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: "alice"},
		Kind:       v4types.KindEvent,
		Type:       "agent.text",
		Payload:    json.RawMessage(`{"text":"hi"}`),
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{"*"},
	}
}

func buildSqliteDeps(t *testing.T, db *sql.DB) pkgharness.Deps {
	t.Helper()
	types, err := LoadTypeLookup(context.Background(), db)
	if err != nil {
		t.Fatalf("load types: %v", err)
	}
	return pkgharness.Deps{
		Store:       NewSQLiteStore(db),
		Actors:      NewSQLiteActors(db),
		Types:       types,
		WorkerLocks: NewSQLiteWorkerLocks(db),
		Dispatcher:  pkgharness.NoopDispatcher{},
		Clock:       func() int64 { return 1700000000_000 },
		ChannelID:   "ch-1",
	}
}

func TestSQLiteStore_HappyPath_InsertEvent(t *testing.T) {
	db := openTestDB(t)
	deps := buildSqliteDeps(t, db)
	env := newSqliteEnv("ev-1")
	r, err := pkgharness.Write(context.Background(), deps, env, validSqliteCallerCtx())
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if r.Dedupe {
		t.Fatalf("first insert should not be dedupe")
	}

	// Second write with same envelope → Step 0.5 dedupe.
	env2 := newSqliteEnv("ev-1")
	r2, err := pkgharness.Write(context.Background(), deps, env2, validSqliteCallerCtx())
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !r2.Dedupe {
		t.Fatalf("retry should be dedupe")
	}
}

func TestSQLiteStore_TerminalUniqueness_PartialIndex(t *testing.T) {
	db := openTestDB(t)
	// Install a business type with single-response convention so we can
	// drive the terminal slot via harness without needing payload_status.
	mustInstallBizType(t, db)
	deps := buildSqliteDeps(t, db)

	// Seed a request message via direct SQL so we can write a response.
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO messages (id, ts, ts_received, channel_id, sender_kind, sender_id,
		 kind, type, payload, parent_id, correlation_id, visibility, audience, is_terminal)
		 VALUES ('req-1', 1700000000000, 1700000000000, 'ch-1', 'agent', 'alice',
		         'request', 'biz.foo', '{}', NULL, 'req-1', 'public', '["bob"]', 0)`); err != nil {
		t.Fatalf("seed request: %v", err)
	}

	// Write first terminal response.
	first := newSqliteEnv("resp-1")
	first.Type = "biz.foo"
	first.Kind = v4types.KindResponse
	first.ParentID = "req-1"
	first.Payload = json.RawMessage(`{"ok":true}`)
	first.Audience = []string{"alice"}
	if _, err := pkgharness.Write(context.Background(), deps, first, validSqliteCallerCtx()); err != nil {
		t.Fatalf("first terminal: %v", err)
	}

	// Second terminal response with different id → terminal_duplicate.
	second := newSqliteEnv("resp-2")
	second.Type = "biz.foo"
	second.Kind = v4types.KindResponse
	second.ParentID = "req-1"
	second.Payload = json.RawMessage(`{"ok":false}`)
	second.Audience = []string{"alice"}
	_, err := pkgharness.Write(context.Background(), deps, second, validSqliteCallerCtx())
	var rerr *pkgharness.RejectError
	if !errors.As(err, &rerr) {
		t.Fatalf("expected RejectError, got %v", err)
	}
	if rerr.Reason != v4types.HarnessTerminalDuplicate {
		t.Fatalf("expected terminal_duplicate, got %q", rerr.Reason)
	}
	if rerr.DedupeResponseID != "resp-1" {
		t.Fatalf("expected dedupe_response_id=resp-1, got %q", rerr.DedupeResponseID)
	}

	// Replay first with same id → idempotent dedupe.
	again := newSqliteEnv("resp-1")
	again.Type = "biz.foo"
	again.Kind = v4types.KindResponse
	again.ParentID = "req-1"
	again.Payload = json.RawMessage(`{"ok":true}`)
	again.Audience = []string{"alice"}
	r, err := pkgharness.Write(context.Background(), deps, again, validSqliteCallerCtx())
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !r.Dedupe {
		t.Fatalf("replay should be dedupe")
	}
}

func TestSQLiteStore_Concurrent_SameID_ContendsForRow(t *testing.T) {
	db := openTestDB(t)
	deps := buildSqliteDeps(t, db)

	const N = 50 // sqlite single writer; 50 already strong enough to surface races
	var wg sync.WaitGroup
	errs := make([]error, N)
	results := make([]*pkgharness.Result, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			env := newSqliteEnv("shared")
			env.CorrelationID = "shared"
			r, err := pkgharness.Write(context.Background(), deps, env, validSqliteCallerCtx())
			errs[idx] = err
			results[idx] = r
		}(i)
	}
	wg.Wait()

	freshSuccess, dedupeSuccess := 0, 0
	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Fatalf("unexpected error idx=%d: %v", i, errs[i])
		}
		if results[i].Dedupe {
			dedupeSuccess++
		} else {
			freshSuccess++
		}
	}
	if freshSuccess != 1 {
		t.Fatalf("expected 1 fresh success, got %d", freshSuccess)
	}
	if dedupeSuccess != N-1 {
		t.Fatalf("expected %d dedupes, got %d", N-1, dedupeSuccess)
	}
}

func TestSQLiteStore_Concurrent_TerminalUniqueness(t *testing.T) {
	db := openTestDB(t)
	mustInstallBizType(t, db)
	deps := buildSqliteDeps(t, db)

	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO messages (id, ts, ts_received, channel_id, sender_kind, sender_id,
		 kind, type, payload, parent_id, correlation_id, visibility, audience, is_terminal)
		 VALUES ('req-conc', 1700000000000, 1700000000000, 'ch-1', 'agent', 'alice',
		         'request', 'biz.foo', '{}', NULL, 'req-conc', 'public', '["bob"]', 0)`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const N = 50
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			env := newSqliteEnv(fmt.Sprintf("term-%d", idx))
			env.Type = "biz.foo"
			env.Kind = v4types.KindResponse
			env.ParentID = "req-conc"
			env.Audience = []string{"alice"}
			env.Payload = json.RawMessage(`{"ok":true}`)
			env.CorrelationID = "req-conc"
			_, err := pkgharness.Write(context.Background(), deps, env, validSqliteCallerCtx())
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	successes, dupes := 0, 0
	for i := 0; i < N; i++ {
		if errs[i] == nil {
			successes++
			continue
		}
		var rerr *pkgharness.RejectError
		if !errors.As(errs[i], &rerr) {
			t.Fatalf("unexpected error: %v", errs[i])
		}
		if rerr.Reason != v4types.HarnessTerminalDuplicate {
			t.Fatalf("unexpected reject: %q", rerr.Reason)
		}
		if rerr.DedupeResponseID == "" {
			t.Fatalf("terminal_duplicate missing dedupe_response_id")
		}
		dupes++
	}
	if successes != 1 {
		t.Fatalf("expected exactly 1 terminal write to win, got %d", successes)
	}
	if dupes != N-1 {
		t.Fatalf("expected %d terminal_duplicate rejects, got %d", N-1, dupes)
	}
}

// mustInstallBizType installs a `biz.foo` business type accepting both
// request + response (single-response terminal). Uses registry.Install
// so the row mirrors production wiring.
func mustInstallBizType(t *testing.T, db *sql.DB) {
	t.Helper()
	schemasJSON := `{"request":{"type":"object"},"response":{"type":"object","properties":{"ok":{"type":"boolean"},"status":{"type":"string"},"reason":{"type":"string"}}}}`
	rows := []registry.TypeRow{{
		Type:               "biz.foo",
		AllowedKinds:       []string{"request", "response"},
		SchemasByKind:      json.RawMessage(schemasJSON),
		HandlerBinding:     "in_worker_bus",
		TerminalConvention: "single-response",
		HandlerActorID:     "",
	}}
	ctx := context.Background()
	if err := store.WithImmediate(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		return registry.Install(ctx, conn, rows, 1700000000)
	}); err != nil {
		t.Fatalf("install biz.foo: %v", err)
	}
}
