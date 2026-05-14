package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	internalharness "github.com/coagent-ai/daemon-go/internal/harness"
	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
	"github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

const (
	testChannelID = "ch-lp"
	testRequester = "alice"
	testReceiver  = "bob"
	testHuman     = "human:alice"
	testTool      = "tool:xhs"
	testBizType   = "biz.foo"
	testT0        = int64(1_700_000_000_000)
)

// openSchedulerDB mirrors the harness/future_scheduler test pattern: a fresh
// channel sqlite under t.TempDir() with full L2 channel DDL, plus the actor
// rows the scheduler tests need.
func openSchedulerDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := store.OpenChannel(context.Background(), filepath.Join(dir, "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	seed := func(id, kind, binding string, deregistered *int64) {
		var bindArg any
		if binding != "" {
			bindArg = binding
		}
		var deregArg any
		if deregistered != nil {
			deregArg = *deregistered
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
			 VALUES (?, ?, ?, ?, ?)`,
			id, kind, bindArg, testT0, deregArg,
		); err != nil {
			t.Fatalf("seed actor %s: %v", id, err)
		}
	}
	seed("system", "system", "", nil)
	seed(testRequester, "agent", "in_worker_bus", nil)
	seed(testReceiver, "agent", "in_worker_bus", nil)
	seed(testHuman, "human", "", nil)
	seed(testTool, "tool", "daemon_rpc", nil)

	// Install a business type the fallback emit can use. The schemas accept
	// both an `ok` body and the `{status:'failed', reason, missing_actor_id}`
	// fallback branch (so harness Step 6 passes for every scheduler reason).
	mustInstallBizType(t, db)
	return db
}

// mustInstallBizType installs `biz.foo` with a request + response schema
// that explicitly accepts the platform fallback payload shape. Mirrors the
// helper in internal/harness/sqlite_store_test.go but with a response schema
// permissive enough for all three scheduler reasons.
func mustInstallBizType(t *testing.T, db *sql.DB) {
	t.Helper()
	schemasJSON := `{
	  "request": {"type": "object"},
	  "response": {
	    "type": "object",
	    "properties": {
	      "ok": {"type": "boolean"},
	      "status": {"type": "string"},
	      "reason": {"type": "string"},
	      "missing_actor_id": {"type": "string"}
	    }
	  }
	}`
	rows := []registry.TypeRow{{
		Type:               testBizType,
		AllowedKinds:       []string{"request", "response"},
		SchemasByKind:      json.RawMessage(schemasJSON),
		HandlerBinding:     "in_worker_bus",
		TerminalConvention: "single-response",
		HandlerActorID:     "",
	}}
	if err := store.WithImmediate(context.Background(), db, func(ctx context.Context, conn *sql.Conn) error {
		return registry.Install(ctx, conn, rows, testT0)
	}); err != nil {
		t.Fatalf("install %s: %v", testBizType, err)
	}
}

// pendingFixture inserts one pending request row directly into messages so
// the scheduler has something to scan. We bypass harness.Write because the
// test point is the scheduler's reaction, not the original write path.
type pendingFixture struct {
	id            string
	senderID      string
	receiver      string // becomes audience[0]
	typ           string
	expiresAt     *int64
	correlationID string
}

func insertPendingRequest(t *testing.T, ctx context.Context, db *sql.DB, p pendingFixture) {
	t.Helper()
	audJSON, err := json.Marshal([]string{p.receiver})
	if err != nil {
		t.Fatalf("marshal audience: %v", err)
	}
	typ := p.typ
	if typ == "" {
		typ = testBizType
	}
	correl := p.correlationID
	if correl == "" {
		correl = p.id
	}
	var expArg any
	if p.expiresAt != nil {
		expArg = *p.expiresAt
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id,
		    kind, type, payload, parent_id, correlation_id,
		    visibility, audience, not_before, expires_at, is_terminal)
		 VALUES (?, ?, ?, ?, 'agent', ?,
		         'request', ?, '{}', NULL, ?,
		         'public', ?, NULL, ?, 0)`,
		p.id, testT0, testT0, testChannelID, p.senderID,
		typ, correl, string(audJSON), expArg,
	); err != nil {
		t.Fatalf("insert pending request %s: %v", p.id, err)
	}
}

// insertExistingTerminal inserts a terminal response row for `parentID` so
// the scheduler's NOT EXISTS clause filters out the parent.
func insertExistingTerminal(t *testing.T, ctx context.Context, db *sql.DB, parentID, respID string) {
	t.Helper()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id,
		    kind, type, payload, parent_id, correlation_id,
		    visibility, audience, is_terminal)
		 VALUES (?, ?, ?, ?, 'agent', ?,
		         'response', ?, '{"ok":true}', ?, ?,
		         'public', '[]', 1)`,
		respID, testT0, testT0, testChannelID, testReceiver,
		testBizType, parentID, parentID,
	); err != nil {
		t.Fatalf("insert terminal response %s: %v", respID, err)
	}
}

// silentLogger keeps test output clean.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func fixedNow(cur *int64) func() int64 {
	return func() int64 { return *cur }
}

// sqliteHarnessWriter wires a real pkg/harness.Write against the channel
// sqlite. The scheduler tests call this so every fallback emit goes through
// the harness 9-step chain (acceptance criterion).
type sqliteHarnessWriter struct {
	deps harness.Deps
}

func (w *sqliteHarnessWriter) Write(ctx context.Context, env *v4types.Envelope, callerCtx harness.CallerCtx) (*harness.Result, error) {
	return harness.Write(ctx, w.deps, env, callerCtx)
}

func newSqliteHarnessWriter(t *testing.T, db *sql.DB) *sqliteHarnessWriter {
	t.Helper()
	types, err := internalharness.LoadTypeLookup(context.Background(), db)
	if err != nil {
		t.Fatalf("LoadTypeLookup: %v", err)
	}
	deps := harness.New(
		internalharness.NewSQLiteStore(db),
		internalharness.NewSQLiteActors(db),
		types,
		nil, // worker locks not needed; scheduler never sets fencing
		testChannelID,
	)
	deps.Clock = func() int64 { return testT0 }
	return &sqliteHarnessWriter{deps: deps}
}

func newScheduler(t *testing.T, db *sql.DB, writer HarnessWriter, now func() int64) *Scheduler {
	t.Helper()
	s, err := NewLongPendingScheduler(db, writer, testChannelID, Config{
		Period: time.Millisecond, // not exercised by Tick-only tests
		Batch:  16,
		Now:    now,
		Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewLongPendingScheduler: %v", err)
	}
	return s
}

// readMessage queries the messages table for a single id and returns the raw
// payload + visibility + audience + ts so tests can verify envelope shape.
func readMessage(t *testing.T, ctx context.Context, db *sql.DB, id string) (payload, audience, visibility string, kind string, parentID string, correlation string, found bool) {
	t.Helper()
	row := db.QueryRowContext(ctx,
		`SELECT payload, audience, visibility, kind, parent_id, correlation_id
		   FROM messages WHERE id = ?`, id)
	var par, corr sql.NullString
	if err := row.Scan(&payload, &audience, &visibility, &kind, &par, &corr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", "", "", "", "", false
		}
		t.Fatalf("read message %s: %v", id, err)
	}
	if par.Valid {
		parentID = par.String
	}
	if corr.Valid {
		correlation = corr.String
	}
	found = true
	return
}

func countMessages(t *testing.T, ctx context.Context, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&n); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	return n
}

// spyWriter records every call and forwards to an inner HarnessWriter.
// Useful for asserting "n fallback emits happened" + "first was fresh,
// second was dedupe" without re-parsing logs.
type spyWriter struct {
	mu    sync.Mutex
	calls []spyCall
	inner HarnessWriter
}

type spyCall struct {
	envelope *v4types.Envelope
	result   *harness.Result
	err      error
}

func (s *spyWriter) Write(ctx context.Context, env *v4types.Envelope, callerCtx harness.CallerCtx) (*harness.Result, error) {
	r, err := s.inner.Write(ctx, env, callerCtx)
	s.mu.Lock()
	cp := *env
	s.calls = append(s.calls, spyCall{envelope: &cp, result: r, err: err})
	s.mu.Unlock()
	return r, err
}

func (s *spyWriter) callsList() []spyCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]spyCall, len(s.calls))
	copy(out, s.calls)
	return out
}

// ---------------------------------------------------------------------------
// 1. Step 1 — agent receiver, expires_at expired -> unanswered_timeout.
// ---------------------------------------------------------------------------

func TestLongPending_Step1_AgentReceiver_UnansweredTimeout(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	exp := testT0 + 1_000
	insertPendingRequest(t, ctx, db, pendingFixture{
		id: "req-1", senderID: testRequester, receiver: testReceiver,
		expiresAt: &exp,
	})

	cur := testT0 + 2_000 // 1 s past expiry
	writer := &spyWriter{inner: newSqliteHarnessWriter(t, db)}
	s := newScheduler(t, db, writer, fixedNow(&cur))

	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	if got := len(writer.callsList()); got != 1 {
		t.Fatalf("Write called %d times, want 1", got)
	}

	wantID := FallbackID("req-1", v4types.TerminalUnansweredTimeout)
	payload, audience, vis, kind, parent, corr, ok := readMessage(t, ctx, db, wantID)
	if !ok {
		t.Fatalf("fallback row %q not found", wantID)
	}
	if kind != "response" {
		t.Errorf("kind = %q, want response", kind)
	}
	if parent != "req-1" {
		t.Errorf("parent_id = %q, want req-1", parent)
	}
	if corr != "req-1" {
		t.Errorf("correlation_id = %q, want req-1", corr)
	}
	if vis != "system" {
		t.Errorf("visibility = %q, want system", vis)
	}
	if audience != `["`+testRequester+`"]` {
		t.Errorf("audience = %q, want [\"%s\"]", audience, testRequester)
	}
	var pl fallbackPayload
	if err := json.Unmarshal([]byte(payload), &pl); err != nil {
		t.Fatalf("parse payload: %v", err)
	}
	if pl.Status != "failed" || pl.Reason != string(v4types.TerminalUnansweredTimeout) {
		t.Errorf("payload = %+v, want failed/unanswered_timeout", pl)
	}
	if pl.MissingActorID != "" {
		t.Errorf("missing_actor_id should be empty for Step 1, got %q", pl.MissingActorID)
	}
}

// ---------------------------------------------------------------------------
// 1a. Step 1 — system receiver also triggers unanswered_timeout.
// ---------------------------------------------------------------------------

func TestLongPending_Step1_SystemReceiver_UnansweredTimeout(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	exp := testT0 + 1_000
	insertPendingRequest(t, ctx, db, pendingFixture{
		id: "req-sys", senderID: testRequester, receiver: "system",
		expiresAt: &exp,
	})

	cur := testT0 + 2_000
	writer := &spyWriter{inner: newSqliteHarnessWriter(t, db)}
	s := newScheduler(t, db, writer, fixedNow(&cur))
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	wantID := FallbackID("req-sys", v4types.TerminalUnansweredTimeout)
	if _, _, _, _, _, _, ok := readMessage(t, ctx, db, wantID); !ok {
		t.Fatalf("fallback row %q not found for system receiver", wantID)
	}
}

// ---------------------------------------------------------------------------
// 2. Step 2 — human receiver with expires_at populated -> human_unanswered_timeout.
// ---------------------------------------------------------------------------

func TestLongPending_Step2_HumanReceiver_HumanUnansweredTimeout(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	exp := testT0 + 1_000
	insertPendingRequest(t, ctx, db, pendingFixture{
		id: "req-h", senderID: testRequester, receiver: testHuman,
		expiresAt: &exp,
	})

	cur := testT0 + 2_000
	writer := &spyWriter{inner: newSqliteHarnessWriter(t, db)}
	s := newScheduler(t, db, writer, fixedNow(&cur))
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	wantID := FallbackID("req-h", v4types.TerminalHumanUnansweredTimeout)
	payload, _, _, _, _, _, ok := readMessage(t, ctx, db, wantID)
	if !ok {
		t.Fatalf("fallback row %q not found", wantID)
	}
	var pl fallbackPayload
	if err := json.Unmarshal([]byte(payload), &pl); err != nil {
		t.Fatalf("parse payload: %v", err)
	}
	if pl.Reason != string(v4types.TerminalHumanUnansweredTimeout) {
		t.Errorf("reason = %q, want human_unanswered_timeout", pl.Reason)
	}
}

// ---------------------------------------------------------------------------
// 2a. Step 2 skipped when human receiver has NULL expires_at (channel
//     config disabled — protocol baseline).
// ---------------------------------------------------------------------------

func TestLongPending_Step2_HumanReceiver_NoExpiry_Skipped(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	// expires_at NULL: protocol baseline for human receiver.
	insertPendingRequest(t, ctx, db, pendingFixture{
		id: "req-h-null", senderID: testRequester, receiver: testHuman,
		expiresAt: nil,
	})

	cur := testT0 + 60_000_000 // 60_000 s later
	writer := &spyWriter{inner: newSqliteHarnessWriter(t, db)}
	s := newScheduler(t, db, writer, fixedNow(&cur))
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	if got := len(writer.callsList()); got != 0 {
		t.Errorf("scheduler should NOT emit for NULL-expires human row, got %d emits", got)
	}
}

// ---------------------------------------------------------------------------
// 3a. Step 3 — deregistered receiver -> receiver_unavailable.
// ---------------------------------------------------------------------------

func TestLongPending_Step3_DeregisteredReceiver_ReceiverUnavailable(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	exp := testT0 + 60_000 // not yet expired — Step 3 doesn't wait
	insertPendingRequest(t, ctx, db, pendingFixture{
		id: "req-d", senderID: testRequester, receiver: testReceiver,
		expiresAt: &exp,
	})
	// Soft-delete the receiver so Step 1 SQL's JOIN (deregistered_at IS NULL)
	// excludes it but Step 3's LEFT JOIN includes it.
	if _, err := db.ExecContext(ctx,
		`UPDATE actor_registry SET deregistered_at = ? WHERE actor_id = ?`,
		testT0+500, testReceiver,
	); err != nil {
		t.Fatalf("deregister %s: %v", testReceiver, err)
	}

	cur := testT0 + 1_000 // before expires_at
	writer := &spyWriter{inner: newSqliteHarnessWriter(t, db)}
	s := newScheduler(t, db, writer, fixedNow(&cur))
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	wantID := FallbackID("req-d", v4types.TerminalReceiverUnavailable)
	payload, _, _, _, _, _, ok := readMessage(t, ctx, db, wantID)
	if !ok {
		t.Fatalf("fallback row %q not found", wantID)
	}
	var pl fallbackPayload
	if err := json.Unmarshal([]byte(payload), &pl); err != nil {
		t.Fatalf("parse payload: %v", err)
	}
	if pl.Reason != string(v4types.TerminalReceiverUnavailable) {
		t.Errorf("reason = %q, want receiver_unavailable", pl.Reason)
	}
	if pl.MissingActorID != testReceiver {
		t.Errorf("missing_actor_id = %q, want %q", pl.MissingActorID, testReceiver)
	}
}

// ---------------------------------------------------------------------------
// 3b. Step 3 — receiver never in actor_registry -> receiver_unavailable.
// ---------------------------------------------------------------------------

func TestLongPending_Step3_MissingReceiver_ReceiverUnavailable(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	// audience[0] = "ghost", but no actor_registry row exists for it.
	exp := testT0 + 60_000
	insertPendingRequest(t, ctx, db, pendingFixture{
		id: "req-g", senderID: testRequester, receiver: "ghost",
		expiresAt: &exp,
	})

	cur := testT0 + 1_000
	writer := &spyWriter{inner: newSqliteHarnessWriter(t, db)}
	s := newScheduler(t, db, writer, fixedNow(&cur))
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	wantID := FallbackID("req-g", v4types.TerminalReceiverUnavailable)
	payload, _, _, _, _, _, ok := readMessage(t, ctx, db, wantID)
	if !ok {
		t.Fatalf("fallback row %q not found", wantID)
	}
	var pl fallbackPayload
	_ = json.Unmarshal([]byte(payload), &pl)
	if pl.MissingActorID != "ghost" {
		t.Errorf("missing_actor_id = %q, want ghost", pl.MissingActorID)
	}
}

// ---------------------------------------------------------------------------
// 4. Repeat-scan dedupe — acceptance criterion: "重复扫描: 同 pending request
//    多次进 scheduler -> 仅第一次 emit 成功，后续 step 0.5 dedupe 返回原 response".
//
// Two layers verified here:
//
//   - SQL filter: after the first Tick emits the fallback terminal, the
//     second Tick's NOT EXISTS clause excludes the now-closed row, so the
//     writer is not invoked again.
//   - Step 0.5 dedupe: even if SQL filtering were imperfect (concurrent
//     schedulers / race window between scan and emit), the deterministic
//     fallback id guarantees harness Step 0.5 dedupe — verified by manually
//     re-emitting the same envelope directly.
// ---------------------------------------------------------------------------

func TestLongPending_RepeatTick_DedupesViaHarnessStep0_5(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	exp := testT0 + 1_000
	insertPendingRequest(t, ctx, db, pendingFixture{
		id: "req-dup", senderID: testRequester, receiver: testReceiver,
		expiresAt: &exp,
	})

	cur := testT0 + 2_000
	writer := &spyWriter{inner: newSqliteHarnessWriter(t, db)}
	s := newScheduler(t, db, writer, fixedNow(&cur))

	// First Tick — fresh emit.
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick #1: %v", err)
	}
	if len(writer.callsList()) != 1 {
		t.Fatalf("first Tick should emit once, got %d", len(writer.callsList()))
	}
	if calls := writer.callsList(); calls[0].result == nil || calls[0].result.Dedupe {
		t.Errorf("first emit should be fresh, got Dedupe=%v", calls[0].result.Dedupe)
	}

	// Second Tick — NOT EXISTS filter excludes the row because the fallback
	// terminal already exists. Writer should NOT be invoked again.
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick #2: %v", err)
	}
	if len(writer.callsList()) != 1 {
		t.Errorf("second Tick should be a no-op (terminal exists), got %d total writes", len(writer.callsList()))
	}

	// Step 0.5 dedupe — directly re-emit the same fallback envelope through
	// the writer to prove deterministic id is honoured. This is the safety
	// net for concurrent-scheduler / race-window scenarios where two Ticks
	// might both scan before either inserts.
	row := &pendingRow{
		ID: "req-dup", TS: testT0, Type: testBizType, SenderID: testRequester,
		CorrelationID: sql.NullString{String: "req-dup", Valid: true},
		AudienceFirst: testReceiver,
	}
	dupEnv, err := buildFallbackEnvelope(row, v4types.TerminalUnansweredTimeout, "", testChannelID, cur)
	if err != nil {
		t.Fatalf("buildFallbackEnvelope: %v", err)
	}
	r2, err := writer.Write(ctx, dupEnv, harness.CallerCtx{Authenticated: true, ActorID: SystemActorID})
	if err != nil {
		t.Fatalf("direct re-emit: %v", err)
	}
	if r2 == nil || !r2.Dedupe {
		t.Errorf("direct re-emit should be Step 0.5 dedupe, got Dedupe=%v", r2 != nil && r2.Dedupe)
	}

	// Only one fallback row in the table even after the explicit re-emit.
	wantID := FallbackID("req-dup", v4types.TerminalUnansweredTimeout)
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE id = ?`, wantID).Scan(&n); err != nil {
		t.Fatalf("count fallback row: %v", err)
	}
	if n != 1 {
		t.Errorf("messages with id=%q count = %d, want 1", wantID, n)
	}
}

// ---------------------------------------------------------------------------
// 5. Tool receiver — Step 1 SQL excludes (actor_kind NOT IN agent/system);
//    Step 2 / Step 3 also don't match (active + not human) -> no emit.
// ---------------------------------------------------------------------------

func TestLongPending_ToolReceiver_NoEmit(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	exp := testT0 + 1_000
	insertPendingRequest(t, ctx, db, pendingFixture{
		id: "req-tool", senderID: testRequester, receiver: testTool,
		expiresAt: &exp,
	})

	cur := testT0 + 2_000
	writer := &spyWriter{inner: newSqliteHarnessWriter(t, db)}
	s := newScheduler(t, db, writer, fixedNow(&cur))
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	if got := len(writer.callsList()); got != 0 {
		t.Errorf("scheduler should NOT emit for tool receiver, got %d emits", got)
	}
}

// ---------------------------------------------------------------------------
// 6. Already has terminal response — none of the steps should re-emit.
// ---------------------------------------------------------------------------

func TestLongPending_AlreadyTerminal_SkippedByAllSteps(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	exp := testT0 + 1_000
	insertPendingRequest(t, ctx, db, pendingFixture{
		id: "req-done", senderID: testRequester, receiver: testReceiver,
		expiresAt: &exp,
	})
	insertExistingTerminal(t, ctx, db, "req-done", "resp-done")
	baselineCount := countMessages(t, ctx, db)

	cur := testT0 + 2_000
	writer := &spyWriter{inner: newSqliteHarnessWriter(t, db)}
	s := newScheduler(t, db, writer, fixedNow(&cur))
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	if got := len(writer.callsList()); got != 0 {
		t.Errorf("scheduler should NOT emit when terminal exists, got %d emits", got)
	}
	if got := countMessages(t, ctx, db); got != baselineCount {
		t.Errorf("message count changed from %d to %d", baselineCount, got)
	}
}

// ---------------------------------------------------------------------------
// 7. NewLongPendingScheduler validates required inputs.
// ---------------------------------------------------------------------------

func TestNewLongPendingScheduler_Validation(t *testing.T) {
	db := openSchedulerDB(t)
	writer := &spyWriter{inner: newSqliteHarnessWriter(t, db)}

	if _, err := NewLongPendingScheduler(nil, writer, "ch", Config{}); err == nil {
		t.Errorf("nil db should error")
	}
	if _, err := NewLongPendingScheduler(db, nil, "ch", Config{}); err == nil {
		t.Errorf("nil writer should error")
	}
	if _, err := NewLongPendingScheduler(db, writer, "", Config{}); err == nil {
		t.Errorf("empty channel should error")
	}
	s, err := NewLongPendingScheduler(db, writer, "ch", Config{})
	if err != nil {
		t.Fatalf("valid config should not error: %v", err)
	}
	// Defaults applied.
	if s.cfg.Period != DefaultPeriod {
		t.Errorf("Period default = %v, want %v", s.cfg.Period, DefaultPeriod)
	}
	if s.cfg.Batch != DefaultBatch {
		t.Errorf("Batch default = %v, want %v", s.cfg.Batch, DefaultBatch)
	}
	if s.cfg.Now == nil {
		t.Errorf("Now default should be wall-clock, got nil")
	}
	if s.cfg.Logger == nil {
		t.Errorf("Logger default should be slog.Default(), got nil")
	}
}

// ---------------------------------------------------------------------------
// 8. Run loop honours ctx cancel — smoke test ensures Run doesn't deadlock.
// ---------------------------------------------------------------------------

func TestLongPending_Run_StopsOnCtxCancel(t *testing.T) {
	db := openSchedulerDB(t)
	cur := testT0
	writer := &spyWriter{inner: newSqliteHarnessWriter(t, db)}
	s := newScheduler(t, db, writer, fixedNow(&cur))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not stop within 2s after cancel")
	}
}

// ---------------------------------------------------------------------------
// 9. FallbackID is the deterministic template used by emit + audit tools.
// ---------------------------------------------------------------------------

func TestFallbackID_Deterministic(t *testing.T) {
	id1 := FallbackID("req-1", v4types.TerminalUnansweredTimeout)
	id2 := FallbackID("req-1", v4types.TerminalUnansweredTimeout)
	if id1 != id2 {
		t.Errorf("FallbackID should be deterministic, got %q vs %q", id1, id2)
	}
	if id1 != "fallback:req-1:unanswered_timeout" {
		t.Errorf("FallbackID = %q, want fallback:req-1:unanswered_timeout", id1)
	}
	id3 := FallbackID("req-1", v4types.TerminalReceiverUnavailable)
	if id3 == id1 {
		t.Errorf("different reasons should yield different ids, got %q twice", id3)
	}
}
