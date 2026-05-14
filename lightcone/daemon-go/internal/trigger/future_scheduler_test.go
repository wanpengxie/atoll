package trigger

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coagent-ai/daemon-go/internal/store"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

// openSchedulerDB mirrors the supervisor / registry test pattern: a
// fresh channel sqlite under t.TempDir() with the full L2 channel DDL
// applied via store.OpenChannel.
func openSchedulerDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "messages.sqlite")
	db, err := store.OpenChannel(context.Background(), path, store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// futureMsg captures every column the scheduler scan + dispatch
// touches; tests slice up a list and call insertFutureMsgs.
type futureMsg struct {
	id         string
	senderKind string
	senderID   string
	kind       string
	typ        string
	visibility string
	audience   string // JSON literal e.g. `["*"]`
	notBefore  *int64
	expiresAt  *int64
}

func futureInt(v int64) *int64 { return &v }

func insertFutureMsgs(t *testing.T, ctx context.Context, db *sql.DB, channelID string, ts int64, msgs []futureMsg) {
	t.Helper()
	for _, m := range msgs {
		_, err := db.ExecContext(ctx,
			`INSERT INTO messages
			   (id, ts, ts_received, channel_id, sender_kind, sender_id,
			    kind, type, payload, parent_id, visibility, audience,
			    not_before, expires_at, is_terminal)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}', NULL, ?, ?, ?, ?, 0)`,
			m.id, ts, ts, channelID, m.senderKind, m.senderID,
			m.kind, m.typ, m.visibility, m.audience,
			nullable(m.notBefore), nullable(m.expiresAt),
		)
		if err != nil {
			t.Fatalf("insert future message %q: %v", m.id, err)
		}
	}
}

func nullable(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

// spyDispatcher records each Dispatch invocation — what envelope was
// handed in and what upstream the scheduler used. Tests assert both
// to verify §5.1 dispatch + §5.3 dispatch-path semantics.
type spyDispatcher struct {
	mu     sync.Mutex
	calls  []spyCall
	result []string // canned Dispatch return
	err    error    // optional error to inject
}

type spyCall struct {
	envID    string
	upstream string
}

func (s *spyDispatcher) Dispatch(_ context.Context, env *v4types.Envelope, upstream string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, spyCall{envID: env.ID, upstream: upstream})
	if s.err != nil {
		return nil, s.err
	}
	if s.result != nil {
		out := make([]string, len(s.result))
		copy(out, s.result)
		return out, nil
	}
	return nil, nil
}

func (s *spyDispatcher) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// readDeliveryColumns inspects the audit columns the scheduler stamps.
func readDeliveryColumns(t *testing.T, ctx context.Context, db *sql.DB, id string) (deliveredAt *int64, failedAt *int64, lastError string) {
	t.Helper()
	var d, f sql.NullInt64
	var e sql.NullString
	err := db.QueryRowContext(ctx,
		`SELECT delivered_at, delivery_failed_at, last_error FROM messages WHERE id = ?`, id,
	).Scan(&d, &f, &e)
	if err != nil {
		t.Fatalf("read delivery columns: %v", err)
	}
	if d.Valid {
		v := d.Int64
		deliveredAt = &v
	}
	if f.Valid {
		v := f.Int64
		failedAt = &v
	}
	if e.Valid {
		lastError = e.String
	}
	return
}

// silentLogger keeps test output clean.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fixedNow returns a closure reading from *cur — tests mutate the
// pointer to advance simulated time.
func fixedNow(cur *int64) func() int64 {
	return func() int64 { return *cur }
}

func newScheduler(t *testing.T, db *sql.DB, disp GatewayDispatcher, now func() int64) *FutureScheduler {
	t.Helper()
	s, err := NewFutureScheduler(db, disp, "ch-future", SchedulerConfig{
		Period: time.Millisecond, // not used by Tick-only tests
		Batch:  16,
		Now:    now,
		Logger: silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewFutureScheduler: %v", err)
	}
	return s
}

// ---------------------------------------------------------------------------
// 1. Future message that has NOT reached not_before is NOT dispatched.
// ---------------------------------------------------------------------------

func TestFutureScheduler_NotYetDue_DoesNotDispatch(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	currentMs := int64(1_700_000_000_000)
	now := fixedNow(&currentMs)

	insertFutureMsgs(t, ctx, db, "ch-future", currentMs, []futureMsg{
		{
			id: "f1", senderKind: "agent", senderID: "agent:a",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: futureInt(currentMs + 5_000), // 5s in the future
		},
	})

	spy := &spyDispatcher{}
	s := newScheduler(t, db, spy, now)

	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if spy.callCount() != 0 {
		t.Errorf("Dispatch called %d times, want 0", spy.callCount())
	}
	delivered, failed, _ := readDeliveryColumns(t, ctx, db, "f1")
	if delivered != nil || failed != nil {
		t.Errorf("f1 should still be pending; delivered=%v failed=%v", delivered, failed)
	}
}

// ---------------------------------------------------------------------------
// 2. Ticket acceptance vector — future message becomes due 5s later,
//    Tick injects it into the gateway, delivered_at is stamped.
// ---------------------------------------------------------------------------

func TestFutureScheduler_DueAfter5s_DispatchesAndMarksDelivered(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	t0 := int64(1_700_000_000_000)
	cur := t0
	now := fixedNow(&cur)

	insertFutureMsgs(t, ctx, db, "ch-future", t0, []futureMsg{
		{
			id: "f1", senderKind: "agent", senderID: "agent:a",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: futureInt(t0 + 5_000), // due 5s after emit
		},
	})

	spy := &spyDispatcher{result: []string{"agent:a", "agent:b"}}
	s := newScheduler(t, db, spy, now)

	// Tick at t0 → not due; nothing happens.
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick @ t0: %v", err)
	}
	if spy.callCount() != 0 {
		t.Fatalf("Dispatch called before due time")
	}

	// Advance simulated time by 5s → row becomes due.
	cur = t0 + 5_000
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick @ t0+5s: %v", err)
	}
	if spy.callCount() != 1 {
		t.Fatalf("Dispatch called %d times, want 1", spy.callCount())
	}
	call := spy.calls[0]
	if call.envID != "f1" {
		t.Errorf("Dispatch envelope id = %q, want %q", call.envID, "f1")
	}
	// L1 §5.3 dispatch-path: scheduler upstream is empty so the
	// gateway does NOT filter the original sender.
	if call.upstream != FutureSchedulerUpstream {
		t.Errorf("Dispatch upstream = %q, want %q", call.upstream, FutureSchedulerUpstream)
	}

	delivered, failed, _ := readDeliveryColumns(t, ctx, db, "f1")
	if delivered == nil {
		t.Errorf("delivered_at should be set, got nil")
	} else if *delivered != cur {
		t.Errorf("delivered_at = %d, want %d", *delivered, cur)
	}
	if failed != nil {
		t.Errorf("delivery_failed_at should be nil, got %d", *failed)
	}
}

// ---------------------------------------------------------------------------
// 3. expires_at < now → row marked expired; gateway NOT invoked.
// ---------------------------------------------------------------------------

func TestFutureScheduler_Expired_MarksAndSkipsDispatch(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	t0 := int64(1_700_000_000_000)
	cur := t0 + 10_000 // 10s after emit
	now := fixedNow(&cur)

	insertFutureMsgs(t, ctx, db, "ch-future", t0, []futureMsg{
		{
			id: "f1", senderKind: "agent", senderID: "agent:a",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: futureInt(t0 + 1_000), // due 1s after emit
			expiresAt: futureInt(t0 + 5_000), // but expired 5s ago
		},
	})

	spy := &spyDispatcher{}
	s := newScheduler(t, db, spy, now)

	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if spy.callCount() != 0 {
		t.Errorf("Dispatch should NOT be called on expired row, got %d calls", spy.callCount())
	}

	delivered, failed, errStr := readDeliveryColumns(t, ctx, db, "f1")
	if delivered != nil {
		t.Errorf("delivered_at should be nil for expired row, got %d", *delivered)
	}
	if failed == nil {
		t.Fatalf("delivery_failed_at should be set, got nil")
	}
	if *failed != cur {
		t.Errorf("delivery_failed_at = %d, want %d", *failed, cur)
	}
	if errStr != FutureSchedulerExpiredError {
		t.Errorf("last_error = %q, want %q", errStr, FutureSchedulerExpiredError)
	}
}

// ---------------------------------------------------------------------------
// 4. Crash-recovery idempotence — running Tick twice on the same row
//    only dispatches once. The second Tick's WHERE clause filters out
//    the already-delivered row.
// ---------------------------------------------------------------------------

func TestFutureScheduler_Tick_IdempotentOnDeliveredRow(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	t0 := int64(1_700_000_000_000)
	cur := t0 + 6_000
	now := fixedNow(&cur)

	insertFutureMsgs(t, ctx, db, "ch-future", t0, []futureMsg{
		{
			id: "f1", senderKind: "agent", senderID: "agent:a",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: futureInt(t0 + 5_000),
		},
	})

	spy := &spyDispatcher{}
	s := newScheduler(t, db, spy, now)

	for i := 0; i < 3; i++ {
		if err := s.Tick(ctx); err != nil {
			t.Fatalf("Tick #%d: %v", i, err)
		}
	}
	if spy.callCount() != 1 {
		t.Errorf("Dispatch called %d times across 3 Ticks, want exactly 1 (idempotent)", spy.callCount())
	}
}

// ---------------------------------------------------------------------------
// 5. Multiple rows ordering — Tick processes in seq ASC order.
// ---------------------------------------------------------------------------

func TestFutureScheduler_Tick_ProcessesInSeqOrder(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	t0 := int64(1_700_000_000_000)
	cur := t0 + 10_000
	now := fixedNow(&cur)

	insertFutureMsgs(t, ctx, db, "ch-future", t0, []futureMsg{
		{
			id: "f1", senderKind: "agent", senderID: "agent:a",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: futureInt(t0 + 1_000),
		},
		{
			id: "f2", senderKind: "agent", senderID: "agent:b",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: futureInt(t0 + 2_000),
		},
		{
			id: "f3", senderKind: "agent", senderID: "agent:c",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: futureInt(t0 + 3_000),
		},
	})

	spy := &spyDispatcher{}
	s := newScheduler(t, db, spy, now)
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	if spy.callCount() != 3 {
		t.Fatalf("Dispatch called %d times, want 3", spy.callCount())
	}
	wantOrder := []string{"f1", "f2", "f3"}
	for i, c := range spy.calls {
		if c.envID != wantOrder[i] {
			t.Errorf("call[%d] envID = %q, want %q", i, c.envID, wantOrder[i])
		}
	}
}

// ---------------------------------------------------------------------------
// 6. Dispatcher error — row stays pending, retry on next Tick.
// ---------------------------------------------------------------------------

func TestFutureScheduler_Dispatch_ErrorRetries(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	t0 := int64(1_700_000_000_000)
	cur := t0 + 6_000
	now := fixedNow(&cur)

	insertFutureMsgs(t, ctx, db, "ch-future", t0, []futureMsg{
		{
			id: "f1", senderKind: "agent", senderID: "agent:a",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: futureInt(t0 + 5_000),
		},
	})

	spy := &spyDispatcher{err: context.DeadlineExceeded}
	s := newScheduler(t, db, spy, now)

	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	// Tick handles dispatch errors internally (logs + skip) without
	// surfacing them at the Tick level.
	delivered, failed, _ := readDeliveryColumns(t, ctx, db, "f1")
	if delivered != nil {
		t.Errorf("delivered_at should be nil after dispatch error, got %d", *delivered)
	}
	if failed != nil {
		t.Errorf("delivery_failed_at should be nil after dispatch error, got %d", *failed)
	}

	// Now drop the error and retry — second Tick should succeed.
	spy.err = nil
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick retry: %v", err)
	}
	if spy.callCount() != 2 {
		t.Errorf("Dispatch should retry; got %d calls, want 2", spy.callCount())
	}
	delivered2, _, _ := readDeliveryColumns(t, ctx, db, "f1")
	if delivered2 == nil {
		t.Errorf("delivered_at should be set after retry success")
	}
}

// ---------------------------------------------------------------------------
// 7. L1 §5.3 dispatch-path semantics end-to-end — wire the real
//    trigger.Gateway and verify the original agent sender appears in
//    the trigger result (scheduler upstream != sender.id).
// ---------------------------------------------------------------------------

func TestFutureScheduler_DispatchPath_KeepsOriginalSender(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	t0 := int64(1_700_000_000_000)
	cur := t0 + 6_000
	now := fixedNow(&cur)

	insertFutureMsgs(t, ctx, db, "ch-future", t0, []futureMsg{
		{
			id: "f1", senderKind: "agent", senderID: "agent:a",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: futureInt(t0 + 5_000),
		},
	})

	// Active channel members include the sender — direct write would
	// filter agent:a but scheduler MUST NOT.
	g, err := NewGateway(&stubActorLookup{active: []string{"agent:a", "agent:b"}}, nil)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}

	// Use the real gateway behind a thin recording wrapper so we can
	// assert the dispatched result.
	rec := &recordingDispatcher{inner: g}
	s := newScheduler(t, db, rec, now)
	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if got := rec.lastResult; len(got) != 2 || got[0] != "agent:a" || got[1] != "agent:b" {
		t.Errorf("scheduler dispatch result = %v, want [agent:a agent:b]", got)
	}
}

// recordingDispatcher forwards to an inner Gateway and stashes the
// return value so tests can assert it.
type recordingDispatcher struct {
	inner      GatewayDispatcher
	lastResult []string
}

func (r *recordingDispatcher) Dispatch(ctx context.Context, env *v4types.Envelope, upstream string) ([]string, error) {
	out, err := r.inner.Dispatch(ctx, env, upstream)
	r.lastResult = out
	return out, err
}

// ---------------------------------------------------------------------------
// 8. NewFutureScheduler validates inputs.
// ---------------------------------------------------------------------------

func TestNewFutureScheduler_Validation(t *testing.T) {
	db := openSchedulerDB(t)
	disp := &spyDispatcher{}
	if _, err := NewFutureScheduler(nil, disp, "ch", SchedulerConfig{}); err == nil {
		t.Errorf("nil db should error")
	}
	if _, err := NewFutureScheduler(db, nil, "ch", SchedulerConfig{}); err == nil {
		t.Errorf("nil dispatcher should error")
	}
	if _, err := NewFutureScheduler(db, disp, "", SchedulerConfig{}); err == nil {
		t.Errorf("empty channel should error")
	}
	if _, err := NewFutureScheduler(db, disp, "ch", SchedulerConfig{}); err != nil {
		t.Errorf("valid config should not error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 9. Run loop stops on ctx cancel — quick smoke test to ensure Run
//    doesn't deadlock.
// ---------------------------------------------------------------------------

func TestFutureScheduler_Run_StopsOnCtxCancel(t *testing.T) {
	db := openSchedulerDB(t)
	cur := int64(1)
	s := newScheduler(t, db, &spyDispatcher{}, fixedNow(&cur))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	// Let the loop spin a few iterations then cancel.
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not stop within 2s after cancel")
	}
}
