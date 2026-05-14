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

// newSchedulerWithCfg lets a test override SchedulerConfig fields (OwnerID,
// ClaimTTL, MaxAttempts) while keeping the common defaults (silent logger,
// fixed-clock Now, channel id). Only fields set on `extra` override the
// defaults; zero values fall back to the test baseline.
func newSchedulerWithCfg(t *testing.T, db *sql.DB, disp GatewayDispatcher, now func() int64, extra SchedulerConfig) *FutureScheduler {
	t.Helper()
	cfg := SchedulerConfig{
		Period: time.Millisecond,
		Batch:  16,
		Now:    now,
		Logger: silentLogger(),
	}
	if extra.OwnerID != "" {
		cfg.OwnerID = extra.OwnerID
	}
	if extra.ClaimTTL > 0 {
		cfg.ClaimTTL = extra.ClaimTTL
	}
	if extra.MaxAttempts > 0 {
		cfg.MaxAttempts = extra.MaxAttempts
	}
	if extra.Batch > 0 {
		cfg.Batch = extra.Batch
	}
	s, err := NewFutureScheduler(db, disp, "ch-future", cfg)
	if err != nil {
		t.Fatalf("NewFutureScheduler: %v", err)
	}
	return s
}

// readClaimColumns peeks at the in-flight claim slot for one row.
func readClaimColumns(t *testing.T, ctx context.Context, db *sql.DB, id string) (owner string, claimedAt *int64, attempts int64) {
	t.Helper()
	var o sql.NullString
	var c sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT claim_owner, claimed_at, attempts FROM messages WHERE id = ?`, id,
	).Scan(&o, &c, &attempts); err != nil {
		t.Fatalf("read claim columns: %v", err)
	}
	if o.Valid {
		owner = o.String
	}
	if c.Valid {
		v := c.Int64
		claimedAt = &v
	}
	return
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

// ---------------------------------------------------------------------------
// 10. FIX-6 §2 acceptance — two schedulers ticking the same due row
//     race; only one Dispatch fires (CAS-claim on delivered_at).
// ---------------------------------------------------------------------------

func TestProcessRow_ConcurrentClaim_OnlyOneDispatch(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	// One due row: not_before just elapsed, no expires_at.
	nb := int64(100)
	insertFutureMsgs(t, ctx, db, "ch-future", 50, []futureMsg{{
		id: "due-1", senderKind: "agent", senderID: "alice",
		kind: "event", typ: "demo", visibility: "public",
		audience: `["*"]`, notBefore: &nb,
	}})

	disp := &spyDispatcher{}
	cur := int64(200)

	// Two scheduler instances sharing the same DB + the same dispatcher.
	s1 := newScheduler(t, db, disp, fixedNow(&cur))
	s2 := newScheduler(t, db, disp, fixedNow(&cur))

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)
	go func() {
		defer wg.Done()
		errCh <- s1.Tick(ctx)
	}()
	go func() {
		defer wg.Done()
		errCh <- s2.Tick(ctx)
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Errorf("Tick error: %v", err)
		}
	}

	// Exactly one Dispatch call — the CAS-claim winner.
	if got := disp.callCount(); got != 1 {
		t.Errorf("Dispatch called %d times across two concurrent Ticks; want 1", got)
	}

	// The row is now marked delivered.
	deliveredAt, failedAt, lastErr := readDeliveryColumns(t, ctx, db, "due-1")
	if deliveredAt == nil {
		t.Errorf("delivered_at should be set after winning claim")
	}
	if failedAt != nil {
		t.Errorf("delivery_failed_at should remain NULL; got %d", *failedAt)
	}
	if lastErr != "" {
		t.Errorf("last_error should be empty; got %q", lastErr)
	}

	// attempts column should be exactly 1 (only one CAS-claim succeeded).
	var attempts int
	if err := db.QueryRowContext(ctx,
		`SELECT attempts FROM messages WHERE id = ?`, "due-1").Scan(&attempts); err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (CAS-claim is single-winner)", attempts)
	}
}

// TestProcessRow_DispatchFailure_ReleasesClaim verifies that when
// Dispatch errors after claim, the claim is rolled back so the next
// Tick can retry.
func TestProcessRow_DispatchFailure_ReleasesClaim(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	nb := int64(100)
	insertFutureMsgs(t, ctx, db, "ch-future", 50, []futureMsg{{
		id: "due-fail", senderKind: "agent", senderID: "alice",
		kind: "event", typ: "demo", visibility: "public",
		audience: `["*"]`, notBefore: &nb,
	}})

	// Dispatcher always errors.
	disp := &spyDispatcher{err: context.DeadlineExceeded}
	cur := int64(200)
	s := newScheduler(t, db, disp, fixedNow(&cur))

	// First Tick — Dispatch fails; claim released.
	if err := s.Tick(ctx); err == nil {
		// Tick swallows per-row errors and logs them; check delivered_at
		// instead.
	}
	deliveredAt, _, _ := readDeliveryColumns(t, ctx, db, "due-fail")
	if deliveredAt != nil {
		t.Fatalf("delivered_at should be NULL after dispatch failure (claim released); got %d", *deliveredAt)
	}

	// Second Tick at a later wall-clock — claim succeeds again (still
	// errors though, because dispatcher still errors).
	cur = 300
	if err := s.Tick(ctx); err == nil {
		// same as above
	}
	deliveredAt2, _, _ := readDeliveryColumns(t, ctx, db, "due-fail")
	if deliveredAt2 != nil {
		t.Fatalf("delivered_at should still be NULL after second failure; got %d", *deliveredAt2)
	}

	// Dispatch was called twice — once per Tick, neither succeeded.
	if disp.callCount() != 2 {
		t.Errorf("Dispatch called %d times after two failing Ticks, want 2", disp.callCount())
	}
	// attempts increments on every claim (even when later released).
	var attempts int
	if err := db.QueryRowContext(ctx,
		`SELECT attempts FROM messages WHERE id = ?`, "due-fail").Scan(&attempts); err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2 (one per Tick claim)", attempts)
	}
}

// ---------------------------------------------------------------------------
// R2-FIX-3 §1 — crash recovery via stale-claim TTL.
//
// Simulate a daemon that crashed after claiming a row but before
// Dispatch completed: claim_owner is set, claimed_at is well in the
// past, delivered_at remains NULL. A restarted scheduler with a small
// ClaimTTL must see the row in its next scan, re-claim it, dispatch
// it, and stamp delivered_at exactly once.
//
// Pre-R2-FIX-3 the scan filtered `delivered_at IS NULL`, but R2 had
// repurposed delivered_at as the in-flight claim, so the crash-window
// row was silently dropped. This test fences that regression.
// ---------------------------------------------------------------------------

func TestProcessRow_CrashRecovery_ReclaimsStaleClaim(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	nb := int64(100)
	insertFutureMsgs(t, ctx, db, "ch-future", 50, []futureMsg{{
		id: "ghost-1", senderKind: "agent", senderID: "alice",
		kind: "event", typ: "demo", visibility: "public",
		audience: `["*"]`, notBefore: &nb,
	}})

	// Simulate the dead daemon: stamp a ghost claim AND bump attempts
	// the way claim() would have.
	if _, err := db.ExecContext(ctx,
		`UPDATE messages
		    SET claim_owner = 'ghost-daemon',
		        claimed_at  = 150,
		        attempts    = 1
		  WHERE id = ?`, "ghost-1",
	); err != nil {
		t.Fatalf("seed ghost claim: %v", err)
	}

	disp := &spyDispatcher{}
	// "Restart" the scheduler with a fresh owner id + short TTL so the
	// 150 ms ghost claim is well past stale.
	cur := int64(10_000)
	s := newSchedulerWithCfg(t, db, disp, fixedNow(&cur), SchedulerConfig{
		OwnerID:     "restarted-daemon",
		ClaimTTL:    50 * time.Millisecond,
		MaxAttempts: 10,
	})

	if err := s.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	if disp.callCount() != 1 {
		t.Fatalf("Dispatch should fire exactly once after stale-claim reclaim, got %d", disp.callCount())
	}
	delivered, failed, _ := readDeliveryColumns(t, ctx, db, "ghost-1")
	if delivered == nil {
		t.Errorf("delivered_at should be stamped after successful reclaim+dispatch")
	} else if *delivered != cur {
		t.Errorf("delivered_at = %d, want %d", *delivered, cur)
	}
	if failed != nil {
		t.Errorf("delivery_failed_at should remain NULL after successful reclaim, got %d", *failed)
	}

	// Claim slot is cleared post-delivery, attempts bumped to 2 (ghost's
	// original 1 + this Tick's +1).
	owner, claimedAt, attempts := readClaimColumns(t, ctx, db, "ghost-1")
	if owner != "" || claimedAt != nil {
		t.Errorf("claim slot should be cleared after delivery; got owner=%q claimed_at=%v", owner, claimedAt)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2 (ghost=1 + reclaim=1)", attempts)
	}
}

// ---------------------------------------------------------------------------
// R2-FIX-3 §1 — scan does NOT return a row that still has a fresh
// in-flight claim. Together with the previous test this fixes the
// silent-loss vs at-least-once invariant.
// ---------------------------------------------------------------------------

func TestScanReady_SkipsActiveClaim(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	nb := int64(100)
	insertFutureMsgs(t, ctx, db, "ch-future", 50, []futureMsg{{
		id: "in-flight", senderKind: "agent", senderID: "alice",
		kind: "event", typ: "demo", visibility: "public",
		audience: `["*"]`, notBefore: &nb,
	}})

	// A fresh claim taken at t=199 — only 1 ms old when scan runs.
	if _, err := db.ExecContext(ctx,
		`UPDATE messages
		    SET claim_owner = 'busy-daemon', claimed_at = 199, attempts = 1
		  WHERE id = ?`, "in-flight",
	); err != nil {
		t.Fatalf("seed fresh claim: %v", err)
	}

	cur := int64(200)
	s := newSchedulerWithCfg(t, db, &spyDispatcher{}, fixedNow(&cur), SchedulerConfig{
		ClaimTTL: 5 * time.Second, // far longer than 1 ms
	})

	rows, err := s.scanReady(ctx, cur)
	if err != nil {
		t.Fatalf("scanReady: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("scan returned %d rows, want 0 (active claim should be invisible)", len(rows))
	}
}

func TestScanReady_ReturnsStaleClaim(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	nb := int64(100)
	insertFutureMsgs(t, ctx, db, "ch-future", 50, []futureMsg{{
		id: "stale-1", senderKind: "agent", senderID: "alice",
		kind: "event", typ: "demo", visibility: "public",
		audience: `["*"]`, notBefore: &nb,
	}})

	// Claim taken 10s ago; TTL is 1s.
	if _, err := db.ExecContext(ctx,
		`UPDATE messages
		    SET claim_owner = 'ghost', claimed_at = 100, attempts = 3
		  WHERE id = ?`, "stale-1",
	); err != nil {
		t.Fatalf("seed stale claim: %v", err)
	}

	cur := int64(10_100)
	s := newSchedulerWithCfg(t, db, &spyDispatcher{}, fixedNow(&cur), SchedulerConfig{
		ClaimTTL: 1 * time.Second,
	})

	rows, err := s.scanReady(ctx, cur)
	if err != nil {
		t.Fatalf("scanReady: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != "stale-1" {
		t.Errorf("scan rows = %+v, want [stale-1]", rows)
	}
}

// ---------------------------------------------------------------------------
// R2-FIX-3 §3 — MaxAttempts cap. A poison row that fails dispatch N
// times stops being re-dispatched once attempts exceeds MaxAttempts;
// the row is moved to delivery_failed_at + last_error =
// "max_attempts_exceeded". Subsequent scans no longer return it.
// ---------------------------------------------------------------------------

func TestProcessRow_MaxAttemptsExceeded_MarksFailed(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	nb := int64(100)
	insertFutureMsgs(t, ctx, db, "ch-future", 50, []futureMsg{{
		id: "poison", senderKind: "agent", senderID: "alice",
		kind: "event", typ: "demo", visibility: "public",
		audience: `["*"]`, notBefore: &nb,
	}})

	disp := &spyDispatcher{err: context.DeadlineExceeded}
	cur := int64(200)
	s := newSchedulerWithCfg(t, db, disp, fixedNow(&cur), SchedulerConfig{
		MaxAttempts: 3,
		ClaimTTL:    time.Millisecond, // make every Tick observe its own claim as stale-able
	})

	// Run more Ticks than MaxAttempts; each advances cur so the previous
	// claim has gone stale and the scan returns the row again.
	for i := 0; i < 5; i++ {
		cur += 10 // > ClaimTTL=1ms, so prior claim is reclaimable
		if err := s.Tick(ctx); err != nil {
			t.Fatalf("Tick %d: %v", i, err)
		}
	}

	// Dispatch should have been called exactly MaxAttempts times — once
	// per claim where attempts <= MaxAttempts. After that, claim() still
	// fires (incrementing attempts to MaxAttempts+1) but processRow
	// short-circuits to markPermanentlyFailed before dispatching.
	if disp.callCount() != 3 {
		t.Errorf("Dispatch called %d times, want 3 (MaxAttempts)", disp.callCount())
	}

	delivered, failed, lastErr := readDeliveryColumns(t, ctx, db, "poison")
	if delivered != nil {
		t.Errorf("delivered_at should be NULL on max-attempts failure, got %d", *delivered)
	}
	if failed == nil {
		t.Fatalf("delivery_failed_at should be set after MaxAttempts")
	}
	if lastErr != FutureSchedulerMaxAttemptsError {
		t.Errorf("last_error = %q, want %q", lastErr, FutureSchedulerMaxAttemptsError)
	}

	// Claim slot is cleared post-failure.
	owner, claimedAt, attempts := readClaimColumns(t, ctx, db, "poison")
	if owner != "" || claimedAt != nil {
		t.Errorf("claim slot should be cleared after max-attempts failure; got owner=%q claimed_at=%v", owner, claimedAt)
	}
	if attempts < 4 {
		t.Errorf("attempts = %d, want >= 4 (MaxAttempts+1 claim that triggers failure)", attempts)
	}

	// Subsequent scan should NOT return the now-failed row.
	rows, err := s.scanReady(ctx, cur)
	if err != nil {
		t.Fatalf("scanReady: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("scan returned %d rows after max-attempts failure, want 0", len(rows))
	}
}

// ---------------------------------------------------------------------------
// R2-FIX-3 §6 — releaseClaim is owner-scoped. A loser scheduler with a
// different ownerID CANNOT release the winner's claim, even if the
// loser's stale view of the row's claimed_at happens to equal the
// winner's.
//
// Pre-fix releaseClaim used `WHERE delivered_at = ?` (exact ms match),
// so two ticks landing in the same wall-clock ms could clobber each
// other. The new owner-scoped clause guarantees only the holder can
// release.
// ---------------------------------------------------------------------------

func TestReleaseClaim_OwnerScopedSafeAgainstSameMs(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	nb := int64(100)
	insertFutureMsgs(t, ctx, db, "ch-future", 50, []futureMsg{{
		id: "race", senderKind: "agent", senderID: "alice",
		kind: "event", typ: "demo", visibility: "public",
		audience: `["*"]`, notBefore: &nb,
	}})

	// Winner takes the claim.
	cur := int64(200)
	winner := newSchedulerWithCfg(t, db, &spyDispatcher{}, fixedNow(&cur), SchedulerConfig{
		OwnerID: "winner",
	})
	gotAttempts, claimed, err := winner.claim(ctx, "race", cur)
	if err != nil {
		t.Fatalf("winner.claim: %v", err)
	}
	if !claimed {
		t.Fatalf("winner.claim returned !claimed; pre-state corrupted?")
	}
	if gotAttempts != 1 {
		t.Errorf("attempts after winner.claim = %d, want 1", gotAttempts)
	}

	// Loser (different ownerID) attempts to release using the same
	// timestamp. Owner-scoped WHERE must reject this UPDATE.
	loser := newSchedulerWithCfg(t, db, &spyDispatcher{}, fixedNow(&cur), SchedulerConfig{
		OwnerID: "loser",
	})
	if err := loser.releaseClaim(ctx, "race"); err != nil {
		t.Fatalf("loser.releaseClaim: %v", err)
	}

	// Winner's claim must still be present.
	owner, claimedAt, attempts := readClaimColumns(t, ctx, db, "race")
	if owner != "winner" {
		t.Errorf("claim_owner = %q, want winner (loser must not be able to release)", owner)
	}
	if claimedAt == nil || *claimedAt != cur {
		t.Errorf("claimed_at = %v, want %d", claimedAt, cur)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}

	// Winner can release its own claim.
	if err := winner.releaseClaim(ctx, "race"); err != nil {
		t.Fatalf("winner.releaseClaim: %v", err)
	}
	owner2, claimedAt2, _ := readClaimColumns(t, ctx, db, "race")
	if owner2 != "" || claimedAt2 != nil {
		t.Errorf("after winner.releaseClaim, claim slot should clear; got owner=%q claimed_at=%v", owner2, claimedAt2)
	}
}

// ---------------------------------------------------------------------------
// R2-FIX-3 §2 — concurrent ticks race claim_owner. Mirrors test #10 but
// additionally asserts the winner's claim_owner is recorded so the
// audit trail names the actual holder; loser sees no row.
// ---------------------------------------------------------------------------

func TestProcessRow_ConcurrentClaim_WinnerOwnsRow(t *testing.T) {
	ctx := context.Background()
	db := openSchedulerDB(t)

	nb := int64(100)
	insertFutureMsgs(t, ctx, db, "ch-future", 50, []futureMsg{{
		id: "race-owner", senderKind: "agent", senderID: "alice",
		kind: "event", typ: "demo", visibility: "public",
		audience: `["*"]`, notBefore: &nb,
	}})

	cur := int64(200)
	disp := &spyDispatcher{}
	s1 := newSchedulerWithCfg(t, db, disp, fixedNow(&cur), SchedulerConfig{OwnerID: "s1"})
	s2 := newSchedulerWithCfg(t, db, disp, fixedNow(&cur), SchedulerConfig{OwnerID: "s2"})

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)
	go func() { defer wg.Done(); errCh <- s1.Tick(ctx) }()
	go func() { defer wg.Done(); errCh <- s2.Tick(ctx) }()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Errorf("Tick error: %v", err)
		}
	}

	if disp.callCount() != 1 {
		t.Errorf("Dispatch called %d times, want 1 (single-winner)", disp.callCount())
	}

	delivered, _, _ := readDeliveryColumns(t, ctx, db, "race-owner")
	if delivered == nil {
		t.Errorf("delivered_at should be set after winner Dispatch")
	}
	// After successful delivery, the claim slot is cleared.
	owner, claimedAt, attempts := readClaimColumns(t, ctx, db, "race-owner")
	if owner != "" || claimedAt != nil {
		t.Errorf("claim slot should be cleared after delivery; got owner=%q claimed_at=%v", owner, claimedAt)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (loser must not double-bump)", attempts)
	}
}
