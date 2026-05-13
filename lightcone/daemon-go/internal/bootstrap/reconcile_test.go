package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/store"
)

// ---------------------------------------------------------------------------
// Reconcile_IncompleteWorkdir — workdir missing → status=rolled_back.
// ---------------------------------------------------------------------------

func TestReconcile_IncompleteWorkdir_RollsBack(t *testing.T) {
	ctx := context.Background()
	saga, daemonDB, _ := newSaga(t)

	// Seed an in_progress row whose workdir does NOT exist on disk
	// (simulating a crash during step 2 mkdir).
	if _, err := daemonDB.ExecContext(ctx,
		`INSERT INTO bootstrap_registry (create_request_id, channel_id, status, workdir_path, started_at)
		 VALUES ('req-incomplete', 'ch-incomplete', 'in_progress', '/nonexistent/path/incomplete', 1)`,
	); err != nil {
		t.Fatalf("seed: %v", err)
	}

	report, err := saga.Reconcile(ctx)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Scanned != 1 {
		t.Errorf("Scanned = %d, want 1", report.Scanned)
	}
	if report.RolledBack != 1 {
		t.Errorf("RolledBack = %d, want 1", report.RolledBack)
	}
	if report.Completed != 0 {
		t.Errorf("Completed = %d, want 0", report.Completed)
	}
	if got := mustStatus(t, ctx, daemonDB, "req-incomplete"); got != StatusRolledBack {
		t.Errorf("status = %q, want rolled_back", got)
	}
}

// ---------------------------------------------------------------------------
// Reconcile_8aOnlyCrash — step 8a (emit) succeeded, step 8b (UPDATE) crashed.
// Reconcile must INSERT OR IGNORE the channel_created event without
// duplicating it and push status to completed.
// ---------------------------------------------------------------------------

func TestReconcile_Step8bCrash_RetryCompletes(t *testing.T) {
	ctx := context.Background()
	saga, daemonDB, workRoot := newSaga(t,
		withFailpoints(map[string]error{fpStep8bComplete: errSimulatedCrash}))

	p := happyParams(t, workRoot, "req-8b", "ch-8b")
	_, err := saga.ChannelCreate(ctx, p)
	if err == nil {
		t.Fatal("expected step 8b failure")
	}
	// After the failed call: status should still be in_progress.
	if got := mustStatus(t, ctx, daemonDB, "req-8b"); got != StatusInProgress {
		t.Fatalf("post-crash status = %q, want in_progress", got)
	}
	// channel_created event already exists (single).
	channelDB, err := store.OpenChannel(ctx, filepath.Join(p.WorkdirPath, channelDBFilename), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	t.Cleanup(func() { _ = channelDB.Close() })
	if n := countRows(t, ctx, channelDB,
		`SELECT COUNT(*) FROM messages WHERE id = ?`,
		channelCreatedEventID("req-8b")); n != 1 {
		t.Fatalf("pre-reconcile event count = %d, want 1", n)
	}

	// Drop the failpoint and run Reconcile.
	saga.failpoints = nil
	report, err := saga.Reconcile(ctx)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Scanned != 1 {
		t.Errorf("Scanned = %d, want 1", report.Scanned)
	}
	if report.Completed != 1 {
		t.Errorf("Completed = %d, want 1", report.Completed)
	}
	if got := mustStatus(t, ctx, daemonDB, "req-8b"); got != StatusCompleted {
		t.Errorf("post-reconcile status = %q, want completed", got)
	}

	// channel_created event still single (INSERT OR IGNORE deduped).
	if n := countRows(t, ctx, channelDB,
		`SELECT COUNT(*) FROM messages WHERE id = ?`,
		channelCreatedEventID("req-8b")); n != 1 {
		t.Errorf("post-reconcile event count = %d, want 1 (dedup)", n)
	}
}

// ---------------------------------------------------------------------------
// Reconcile_Step8aOnlyCrash — step 8a NEVER ran (transaction rolled back).
// Workdir + channel sqlite still present (no compensation in this scenario
// because we synthesise the in_progress row by hand). Reconcile re-emits
// step 8a (INSERT OR IGNORE) and completes.
// ---------------------------------------------------------------------------

func TestReconcile_Step8aFresh_RetryCompletes(t *testing.T) {
	ctx := context.Background()
	saga, daemonDB, workRoot := newSaga(t)

	// Build a complete workdir manually: mkdir + OpenChannel (DDL only).
	workdirPath := filepath.Join(workRoot, "ch-fresh")
	if err := os.MkdirAll(workdirPath, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	channelDB, err := store.OpenChannel(ctx, filepath.Join(workdirPath, channelDBFilename), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = channelDB.Close() })

	// Seed an in_progress row pointing at the prepared workdir.
	if _, err := daemonDB.ExecContext(ctx,
		`INSERT INTO bootstrap_registry (create_request_id, channel_id, status, workdir_path, started_at)
		 VALUES ('req-fresh', 'ch-fresh', 'in_progress', ?, 1)`,
		workdirPath,
	); err != nil {
		t.Fatalf("seed: %v", err)
	}

	report, err := saga.Reconcile(ctx)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Completed != 1 {
		t.Errorf("Completed = %d, want 1", report.Completed)
	}
	if got := mustStatus(t, ctx, daemonDB, "req-fresh"); got != StatusCompleted {
		t.Errorf("status = %q, want completed", got)
	}
	if n := countRows(t, ctx, channelDB,
		`SELECT COUNT(*) FROM messages WHERE id = ?`,
		channelCreatedEventID("req-fresh")); n != 1 {
		t.Errorf("channel_created event count = %d, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// ListChannels — returns only completed rows, sorted by completed_at.
// ---------------------------------------------------------------------------

func TestListChannels_FiltersByStatus(t *testing.T) {
	ctx := context.Background()
	saga, daemonDB, _ := newSaga(t)

	// Seed three rows: one completed, one in_progress, one rolled_back.
	insert := func(reqID, chID, status string, completedAt any) {
		if _, err := daemonDB.ExecContext(ctx,
			`INSERT INTO bootstrap_registry
			   (create_request_id, channel_id, status, workdir_path, started_at, completed_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			reqID, chID, status, "/tmp/"+chID, 1, completedAt,
		); err != nil {
			t.Fatalf("insert %s: %v", reqID, err)
		}
	}
	insert("req-a", "ch-a", StatusCompleted, int64(100))
	insert("req-b", "ch-b", StatusCompleted, int64(200))
	insert("req-c", "ch-c", StatusInProgress, nil)
	insert("req-d", "ch-d", StatusRolledBack, nil)

	out, err := saga.ListChannels(ctx)
	if err != nil {
		t.Fatalf("ListChannels: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2 (only completed)", len(out))
	}
	if out[0].ChannelID != "ch-a" || out[1].ChannelID != "ch-b" {
		t.Errorf("ordering = [%s,%s], want [ch-a,ch-b]",
			out[0].ChannelID, out[1].ChannelID)
	}
	if out[0].CompletedAt != 100 || out[1].CompletedAt != 200 {
		t.Errorf("completed_at = [%d,%d], want [100,200]",
			out[0].CompletedAt, out[1].CompletedAt)
	}
}

// ---------------------------------------------------------------------------
// Reconcile_EmptyTable — no rows → no-op, empty report.
// ---------------------------------------------------------------------------

func TestReconcile_EmptyTable_NoOp(t *testing.T) {
	ctx := context.Background()
	saga, _, _ := newSaga(t)

	report, err := saga.Reconcile(ctx)
	if err != nil {
		t.Fatalf("Reconcile empty: %v", err)
	}
	if report.Scanned != 0 || report.RolledBack != 0 || report.Completed != 0 {
		t.Errorf("expected zero report, got %+v", report)
	}
}

// ---------------------------------------------------------------------------
// Reconcile_TripleRetry — running Reconcile twice in a row is idempotent.
// (after first pass the in_progress row is gone, the second pass scans 0)
// ---------------------------------------------------------------------------

func TestReconcile_DoubleRunIdempotent(t *testing.T) {
	ctx := context.Background()
	saga, daemonDB, _ := newSaga(t)

	if _, err := daemonDB.ExecContext(ctx,
		`INSERT INTO bootstrap_registry (create_request_id, channel_id, status, workdir_path, started_at)
		 VALUES ('req-dup', 'ch-dup', 'in_progress', '/nonexistent/dup', 1)`,
	); err != nil {
		t.Fatalf("seed: %v", err)
	}

	first, err := saga.Reconcile(ctx)
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if first.RolledBack != 1 {
		t.Errorf("first.RolledBack = %d, want 1", first.RolledBack)
	}

	second, err := saga.Reconcile(ctx)
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if second.Scanned != 0 {
		t.Errorf("second.Scanned = %d, want 0", second.Scanned)
	}
}

// errSimulatedCrash is the sentinel used to drive step-8b crash tests.
var errSimulatedCrash = simulatedCrashErr{}

type simulatedCrashErr struct{}

func (simulatedCrashErr) Error() string { return "simulated crash" }

// _ assignment ensures the package import is exercised even when one of
// the helpers is removed during edits.
var _ = strings.TrimSpace
