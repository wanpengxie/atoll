package adapter

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
)

func TestCorrelationTracker_TrackRecoverForget(t *testing.T) {
	db, _ := openAdapterChannel(t)
	if _, err := db.ExecContext(context.Background(), CorrelationTrackerDDL); err != nil {
		t.Fatalf("apply DDL: %v", err)
	}
	clock := int64(testT0)
	tr := newCorrelationTracker(db, "demo", testChannelID, testSystemID,
		nil, fixedClock(&clock), silentLogger())

	ctx := context.Background()
	if err := tr.Track(ctx, "req-1", "ext-1", testT0+10_000); err != nil {
		t.Fatalf("Track: %v", err)
	}
	got, ok, err := tr.Recover(ctx, "ext-1")
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if !ok || got != "req-1" {
		t.Fatalf("Recover('ext-1') = (%q, %v); want ('req-1', true)", got, ok)
	}

	// Re-track same external_id with a different request — overwrite.
	if err := tr.Track(ctx, "req-2", "ext-1", testT0+20_000); err != nil {
		t.Fatalf("Track overwrite: %v", err)
	}
	got, _, _ = tr.Recover(ctx, "ext-1")
	if got != "req-2" {
		t.Fatalf("after overwrite Recover('ext-1') = %q; want 'req-2'", got)
	}

	// Forget by request_id clears cache + row.
	if err := tr.Forget(ctx, "req-2"); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	_, ok, _ = tr.Recover(ctx, "ext-1")
	if ok {
		t.Fatalf("after Forget Recover('ext-1') should be missing")
	}
}

func TestCorrelationTracker_RecoverFromSQLiteAfterRestart(t *testing.T) {
	db, _ := openAdapterChannel(t)
	if _, err := db.ExecContext(context.Background(), CorrelationTrackerDDL); err != nil {
		t.Fatalf("apply DDL: %v", err)
	}
	clock := int64(testT0)
	tr1 := newCorrelationTracker(db, "demo", testChannelID, testSystemID,
		nil, fixedClock(&clock), silentLogger())
	if err := tr1.Track(context.Background(), "req-r", "ext-r", testT0+1_000); err != nil {
		t.Fatalf("Track: %v", err)
	}

	// Fresh tracker = simulates daemon restart with empty cache.
	tr2 := newCorrelationTracker(db, "demo", testChannelID, testSystemID,
		nil, fixedClock(&clock), silentLogger())
	got, ok, err := tr2.Recover(context.Background(), "ext-r")
	if err != nil {
		t.Fatalf("Recover after restart: %v", err)
	}
	if !ok || got != "req-r" {
		t.Fatalf("Recover after restart = (%q, %v); want ('req-r', true)", got, ok)
	}
}

func TestCorrelationTracker_GCExpiresPastGrace(t *testing.T) {
	db, deps := openAdapterChannel(t)
	if _, err := db.ExecContext(context.Background(), CorrelationTrackerDDL); err != nil {
		t.Fatalf("apply DDL: %v", err)
	}
	clock := int64(testT0)
	writer := DefaultHarnessWriter(deps)
	rec := newRecordingWriter(writer)
	tr := newCorrelationTracker(db, "demo", testChannelID, testSystemID,
		rec, fixedClock(&clock), silentLogger())

	ctx := context.Background()
	// Three entries: two expired, one still in-flight.
	deadlines := map[string]int64{
		"ext-a": testT0,           // expired
		"ext-b": testT0 + 60_000,  // expired soon after grace
		"ext-c": testT0 + 600_000, // still live
	}
	for ext, dl := range deadlines {
		if err := tr.Track(ctx, "req-"+ext, ext, dl); err != nil {
			t.Fatalf("Track %s: %v", ext, err)
		}
	}

	// Advance clock so ext-a + ext-b are past grace; ext-c isn't.
	atomic.StoreInt64(&clock, testT0+60_000+DefaultGCGraceMs+1)
	stats, err := tr.gc(ctx, atomic.LoadInt64(&clock), DefaultGCGraceMs)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if stats.Scanned != 2 || stats.Evicted != 2 {
		t.Fatalf("gc stats = %+v; want scanned=2, evicted=2", stats)
	}

	// In-flight entry still recoverable.
	got, ok, _ := tr.Recover(ctx, "ext-c")
	if !ok || got != "req-ext-c" {
		t.Fatalf("Recover('ext-c') after gc = (%q, %v); want ('req-ext-c', true)", got, ok)
	}
	// Expired entries gone.
	for _, ext := range []string{"ext-a", "ext-b"} {
		_, ok, _ := tr.Recover(ctx, ext)
		if ok {
			t.Fatalf("Recover(%q) after gc should be missing", ext)
		}
	}

	// Each eviction emitted one correlation_gc event with deterministic id.
	calls := rec.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected 2 gc emit calls, got %d", len(calls))
	}
	for _, c := range calls {
		if !strings.HasPrefix(c.envelope.ID, "correlation_gc:demo:") {
			t.Fatalf("envelope.id = %q; want correlation_gc:demo:* prefix", c.envelope.ID)
		}
		if c.err != nil {
			t.Fatalf("emit error: %v", c.err)
		}
		if !c.result.OK {
			t.Fatalf("emit reject: %+v", c.result.Error)
		}
	}
}

func TestCorrelationTracker_TrackValidation(t *testing.T) {
	db, _ := openAdapterChannel(t)
	if _, err := db.ExecContext(context.Background(), CorrelationTrackerDDL); err != nil {
		t.Fatalf("apply DDL: %v", err)
	}
	clock := int64(testT0)
	tr := newCorrelationTracker(db, "demo", testChannelID, testSystemID,
		nil, fixedClock(&clock), silentLogger())

	cases := []struct {
		name       string
		requestID  string
		externalID string
		deadlineMs int64
		wantSubstr string
	}{
		{"missing requestID", "", "ext", testT0, "requestID is required"},
		{"missing externalID", "req", "", testT0, "externalID is required"},
		{"zero deadline", "req", "ext", 0, "deadlineMs must be > 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tr.Track(context.Background(), tc.requestID, tc.externalID, tc.deadlineMs)
			if err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("err = %v, want substring %q", err, tc.wantSubstr)
			}
		})
	}
}

// TestCorrelationTracker_GCEmitDeterministicID covers the L2 §8.5
// dedupe property: a second GC sweep on the same expired entry would
// produce the same envelope id, so the harness must dedupe the second
// emit (no row count growth). We trigger that by GC-ing once, then
// re-inserting the same (adapter, external) row + GC-ing again — the
// event id should already exist in `messages` and the harness Step 0.5
// dedupe should return the existing row.
func TestCorrelationTracker_GCEmitDeterministicID(t *testing.T) {
	db, deps := openAdapterChannel(t)
	if _, err := db.ExecContext(context.Background(), CorrelationTrackerDDL); err != nil {
		t.Fatalf("apply DDL: %v", err)
	}
	clock := int64(testT0)
	tr := newCorrelationTracker(db, "demo", testChannelID, testSystemID,
		DefaultHarnessWriter(deps), fixedClock(&clock), silentLogger())

	ctx := context.Background()
	if err := tr.Track(ctx, "req-x", "ext-x", testT0+1_000); err != nil {
		t.Fatalf("Track: %v", err)
	}
	atomic.StoreInt64(&clock, testT0+DefaultGCGraceMs+10_000)
	if _, err := tr.gc(ctx, atomic.LoadInt64(&clock), DefaultGCGraceMs); err != nil {
		t.Fatalf("gc 1: %v", err)
	}
	// Re-track the same (adapter, external) to make the row eligible
	// for a second sweep at the same clock — verifies dedupe rather
	// than insert-then-fail.
	if err := tr.Track(ctx, "req-x", "ext-x", testT0+1_000); err != nil {
		t.Fatalf("Track 2: %v", err)
	}
	stats, err := tr.gc(ctx, atomic.LoadInt64(&clock), DefaultGCGraceMs)
	if err != nil {
		t.Fatalf("gc 2: %v", err)
	}
	if stats.Evicted != 1 {
		t.Fatalf("second gc evicted = %d; want 1", stats.Evicted)
	}

	// Channel sqlite should have exactly one correlation_gc row.
	var count int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages WHERE id = ?`,
		"correlation_gc:demo:ext-x",
	).Scan(&count); err != nil {
		t.Fatalf("count gc rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 correlation_gc row, got %d", count)
	}
}

// Ensure the lookup interface is plumbed correctly.
var _ CorrelationTracker = (*correlationTracker)(nil)
var _ pkgharness.WriteResult = pkgharness.WriteResult{}
