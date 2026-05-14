package e2e

// Scenario 3 — agent_A ask agent_B (kind=request) → B never responds
// → 24h later scheduler emits `unanswered_timeout` terminal response
// (v4 audit view A #3).
//
// Production sequence:
//
//  1. Alice writes a `biz.foo` request to Bob with expires_at = now+24h.
//  2. Bob never produces a kind=response.
//  3. Wall-clock advances past expires_at (test uses an atomic clock
//     pointer rather than time.Sleep — the spec's "24h" is the
//     production budget; here we just want the comparison `expires_at
//     < now` to fire).
//  4. scheduler.Tick walks the long-pending table, finds the row in
//     Step 1 (agent receiver, expires_at expired), and emits a
//     `unanswered_timeout` fallback through the harness 9-step chain.
//
// Acceptance: the channel log holds exactly ONE terminal response
// row whose payload carries `status:'failed'` and
// `reason:'unanswered_timeout'`.
//
// The unit-level coverage for scheduler is in
// internal/scheduler/long_pending_test.go — this e2e test verifies
// the SAME scheduler operates correctly against the SAME channel
// fixture every other scenario uses (so a future refactor that
// breaks scheduler-vs-harness interop is caught here).

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coagent-ai/daemon-go/internal/scheduler"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// TestScenario3_UnansweredTimeout_24h_MockClock fires the scheduler
// Tick after advancing the mock clock past a 24h expires_at. The
// invariant verified: exactly one fallback terminal response lands,
// reason = "unanswered_timeout".
func TestScenario3_UnansweredTimeout_24h_MockClock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fix := openE2EChannel(t)

	// -----------------------------------------------------------------
	// Step 1: alice writes biz.foo request to bob with expires_at = T0 + 24h.
	// We use the harness so the row's normalize / canonical_hash /
	// audience checks all match production exactly.
	// -----------------------------------------------------------------
	const requestID = "req-ask-bob-24h"
	expires := T0 + 24*int64(time.Hour/time.Millisecond) // ms
	req := requestEnvelope(requestID, Alice, BizFoo, Bob, `{"question":"need_eta"}`)
	req.ExpiresAt = &expires
	writeHarness(t, ctx, fix, req, agentCallerCtx(Alice))

	if got := countMessagesByType(t, ctx, fix.DB, "request", BizFoo); got != 1 {
		t.Fatalf("biz.foo request rows after seed = %d, want 1", got)
	}
	// Before the clock advances, the scheduler must NOT emit (the
	// request is still within its budget).
	preTickCount := countTerminalResponses(t, ctx, fix.DB, requestID)
	if preTickCount != 0 {
		t.Fatalf("pre-tick terminal responses = %d, want 0 (still within budget)", preTickCount)
	}
	pre, err := scheduler.NewLongPendingScheduler(fix.DB,
		buildE2EHarnessWriter(fix), ChannelID,
		scheduler.Config{
			Now:    fix.Clock,
			Logger: silentLogger(),
		},
	)
	if err != nil {
		t.Fatalf("scheduler init: %v", err)
	}
	if err := pre.Tick(ctx); err != nil {
		t.Fatalf("pre-advance Tick: %v", err)
	}
	if got := countTerminalResponses(t, ctx, fix.DB, requestID); got != 0 {
		t.Fatalf("post-pre-advance terminal responses = %d, want 0 (budget not yet exhausted)", got)
	}

	// -----------------------------------------------------------------
	// Step 2: advance the mock clock past 24h + 1 ms. Same atomic
	// pointer the harness deps observe, so every subsystem sees the
	// new wall-clock.
	// -----------------------------------------------------------------
	atomic.StoreInt64(fix.NowPtr, expires+1)

	// -----------------------------------------------------------------
	// Step 3: scheduler.Tick. With the clock now past expires_at, the
	// long-pending Step 1 SQL matches the bob-bound request, and the
	// fallback envelope flows through the harness.
	// -----------------------------------------------------------------
	if err := pre.Tick(ctx); err != nil {
		t.Fatalf("post-advance Tick: %v", err)
	}

	if got := countTerminalResponses(t, ctx, fix.DB, requestID); got != 1 {
		t.Fatalf("post-advance terminal responses = %d, want 1 (unanswered_timeout)", got)
	}
	payload, senderID := terminalResponse(t, ctx, fix.DB, requestID)
	if senderID != SystemActorID {
		t.Errorf("fallback sender_id = %q, want %q", senderID, SystemActorID)
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(payload), &body); err != nil {
		t.Fatalf("decode fallback payload: %v", err)
	}
	if body["status"] != "failed" {
		t.Errorf("fallback status = %v, want failed", body["status"])
	}
	if body["reason"] != string(v4types.TerminalUnansweredTimeout) {
		t.Errorf("fallback reason = %v, want %q", body["reason"], v4types.TerminalUnansweredTimeout)
	}

	// -----------------------------------------------------------------
	// Step 4: idempotency — running Tick again after the emit MUST NOT
	// double the fallback. The scheduler's NOT EXISTS clause filters
	// the now-closed request; the harness Step 0.5 dedupe path catches
	// any race window if NOT EXISTS misses.
	// -----------------------------------------------------------------
	if err := pre.Tick(ctx); err != nil {
		t.Fatalf("idempotent Tick: %v", err)
	}
	if got := countTerminalResponses(t, ctx, fix.DB, requestID); got != 1 {
		t.Fatalf("after idempotent Tick: terminal responses = %d, want 1", got)
	}
}

// buildE2EHarnessWriter mints a scheduler.HarnessWriter wired to the
// fixture's deps so the scheduler's fallback emit traverses the SAME
// harness instance every other scenario uses. The wrapper is local to
// this file because scenario 5 builds its own writer too — sharing a
// helper here keeps the test files isolated.
func buildE2EHarnessWriter(fix *E2EFixture) scheduler.HarnessWriter {
	return scheduler.HarnessWriteFunc(func(ctx context.Context, env *v4types.Envelope, callerCtx pkgharness.CallerCtx) (*pkgharness.Result, error) {
		return pkgharness.Write(ctx, fix.Deps, env, callerCtx)
	})
}
