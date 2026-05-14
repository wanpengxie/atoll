package e2e

// Scenario 5 — admin deregister tool:xhs-adapter → agent ask
// xhs.publish → scheduler Step 3 emits `receiver_unavailable`
// terminal (v4 audit view A #5).
//
// Production sequence:
//
//  1. Alice writes an xhs.publish request to tool:xhs-adapter
//     through the harness while the adapter is still active.
//  2. Admin runs the deregistration tooling — represented here by a
//     direct `UPDATE actor_registry SET deregistered_at = ?`. The
//     row stays in the table so historical audits remain queryable;
//     scheduler Step 3 distinguishes "deregistered" vs "missing"
//     via the LEFT JOIN.
//  3. scheduler.Tick runs Step 3 (LEFT JOIN on actor_registry),
//     finds the request whose audience[0] is now deregistered, and
//     emits the `receiver_unavailable` fallback via the harness.
//
// Acceptance: the channel log holds exactly ONE terminal response
// row whose payload carries `status:'failed'`,
// `reason:'receiver_unavailable'`, and `missing_actor_id` matching
// the deregistered actor id.
//
// Note: Step 3 does NOT wait for expires_at — this is the difference
// from scenario 3. We assert the lack of wait by running Tick while
// the original expires_at is still in the future.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/adapters/xhs"
	"github.com/coagent-ai/daemon-go/internal/scheduler"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// TestScenario5_AdminDeregister_ReceiverUnavailable runs the full
// composition. Distinct from internal/scheduler/long_pending_test.go
// (which uses bespoke biz types) — this test re-uses the production
// xhs.publish type so the scheduler-vs-real-adapter-type interop is
// exercised.
func TestScenario5_AdminDeregister_ReceiverUnavailable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fix := openE2EChannel(t)

	// -----------------------------------------------------------------
	// Step 1: alice writes the xhs.publish request while the adapter
	// is still healthy.
	// -----------------------------------------------------------------
	const requestID = "req-publish-before-deregister"
	expires := T0 + 60_000 // 60 s budget — irrelevant for Step 3 but realistic
	req := requestEnvelope(
		requestID, Alice, xhs.TypePublish, xhs.AdapterActorID,
		`{"title":"about-to-vanish","content":"adapter-going-away"}`,
	)
	req.ExpiresAt = &expires
	writeHarness(t, ctx, fix, req, agentCallerCtx(Alice))

	if got := countMessagesByType(t, ctx, fix.DB, "request", xhs.TypePublish); got != 1 {
		t.Fatalf("xhs.publish request rows = %d, want 1", got)
	}

	// -----------------------------------------------------------------
	// Step 2: admin deregisters tool:xhs-adapter. We simulate the
	// L1 §12.2 deregistration write directly — the production admin
	// CLI goes through the bootstrap saga's deregistration RPC, but
	// the end state is the same.
	// -----------------------------------------------------------------
	deregisterAt := T0 + 100 // 100 ms after seed; well before request expires
	if _, err := fix.DB.ExecContext(ctx,
		`UPDATE actor_registry SET deregistered_at = ? WHERE actor_id = ?`,
		deregisterAt, xhs.AdapterActorID,
	); err != nil {
		t.Fatalf("simulate admin deregister: %v", err)
	}

	// -----------------------------------------------------------------
	// Step 3: scheduler.Tick. The wall-clock is still well before
	// expires_at (we don't advance fix.NowPtr) — Step 3 MUST fire
	// regardless of the budget.
	// -----------------------------------------------------------------
	sch, err := scheduler.NewLongPendingScheduler(fix.DB,
		buildE2EHarnessWriter(fix), ChannelID,
		scheduler.Config{
			Now:    fix.Clock,
			Logger: silentLogger(),
		},
	)
	if err != nil {
		t.Fatalf("scheduler init: %v", err)
	}
	if err := sch.Tick(ctx); err != nil {
		t.Fatalf("scheduler.Tick: %v", err)
	}

	// -----------------------------------------------------------------
	// Step 4: assert the fallback terminal landed with the right
	// payload shape. The reason MUST be receiver_unavailable (not
	// unanswered_timeout — that would be Step 1).
	// -----------------------------------------------------------------
	if got := countTerminalResponses(t, ctx, fix.DB, requestID); got != 1 {
		t.Fatalf("terminal responses after deregister + Tick = %d, want 1", got)
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
	if body["reason"] != string(v4types.TerminalReceiverUnavailable) {
		t.Errorf("fallback reason = %v, want %q", body["reason"], v4types.TerminalReceiverUnavailable)
	}
	if body["missing_actor_id"] != xhs.AdapterActorID {
		t.Errorf("fallback missing_actor_id = %v, want %q", body["missing_actor_id"], xhs.AdapterActorID)
	}

	// -----------------------------------------------------------------
	// Step 5: idempotency — a second Tick produces no new row. The
	// SQL's NOT EXISTS clause filters the now-closed request; harness
	// Step 0.5 is the dedupe backstop if the SQL ever races.
	// -----------------------------------------------------------------
	if err := sch.Tick(ctx); err != nil {
		t.Fatalf("idempotent Tick: %v", err)
	}
	if got := countTerminalResponses(t, ctx, fix.DB, requestID); got != 1 {
		t.Fatalf("after idempotent Tick: terminal responses = %d, want 1", got)
	}
}
