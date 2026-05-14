package e2e

// Scenario 4 — adapter callback retry, same correlation_id, dedupe to
// a single terminal response (v4 audit view A #4).
//
//	xhs.publish request → adapter pushes WS frame → device fires same
//	callback twice (network retry) → ctx.Respond writes one terminal
//	response then `Forget`s the correlation entry → second callback
//	hits Recover-miss → xhs.OnExternalCallback drops silently per the
//	"orphan callback" branch in internal/adapters/xhs/xhs.go.
//
// Channel-log invariant verified: exactly one kind=response,
// is_terminal=1 row for the request id. The One Law (L1 §10.2.1
// Step 8 terminal uniqueness) is the upstream safety net; the
// adapter's Forget hook is the *primary* dedupe path. This test
// asserts the primary path holds without relying on Step 8 catching
// the duplicate as `terminal_duplicate`.
//
// Why two callbacks (not 3+): the spec's spec wording ("adapter 网络
// 重试") matches the production failure mode — the extension or its
// transport retries one extra time. A two-shot retry adequately
// exercises Forget's idempotency. We additionally test a 3rd / 4th
// replay below to confirm the pattern extends past the retry budget.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/adapters/xhs"
)

// TestScenario4_DuplicateCallback_ChannelLogHasOneTerminal exercises
// the retry-shaped dedupe path. The assertion shape mirrors the
// ticket acceptance line:
//
//	"channel log 只有一条 terminal response"
//
// The test goes further than the minimum required and asserts the
// per-callback observability behaviour: the second OnExternalCallback
// does NOT return an error to the caller (consistent with the spec's
// orphan-drop semantics — the framework does not punish a benign
// retry), but it also does NOT create a new row.
func TestScenario4_DuplicateCallback_ChannelLogHasOneTerminal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fix := openE2EChannel(t)
	stack := buildE2EAdapterManager(t, fix, DeviceID)

	// -----------------------------------------------------------------
	// Write the xhs.publish request through the harness, then dispatch.
	// We bypass the user-event scaffold scenario 1 uses — this test
	// focuses on the callback dedupe; preserving the publish prologue
	// would just add noise.
	// -----------------------------------------------------------------
	requestID := "req-publish-retry"
	publishReq := requestEnvelope(
		requestID, Alice, xhs.TypePublish, xhs.AdapterActorID,
		`{"title":"retry-smoke","content":"network-retry"}`,
	)
	writeHarness(t, ctx, fix, publishReq, agentCallerCtx(Alice))
	if err := stack.Manager.Dispatch(ctx, publishReq); err != nil {
		t.Fatalf("Manager.Dispatch: %v", err)
	}

	// One WS frame pushed; the adapter has registered a correlation
	// entry for requestID.
	if got := len(stack.Device.Sends()); got != 1 {
		t.Fatalf("WS push count = %d, want 1", got)
	}

	// -----------------------------------------------------------------
	// First callback — extension reports success. ctx.Respond writes
	// the terminal response and calls Forget on the correlation entry.
	// -----------------------------------------------------------------
	callbackBody := []byte(`{
		"correlation_id":"` + requestID + `",
		"device_id":"` + DeviceID + `",
		"status":"ok",
		"result":{"note_id":"n-retry-1","url":"https://xhs.example/n-retry-1"}
	}`)
	if err := stack.Manager.OnExternalCallback(ctx, xhs.AdapterName, callbackBody); err != nil {
		t.Fatalf("first OnExternalCallback: %v", err)
	}
	if got := countTerminalResponses(t, ctx, fix.DB, requestID); got != 1 {
		t.Fatalf("after first callback: terminal responses = %d, want 1", got)
	}

	// -----------------------------------------------------------------
	// Second callback — network retry. Correlation entry already
	// forgotten by Step 6 of Respond; xhs.OnExternalCallback hits the
	// `Recover(...) → ok=false` branch and returns nil silently. No
	// new row appears in the channel log.
	// -----------------------------------------------------------------
	if err := stack.Manager.OnExternalCallback(ctx, xhs.AdapterName, callbackBody); err != nil {
		t.Fatalf("second OnExternalCallback (retry): %v", err)
	}
	if got := countTerminalResponses(t, ctx, fix.DB, requestID); got != 1 {
		t.Fatalf("after retry callback: terminal responses = %d, want 1 (dedupe failed)", got)
	}

	// -----------------------------------------------------------------
	// Third + fourth replays — keep firing the same body. The dedupe
	// pattern MUST hold regardless of how many retries the transport
	// performs (some devices misbehave under flaky networks).
	// -----------------------------------------------------------------
	for i := 0; i < 2; i++ {
		if err := stack.Manager.OnExternalCallback(ctx, xhs.AdapterName, callbackBody); err != nil {
			t.Fatalf("replay #%d OnExternalCallback: %v", i+2, err)
		}
	}
	if got := countTerminalResponses(t, ctx, fix.DB, requestID); got != 1 {
		t.Fatalf("after 4 callbacks total: terminal responses = %d, want 1", got)
	}

	// -----------------------------------------------------------------
	// Final assertion — the single response row carries the original
	// extension result body so consumer agents downstream don't observe
	// the dedupe as a data-loss event.
	// -----------------------------------------------------------------
	payload, senderID := terminalResponse(t, ctx, fix.DB, requestID)
	if senderID != xhs.AdapterActorID {
		t.Errorf("response sender_id = %q, want %q", senderID, xhs.AdapterActorID)
	}
	var resp map[string]any
	if err := json.Unmarshal([]byte(payload), &resp); err != nil {
		t.Fatalf("decode response payload: %v", err)
	}
	if resp["status"] != "completed" {
		t.Errorf("response.status = %v, want completed", resp["status"])
	}
	if resp["note_id"] != "n-retry-1" {
		t.Errorf("response.note_id = %v, want n-retry-1", resp["note_id"])
	}
}

// TestScenario4_DuplicateCallback_ConflictingBodyStillDedupes is the
// adversarial twin of the happy retry: the second callback claims a
// *different* result (e.g. the extension flipped its mind, or two
// concurrent device shards both reported the same correlation_id).
// The framework MUST still surface a single channel-log row — the
// second callback is dropped on Recover-miss before any harness write
// is attempted. The original `result` body wins by ordering, not by
// content comparison.
//
// This test guards the "no L0 Sender corruption" invariant: even when
// the retransmit is a lie, the channel log can never end up with two
// terminal responses to one request, and the visible body matches the
// first emitter's view.
func TestScenario4_DuplicateCallback_ConflictingBodyStillDedupes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fix := openE2EChannel(t)
	stack := buildE2EAdapterManager(t, fix, DeviceID)

	requestID := "req-publish-conflict"
	publishReq := requestEnvelope(
		requestID, Alice, xhs.TypePublish, xhs.AdapterActorID,
		`{"title":"conflict","content":"adversarial-callback"}`,
	)
	writeHarness(t, ctx, fix, publishReq, agentCallerCtx(Alice))
	if err := stack.Manager.Dispatch(ctx, publishReq); err != nil {
		t.Fatalf("Manager.Dispatch: %v", err)
	}

	// First callback wins.
	first := []byte(`{
		"correlation_id":"` + requestID + `",
		"device_id":"` + DeviceID + `",
		"status":"ok",
		"result":{"note_id":"winner-note","url":"https://xhs.example/winner"}
	}`)
	if err := stack.Manager.OnExternalCallback(ctx, xhs.AdapterName, first); err != nil {
		t.Fatalf("winning OnExternalCallback: %v", err)
	}

	// Second callback — same correlation_id, fabricated `error` shape.
	second := []byte(`{
		"correlation_id":"` + requestID + `",
		"device_id":"` + DeviceID + `",
		"status":"error",
		"error":{"reason":"unexpected_retry","code":"E_PHANTOM"}
	}`)
	if err := stack.Manager.OnExternalCallback(ctx, xhs.AdapterName, second); err != nil {
		t.Fatalf("conflicting OnExternalCallback: %v", err)
	}

	if got := countTerminalResponses(t, ctx, fix.DB, requestID); got != 1 {
		t.Fatalf("terminal responses = %d, want 1 even after conflicting retry", got)
	}
	payload, _ := terminalResponse(t, ctx, fix.DB, requestID)
	var resp map[string]any
	if err := json.Unmarshal([]byte(payload), &resp); err != nil {
		t.Fatalf("decode response payload: %v", err)
	}
	if resp["status"] != "completed" {
		t.Errorf("response.status = %v, want completed (the WINNER's body must persist)", resp["status"])
	}
	if resp["note_id"] != "winner-note" {
		t.Errorf("response.note_id = %v, want winner-note (the WINNER's body must persist)", resp["note_id"])
	}
	// Reason from the loser must NOT leak into the surviving response.
	if r, ok := resp["reason"]; ok && r != "" {
		t.Errorf("response.reason = %v should be absent on the winning completed body", r)
	}
}
