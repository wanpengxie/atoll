package e2e

// Scenario 1 — publish xhs happy path (v4 audit view A #1).
//
//	user "publish xhs" event → channel-agent emits xhs.publish request
//	→ xhs adapter pushes WS frame → mock Chrome extension callback (ok)
//	→ adapter.Respond writes terminal response → channel log carries
//	  request + terminal response + the user event.
//
// What this test exercises end-to-end:
//
//   - harness 9-step Write on the user event (kind=event, audience=['*'])
//   - harness 9-step Write on the agent-emitted xhs.publish request
//     (kind=request, audience=[tool:xhs-adapter]) so the adapter
//     framework sees a properly normalised envelope.
//   - adapter.Manager.Dispatch → xhs.Module.Handle → MockDeviceClient.PushCommand
//     (we read the WS frame off the mock and assert its shape).
//   - adapter.Manager.OnExternalCallback → xhs.Module.OnExternalCallback
//     → ctx.Respond → harness 9-step Write on the terminal response.
//
// Channel-agent reasoning (the LLM step) is intentionally folded: we
// simulate the channel-agent's emit decision by directly calling
// harness.Write with the request envelope. Realistic channel-agent
// reasoning is covered by internal/worker/runtime_test.go's go-kimi
// integration. T16's focus is the dataflow chain, not LLM routing.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/adapters/xhs"
)

// TestScenario1_PublishXHSHappyPath drives the publish chain through
// every subsystem and verifies the channel log carries the expected
// three rows in seq order:
//
//  1. agent.text-style event (user → broadcast)
//  2. xhs.publish request    (channel-agent → tool:xhs-adapter)
//  3. xhs.publish response   (tool:xhs-adapter → channel-agent, terminal)
//
// The assertion shape mirrors the L1 §10.2 invariant: every request
// in a single-response type ends with exactly one terminal response.
func TestScenario1_PublishXHSHappyPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fix := openE2EChannel(t)
	adapterStack := buildE2EAdapterManager(t, fix, DeviceID)

	// -----------------------------------------------------------------
	// Step 1: user emits "publish xhs" event (broadcast).
	// -----------------------------------------------------------------
	//
	// We use `agent.text` (a core type) as the user event payload — the
	// production user-message path lands here per L1 §6.1. The body
	// just carries the human's intent; the channel-agent's reasoning
	// step that follows is what produces the xhs.publish request.
	userEvent := eventEnvelope(
		"ev-user-publish",
		Alice, // alice stands in for the user-facing agent that proxies the user; L1 §6.1
		"agent.text",
		`{"text":"please publish my xhs note titled hello-world"}`,
	)
	writeHarness(t, ctx, fix, userEvent, agentCallerCtx(Alice))

	// -----------------------------------------------------------------
	// Step 2: channel-agent emits xhs.publish request (folded reasoning).
	// -----------------------------------------------------------------
	requestPayload := `{"title":"hello-world","content":"e2e publish smoke","device_id":"` + DeviceID + `"}`
	publishReq := requestEnvelope(
		"req-publish-1",
		Alice,
		xhs.TypePublish,
		xhs.AdapterActorID,
		requestPayload,
	)
	writeHarness(t, ctx, fix, publishReq, agentCallerCtx(Alice))

	// Sanity: the request row is in the channel log.
	if got := countMessagesByType(t, ctx, fix.DB, "request", xhs.TypePublish); got != 1 {
		t.Fatalf("xhs.publish request rows = %d, want 1", got)
	}

	// -----------------------------------------------------------------
	// Step 3: adapter.Manager.Dispatch → xhs.Module.Handle → mock WS push.
	// -----------------------------------------------------------------
	if err := adapterStack.Manager.Dispatch(ctx, publishReq); err != nil {
		t.Fatalf("Manager.Dispatch: %v", err)
	}

	sends := adapterStack.Device.Sends()
	if len(sends) != 1 {
		t.Fatalf("MockDeviceClient.Sends = %d, want 1", len(sends))
	}
	frame := sends[0]
	if frame.DeviceID != DeviceID {
		t.Errorf("WS frame DeviceID = %q, want %q", frame.DeviceID, DeviceID)
	}
	if frame.Command.Cmd != "publish" {
		t.Errorf("WS frame cmd = %q, want %q", frame.Command.Cmd, "publish")
	}
	if frame.Command.CorrelationID != publishReq.ID {
		t.Errorf("WS frame correlation_id = %q, want %q", frame.Command.CorrelationID, publishReq.ID)
	}
	// The adapter strips device_id out of params (it lives in the
	// outer frame). Belt-and-braces — the unit test already proves this
	// but the e2e re-asserts the wire shape future readers will care
	// about.
	if _, leaked := frame.Command.Params["device_id"]; leaked {
		t.Errorf("device_id leaked into WS frame params: %+v", frame.Command.Params)
	}

	// -----------------------------------------------------------------
	// Step 4: simulate Chrome extension callback (status=ok).
	// -----------------------------------------------------------------
	callbackBody := `{
		"correlation_id":"` + publishReq.ID + `",
		"device_id":"` + DeviceID + `",
		"status":"ok",
		"result":{
			"note_id":"n-e2e-001",
			"url":"https://xhs.example/n-e2e-001"
		}
	}`
	if err := adapterStack.Manager.OnExternalCallback(ctx, xhs.AdapterName, []byte(callbackBody)); err != nil {
		t.Fatalf("Manager.OnExternalCallback: %v", err)
	}

	// -----------------------------------------------------------------
	// Step 5: assert the terminal response landed in the channel log.
	// -----------------------------------------------------------------
	if got := countTerminalResponses(t, ctx, fix.DB, publishReq.ID); got != 1 {
		t.Fatalf("terminal responses for %s = %d, want 1", publishReq.ID, got)
	}
	payload, senderID := terminalResponse(t, ctx, fix.DB, publishReq.ID)
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
	if resp["note_id"] != "n-e2e-001" {
		t.Errorf("response.note_id = %v, want n-e2e-001", resp["note_id"])
	}
	if resp["url"] != "https://xhs.example/n-e2e-001" {
		t.Errorf("response.url = %v, want https://xhs.example/n-e2e-001", resp["url"])
	}
	// Per v4-message-definition §1.2.5: device_id stays in the payload,
	// not in sender.id. The chain MUST surface it to the originating
	// agent so retry policy can pick the same device next time.
	if resp["device_id"] != DeviceID {
		t.Errorf("response.device_id = %v, want %q", resp["device_id"], DeviceID)
	}

	// -----------------------------------------------------------------
	// Channel log surface: user event + request + response = 3 rows.
	// -----------------------------------------------------------------
	totalRows := countAllMessages(t, ctx, fix)
	if totalRows != 3 {
		t.Errorf("messages count = %d, want 3 (user event + request + response)", totalRows)
	}
}

// countAllMessages is a scenario-1 local helper — common_test.go keeps
// the per-(kind,type) variant since other scenarios need finer-grained
// filters. Living here keeps the assertion vocabulary close to the
// happy-path narrative.
func countAllMessages(t *testing.T, ctx context.Context, fix *E2EFixture) int {
	t.Helper()
	var n int
	if err := fix.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&n); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	return n
}
