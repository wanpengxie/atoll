package adapter

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/canonical"
)

// TestRespond_HappyPath_DeterministicID emits a fresh terminal response
// and verifies (a) the envelope id matches the deterministic formula
// and (b) the row landed in channel sqlite via the harness.
func TestRespond_HappyPath_DeterministicID(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)
	writer := DefaultHarnessWriter(deps)

	insertRequest(t, db, requestRow{
		ID:       "req-1",
		Type:     testAdapterType,
		SenderID: testAgentID,
		Audience: testAdapterActor,
	})

	cfg := respondConfig{
		adapterName:  testAdapterName,
		adapterActor: testAdapterActor,
		channelID:    testChannelID,
		binding:      "daemon_rpc",
		writer:       writer,
		store:        deps.Store,
		clock:        fixedClock(&clock),
		logger:       silentLogger(),
	}

	payload := json.RawMessage(`{"note_id":"n123","url":"https://x.example/n123"}`)
	res, err := respond(context.Background(), cfg, "req-1", payload, RespondOptions{Status: StatusCompleted})
	if err != nil {
		t.Fatalf("respond: %v", err)
	}

	// Reconstruct the expected id.
	expected := mustExpectedRespondID(t, "req-1", payload, StatusCompleted, "", nil)
	if res.ID != expected {
		t.Fatalf("RespondResult.ID = %q; want %q", res.ID, expected)
	}
	if res.Dedupe {
		t.Fatalf("first respond should not be dedupe; got Dedupe=true")
	}
	if res.CorrelationID == "" {
		t.Fatalf("CorrelationID should be populated, got empty")
	}

	// Verify channel sqlite has the row + payload merged status/reason.
	kind, body, parent, found := readMessage(t, db, expected)
	if !found {
		t.Fatalf("expected row %q in messages", expected)
	}
	if kind != "response" {
		t.Fatalf("kind = %q; want response", kind)
	}
	if parent != "req-1" {
		t.Fatalf("parent_id = %q; want req-1", parent)
	}
	if !strings.Contains(body, `"status":"completed"`) {
		t.Fatalf("payload missing status: %s", body)
	}
	if !strings.Contains(body, `"note_id":"n123"`) {
		t.Fatalf("payload missing user fields: %s", body)
	}
}

// TestRespond_IdempotentSameID — second respond with same payload
// dedupes via harness Step 0.5 same-id retry.
func TestRespond_IdempotentSameID(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)
	writer := DefaultHarnessWriter(deps)

	insertRequest(t, db, requestRow{
		ID:       "req-2",
		Type:     testAdapterType,
		SenderID: testAgentID,
		Audience: testAdapterActor,
	})

	cfg := respondConfig{
		adapterName:  testAdapterName,
		adapterActor: testAdapterActor,
		channelID:    testChannelID,
		binding:      "daemon_rpc",
		writer:       writer,
		store:        deps.Store,
		clock:        fixedClock(&clock),
		logger:       silentLogger(),
	}

	payload := json.RawMessage(`{"x":1}`)
	first, err := respond(context.Background(), cfg, "req-2", payload, RespondOptions{Status: StatusCompleted})
	if err != nil {
		t.Fatalf("first respond: %v", err)
	}
	// Same payload + same clock → harness Step 0.5 dedupes by id +
	// canonical_hash. Adapter retries within one tick of the same
	// emission collapse without ever creating a duplicate row. (Later
	// retries with a different ts produce a different canonical_hash;
	// the L2 §8.5 contract relies on adapters reissuing quickly.)
	second, err := respond(context.Background(), cfg, "req-2", payload, RespondOptions{Status: StatusCompleted})
	if err != nil {
		t.Fatalf("second respond: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("ids differ: first=%q second=%q", first.ID, second.ID)
	}
	if !second.Dedupe {
		t.Fatalf("second respond should be Dedupe=true")
	}

	// Only one terminal row should exist.
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM messages WHERE parent_id = ? AND kind='response'`,
		"req-2").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 terminal response, got %d", count)
	}
}

// TestRespond_TerminalDuplicateDifferentID — another emitter (e.g. F3
// timer) has already written a terminal for the request with a
// different id. The L2 §8.5 race table says adapter should treat that
// as idempotent success. We seed a pre-existing terminal then verify
// respond reports Dedupe=true with the winner's id, not the new one.
func TestRespond_TerminalDuplicateDifferentID(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)
	writer := DefaultHarnessWriter(deps)

	insertRequest(t, db, requestRow{
		ID:       "req-3",
		Type:     testAdapterType,
		SenderID: testAgentID,
		Audience: testAdapterActor,
	})
	// Pre-seed a winner with a different deterministic id.
	insertTerminalResponse(t, db, "req-3", "winner:req-3")

	cfg := respondConfig{
		adapterName:  testAdapterName,
		adapterActor: testAdapterActor,
		channelID:    testChannelID,
		binding:      "daemon_rpc",
		writer:       writer,
		store:        deps.Store,
		clock:        fixedClock(&clock),
		logger:       silentLogger(),
	}

	res, err := respond(context.Background(), cfg, "req-3",
		json.RawMessage(`{"x":2}`), RespondOptions{Status: StatusCompleted})
	if err != nil {
		t.Fatalf("respond: %v", err)
	}
	if !res.Dedupe {
		t.Fatalf("expected Dedupe=true on terminal_duplicate race")
	}
	if res.ID != "winner:req-3" {
		t.Fatalf("Dedupe id = %q; want 'winner:req-3'", res.ID)
	}
}

// TestRespond_RejectsBadInputs covers validation paths.
func TestRespond_RejectsBadInputs(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)
	insertRequest(t, db, requestRow{
		ID:       "req-4",
		Type:     testAdapterType,
		SenderID: testAgentID,
		Audience: testAdapterActor,
	})

	cfg := respondConfig{
		adapterName:  testAdapterName,
		adapterActor: testAdapterActor,
		channelID:    testChannelID,
		binding:      "daemon_rpc",
		writer:       DefaultHarnessWriter(deps),
		store:        deps.Store,
		clock:        fixedClock(&clock),
		logger:       silentLogger(),
	}

	cases := []struct {
		name       string
		requestID  string
		payload    json.RawMessage
		opts       RespondOptions
		wantSubstr string
	}{
		{"missing request_id", "", json.RawMessage(`{}`), RespondOptions{Status: StatusCompleted}, "requestID is required"},
		{"invalid status", "req-4", json.RawMessage(`{}`), RespondOptions{Status: "ok"}, "opts.Status"},
		{"missing request row", "nope", json.RawMessage(`{}`), RespondOptions{Status: StatusCompleted}, "not found"},
		{"non-object payload", "req-4", json.RawMessage(`["array"]`), RespondOptions{Status: StatusCompleted}, "JSON object"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := respond(context.Background(), cfg, tc.requestID, tc.payload, tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("err = %v; want substring %q", err, tc.wantSubstr)
			}
		})
	}
}

// TestRespond_MergeDetailFields verifies opts.Detail keys land in the
// final payload AFTER user payload but BEFORE status/reason (so
// detail can override user fields but never the protocol-mandated
// `status`).
func TestMergeRespondPayload(t *testing.T) {
	cases := []struct {
		name     string
		input    json.RawMessage
		opts     RespondOptions
		wantKeys map[string]any
	}{
		{
			name:  "nil payload",
			input: nil,
			opts:  RespondOptions{Status: StatusCompleted},
			wantKeys: map[string]any{
				"status": "completed",
			},
		},
		{
			name:  "with reason",
			input: json.RawMessage(`{"foo":"bar"}`),
			opts:  RespondOptions{Status: StatusFailed, Reason: "timeout"},
			wantKeys: map[string]any{
				"foo":    "bar",
				"status": "failed",
				"reason": "timeout",
			},
		},
		{
			name:  "detail override + status wins",
			input: json.RawMessage(`{"status":"raw","foo":1}`),
			opts: RespondOptions{
				Status: StatusFailed,
				Reason: "internal",
				Detail: map[string]any{"http_status": 502},
			},
			wantKeys: map[string]any{
				"foo":         json.Number("1"),
				"http_status": json.Number("502"),
				"status":      "failed",
				"reason":      "internal",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := mergeRespondPayload(tc.input, tc.opts)
			if err != nil {
				t.Fatalf("merge: %v", err)
			}
			dec := json.NewDecoder(strings.NewReader(string(out)))
			dec.UseNumber()
			var got map[string]any
			if err := dec.Decode(&got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			for k, want := range tc.wantKeys {
				if got[k] != want {
					t.Fatalf("key %q = %v (%T); want %v (%T)", k, got[k], got[k], want, want)
				}
			}
		})
	}
}

// mustExpectedRespondID rebuilds the deterministic id the framework
// would produce for the given (request, payload, opts) combination —
// used to verify Respond's id derivation without re-implementing the
// formula in every test.
func mustExpectedRespondID(t *testing.T, requestID string, payload json.RawMessage, status Status, reason string, detail map[string]any) string {
	t.Helper()
	merged, err := mergeRespondPayload(payload, RespondOptions{Status: status, Reason: reason, Detail: detail})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	hash, err := canonical.CanonicalHashPayload(merged)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	return "response:" + requestID + ":" + hash
}
