package coagent

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// TestWriteReject_EmitsStructuredJSON — FIX-6 §9 acceptance: stderr
// gets BOTH a human-readable header line (legacy contract) AND a
// trailing structured JSON line that downstream parsers (xhs-cli
// RealProvider) can grep for the harness reason verbatim.
func TestWriteReject_EmitsStructuredJSON(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	re := &RejectError{
		Reason:             v4types.HarnessRejectReason("schema_violation"),
		Detail:             "payload.title required",
		DedupeResponseID:   "resp-abc",
		MessageIDIfPartial: "msg-xyz",
	}
	writeReject(buf, "ask", re)

	out := buf.String()
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("writeReject lines = %d, want 2 (prefix + JSON); got:\n%s", len(lines), out)
	}

	// Prefix line keeps the legacy contract — grep-able by reason.
	if !strings.HasPrefix(lines[0], "coagent: ask rejected: schema_violation") {
		t.Errorf("prefix line = %q, want prefix 'coagent: ask rejected: schema_violation'", lines[0])
	}
	if !strings.Contains(lines[0], "payload.title required") {
		t.Errorf("prefix line %q missing detail", lines[0])
	}

	// Trailing JSON line parses + carries every field flat.
	var body struct {
		Error              string `json:"error"`
		Reason             string `json:"reason"`
		Detail             string `json:"detail"`
		DedupeResponseID   string `json:"dedupe_response_id"`
		MessageIDIfPartial string `json:"message_id_if_partial"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &body); err != nil {
		t.Fatalf("trailing line is not JSON (%v): %q", err, lines[1])
	}
	if body.Error != "reject" {
		t.Errorf("JSON.error = %q, want 'reject'", body.Error)
	}
	if body.Reason != "schema_violation" {
		t.Errorf("JSON.reason = %q, want 'schema_violation'", body.Reason)
	}
	if body.Detail != "payload.title required" {
		t.Errorf("JSON.detail = %q", body.Detail)
	}
	if body.DedupeResponseID != "resp-abc" {
		t.Errorf("JSON.dedupe_response_id = %q", body.DedupeResponseID)
	}
	if body.MessageIDIfPartial != "msg-xyz" {
		t.Errorf("JSON.message_id_if_partial = %q", body.MessageIDIfPartial)
	}
}

// TestWriteReject_OmitsEmptyOptionalFields — when DedupeResponseID /
// MessageIDIfPartial are blank, the JSON object should not carry them.
func TestWriteReject_OmitsEmptyOptionalFields(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	re := &RejectError{
		Reason: v4types.HarnessRejectReason("audience_invalid"),
		Detail: "audience cannot be '*'",
	}
	writeReject(buf, "answer", re)

	out := buf.String()
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("writeReject lines = %d, want 2; got:\n%s", len(lines), out)
	}
	if strings.Contains(lines[1], "dedupe_response_id") {
		t.Errorf("JSON unexpectedly carries dedupe_response_id: %q", lines[1])
	}
	if strings.Contains(lines[1], "message_id_if_partial") {
		t.Errorf("JSON unexpectedly carries message_id_if_partial: %q", lines[1])
	}
}
