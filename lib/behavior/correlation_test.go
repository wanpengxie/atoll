package behavior

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

// isFinalResponse returns false for a response whose payload is not valid JSON
// (the unmarshal-error guard): it cannot be parsed, so it is not final.
func TestIsFinalResponse_UnparseablePayload(t *testing.T) {
	env := &message.Envelope{
		Kind:    message.KindResponse,
		Payload: json.RawMessage(`not-json`),
	}
	if isFinalResponse(env) {
		t.Fatal("an unparseable payload must not be treated as final")
	}
}

// isFinalResponse returns false for a non-response envelope.
func TestIsFinalResponse_NonResponse(t *testing.T) {
	if isFinalResponse(&message.Envelope{Kind: message.KindEvent}) {
		t.Fatal("a non-response must not be final")
	}
	if isFinalResponse(nil) {
		t.Fatal("nil must not be final")
	}
}

// isFinalResponse returns true for a final status and false for a provisional.
func TestIsFinalResponse_StatusDriven(t *testing.T) {
	final := &message.Envelope{Kind: message.KindResponse, Payload: json.RawMessage(`{"status":"completed"}`)}
	if !isFinalResponse(final) {
		t.Fatal("completed must be final")
	}
	prov := &message.Envelope{Kind: message.KindResponse, Payload: json.RawMessage(`{"status":"processing"}`)}
	if isFinalResponse(prov) {
		t.Fatal("processing must not be final")
	}
}

// ParseResponseStatus extracts status from payload.
func TestParseResponseStatus(t *testing.T) {
	tests := []struct {
		name string
		raw  json.RawMessage
		want string
	}{
		{"completed", json.RawMessage(`{"status":"completed"}`), "completed"},
		{"failed", json.RawMessage(`{"status":"failed"}`), "failed"},
		{"empty payload", nil, ""},
		{"invalid json", json.RawMessage(`{bad`), ""},
		{"no status", json.RawMessage(`{"foo":"bar"}`), ""},
		{"whitespace status", json.RawMessage(`{"status":" processing "}`), "processing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseResponseStatus(tt.raw)
			if got != tt.want {
				t.Fatalf("ParseResponseStatus = %q, want %q", got, tt.want)
			}
		})
	}
}

// ParseFinalStatus reports whether the status is Layer 1 final.
func TestParseFinalStatus(t *testing.T) {
	tests := []struct {
		raw       []byte
		wantFinal bool
	}{
		{[]byte(`{"status":"completed"}`), true},
		{[]byte(`{"status":"failed"}`), true},
		{[]byte(`{"status":"processing"}`), false},
		{nil, false},
	}
	for _, tt := range tests {
		status, final := ParseFinalStatus(tt.raw)
		if final != tt.wantFinal {
			t.Fatalf("ParseFinalStatus(%s) = (%q, %v), want final=%v", tt.raw, status, final, tt.wantFinal)
		}
	}
}

// CorrelationID picks the first non-empty id in priority order.
func TestCorrelationID(t *testing.T) {
	if got := CorrelationID("a", "b", "c"); got != "a" {
		t.Fatalf("want trigger corr id, got %q", got)
	}
	if got := CorrelationID("", "b", "c"); got != "b" {
		t.Fatalf("want envelope corr id, got %q", got)
	}
	if got := CorrelationID("", "", "c"); got != "c" {
		t.Fatalf("want envelope id, got %q", got)
	}
}

// BuildResponseFromRequest rejects a nil request.
func TestBuildResponseFromRequest_NilRequest(t *testing.T) {
	_, err := BuildResponseFromRequest(nil, fixedClock(1), ResponseSpec{Status: "completed"})
	if err == nil {
		t.Fatal("nil request must error")
	}
}

// BuildResponseFromRequest defaults correlation_id to the request id when the
// request carries no correlation_id.
func TestBuildResponseFromRequest_CorrelationDefaultsToRequestID(t *testing.T) {
	req := newRequest("r1", nil) // newRequest leaves CorrelationID empty
	if req.CorrelationID != "" {
		t.Fatalf("fixture precondition: request must have empty correlation_id, got %q", req.CorrelationID)
	}
	env, err := BuildResponseFromRequest(req, fixedClock(1), ResponseSpec{Status: "completed"})
	if err != nil {
		t.Fatalf("build err: %v", err)
	}
	if env.CorrelationID != "r1" {
		t.Fatalf("correlation_id = %q, want defaulted to request id r1", env.CorrelationID)
	}
}

// BuildResponseFromRequest inherits an explicit correlation_id from the request.
func TestBuildResponseFromRequest_InheritsCorrelationID(t *testing.T) {
	req := newRequest("r1", nil)
	req.CorrelationID = "corr-root"
	env, err := BuildResponseFromRequest(req, fixedClock(1), ResponseSpec{Status: "completed"})
	if err != nil {
		t.Fatalf("build err: %v", err)
	}
	if env.CorrelationID != "corr-root" {
		t.Fatalf("correlation_id = %q, want inherited corr-root", env.CorrelationID)
	}
}

// BuildResponseFromRequest uses an explicit visibility override when supplied,
// else inherits the request visibility.
func TestBuildResponseFromRequest_VisibilityOverride(t *testing.T) {
	req := newRequest("r1", nil) // visibility "channel"
	env, err := BuildResponseFromRequest(req, fixedClock(1), ResponseSpec{
		Status:     "completed",
		Visibility: message.Visibility("private"),
	})
	if err != nil {
		t.Fatalf("build err: %v", err)
	}
	if env.Visibility != message.Visibility("private") {
		t.Fatalf("visibility = %q, want override private", env.Visibility)
	}

	env2, _ := BuildResponseFromRequest(req, fixedClock(1), ResponseSpec{Status: "completed"})
	if env2.Visibility != req.Visibility {
		t.Fatalf("visibility = %q, want inherited %q", env2.Visibility, req.Visibility)
	}
}

// BuildResponseFromRequest surfaces a MergeResponsePayload error (non-object
// payload).
func TestBuildResponseFromRequest_PayloadError(t *testing.T) {
	req := newRequest("r1", nil)
	_, err := BuildResponseFromRequest(req, fixedClock(1), ResponseSpec{
		Status:  "completed",
		Payload: json.RawMessage(`[]`),
	})
	if err == nil {
		t.Fatal("non-object payload must error")
	}
}
