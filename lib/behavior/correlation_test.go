package behavior

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

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
		// Edge coverage folded in from the deleted ParseResponseStatus/
		// isFinalResponse helpers (期12 S6 拆删的等价覆盖).
		{[]byte(`not-json`), false},
		{[]byte(`{bad`), false},
		{[]byte(`{"foo":"bar"}`), false},
		{[]byte(`{"status":" completed "}`), true},
	}
	for _, tt := range tests {
		status, final := ParseFinalStatus(tt.raw)
		if final != tt.wantFinal {
			t.Fatalf("ParseFinalStatus(%s) = (%q, %v), want final=%v", tt.raw, status, final, tt.wantFinal)
		}
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
		Visibility: message.VisibilitySystem,
	})
	if err != nil {
		t.Fatalf("build err: %v", err)
	}
	if env.Visibility != message.VisibilitySystem {
		t.Fatalf("visibility = %q, want override system", env.Visibility)
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
