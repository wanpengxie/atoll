package xhs

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func sampleRequest(t string, payload string) *message.Envelope {
	return &message.Envelope{
		ID:        "env-1",
		ChannelID: "channel-1",
		Sender:    message.Sender{Kind: actor.KindAgent, ID: "agent:writer"},
		Kind:      message.KindRequest,
		Type:      t,
		Payload:   []byte(payload),
		TS:        1_000_000,
	}
}

// TestBuildCommandStripsXhsPrefix verifies cmd is the type without the
// "xhs." prefix (M1.3 compatibility).
func TestBuildCommandStripsXhsPrefix(t *testing.T) {
	env := sampleRequest(TypePublish, `{"title":"hi"}`)
	cmd, err := buildCommand(env)
	if err != nil {
		t.Fatalf("buildCommand: %v", err)
	}
	if cmd.Cmd != "publish" {
		t.Errorf("Cmd=%q want \"publish\"", cmd.Cmd)
	}
	if cmd.Type != CommandWireType {
		t.Errorf("Type=%q want %q", cmd.Type, CommandWireType)
	}
	if cmd.CorrelationID != env.ID.String() {
		t.Errorf("CorrelationID=%q want %q", cmd.CorrelationID, env.ID)
	}
	if cmd.Params["title"] != "hi" {
		t.Errorf("Params should pass through; got %v", cmd.Params)
	}
}

// TestBuildCommandTreatsPayloadAsDomainData verifies routing metadata is
// not extracted from payload. The adapter forwards the caller's object as
// command params; route selection lives in the envelope audience.
func TestBuildCommandTreatsPayloadAsDomainData(t *testing.T) {
	env := sampleRequest(TypePublish, `{"title":"hi","extra":1}`)
	cmd, err := buildCommand(env)
	if err != nil {
		t.Fatalf("buildCommand: %v", err)
	}
	if cmd.Params["extra"] != float64(1) { // JSON numbers decode to float64
		t.Errorf("Params extras should pass through; got %v", cmd.Params)
	}
}

// TestBuildCommandRejectsBadInputs covers the precondition error paths.
func TestBuildCommandRejectsBadInputs(t *testing.T) {
	if _, err := buildCommand(nil); err == nil {
		t.Error("nil envelope should error")
	}
	env := sampleRequest("", `{}`)
	if _, err := buildCommand(env); err == nil {
		t.Error("empty type should error")
	}
	env = sampleRequest("not.xhs", `{}`)
	if _, err := buildCommand(env); err == nil {
		t.Error("type without xhs. prefix should error")
	}
	env = sampleRequest(TypePublish, `{`)
	if _, err := buildCommand(env); err == nil {
		t.Error("malformed payload should error")
	}
	env = sampleRequest(TypePublish, `{}`)
	env.ID = ""
	if _, err := buildCommand(env); err == nil {
		t.Error("blank envelope id should error")
	}
}

// TestParseCallbackHappy round-trips a typical callback body.
func TestParseCallbackHappy(t *testing.T) {
	raw := []byte(`{"correlation_id":"env-1","device_id":"d","status":"ok","result":{"note_id":"n"}}`)
	cb, err := parseCallback(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cb.CorrelationID != "env-1" || cb.DeviceID != "d" || cb.Status != "ok" {
		t.Errorf("header decode mismatch: %+v", cb)
	}
	if cb.Result["note_id"] != "n" {
		t.Errorf("result decode mismatch: %+v", cb.Result)
	}
}

// TestParseCallbackRejectsEmptyOrMissingID
func TestParseCallbackRejectsEmptyOrMissingID(t *testing.T) {
	if _, err := parseCallback(nil); err == nil {
		t.Error("nil payload should error")
	}
	if _, err := parseCallback([]byte(`{"status":"ok"}`)); err == nil {
		t.Error("missing correlation_id should error")
	}
}

// TestBuildRespondPayloadPublishHappy fold the per-type result + keep
// device_id (xhs.publish is the only schema that declares it).
func TestBuildRespondPayloadPublishHappy(t *testing.T) {
	cb := Callback{
		CorrelationID: "env-1",
		DeviceID:      "device-1",
		Status:        "ok",
		Result:        map[string]any{"note_id": "n1", "url": "https://x", "garbage": 1},
	}
	body, status, reason, err := buildRespondPayload(cb, TypePublish)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if status != "completed" || reason != "" {
		t.Errorf("status/reason mismatch: %s / %s", status, reason)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload["device_id"] != "device-1" {
		t.Errorf("publish should preserve device_id; got %v", payload["device_id"])
	}
	if payload["note_id"] != "n1" || payload["url"] != "https://x" {
		t.Errorf("allowed keys missing: %v", payload)
	}
	if _, present := payload["garbage"]; present {
		t.Error("stowaway key should be dropped (R4-FIX-A)")
	}
}

// TestBuildRespondPayloadSearchStowawayDropped is the canonical
// R4-FIX-A regression: search callback echoes note_id; adapter must
// drop both note_id and device_id because neither belong to search's
// L4 §2.2 schema.
func TestBuildRespondPayloadSearchStowawayDropped(t *testing.T) {
	cb := Callback{
		CorrelationID: "env-1",
		DeviceID:      "device-1",
		Status:        "ok",
		Result:        map[string]any{"results": []any{"a", "b"}, "note_id": "n1"},
	}
	body, _, _, err := buildRespondPayload(cb, TypeSearch)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	var payload map[string]any
	_ = json.Unmarshal(body, &payload)
	if _, present := payload["device_id"]; present {
		t.Error("search response must not carry device_id (R4-FIX-A)")
	}
	if _, present := payload["note_id"]; present {
		t.Error("search response must not carry stowaway note_id (R4-FIX-A)")
	}
	if _, ok := payload["results"]; !ok {
		t.Errorf("search response should preserve results; got %v", payload)
	}
}

// TestBuildRespondPayloadFailureSchema verifies the failure path's
// per-type allow-list (publish: retry_after; others: empty).
func TestBuildRespondPayloadFailureSchema(t *testing.T) {
	cb := Callback{
		CorrelationID: "env-1",
		Status:        "error",
		ErrorObj:      map[string]any{"reason": "boom", "retry_after": 30, "stowaway": "x"},
	}

	// publish: retry_after allowed.
	body, status, reason, _ := buildRespondPayload(cb, TypePublish)
	if status != "failed" || reason != string(message.TerminalReceiverInternalError) {
		t.Errorf("status/reason mismatch: %s / %s", status, reason)
	}
	publish := map[string]any{}
	_ = json.Unmarshal(body, &publish)
	if publish["error_code"] != "boom" {
		t.Errorf("publish should preserve callback reason as error_code; got %v", publish["error_code"])
	}
	if publish["retry_after"] != float64(30) {
		t.Errorf("publish should preserve retry_after; got %v", publish)
	}
	if _, present := publish["stowaway"]; present {
		t.Error("publish stowaway must be dropped")
	}

	// search: retry_after NOT allowed (fresh map; json.Unmarshal does
	// not clear destination maps).
	body, _, _, _ = buildRespondPayload(cb, TypeSearch)
	search := map[string]any{}
	_ = json.Unmarshal(body, &search)
	if _, present := search["retry_after"]; present {
		t.Error("search should drop retry_after (R4-FIX-A)")
	}
}

// TestBuildRespondPayloadUnknownStatus
func TestBuildRespondPayloadUnknownStatus(t *testing.T) {
	cb := Callback{CorrelationID: "env-1", Status: "vibrating"}
	body, status, reason, _ := buildRespondPayload(cb, TypePublish)
	if status != "failed" {
		t.Errorf("unknown status should resolve to failed; got %q", status)
	}
	if reason != string(message.TerminalReceiverInternalError) {
		t.Errorf("reason=%q want receiver_internal_error", reason)
	}
	var payload map[string]any
	_ = json.Unmarshal(body, &payload)
	if payload["error_code"] != "callback_status_unknown" {
		t.Errorf("payload.error_code=%v want callback_status_unknown", payload["error_code"])
	}
}

// TestErrorReasonFallback covers the helper picking reason / code /
// final fallback.
func TestErrorReasonFallback(t *testing.T) {
	if got := errorReason(map[string]any{"reason": "boom"}); got != "boom" {
		t.Errorf("reason path: %q", got)
	}
	if got := errorReason(map[string]any{"code": "ERR"}); got != "ERR" {
		t.Errorf("code path: %q", got)
	}
	if got := errorReason(map[string]any{}); got != "callback_failed" {
		t.Errorf("empty map fallback: %q", got)
	}
	if got := errorReason(nil); got != "callback_failed" {
		t.Errorf("nil fallback: %q", got)
	}
}

// TestNormaliseStatus covers the legacy synonym fold-down.
func TestNormaliseStatus(t *testing.T) {
	cases := map[string]callbackOutcome{
		"ok":        outcomeOK,
		"completed": outcomeOK,
		"success":   outcomeOK,
		"OK":        outcomeOK,
		"error":     outcomeError,
		"failed":    outcomeError,
		"failure":   outcomeError,
		"Failure":   outcomeError,
		"":          outcomeUnknown,
		"vibrating": outcomeUnknown,
		"  ok  ":    outcomeOK,
	}
	for in, want := range cases {
		if got := normaliseStatus(in); got != want {
			t.Errorf("normaliseStatus(%q)=%q want %q", in, got, want)
		}
	}
}

// TestPayloadParamsKeyOrder ensures we don't mutate the caller's
// payload map. Builds the same input twice and compares.
func TestBuildCommandDoesNotMutatePayloadView(t *testing.T) {
	env := sampleRequest(TypePublish, `{"title":"hi","device_id":"d"}`)
	cmd, err := buildCommand(env)
	if err != nil {
		t.Fatalf("buildCommand: %v", err)
	}
	// Re-decode the envelope payload and confirm device_id is still present
	// — i.e. buildCommand built a fresh map rather than mutating the
	// unmarshalled view.
	var orig map[string]any
	if err := json.Unmarshal(env.Payload, &orig); err != nil {
		t.Fatal(err)
	}
	if _, ok := orig["device_id"]; !ok {
		t.Error("original payload device_id missing; buildCommand mutated")
	}
	if !strings.HasPrefix(cmd.Cmd, "publish") {
		t.Errorf("cmd shape changed: %s", cmd.Cmd)
	}
}
