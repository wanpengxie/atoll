package framework

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestValidateSchemaShape(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{"empty", "", false},
		{"object", `{"type":"object","required":["x"]}`, false},
		{"array", `{"type":"array","items":{"type":"string"}}`, false},
		{"unknown-type", `{"type":"wat"}`, true},
		{"malformed-json", `{not json`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSchemaShape(json.RawMessage(tc.raw))
			if (err != nil) != tc.wantErr {
				t.Errorf("err=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestValidatePayload(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","required":["title"],"properties":{"title":{"type":"string"}}}`)
	if err := ValidatePayload(schema, []byte(`{"title":"x"}`)); err != nil {
		t.Errorf("valid payload: %v", err)
	}
	if err := ValidatePayload(schema, []byte(`{}`)); err == nil {
		t.Error("missing required should fail")
	}
	if err := ValidatePayload(schema, []byte(`{"title":42}`)); err == nil {
		t.Error("wrong type should fail")
	}
	if err := ValidatePayload(nil, []byte(`{}`)); err != nil {
		t.Errorf("nil schema should accept: %v", err)
	}
}

func TestValidateFallbackResponseSchema(t *testing.T) {
	// Schema that accepts {status,reason} object — passes all 3 samples.
	ok := json.RawMessage(`{
		"type":"object",
		"required":["status","reason"],
		"properties":{"status":{"type":"string"},"reason":{"type":"string"}}
	}`)
	if err := ValidateFallbackResponseSchema(ok); err != nil {
		t.Errorf("ok schema should pass: %v", err)
	}

	// Schema with enum that EXCLUDES one of the system fallback reasons.
	bad := json.RawMessage(`{
		"type":"object",
		"required":["status","reason"],
		"properties":{"reason":{"enum":["unanswered_timeout","adapter_default_timeout"]}}
	}`)
	if err := ValidateFallbackResponseSchema(bad); err == nil {
		t.Error("schema excluding receiver_unavailable should fail")
	}

	if err := ValidateFallbackResponseSchema(nil); err == nil {
		t.Error("nil fallback schema must reject (install rule)")
	}
}

func TestValidateTypeSchema_HappyPath(t *testing.T) {
	ts := adapter.TypeSchema{
		AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
		SchemasByKind: map[message.Kind]json.RawMessage{
			message.KindRequest:  json.RawMessage(`{"type":"object"}`),
			message.KindResponse: json.RawMessage(`{"type":"object"}`),
		},
		FallbackResponseSchema: json.RawMessage(`{"type":"object","required":["status","reason"]}`),
		TerminalConvention:     "payload_status",
	}
	if err := ValidateTypeSchema("biz.x", ts); err != nil {
		t.Errorf("happy path: %v", err)
	}
}

func TestValidateTypeSchema_RejectsBadCases(t *testing.T) {
	cases := []struct {
		name   string
		ts     adapter.TypeSchema
		reason message.InstallReason
	}{
		{
			"empty-allowed-kinds",
			adapter.TypeSchema{},
			message.InstallTypeRegistryInvalid,
		},
		{
			"invalid-kind",
			adapter.TypeSchema{AllowedKinds: []message.Kind{"weird"}},
			message.InstallTypeRegistryInvalid,
		},
		{
			"schema-key-not-in-allowed",
			adapter.TypeSchema{
				AllowedKinds: []message.Kind{message.KindEvent},
				SchemasByKind: map[message.Kind]json.RawMessage{
					message.KindRequest: json.RawMessage(`{}`),
				},
			},
			message.InstallTypeRegistryInvalid,
		},
		{
			"missing-fallback-for-request",
			adapter.TypeSchema{
				AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
			},
			message.InstallFallbackResponseSchemaInvalid,
		},
		{
			"bad-terminal-convention",
			adapter.TypeSchema{
				AllowedKinds:       []message.Kind{message.KindEvent},
				TerminalConvention: "weird-mode",
			},
			message.InstallTypeRegistryInvalid,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTypeSchema("biz.x", tc.ts)
			if err == nil {
				t.Fatalf("expected error")
			}
			var ie *InstallError
			if !errors.As(err, &ie) {
				t.Fatalf("expected *InstallError, got %T: %v", err, err)
			}
			if ie.Reason != tc.reason {
				t.Errorf("reason=%s want=%s (err=%v)", ie.Reason, tc.reason, err)
			}
		})
	}
}
