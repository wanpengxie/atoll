package framework

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func newBytesReader(b []byte) io.Reader { return bytes.NewReader(b) }

// ValidateSchemaShape runs structural checks on a JSON Schema fragment
// (raw bytes). It is intentionally minimal — it only verifies the bytes
// parse to a JSON object and that the top-level `type` field, when
// present, is "object" / "array" / a scalar type per JSON Schema
// Draft 2020-12.
//
// The strict per-payload validator lives in ValidatePayload below; the
// shape check exists so install can refuse obviously malformed schemas
// without paying the cost of building a validator pipeline.
//
// Returns nil for empty / nil input (caller decides whether emptiness
// is acceptable). Returns an error wrapping the offending key when
// shape is wrong.
func ValidateSchemaShape(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		return fmt.Errorf("schema parse: %w", err)
	}
	if t, ok := v["type"].(string); ok {
		switch t {
		case "object", "array", "string", "number", "integer", "boolean", "null":
			// ok
		default:
			return fmt.Errorf("schema: unknown top-level type %q", t)
		}
	}
	return nil
}

// ValidatePayload runs the harness step 6 payload schema check (L1
// §10.2). The validator supports a JSON Schema subset adequate for M1.5
// install-time validation:
//
//   - top-level type=object: enforce required[] non-empty keys present
//     and properties[*].type matches the actual value type
//   - top-level type=array: enforce items.type when present
//   - top-level enum: payload must equal one of enum values
//
// Larger JSON Schema features (oneOf, anyOf, $ref, allOf, format, etc.)
// are accepted-as-valid (treated as no-op) so adapters can write richer
// schemas without breaking install. FIX-T9 may swap in a full Draft
// 2020-12 validator (e.g. santhosh-tekuri/jsonschema) without touching
// callers.
//
// Returns nil when payload satisfies the schema. Returns a non-nil
// error describing the first mismatch otherwise.
func ValidatePayload(schema json.RawMessage, payload []byte) error {
	if len(schema) == 0 {
		// No schema → accept any well-formed JSON object (harness step 6
		// guarantees the caller passed payload through normalize first).
		return nil
	}
	var s map[string]any
	if err := json.Unmarshal(schema, &s); err != nil {
		return fmt.Errorf("schema parse: %w", err)
	}

	var v any
	if len(payload) == 0 {
		return errors.New("payload empty")
	}
	dec := json.NewDecoder(newBytesReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return fmt.Errorf("payload parse: %w", err)
	}

	return validateAgainst(s, v)
}

// validateAgainst runs the minimal JSON Schema subset against `v`.
func validateAgainst(schema map[string]any, v any) error {
	if enumRaw, ok := schema["enum"]; ok {
		enum, _ := enumRaw.([]any)
		if !enumContains(enum, v) {
			return fmt.Errorf("value not in enum: %v", v)
		}
		return nil
	}

	t, _ := schema["type"].(string)
	switch t {
	case "object":
		obj, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("expected object, got %T", v)
		}
		if req, ok := schema["required"].([]any); ok {
			for _, k := range req {
				key, _ := k.(string)
				if _, present := obj[key]; !present {
					return fmt.Errorf("required field %q missing", key)
				}
			}
		}
		if props, ok := schema["properties"].(map[string]any); ok {
			for k, sub := range props {
				subSchema, _ := sub.(map[string]any)
				if subSchema == nil {
					continue
				}
				val, present := obj[k]
				if !present {
					continue
				}
				if err := validateAgainst(subSchema, val); err != nil {
					return fmt.Errorf("field %q: %w", k, err)
				}
			}
		}
		return nil
	case "array":
		arr, ok := v.([]any)
		if !ok {
			return fmt.Errorf("expected array, got %T", v)
		}
		if items, ok := schema["items"].(map[string]any); ok {
			for i, el := range arr {
				if err := validateAgainst(items, el); err != nil {
					return fmt.Errorf("items[%d]: %w", i, err)
				}
			}
		}
		return nil
	case "string":
		if _, ok := v.(string); !ok {
			return fmt.Errorf("expected string, got %T", v)
		}
	case "number", "integer":
		switch n := v.(type) {
		case json.Number:
			if t == "integer" {
				if _, err := n.Int64(); err != nil {
					return fmt.Errorf("expected integer, got %s", n)
				}
			}
		case float64:
			// json.Decoder without UseNumber would yield float64; tolerate.
		default:
			return fmt.Errorf("expected number, got %T", v)
		}
	case "boolean":
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("expected bool, got %T", v)
		}
	case "null":
		if v != nil {
			return fmt.Errorf("expected null, got %T", v)
		}
	case "":
		// No type constraint — pass.
	}
	return nil
}

func enumContains(enum []any, v any) bool {
	for _, e := range enum {
		if fmt.Sprintf("%v", e) == fmt.Sprintf("%v", v) {
			return true
		}
	}
	return false
}

// fallbackResponseSamples returns the 3 system fallback payloads the
// L2 §1.4.2 install rule mandates a response schema accept.
func fallbackResponseSamples() [][]byte {
	return [][]byte{
		[]byte(`{"status":"failed","reason":"unanswered_timeout"}`),
		[]byte(`{"status":"failed","reason":"adapter_default_timeout"}`),
		[]byte(`{"status":"failed","reason":"receiver_unavailable"}`),
	}
}

// ValidateFallbackResponseSchema runs the 3-sample check L2 §1.4.2
// install requires for any type whose AllowedKinds includes request:
// the schema MUST accept each fallback payload. Returns the offending
// sample's error on the first failure.
func ValidateFallbackResponseSchema(schema json.RawMessage) error {
	if len(schema) == 0 {
		return errors.New("fallback response schema empty")
	}
	for _, sample := range fallbackResponseSamples() {
		if err := ValidatePayload(schema, sample); err != nil {
			return fmt.Errorf("sample %s: %w", string(sample), err)
		}
	}
	return nil
}

// ValidateTypeSchema runs the install-time validation rules for one
// type's TypeSchema view per L2 §1.4.2.
//
// Returns nil on success. Returns an *InstallError wrapping the
// matching message.InstallReason on failure — caller errors.As-es it
// to map to RPC reject reason.
//
// nolint:gocyclo // closed-set install checks are intentionally inline.
func ValidateTypeSchema(typeName string, ts adapter.TypeSchema) error {
	if len(ts.AllowedKinds) == 0 {
		return fmt.Errorf("%w: type=%s allowed_kinds empty",
			asInstallError(message.InstallTypeRegistryInvalid), typeName)
	}
	seenKind := map[message.Kind]bool{}
	for _, k := range ts.AllowedKinds {
		switch k {
		case message.KindEvent, message.KindRequest, message.KindResponse:
		default:
			return fmt.Errorf("%w: type=%s allowed_kinds contains invalid kind %q",
				asInstallError(message.InstallTypeRegistryInvalid), typeName, k)
		}
		if seenKind[k] {
			return fmt.Errorf("%w: type=%s allowed_kinds contains duplicate %q",
				asInstallError(message.InstallTypeRegistryInvalid), typeName, k)
		}
		seenKind[k] = true
	}

	for k := range ts.SchemasByKind {
		if !seenKind[k] {
			return fmt.Errorf("%w: type=%s schemas_by_kind[%s] not in allowed_kinds",
				asInstallError(message.InstallTypeRegistryInvalid), typeName, k)
		}
	}

	for k, raw := range ts.SchemasByKind {
		if err := ValidateSchemaShape(raw); err != nil {
			return fmt.Errorf("%w: type=%s schemas_by_kind[%s] invalid: %v",
				asInstallError(message.InstallTypeRegistryInvalid), typeName, k, err)
		}
	}

	if seenKind[message.KindRequest] {
		if err := ValidateFallbackResponseSchema(ts.FallbackResponseSchema); err != nil {
			return fmt.Errorf("%w: type=%s: %v",
				asInstallError(message.InstallFallbackResponseSchemaInvalid), typeName, err)
		}
	}

	if ts.TerminalConvention != "" &&
		ts.TerminalConvention != string(TerminalPayloadStatus) &&
		ts.TerminalConvention != string(TerminalSingleResponse) {
		return fmt.Errorf("%w: type=%s terminal_convention %q invalid",
			asInstallError(message.InstallTypeRegistryInvalid), typeName, ts.TerminalConvention)
	}

	return nil
}
