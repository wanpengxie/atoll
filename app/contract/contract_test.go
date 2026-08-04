package contract

import (
	"bytes"
	"os"
	"reflect"
	"testing"
)

func TestGoldenSchemaIsCurrent(t *testing.T) {
	want, err := GenerateSchema()
	if err != nil {
		t.Fatal(err)
	}
	want = append(want, '\n')
	got, err := os.ReadFile("testdata/engine-api.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("golden schema drifted; run go generate ./app/contract")
	}
}

func TestRegistryMethodsCarrySince(t *testing.T) {
	for _, method := range Methods() {
		if method.Since == "" {
			t.Errorf("%s %s has no since version", method.Method, method.Path)
		}
	}
}

func TestErrorCodeVocabularyIsUnique(t *testing.T) {
	seen := map[ErrorCode]bool{}
	for _, code := range ErrorCodes() {
		if code == "" || seen[code] {
			t.Fatalf("invalid or duplicate error code %q", code)
		}
		seen[code] = true
	}
}

func TestNormalizeErrorCodeClosesProducerVocabulary(t *testing.T) {
	if got := NormalizeErrorCode(CodeForbidden); got != CodeForbidden {
		t.Fatalf("known code normalized to %q", got)
	}
	if got := NormalizeErrorCode(ErrorCode("internal_subsystem_code")); got != CodeInternal {
		t.Fatalf("unknown code normalized to %q, want %q", got, CodeInternal)
	}
}

func TestSchemaDescribesConcreteRESTInputsAndOutputs(t *testing.T) {
	defs := schemaDefinitions()

	assertProperties := func(name string, want ...string) map[string]any {
		t.Helper()
		def, ok := defs[name].(map[string]any)
		if !ok {
			t.Fatalf("%s schema is %T", name, defs[name])
		}
		props, ok := def["properties"].(map[string]any)
		if !ok {
			t.Fatalf("%s has no concrete properties: %#v", name, def)
		}
		for _, field := range want {
			if _, ok := props[field]; !ok {
				t.Errorf("%s schema missing %q", name, field)
			}
		}
		return def
	}

	query := assertProperties("MessagePageQuery", "after_seq", "limit")
	if query["additionalProperties"] != false {
		t.Fatalf("request query must be fail-closed: %#v", query)
	}
	assertProperties("ChannelPath", "chID")
	assertProperties("Principal", "id", "email", "display_name")
	assertProperties("Channel", "id", "parent_id", "owner_principal")
	assertProperties("MessagePage", "messages", "scanned_through_seq")
}

func TestSchemaKeepsConsumerUnknownValueFallback(t *testing.T) {
	defs := schemaDefinitions()
	errorProps := defs["Error"].(map[string]any)["properties"].(map[string]any)
	code := errorProps["code"].(map[string]any)
	if _, closed := code["enum"]; closed {
		t.Fatalf("downstream error code must not be a JSON Schema enum: %#v", code)
	}
	if !reflect.DeepEqual(code["x-known-values"], ErrorCodes()) {
		t.Fatalf("error known values=%#v want %#v", code["x-known-values"], ErrorCodes())
	}

	upstream := defs["UpstreamEnvelope"].(map[string]any)["properties"].(map[string]any)["frame_type"].(map[string]any)
	if _, ok := upstream["enum"]; !ok {
		t.Fatalf("upstream frame type must be closed: %#v", upstream)
	}
	downstream := defs["DownstreamEnvelope"].(map[string]any)["properties"].(map[string]any)["frame_type"].(map[string]any)
	if _, closed := downstream["enum"]; closed {
		t.Fatalf("downstream frame type must allow unknown values: %#v", downstream)
	}
	if _, ok := downstream["x-known-values"]; !ok {
		t.Fatalf("downstream frame type lost its known-value hints: %#v", downstream)
	}
}
