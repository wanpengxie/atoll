package contract

import (
	"bytes"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/registry"
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

func TestExperimentalMethodsAreNamespaced(t *testing.T) {
	var found bool
	for _, method := range Methods() {
		prefixed := strings.HasPrefix(method.Path, "/api/experimental/")
		if method.Experimental != prefixed {
			t.Errorf("%s %s experimental=%v", method.Method, method.Path, method.Experimental)
		}
		found = found || method.Experimental
	}
	if !found {
		t.Fatal("experimental mechanism has no reachable vertical slice")
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

func TestAgentMessagePayloadSchemaIsOpenAndProviderFacing(t *testing.T) {
	def := schemaDefinitions()["AgentMessagePayload"].(map[string]any)
	if def["additionalProperties"] != true {
		t.Fatalf("agent payload content vocabulary must stay open: %#v", def)
	}
	props := def["properties"].(map[string]any)
	for _, name := range []string{"expected_turn_id", "text"} {
		if _, ok := props[name]; !ok {
			t.Fatalf("agent payload schema missing %q", name)
		}
	}
}

func TestSubmitSchemaDocumentsFrozenIdempotencyFingerprint(t *testing.T) {
	description, _ := schemaDefinitions()["SubmitPayload"].(map[string]any)["description"].(string)
	for _, term := range []string{"(channel_id,id)", "msg_type", "payload", "audience", "expires_at_ms", "excludes ref"} {
		if !strings.Contains(description, term) {
			t.Fatalf("SubmitPayload fingerprint description omits %q: %s", term, description)
		}
	}
}

func TestWebSocketErrorSchemaExposesIdempotencyAsKnownValue(t *testing.T) {
	props := schemaDefinitions()["ErrorPayload"].(map[string]any)["properties"].(map[string]any)
	code := props["code"].(map[string]any)
	if _, closed := code["enum"]; closed {
		t.Fatalf("downstream websocket codes must allow unknown values: %#v", code)
	}
	known := code["x-known-values"].([]string)
	for _, value := range known {
		if value == subjectgate.CodeIdempotencyConflict {
			return
		}
	}
	t.Fatalf("websocket code hints omit %q: %v", subjectgate.CodeIdempotencyConflict, known)
}

func TestActivityRegistryDrivesContractSchemas(t *testing.T) {
	table := activityTable()
	defs := schemaDefinitions()
	decls := registry.ActivityTypes()
	// 1:1 mapping is the invariant; the vocabulary COUNT is deliberately not
	// pinned (activity types are append-only — a literal count would redden on
	// every legitimate addition, archtest 纪律"红=改错了恒非改了").
	if len(table) != len(decls) {
		t.Fatalf("activity table=%d registry=%d", len(table), len(decls))
	}
	for _, decl := range decls {
		wireType := string(decl.Type)
		if !strings.HasPrefix(wireType, "activity.") {
			t.Fatalf("activity type is not fully qualified: %q", wireType)
		}
		entry, ok := table[wireType]
		if !ok || entry.Payload != decl.SchemaName {
			t.Fatalf("activity %q mapping=%+v", wireType, entry)
		}
		if _, ok := defs[decl.SchemaName]; !ok {
			t.Fatalf("activity %q schema %q missing", wireType, decl.SchemaName)
		}
	}
}
