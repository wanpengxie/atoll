package coderunner

import (
	"encoding/json"
	"testing"
)

func TestRPCMessageClassification(t *testing.T) {
	var m rpcMessage
	if err := json.Unmarshal([]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"x"}}`), &m); err != nil || !m.isRequest() || m.isNotification() || m.isResponse() {
		t.Fatalf("request classified wrong: %+v err=%v", m, err)
	}
	m = rpcMessage{}
	if err := json.Unmarshal([]byte(`{"jsonrpc":"2.0","method":"notifications/message","params":{}}`), &m); err != nil || !m.isNotification() {
		t.Fatalf("notification classified wrong: %+v", m)
	}
	m = rpcMessage{}
	if err := json.Unmarshal([]byte(`{"jsonrpc":"2.0","id":"abc","result":{}}`), &m); err != nil || !m.isResponse() {
		t.Fatalf("response classified wrong: %+v", m)
	}
}

func TestToolNamesAreModelSafe(t *testing.T) {
	for in, want := range map[string]string{
		"echo__echo.say":               "echo__echo_say",
		"mcp:github__github.search":    "mcp_github__github_search",
		"system__system.member.list":   "system__system_member_list",
		"already-safe_Name-1__word_ok": "already-safe_Name-1__word_ok",
	} {
		if got := sanitizeToolName(in); got != want {
			t.Fatalf("sanitize(%q)=%q want %q", in, got, want)
		}
	}
	if toolName("mcp:github", "github.search") != "mcp_github__github_search" {
		t.Fatal("toolName did not compose requirement and word")
	}
}

func TestInputSchemaAndArgumentsRoundTrip(t *testing.T) {
	object := json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}}}`)
	if got := mcpInputSchema(object); string(got) != string(object) {
		t.Fatalf("object schema must pass through, got %s", got)
	}
	if got := mcpInputSchema(nil); string(got) != `{"type":"object"}` {
		t.Fatalf("empty schema must become an open object, got %s", got)
	}
	var wrapped map[string]any
	if err := json.Unmarshal(mcpInputSchema(json.RawMessage(`{"type":"string"}`)), &wrapped); err != nil || wrapped["type"] != "object" {
		t.Fatalf("scalar schema must be wrapped, got %v", wrapped)
	}
	if got := wordInput(json.RawMessage(`{"$input":"hi"}`)); string(got) != `"hi"` {
		t.Fatalf("wrapped input must unwrap, got %s", got)
	}
	if got := wordInput(json.RawMessage(`{"text":"hi"}`)); string(got) != `{"text":"hi"}` {
		t.Fatalf("object input must pass through, got %s", got)
	}
	if got := wordInput(nil); string(got) != "null" {
		t.Fatalf("absent arguments must become a null input, got %s", got)
	}
}
