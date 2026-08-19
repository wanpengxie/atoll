package mcp

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTranslateToolKeepsRawSchemasAndOnlyFlattensTopLevel(t *testing.T) {
	input := json.RawMessage(`{"type":"object","$defs":{"Customer":{"type":"object"}},"properties":{"customer":{"$ref":"#/$defs/Customer"},"level":{"type":"string","enum":["low","high"],"description":"Priority."}},"required":["customer"]}`)
	output := json.RawMessage(`{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"]}`)
	meta := translateTool(tool{Name: "create_order", Description: "Create.", InputSchema: input, OutputSchema: output})
	if string(meta.InputSchema) != string(input) || string(meta.OutputSchema) != string(output) {
		t.Fatalf("schemas changed: input=%s output=%s", meta.InputSchema, meta.OutputSchema)
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	for _, legacy := range []string{"notes", "payload_fields"} {
		if strings.Contains(string(raw), legacy) {
			t.Fatalf("legacy field %q leaked: %s", legacy, raw)
		}
	}
}
func TestTranslateInputRequiredCarriesContinuation(t *testing.T) {
	state := "opaque-state"
	payload, _, err := translateCallResult(callResult{
		ResultType:    "input_required",
		InputRequests: json.RawMessage(`{"confirm":{"method":"elicitation/create","params":{"message":"Continue?"}}}`),
		RequestState:  &state,
	})
	if err != nil {
		t.Fatal(err)
	}
	continuation := payload["_continuation"].(map[string]any)
	if continuation["reason"] != "input_required" || continuation["state"] != state {
		t.Fatalf("continuation=%#v", continuation)
	}
	if _, ok := continuation["requests"].(map[string]any)["confirm"]; !ok {
		t.Fatalf("requests=%#v", continuation["requests"])
	}
}

func TestTranslateCallResultSeparatesTextNonTextAndStructured(t *testing.T) {
	result := callResult{
		Content: []json.RawMessage{
			json.RawMessage(`{"type":"text","text":"a"}`),
			json.RawMessage(`{"type":"image","data":"AA==","mimeType":"image/png"}`),
			json.RawMessage(`{"type":"text","text":"b"}`),
		},
		StructuredContent: json.RawMessage(`{"n":2}`),
	}
	payload, _, err := translateCallResult(result)
	if err != nil {
		t.Fatal(err)
	}
	if payload["text"] != "ab" || len(payload["content"].([]any)) != 1 {
		t.Fatalf("payload=%#v", payload)
	}
	if payload["structured_content"].(map[string]any)["n"] != float64(2) {
		t.Fatalf("structured=%#v", payload["structured_content"])
	}
}

func TestBuildSnapshotTypePrefixComesOnlyFromConfigName(t *testing.T) {
	discover := discovery{Instructions: "Use the fixture exactly."}
	discover.Meta.ServerInfo = json.RawMessage(`{"name":"self-reported","title":"Self-reported title","version":"1.2.3","description":"Self-reported description.","website_url":"https://server.example","icons":[{"src":"https://server.example/icon.png","mimeType":"image/png"}]}`)
	tools := toolList{Tools: []tool{
		{Name: "echo", InputSchema: json.RawMessage(`{"type":"object"}`)},
		{Name: "add", InputSchema: json.RawMessage(`{"type":"object"}`)},
	}}

	first := buildSnapshot("local-one", discover, tools)
	second := buildSnapshot("local-two", discover, tools)
	for _, toolName := range []string{"echo", "add"} {
		if _, ok := first.types["local-one."+toolName]; !ok {
			t.Fatalf("first snapshot omitted local-one.%s: %v", toolName, first.types)
		}
		if _, ok := second.types["local-two."+toolName]; !ok {
			t.Fatalf("second snapshot omitted local-two.%s: %v", toolName, second.types)
		}
		if _, ok := second.types["local-one."+toolName]; ok {
			t.Fatalf("renamed snapshot retained old prefix for %s: %v", toolName, second.types)
		}
	}
	for _, text := range []string{first.description, first.skillDoc} {
		for _, want := range []string{
			"self-reported", "Self-reported title", "1.2.3", "Self-reported description.",
			"https://server.example", "https://server.example/icon.png", discover.Instructions, "本地名是 `local-one`",
		} {
			if !strings.Contains(text, want) {
				t.Fatalf("self-description %q omitted %q", text, want)
			}
		}
	}
}

func TestCacheHintsStayInternalToDescribe(t *testing.T) {
	discover := discovery{SupportedVersions: []string{protocolVersion}, TTLMS: 60_000, CacheScope: "public"}
	listing := toolList{
		TTLMS: 0, CacheScope: "private",
		Tools: []tool{{Name: "echo", InputSchema: json.RawMessage(`{"type":"object"}`)}},
	}
	snapshot := buildSnapshot("srv", discover, listing)
	raw, err := json.Marshal(snapshot.types)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "ttlMs") || strings.Contains(string(raw), "cacheScope") {
		t.Fatalf("cache hint leaked into describe types: %s", raw)
	}
}
