package mcp

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTranslateToolKeepsRawSchemaAndOnlyFlattensTopLevel(t *testing.T) {
	raw := json.RawMessage(`{"type":"object","$defs":{"Customer":{"type":"object"}},"properties":{"customer":{"$ref":"#/$defs/Customer"},"level":{"type":"string","enum":["low","high"],"description":"Priority."}},"required":["customer"]}`)
	meta := translateTool(tool{Name: "create_order", Description: "Create.", InputSchema: raw})
	if !strings.HasPrefix(meta.Notes, "过渡形：") || !strings.Contains(meta.Notes, string(raw)) {
		t.Fatalf("raw schema not preserved in transitional notes: %q", meta.Notes)
	}
	if len(meta.PayloadFields) != 2 {
		t.Fatalf("payload fields=%#v", meta.PayloadFields)
	}
	byName := map[string]bool{}
	for _, field := range meta.PayloadFields {
		byName[field.Name] = field.Required
	}
	if !byName["customer"] || byName["level"] {
		t.Fatalf("required translation=%v", byName)
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
