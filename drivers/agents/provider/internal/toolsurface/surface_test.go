package toolsurface

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/lib/metatool"
)

func canonicalCatalog() []driverproto.ToolSpec {
	tools := metatool.MetaTools()
	out := make([]driverproto.ToolSpec, 0, len(tools))
	for _, tool := range tools {
		out = append(out, driverproto.ToolSpec{Name: tool.Spec.Name, Description: tool.Spec.Description, Schema: tool.Spec.Schema})
	}
	return out
}

func TestProviderCatalogAndResultGolden(t *testing.T) {
	codex, err := Assemble(canonicalCatalog(), Codex)
	if err != nil {
		t.Fatal(err)
	}
	claude, err := Assemble(canonicalCatalog(), Claude)
	if err != nil {
		t.Fatal(err)
	}
	if len(codex.Entries()) != 9 || len(claude.Entries()) != 9 {
		t.Fatalf("catalog sizes codex=%d claude=%d", len(codex.Entries()), len(claude.Entries()))
	}
	for i, left := range codex.Entries() {
		right := claude.Entries()[i]
		if left.Canonical != right.Canonical || left.Spec.Description != right.Spec.Description || !reflect.DeepEqual(left.Spec.Schema, right.Spec.Schema) {
			t.Fatalf("catalog drift at %d: codex=%+v claude=%+v", i, left, right)
		}
		if left.Wire != left.Canonical || right.Wire != right.Canonical || right.Exposed != ClaudeExposedPrefix+right.Canonical {
			t.Fatalf("name projection drift: codex=%+v claude=%+v", left, right)
		}
	}
	canonical := driverproto.ToolResult{Text: `{"status":"accepted","to_wait":{"tool":"await_result","params":{"request_id":"r1"}},"guidance":"claim with await_result"}`}
	codexResult := codex.MapResult(canonical)
	claudeResult := claude.MapResult(canonical)
	var canonicalValue, codexValue any
	_ = json.Unmarshal([]byte(canonical.Text), &canonicalValue)
	_ = json.Unmarshal([]byte(codexResult.Text), &codexValue)
	if !reflect.DeepEqual(codexValue, canonicalValue) {
		t.Fatalf("codex result=%s", codexResult.Text)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(claudeResult.Text), &got); err != nil {
		t.Fatal(err)
	}
	wantTool := ClaudeExposedPrefix + "await_result"
	if got["to_wait"].(map[string]any)["tool"] != wantTool || !strings.Contains(got["guidance"].(string), wantTool) {
		t.Fatalf("claude mapped result=%v", got)
	}
	if !strings.Contains(claude.Guide(), wantTool) || strings.Contains(codex.Guide(), ClaudeExposedPrefix) {
		t.Fatalf("guides drifted: codex=%q claude=%q", codex.Guide(), claude.Guide())
	}
}

func TestCatalogAndResultBounds(t *testing.T) {
	tooMany := make([]driverproto.ToolSpec, MaxCatalogTools+1)
	for i := range tooMany {
		tooMany[i] = driverproto.ToolSpec{Name: string(rune('a' + i)), Schema: json.RawMessage(`{"type":"object"}`)}
	}
	if _, err := Assemble(tooMany, Codex); err == nil {
		t.Fatal("oversized catalog accepted")
	}
	surface, err := Assemble(canonicalCatalog(), Codex)
	if err != nil {
		t.Fatal(err)
	}
	result := surface.MapResult(driverproto.ToolResult{Text: strings.Repeat("x", MaxResultBytes+1)})
	if !result.IsError || !strings.Contains(result.Text, "transport limit") {
		t.Fatalf("oversized result=%+v", result)
	}
}

func TestExecutionFailuresAreAlwaysStructured(t *testing.T) {
	surface, err := Assemble(canonicalCatalog(), Codex)
	if err != nil {
		t.Fatal(err)
	}
	got := surface.MapResult(driverproto.ToolResult{Text: "effect scope is not open", IsError: true})
	if !got.IsError || !json.Valid([]byte(got.Text)) || !strings.Contains(got.Text, `"ok":false`) {
		t.Fatalf("mapped failure=%+v", got)
	}
}

func TestResultNameMappingRespectsIdentifierBoundaries(t *testing.T) {
	surface, err := Assemble(canonicalCatalog(), Claude)
	if err != nil {
		t.Fatal(err)
	}
	got := surface.MapResult(driverproto.ToolResult{Text: `{"state":"cancelled","tool":"cancel","guidance":"call cancel(request_id)"}`})
	if !strings.Contains(got.Text, `"state":"cancelled"`) || strings.Contains(got.Text, "mcp__atoll__cancelled") || !strings.Contains(got.Text, `"tool":"mcp__atoll__cancel"`) {
		t.Fatalf("mapped result=%s", got.Text)
	}
}
