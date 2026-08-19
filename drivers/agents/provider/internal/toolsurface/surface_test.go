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
	codex, err := Assemble(canonicalCatalog(), Codex, driverproto.Situation{})
	if err != nil {
		t.Fatal(err)
	}
	claude, err := Assemble(canonicalCatalog(), Claude, driverproto.Situation{})
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
	if _, err := Assemble(tooMany, Codex, driverproto.Situation{}); err == nil {
		t.Fatal("oversized catalog accepted")
	}
	surface, err := Assemble(canonicalCatalog(), Codex, driverproto.Situation{})
	if err != nil {
		t.Fatal(err)
	}
	result := surface.MapResult(driverproto.ToolResult{Text: strings.Repeat("x", MaxResultBytes+1)})
	if !result.IsError || !strings.Contains(result.Text, "transport limit") {
		t.Fatalf("oversized result=%+v", result)
	}
}

func TestExecutionFailuresAreAlwaysStructured(t *testing.T) {
	surface, err := Assemble(canonicalCatalog(), Codex, driverproto.Situation{})
	if err != nil {
		t.Fatal(err)
	}
	got := surface.MapResult(driverproto.ToolResult{Text: "effect scope is not open", IsError: true})
	if !got.IsError || !json.Valid([]byte(got.Text)) || !strings.Contains(got.Text, `"ok":false`) {
		t.Fatalf("mapped failure=%+v", got)
	}
}

func TestResultNameMappingRespectsIdentifierBoundaries(t *testing.T) {
	surface, err := Assemble(canonicalCatalog(), Claude, driverproto.Situation{})
	if err != nil {
		t.Fatal(err)
	}
	got := surface.MapResult(driverproto.ToolResult{Text: `{"state":"cancelled","tool":"cancel","guidance":"call cancel(request_id)"}`})
	if !strings.Contains(got.Text, `"state":"cancelled"`) || strings.Contains(got.Text, "mcp__atoll__cancelled") || !strings.Contains(got.Text, `"tool":"mcp__atoll__cancel"`) {
		t.Fatalf("mapped result=%s", got.Text)
	}
}

// The guide's reach paragraph is the model's only statement of what it can do
// beyond its own channel, and the two homes genuinely differ: dispatch accepts
// membrane words from c0 and from nowhere else. A guide that told a sub-channel
// agent it could manage other channels would send it into guaranteed failures;
// a guide that told the c0 agent it could not — which is what shipped — cost an
// entire conversation in which c0 insisted a sub-channel roster was unreadable.
func TestGuideReachMatchesActualReach(t *testing.T) {
	guideFor := func(home driverproto.Situation) string {
		surface, err := Assemble(canonicalCatalog(), Codex, home)
		if err != nil {
			t.Fatal(err)
		}
		return surface.Guide()
	}

	core := guideFor(driverproto.Situation{Channel: "c0", IsCore: true})
	if !strings.Contains(core, "system.member.") {
		t.Error("c0 guide does not mention the membrane words c0 may send to a peer")
	}
	if !strings.Contains(core, "c0") {
		t.Error("c0 guide does not name the channel the agent is in")
	}

	leaf := guideFor(driverproto.Situation{Channel: "c0.project", IsCore: false})
	if strings.Contains(leaf, "system.member.*") {
		t.Error("sub-channel guide offers membrane words its peer will refuse")
	}
	if !strings.Contains(leaf, "c0.project") {
		t.Error("sub-channel guide does not name the channel the agent is in")
	}

	unknown := guideFor(driverproto.Situation{})
	if strings.Contains(unknown, "system.member.*") {
		t.Error("home-less guide claims a cross-channel reach it cannot verify")
	}

	for name, guide := range map[string]string{"core": core, "leaf": leaf, "unknown": unknown} {
		if !strings.Contains(guide, "list_actors") {
			t.Errorf("%s guide dropped the shared tool half", name)
		}
	}
}
