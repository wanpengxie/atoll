package base

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/introspect"
)

// agent.select's value domain rides the instance manifest (design §4.2): one
// oneOf branch per legal (model, effort) pair, every branch requiring the FULL
// pair, titles beside the consts. A spec without selections keeps the plain
// class word — the frontend reads "no oneOf" as "no switching menu".

func selectSchemaOf(t *testing.T, spec runtimeproto.Spec) map[string]any {
	t.Helper()
	m := instanceManifest(spec)
	if err := introspect.ValidateManifest(m); err != nil {
		t.Fatal(err)
	}
	raw := m.Words[TypeSelect].InputSchema
	if len(raw) == 0 {
		return nil
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("schema unmarshal: %v", err)
	}
	return schema
}

func TestSelectManifestWeavesSelectionsAsOneOfPairs(t *testing.T) {
	spec := runtimeproto.Spec{
		Selections: []runtimeproto.TurnOptions{
			{Model: "claude-opus-5", Effort: "medium"},
			{Model: "claude-opus-5", Effort: "high"},
			{Model: "claude-sonnet-5", Effort: "medium"},
		},
		SelectionTitles: []runtimeproto.SelectionTitle{
			{Model: "Opus", Effort: "中"},
			{Model: "Opus", Effort: "高"},
			{},
		},
	}
	schema := selectSchemaOf(t, spec)
	if schema == nil {
		t.Fatal("selections configured but no schema woven")
	}
	branches := schema["oneOf"].([]any)
	if len(branches) != 3 {
		t.Fatalf("oneOf branches = %d", len(branches))
	}
	// EVERY branch must require the full pair by name — without required, {}
	// matches several branches and oneOf semantics break (design §4.2).
	for i, raw := range branches {
		required := raw.(map[string]any)["required"].([]any)
		if len(required) != 2 || required[0] != "model" || required[1] != "effort" {
			t.Fatalf("branch %d required=%v", i, required)
		}
	}
	first := branches[0].(map[string]any)
	model := first["properties"].(map[string]any)["model"].(map[string]any)
	if model["const"] != "claude-opus-5" || model["title"] != "Opus" {
		t.Fatalf("first model spec = %v", model)
	}
	// A selection without titles states bare consts — no empty-string titles.
	third := branches[2].(map[string]any)
	if _, has := third["properties"].(map[string]any)["model"].(map[string]any)["title"]; has {
		t.Fatal("untitled selection must not carry an empty title")
	}
	// additionalProperties stays open: backend decoding is lenient, the schema
	// must not pretend to be strict.
	if _, has := schema["additionalProperties"]; has {
		t.Fatal("schema must not claim additionalProperties:false")
	}
}

func TestSelectManifestWithoutSelectionsKeepsClassWord(t *testing.T) {
	if schema := selectSchemaOf(t, runtimeproto.Spec{}); schema != nil {
		t.Fatalf("no selections but schema woven: %v", schema)
	}
}

func TestSelectManifestInstancesDoNotShareState(t *testing.T) {
	a := selectSchemaOf(t, runtimeproto.Spec{Selections: []runtimeproto.TurnOptions{{Model: "m-a", Effort: "low"}}})
	b := selectSchemaOf(t, runtimeproto.Spec{Selections: []runtimeproto.TurnOptions{{Model: "m-b", Effort: "high"}}})
	modelOf := func(schema map[string]any) string {
		branch := schema["oneOf"].([]any)[0].(map[string]any)
		return branch["properties"].(map[string]any)["model"].(map[string]any)["const"].(string)
	}
	if modelOf(a) != "m-a" || modelOf(b) != "m-b" {
		t.Fatalf("instance schemas crossed: a=%s b=%s", modelOf(a), modelOf(b))
	}
	// The shared class-level word must stay untouched (map aliasing guard).
	if len(Manifest("agent", nil).Words[TypeSelect].InputSchema) != 0 {
		t.Fatal("class-level agent.select word was mutated by instance weaving")
	}
}
