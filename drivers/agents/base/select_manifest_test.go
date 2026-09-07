package base

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/introspect"
)

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

func TestSelectManifestIsStableAcrossProviderSelections(t *testing.T) {
	a := selectSchemaOf(t, runtimeproto.Spec{Selections: []runtimeproto.TurnOptions{{Model: "m-a", Effort: "low"}}})
	b := selectSchemaOf(t, runtimeproto.Spec{Selections: []runtimeproto.TurnOptions{{Model: "m-b", Effort: "high"}}})
	encodedA, _ := json.Marshal(a)
	encodedB, _ := json.Marshal(b)
	if string(encodedA) != string(encodedB) {
		t.Fatalf("dynamic values leaked into manifest: a=%s b=%s", encodedA, encodedB)
	}
	if _, ok := a["oneOf"]; ok {
		t.Fatal("agent.select values belong to agent.options, not oneOf")
	}
	required := a["required"].([]any)
	if len(required) != 1 || required[0] != "model" {
		t.Fatalf("required=%v", required)
	}
}

func TestOptionsWordIsOrdinaryActorWord(t *testing.T) {
	m := instanceManifest(runtimeproto.Spec{Name: "codex"})
	word, ok := m.Words[TypeOptions]
	if !ok || len(word.InputSchema) == 0 {
		t.Fatalf("agent.options word=%+v present=%v", word, ok)
	}
	if m.Class != "codex" {
		t.Fatalf("class=%q", m.Class)
	}
}
