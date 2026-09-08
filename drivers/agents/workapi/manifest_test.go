package workapi

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/lib/introspect"
)

func TestManifestPublishesOneWorkAwareAgentContract(t *testing.T) {
	m := Manifest("native", map[string]bool{CapabilityMultiWork: true, CapabilityWorkTextSteer: true})
	if err := introspect.ValidateManifest(m); err != nil {
		t.Fatal(err)
	}
	if m.Class != "native" || !m.Capabilities[CapabilityWorkProtocolV1] || !m.Capabilities[CapabilityMultiWork] || !m.Capabilities[CapabilityWorkTextSteer] {
		t.Fatalf("manifest identity/capabilities = %+v", m)
	}
	for _, word := range []string{TypeAsk, TypeStatus, TypeResult, TypeSteer, TypeInterrupt} {
		spec, ok := m.Words[word]
		if !ok || len(spec.InputSchema) == 0 || len(spec.Description) == 0 {
			t.Fatalf("word %s = %+v, present=%v", word, spec, ok)
		}
		var schema map[string]any
		if err := json.Unmarshal(spec.InputSchema, &schema); err != nil {
			t.Fatalf("word %s schema: %v", word, err)
		}
	}
	if len(m.Words) != 5 {
		t.Fatalf("unexpected words: %v", m.Words)
	}
}

func TestManifestDoesNotMutateCallerCapabilities(t *testing.T) {
	in := map[string]bool{CapabilityMultiWork: false}
	_ = Manifest("native", in)
	if _, ok := in[CapabilityWorkProtocolV1]; ok {
		t.Fatalf("caller capability map was mutated: %v", in)
	}
}
