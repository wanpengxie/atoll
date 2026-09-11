package api

import (
	"encoding/json"
	"testing"
)

func TestStartInputSchemaMatchesFixedAssignmentWire(t *testing.T) {
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal([]byte(StartInputSchema), &schema); err != nil {
		t.Fatal(err)
	}
	for _, retired := range []string{"prompt", "tools"} {
		if _, ok := schema.Properties[retired]; ok {
			t.Fatalf("assignment schema still exposes Looper-owned %q", retired)
		}
	}
	var selection struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(schema.Properties["selection"], &selection); err != nil {
		t.Fatal(err)
	}
	if _, ok := selection.Properties["effort"]; !ok {
		t.Fatal("assignment schema omits the selection.effort wire field")
	}
}
