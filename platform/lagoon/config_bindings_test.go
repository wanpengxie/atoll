package lagoon

import (
	"encoding/json"
	"testing"
)

func TestCreationBindingsAreExplicitAndClassIndependent(t *testing.T) {
	for _, field := range []string{"host", "custom_handle_destination"} {
		raw, err := bindCreationConfig(json.RawMessage(`{"host":"unrelated-channel","business":42}`), map[string]string{field: "parent_channel_id"}, "parent-id")
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got[field] != "parent-id" || got["business"] != float64(42) {
			t.Fatalf("bound=%s", raw)
		}
		if field != "host" && got["host"] != "unrelated-channel" {
			t.Fatalf("unselected host overwritten: %s", raw)
		}
	}
	if _, err := bindCreationConfig(nil, map[string]string{"host": "unknown"}, "parent-id"); err == nil {
		t.Fatal("unknown source accepted")
	}
}
