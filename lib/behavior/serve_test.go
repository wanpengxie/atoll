package behavior

import (
	"encoding/json"
	"testing"
)

// MergeResponsePayload must treat an empty / JSON-null payload as an empty
// object: unmarshalling the literal `null` into a map nils it (no error), so
// without a guard the {status} merge would assign into a nil map and panic.
func TestMergeResponsePayload_NullAndEmptyTreatedAsObject(t *testing.T) {
	for _, in := range []string{"null", "", "{}", `{"a":1}`} {
		out, err := MergeResponsePayload(json.RawMessage(in), "completed", "")
		if err != nil {
			t.Fatalf("payload %q: unexpected err %v", in, err)
		}
		var m map[string]any
		if e := json.Unmarshal(out, &m); e != nil {
			t.Fatalf("payload %q: result not a JSON object: %v", in, e)
		}
		if m["status"] != "completed" {
			t.Fatalf("payload %q: status=%v want completed", in, m["status"])
		}
	}
}

// A non-object payload (number / array / bare string) is a real error, not a
// silent empty object.
func TestMergeResponsePayload_NonObjectRejected(t *testing.T) {
	for _, in := range []string{"5", "[]", `"x"`} {
		if _, err := MergeResponsePayload(json.RawMessage(in), "completed", ""); err == nil {
			t.Fatalf("payload %q: expected error for non-object payload", in)
		}
	}
}
