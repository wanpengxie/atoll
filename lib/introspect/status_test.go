package introspect

import (
	"encoding/json"
	"testing"
)

func TestQueryStatusName(t *testing.T) {
	if QueryStatus != "actor.status" {
		t.Fatalf("QueryStatus drifted: %q", QueryStatus)
	}
}

func TestAnswerStatus_RoundTrip(t *testing.T) {
	got := AnswerStatus("tool:xhs", map[string]any{"device_online": true})
	if got.ActorID != "tool:xhs" {
		t.Fatalf("actor_id=%q want tool:xhs", got.ActorID)
	}
	if v, ok := got.Snapshot["device_online"].(bool); !ok || !v {
		t.Fatalf("snapshot device_online=%v want true", got.Snapshot["device_online"])
	}

	// Wire shape: marshal → the slot key is "status_snapshot" (never "status",
	// which the response wrapper reserves for the terminal), and actor_id is present.
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(b, &keys); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, k := range []string{"actor_id", "status_snapshot"} {
		if _, ok := keys[k]; !ok {
			t.Fatalf("Status wire shape missing %q: %s", k, b)
		}
	}
	if _, collides := keys["status"]; collides {
		t.Fatalf("Status wire shape has a top-level \"status\" key (collides with terminal): %s", b)
	}
}

func TestAnswerStatus_NilSnapshotNormalised(t *testing.T) {
	got := AnswerStatus("echo", nil)
	if got.Snapshot == nil {
		t.Fatal("nil snapshot not normalised to empty object")
	}
	b, _ := json.Marshal(got)
	if string(b) != `{"actor_id":"echo","status_snapshot":{}}` {
		t.Fatalf("nil-snapshot wire = %s", b)
	}
}

func TestParseStatusRequest(t *testing.T) {
	if _, err := ParseStatusRequest(nil); err != nil {
		t.Fatalf("nil payload: err=%v", err)
	}
	if _, err := ParseStatusRequest([]byte(`{}`)); err != nil {
		t.Fatalf("empty-object payload: err=%v", err)
	}
	if _, err := ParseStatusRequest([]byte(`{`)); err == nil {
		t.Fatal("malformed payload: want error")
	}
}
