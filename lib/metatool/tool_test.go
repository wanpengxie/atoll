package metatool

import (
	"testing"
)

func TestPayloadHintWithActorAndType(t *testing.T) {
	hint := payloadHint("tool:xhs", "xhs.publish")
	expected := `Call describe_type("tool:xhs", "xhs.publish") to see payload_example`
	if hint != expected {
		t.Fatalf("expected %q, got %q", expected, hint)
	}
}

func TestPayloadHintEmpty(t *testing.T) {
	hint := payloadHint("", "")
	expected := "Call list_actors to see actors, then describe_actor for their types"
	if hint != expected {
		t.Fatalf("expected %q, got %q", expected, hint)
	}
}

func TestPayloadHintPartialActorOnly(t *testing.T) {
	hint := payloadHint("tool:xhs", "")
	expected := "Call list_actors to see actors, then describe_actor for their types"
	if hint != expected {
		t.Fatalf("expected %q, got %q", expected, hint)
	}
}

func TestPayloadHintPartialTypeOnly(t *testing.T) {
	hint := payloadHint("", "xhs.publish")
	expected := "Call list_actors to see actors, then describe_actor for their types"
	if hint != expected {
		t.Fatalf("expected %q, got %q", expected, hint)
	}
}

func TestToolSpecFields(t *testing.T) {
	// Verify the ToolSpec struct can hold data correctly (data-only type).
	spec := ToolSpec{
		Name:        "test_tool",
		Description: "A test tool",
		Schema:      []byte(`{"type":"object"}`),
	}
	if spec.Name != "test_tool" {
		t.Fatalf("expected name=test_tool, got %q", spec.Name)
	}
	if spec.Description != "A test tool" {
		t.Fatalf("expected description=A test tool, got %q", spec.Description)
	}
	if string(spec.Schema) != `{"type":"object"}` {
		t.Fatalf("expected schema, got %q", string(spec.Schema))
	}
}
