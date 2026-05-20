package xhs

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// TestDefaultInstallSpec asserts the canonical seed bundle: one tool
// actor + 5 R/R types + 1 event-only.
func TestDefaultInstallSpec(t *testing.T) {
	spec := DefaultInstallSpec(0)
	if spec.Actor.ID != DefaultAdapterActorID {
		t.Errorf("actor id=%q want %q", spec.Actor.ID, DefaultAdapterActorID)
	}
	if spec.Actor.Kind != actor.KindTool {
		t.Errorf("actor kind=%q want %q", spec.Actor.Kind, actor.KindTool)
	}
	if spec.Actor.Binding != actor.BindingRuntimeInboundViaRelay {
		t.Errorf("actor binding=%q want %q", spec.Actor.Binding, actor.BindingRuntimeInboundViaRelay)
	}
	if len(spec.Types) != len(AllTypes) {
		t.Fatalf("types len=%d want %d", len(spec.Types), len(AllTypes))
	}
	eventCount := 0
	for _, s := range spec.Types {
		if s.HandlerActorID != DefaultAdapterActorID {
			t.Errorf("type %s handler actor=%q want %q", s.Type, s.HandlerActorID, DefaultAdapterActorID)
		}
		if s.HandlerBinding != actor.BindingRuntimeInboundViaRelay {
			t.Errorf("type %s binding=%q", s.Type, s.HandlerBinding)
		}
		if s.MaxPendingMs != DefaultMaxPendingMs {
			t.Errorf("type %s max pending=%d want %d", s.Type, s.MaxPendingMs, DefaultMaxPendingMs)
		}
		if s.AllowEvent {
			if s.Type != TypeNoteArchived {
				t.Errorf("only %q should be event-only; got %q", TypeNoteArchived, s.Type)
			}
			eventCount++
		}
	}
	if eventCount != 1 {
		t.Errorf("expected 1 event-only type seed; got %d", eventCount)
	}
}

// TestDefaultInstallSpecMaxPendingOverride checks the explicit
// override path.
func TestDefaultInstallSpecMaxPendingOverride(t *testing.T) {
	spec := DefaultInstallSpec(123456)
	for _, s := range spec.Types {
		if s.MaxPendingMs != 123456 {
			t.Errorf("type %s max pending=%d want 123456", s.Type, s.MaxPendingMs)
		}
	}
}

// TestWithActorIDPropagates ensures override updates every row.
func TestWithActorIDPropagates(t *testing.T) {
	base := DefaultInstallSpec(0)
	override := actor.ActorID("tool:xhs-adapter-shadow")
	got := base.WithActorID(override)
	if got.Actor.ID != override {
		t.Errorf("actor id not propagated: %q", got.Actor.ID)
	}
	for _, s := range got.Types {
		if s.HandlerActorID != override {
			t.Errorf("type %s handler actor not propagated: %q", s.Type, s.HandlerActorID)
		}
	}
}

// TestWithActorIDIgnoresBlank no-op when given empty id.
func TestWithActorIDIgnoresBlank(t *testing.T) {
	base := DefaultInstallSpec(0)
	got := base.WithActorID("")
	if got.Actor.ID != base.Actor.ID {
		t.Errorf("blank override should be a no-op; got %q", got.Actor.ID)
	}
}
