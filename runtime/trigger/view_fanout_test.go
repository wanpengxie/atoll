package trigger_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/trigger"
)

// TestViewFanout_PublicEnumeratesAllActive — visibility=public pushes to
// every active channel member view cache (proto-layer1 §4.1.3). Trigger
// audience does not narrow the view set.
func TestViewFanout_PublicEnumeratesAllActive(t *testing.T) {
	reg := makeReg() // alpha, beta, user:demo, system
	env := &message.Envelope{
		ID:         "evt-public",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
		Kind:       message.KindEvent,
		Type:       "agent.text",
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:beta"}, // narrower than view set
	}
	got, err := trigger.ViewFanout(context.Background(), env, reg, nil)
	if err != nil {
		t.Fatalf("ViewFanout: %v", err)
	}
	want := []actor.ActorID{"agent:alpha", "agent:beta", actor.SystemActorID, "user:demo"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("public fanout = %v, want %v", got, want)
	}
}

// TestViewFanout_PrivateAudienceOnly — visibility=private restricts view
// to audience ∪ {sender}.
func TestViewFanout_PrivateAudienceOnly(t *testing.T) {
	reg := makeReg()
	env := &message.Envelope{
		ID:         "evt-private",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
		Kind:       message.KindEvent,
		Type:       "agent.text",
		Visibility: message.VisibilityPrivate,
		Audience:   message.Audience{"agent:beta"},
	}
	got, err := trigger.ViewFanout(context.Background(), env, reg, nil)
	if err != nil {
		t.Fatalf("ViewFanout: %v", err)
	}
	want := []actor.ActorID{"agent:alpha", "agent:beta"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("private fanout = %v, want %v (audience ∪ {sender})", got, want)
	}
}

// TestViewFanout_PrivateMultipleAudience — multi-actor audience under
// private visibility expands to union with sender.
func TestViewFanout_PrivateMultipleAudience(t *testing.T) {
	reg := makeReg()
	env := &message.Envelope{
		ID:         "evt-private-multi",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
		Kind:       message.KindEvent,
		Type:       "agent.text",
		Visibility: message.VisibilityPrivate,
		Audience:   message.Audience{"agent:beta", "user:demo"},
	}
	got, err := trigger.ViewFanout(context.Background(), env, reg, nil)
	if err != nil {
		t.Fatalf("ViewFanout: %v", err)
	}
	want := []actor.ActorID{"agent:alpha", "agent:beta", "user:demo"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("private multi fanout = %v, want %v", got, want)
	}
}

// TestViewFanout_PrivateSenderInAudience — sender appears in audience;
// dedup keeps a single entry in the resulting set.
func TestViewFanout_PrivateSenderInAudience(t *testing.T) {
	reg := makeReg()
	env := &message.Envelope{
		ID:         "evt-private-dedup",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
		Kind:       message.KindEvent,
		Type:       "agent.text",
		Visibility: message.VisibilityPrivate,
		Audience:   message.Audience{"agent:alpha", "agent:beta"},
	}
	got, err := trigger.ViewFanout(context.Background(), env, reg, nil)
	if err != nil {
		t.Fatalf("ViewFanout: %v", err)
	}
	want := []actor.ActorID{"agent:alpha", "agent:beta"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("dedup fanout = %v, want %v", got, want)
	}
}

// TestViewFanout_PublicSystemEmit_NotSpecialCased — system actor emit is
// not special-cased per proto-layer1 §4.1.3 "System actor emit 不特殊化"
// — public visibility broadcasts to all members regardless of sender kind.
func TestViewFanout_PublicSystemEmit_NotSpecialCased(t *testing.T) {
	reg := makeReg()
	env := &message.Envelope{
		ID:         "evt-system-public",
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "system.actor.registered",
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:beta"},
	}
	got, err := trigger.ViewFanout(context.Background(), env, reg, nil)
	if err != nil {
		t.Fatalf("ViewFanout: %v", err)
	}
	want := []actor.ActorID{"agent:alpha", "agent:beta", actor.SystemActorID, "user:demo"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("system public fanout = %v, want %v", got, want)
	}
}

// TestViewFanout_PrivateSystemEmit_ScopedToSelfAndAudience — system actor
// emitting visibility=private + audience=[itself] reaches only the system
// actor view (ops events example from §4.1.3 informative guidance).
func TestViewFanout_PrivateSystemEmit_ScopedToSelfAndAudience(t *testing.T) {
	reg := makeReg()
	env := &message.Envelope{
		ID:         "evt-system-private",
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "system.placement.reclaimed",
		Visibility: message.VisibilityPrivate,
		Audience:   message.Audience{actor.SystemActorID},
	}
	got, err := trigger.ViewFanout(context.Background(), env, reg, nil)
	if err != nil {
		t.Fatalf("ViewFanout: %v", err)
	}
	want := []actor.ActorID{actor.SystemActorID}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("system private fanout = %v, want %v", got, want)
	}
}

// TestViewFanout_SystemVisibilityDefaultProjectionSkipsSubscribers documents
// coagent's implementation-layer default projection policy: visibility=system
// messages stay in the log, but do not fan out to ordinary WS/UI subscribers.
func TestViewFanout_SystemVisibilityDefaultProjectionSkipsSubscribers(t *testing.T) {
	reg := makeReg()
	env := &message.Envelope{
		ID:         "evt-system-visibility",
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "core.system_event",
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{"agent:beta"},
	}
	got, err := trigger.ViewFanout(context.Background(), env, reg, nil)
	if err != nil {
		t.Fatalf("ViewFanout: %v", err)
	}
	if got != nil {
		t.Errorf("system visibility default projection fanout = %v, want nil", got)
	}
}

// TestViewFanout_DroppedDeregistered — visibility=public must NOT enumerate
// deregistered members (ListActive filter).
func TestViewFanout_DroppedDeregistered(t *testing.T) {
	reg := makeReg()
	if err := reg.Deregister(context.Background(), "agent:beta", 42); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	env := &message.Envelope{
		ID:         "evt-public-dereg",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
		Kind:       message.KindEvent,
		Type:       "agent.text",
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:beta"},
	}
	got, err := trigger.ViewFanout(context.Background(), env, reg, nil)
	if err != nil {
		t.Fatalf("ViewFanout: %v", err)
	}
	for _, id := range got {
		if id == "agent:beta" {
			t.Errorf("deregistered actor leaked into public fanout: %v", got)
		}
	}
}

// TestViewFanout_ExplicitMembersOverrideRegistry — when caller supplies an
// explicit member list, view fanout uses it as the universe (server-side
// viewcache pattern from view_fanout.go doc).
func TestViewFanout_ExplicitMembersOverrideRegistry(t *testing.T) {
	reg := makeReg()
	env := &message.Envelope{
		ID:         "evt-explicit-members",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
		Kind:       message.KindEvent,
		Type:       "agent.text",
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:beta"},
	}
	members := []actor.ActorID{"user:demo"} // narrow override
	got, err := trigger.ViewFanout(context.Background(), env, reg, members)
	if err != nil {
		t.Fatalf("ViewFanout: %v", err)
	}
	want := []actor.ActorID{"user:demo"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("explicit members fanout = %v, want %v", got, want)
	}
}

// TestViewFanout_EmptyVisibilityDefaultsPublic — pre-normalize envelopes
// arrive with visibility="" (Step Normalize defaults to public). View
// fanout tolerates the pre-normalize shape per its doc.
func TestViewFanout_EmptyVisibilityDefaultsPublic(t *testing.T) {
	reg := newMemRegistry(actor.Record{ID: "agent:alpha", Kind: actor.KindAgent, CreatedAt: 1})
	env := &message.Envelope{
		ID:       "evt-empty-visibility",
		Sender:   message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
		Kind:     message.KindEvent,
		Type:     "agent.text",
		Audience: message.Audience{"agent:alpha"},
		// Visibility intentionally empty
	}
	got, err := trigger.ViewFanout(context.Background(), env, reg, nil)
	if err != nil {
		t.Fatalf("ViewFanout: %v", err)
	}
	want := []actor.ActorID{"agent:alpha"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("empty visibility fanout = %v, want %v", got, want)
	}
}

// TestViewFanout_NilEnvelope — guard against nil envelope.
func TestViewFanout_NilEnvelope(t *testing.T) {
	reg := makeReg()
	if _, err := trigger.ViewFanout(context.Background(), nil, reg, nil); err == nil {
		t.Error("expected error on nil envelope, got nil")
	}
}

// TestViewFanout_RequiresRegistryOrMembers — at least one source needed.
func TestViewFanout_RequiresRegistryOrMembers(t *testing.T) {
	env := &message.Envelope{
		ID:         "evt-nil-sources",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
		Kind:       message.KindEvent,
		Type:       "agent.text",
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:beta"},
	}
	if _, err := trigger.ViewFanout(context.Background(), env, nil, nil); err == nil {
		t.Error("expected error when registry+members both nil, got nil")
	}
}
