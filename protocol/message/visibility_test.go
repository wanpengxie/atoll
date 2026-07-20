package message

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

func TestShouldDeliverKeepsAddressingSeparateFromReadableVisibility(t *testing.T) {
	const human actor.ActorID = "human:a"
	for _, visibility := range []Visibility{VisibilityPublic, VisibilityPrivate} {
		env := &Envelope{Visibility: visibility, Audience: Audience{human}}
		if !ShouldDeliver(human, env) || ShouldDeliver("human:b", env) {
			t.Fatalf("visibility=%s addressing verdict changed", visibility)
		}
	}
	systemRequest := &Envelope{Visibility: VisibilitySystem, Audience: Audience{human}}
	if !ShouldDeliver(human, systemRequest) || ShouldDeliver(actor.SystemActorID, systemRequest) {
		t.Fatalf("system request ignored audience: human=%v system=%v", ShouldDeliver(human, systemRequest), ShouldDeliver(actor.SystemActorID, systemRequest))
	}
	auditEvent := &Envelope{Visibility: VisibilitySystem, Audience: Audience{actor.SystemActorID}}
	if ShouldDeliver(human, auditEvent) || !ShouldDeliver(actor.SystemActorID, auditEvent) {
		t.Fatalf("audit audience escaped: human=%v system=%v", ShouldDeliver(human, auditEvent), ShouldDeliver(actor.SystemActorID, auditEvent))
	}
}
