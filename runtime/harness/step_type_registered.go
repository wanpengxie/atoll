package harness

import (
	"context"
	"strings"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// reservedBootstrapTypeSet is the reserved bootstrap type set. Only the
// channel system actor may emit these envelope.type values; any other
// sender is rejected.
//
// Keys reference protocol/actor constants (the frozen closed set) rather
// than bare literals so a kernel rename can never silently diverge here.
var reservedBootstrapTypeSet = map[string]struct{}{
	actor.ReservedSystemActorRegistered:   {},
	actor.ReservedSystemActorDeregistered: {},
}

// stepTypeRegistered enforces the substrate's RESERVED-NAMESPACE AUTHORITY for
// the `system.*` events: those may only be emitted by the channel system actor,
// protecting the substrate's own mirror-event vocabulary from forgery. Every
// OTHER type — business OR the actor.* introspection convention — passes
// through: the substrate is type-AGNOSTIC. (actor.* is NOT special-cased: the
// generic sender-consistency step already prevents forging an answer about
// another actor, so the actor.* convention is an upper-layer concern, not
// gated here — only the system.* namespace is substrate-authoritative.)
type stepTypeRegistered struct{}

func newStepTypeRegistered(Deps) step { return &stepTypeRegistered{} }

func (s *stepTypeRegistered) ID() stepID { return StepTypeRegistered }

func (s *stepTypeRegistered) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
	// Reserved namespace authority. Even before checking registry
	// membership, reject any non-system sender trying to forge a reserved
	// system event.
	if strings.HasPrefix(env.Type, message.ReservedTypePrefix) {
		if _, reserved := reservedBootstrapTypeSet[env.Type]; reserved {
			if env.Sender.Kind != actor.KindSystem || env.Sender.ID != actor.SystemActorID {
				return outcome{
					RejectReason: HarnessReservedTypeUnauthorizedSender,
					Detail:       "reserved system type may only be emitted by channel system actor: " + env.Type,
				}, nil
			}
			return outcome{}, nil
		}
		return outcome{
			RejectReason: HarnessTypeUnknown,
			Detail:       "non-reserved system namespace type is not installable: " + env.Type,
		}, nil
	}
	// Any non-system type — business OR actor.* introspection — passes.
	// The substrate does not gatekeep non-system vocabulary (type-agnostic); an
	// unknown/typo'd type is delivered to its addressed actor, which rejects it
	// (closure then materialises a terminal) — the Erlang model, not a
	// write-time registry.
	return outcome{}, nil
}
