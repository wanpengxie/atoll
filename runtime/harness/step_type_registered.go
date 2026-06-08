package harness

import (
	"context"
	"strings"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// reservedBootstrapTypeSet is the proto-layer1 §6.2.0 Reserved Bootstrap
// Type Set. Only the channel system actor may emit these envelope.type
// values; any other sender is rejected per §2.5.
//
// Keys reference kernel/actor constants (the frozen closed set, §1.4) rather
// than bare literals so a kernel rename can never silently diverge here.
var reservedBootstrapTypeSet = map[string]struct{}{
	actor.ReservedSystemChannelCreated:    {},
	actor.ReservedSystemActorRegistered:   {},
	actor.ReservedSystemActorDeregistered: {},
	actor.ReservedSystemConfigUpdated:     {},
}

// stepTypeRegistered enforces the substrate's RESERVED-NAMESPACE AUTHORITY for
// the `system.*` events: those may only be emitted by the channel system actor,
// protecting the substrate's own mirror-event vocabulary from forgery. Every
// OTHER type — core, business, OR the actor.* introspection convention — passes
// through: the substrate is type-AGNOSTIC. (actor.* is NOT special-cased: the
// generic sender-consistency step already prevents forging an answer about
// another actor, so the actor.* convention is an upper-layer concern, not
// gated here — only the system.* namespace is substrate-authoritative.)
type stepTypeRegistered struct{}

func newStepTypeRegistered(Deps) step { return &stepTypeRegistered{} }

func (s *stepTypeRegistered) ID() stepID { return StepTypeRegistered }

func (s *stepTypeRegistered) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
	// Reserved namespace authority — proto-layer1 §2.5 + §6.2.0. Even
	// before checking registry membership, reject any non-system sender
	// trying to forge a reserved system event.
	if strings.HasPrefix(env.Type, "system.") {
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
	// Any non-system type — core, business, OR actor.* introspection — passes.
	// The substrate does not gatekeep non-system vocabulary (type-agnostic); an
	// unknown/typo'd type is delivered to its addressed actor, which rejects it
	// (closure then materialises a terminal) — the Erlang model, not a
	// write-time registry.
	return outcome{}, nil
}
