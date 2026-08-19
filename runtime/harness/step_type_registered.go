package harness

import (
	"context"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

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
		entry, ok := message.Parse(env.Type)
		if !ok {
			return outcome{
				RejectReason: HarnessTypeUnknown,
				Detail:       "unknown system type: " + env.Type,
			}, nil
		}
		// A response inherits its request's type. The table constrains the
		// authored word (request/event), while response pairing owns the reply
		// shape and author rules.
		if env.Kind != message.KindResponse && env.Kind != entry.Kind {
			return outcome{
				RejectReason: HarnessKindNotAllowedForType,
				Detail:       fmt.Sprintf("system type %s requires kind=%s", env.Type, entry.Kind),
			}, nil
		}
		if env.Kind == message.KindEvent {
			if env.Sender.Kind != actor.KindSystem || env.Sender.ID != actor.SystemActorID {
				return outcome{
					RejectReason: HarnessReservedTypeUnauthorizedSender,
					Detail:       "system event may only be emitted by channel system actor: " + env.Type,
				}, nil
			}
		}
		return outcome{}, nil
	}
	// Any non-system type — business OR actor.* introspection — passes.
	// The substrate does not gatekeep non-system vocabulary (type-agnostic); an
	// unknown/typo'd type is delivered to its addressed actor, which rejects it
	// (closure then materialises a terminal) — the Erlang model, not a
	// write-time registry.
	return outcome{}, nil
}
