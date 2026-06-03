package harness

import (
	"context"
	"strings"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
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
	actor.ReservedSystemTypeInstalled:     {},
	actor.ReservedSystemTypeDeprecated:    {},
	actor.ReservedSystemConfigUpdated:     {},
}

type reservedActorTypeRule struct {
	AllowedKinds []message.Kind
	SystemOnly   bool
}

// Keys reference kernel/actor constants (§1.4): the per-type kind/sender
// metadata below is harness-internal validation policy, but the type NAMES
// are the kernel's frozen vocabulary — keyed off the constants so they cannot
// drift from kernel. (kernel taxonomy splits these across reserved actor-type constants
// / system-event-type constants; the grouping here is a separate axis — what
// validation rule applies — not a membership claim.)
var reservedActorTypeSet = map[string]reservedActorTypeRule{
	actor.ReservedActorStatus: {
		AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
	},
	// actor.describe returns the actor's static Declaration projection
	// (description / skill_doc / per-type payload_example / payload_fields
	// / error_codes+recovery / notes). Framework-intercepted in the
	// adapter dispatch path; never reaches Module.Handle. Pull-based:
	// no server-side mirror, no event emission — daemon is the single
	// source of truth, SDK reads via CallActor.
	actor.ReservedActorDescribe: {
		AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
	},
	// actor.list returns the channel's live active-actor + request-type
	// catalog. Unlike actor.describe (per-actor, framework-intercepted on
	// the target tool actor's dispatch path), actor.list is channel-wide:
	// the daemon answers it directly from the channel registry + type
	// registry (truth ownership, INVARIANT-2) when addressed to the
	// channel system actor. Pull-based, live on every call — no frozen
	// bootstrap snapshot, no server-side mirror.
	actor.ReservedActorList: {
		AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
	},
}

// stepTypeRegistered enforces the substrate's RESERVED-NAMESPACE AUTHORITY:
// the `system.*` reserved bootstrap types and `actor.*` reserved types may only
// be emitted by the channel system actor (protecting the substrate's own mirror-
// event + reserved-query vocabulary from forgery). Every OTHER type — core or
// business — passes through: the substrate is type-AGNOSTIC about business
// vocabulary (a type is an opaque label; whether "xhs.publish" is a known
// capability is a domain agreement validated by the receiving actor + the
// caller's catalog, NOT a substrate registry). There is no type_registry lookup.
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
	if strings.HasPrefix(env.Type, "actor.") {
		rule, reserved := reservedActorTypeSet[env.Type]
		if !reserved {
			return outcome{
				RejectReason: HarnessTypeUnknown,
				Detail:       "non-reserved actor namespace type is not installable: " + env.Type,
			}, nil
		}
		if rule.SystemOnly && (env.Sender.Kind != actor.KindSystem || env.Sender.ID != actor.SystemActorID) {
			return outcome{
				RejectReason: HarnessReservedTypeUnauthorizedSender,
				Detail:       "reserved actor type may only be emitted by channel system actor: " + env.Type,
			}, nil
		}
		return outcome{}, nil
	}

	// Any non-reserved type — core or business — passes. The substrate does
	// not gatekeep business vocabulary (type-agnostic); an unknown/typo'd type
	// is delivered to its addressed actor, which rejects it (closure then
	// materialises a terminal) — the Erlang model, not a write-time registry.
	return outcome{}, nil
}
