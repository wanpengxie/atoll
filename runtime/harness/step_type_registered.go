package harness

import (
	"context"
	"strings"

	"github.com/wanpengxie/ActOS/kernel/actor"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// reservedBootstrapTypeSet is the proto-layer1 §6.2.0 Reserved Bootstrap
// Type Set. Only the channel system actor may emit these envelope.type
// values; any other sender is rejected per §2.5.
var reservedBootstrapTypeSet = map[string]struct{}{
	"system.channel.created":     {},
	"system.actor.registered":    {},
	"system.actor.deregistered":  {},
	"system.type.installed":      {},
	"system.type.deprecated":     {},
	"system.config.updated":      {},
	"system.placement.reclaimed": {},
}

type reservedActorTypeRule struct {
	AllowedKinds []message.Kind
	SystemOnly   bool
}

var reservedActorTypeSet = map[string]reservedActorTypeRule{
	"actor.status": {
		AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
	},
	// actor.describe returns the actor's static Declaration projection
	// (description / skill_doc / per-type payload_example / payload_fields
	// / error_codes+recovery / notes). Framework-intercepted in the
	// adapter dispatch path; never reaches Module.Handle. Pull-based:
	// no server-side mirror, no event emission — daemon is the single
	// source of truth, SDK reads via CallActor.
	"actor.describe": {
		AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
	},
	// actor.list returns the channel's live active-actor + request-type
	// catalog. Unlike actor.describe (per-actor, framework-intercepted on
	// the target tool actor's dispatch path), actor.list is channel-wide:
	// the daemon answers it directly from the channel registry + type
	// registry (truth ownership, INVARIANT-2) when addressed to the
	// channel system actor. Pull-based, live on every call — no frozen
	// bootstrap snapshot, no server-side mirror.
	"actor.list": {
		AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
	},
	"actor.readiness.changed": {
		AllowedKinds: []message.Kind{message.KindEvent},
		SystemOnly:   true,
	},
	// actor.presence.changed carries the channel system actor's ephemeral
	// physical-presence projection (compute lease attach/heartbeat/detach). Like
	// readiness.changed it is SystemOnly — only the channel system author (the
	// home, on behalf of the fleet) emits it; the sysactor cell folds it in.
	"actor.presence.changed": {
		AllowedKinds: []message.Kind{message.KindEvent},
		SystemOnly:   true,
	},
}

// stepTypeRegistered implements proto-layer1 §2.5 step 5 — `type ∈ (core
// ∪ type_registry)` plus the reserved-namespace authority check. Core
// types pass through to the kind/audience step; business types require a
// TypeRegistry lookup hit; system.* types in the §6.2.0 reserved set
// must be emitted by the channel system actor only.
type stepTypeRegistered struct {
	deps Deps
}

func newStepTypeRegistered(d Deps) khar.Step { return &stepTypeRegistered{deps: d} }

func (s *stepTypeRegistered) ID() khar.StepID { return khar.StepTypeRegistered }

func (s *stepTypeRegistered) Run(ctx context.Context, env *message.Envelope) (khar.Outcome, error) {
	// Reserved namespace authority — proto-layer1 §2.5 + §6.2.0. Even
	// before checking registry membership, reject any non-system sender
	// trying to forge a reserved system event.
	if strings.HasPrefix(env.Type, "system.") {
		if _, reserved := reservedBootstrapTypeSet[env.Type]; reserved {
			if env.Sender.Kind != actor.KindSystem || env.Sender.ID != actor.SystemActorID {
				return khar.Outcome{
					RejectReason: message.HarnessReservedTypeUnauthorizedSender,
					Detail:       "reserved system type may only be emitted by channel system actor: " + env.Type,
				}, nil
			}
			return khar.Outcome{}, nil
		}
		return khar.Outcome{
			RejectReason: message.HarnessTypeUnknown,
			Detail:       "non-reserved system namespace type is not installable: " + env.Type,
		}, nil
	}
	if strings.HasPrefix(env.Type, "actor.") {
		rule, reserved := reservedActorTypeSet[env.Type]
		if !reserved {
			return khar.Outcome{
				RejectReason: message.HarnessTypeUnknown,
				Detail:       "non-reserved actor namespace type is not installable: " + env.Type,
			}, nil
		}
		if rule.SystemOnly && (env.Sender.Kind != actor.KindSystem || env.Sender.ID != actor.SystemActorID) {
			return khar.Outcome{
				RejectReason: message.HarnessReservedTypeUnauthorizedSender,
				Detail:       "reserved actor type may only be emitted by channel system actor: " + env.Type,
			}, nil
		}
		return khar.Outcome{}, nil
	}

	if _, isCore := message.CoreTypeTable[env.Type]; isCore {
		return khar.Outcome{}, nil
	}
	if s.deps.TypeRegistry == nil {
		return khar.Outcome{
			RejectReason: message.HarnessTypeUnknown,
			Detail:       "type registry not wired; only core types allowed",
		}, nil
	}
	if _, ok, err := s.deps.TypeRegistry.Lookup(ctx, env.Type); err != nil {
		return khar.Outcome{}, err
	} else if !ok {
		return khar.Outcome{
			RejectReason: message.HarnessTypeUnknown,
			Detail:       "type not registered: " + env.Type,
		}, nil
	}
	return khar.Outcome{}, nil
}
