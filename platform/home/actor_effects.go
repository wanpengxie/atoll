package home

import (
	"context"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// homeActorEffects is the composition-root tail of committed Controller
// transitions. It never mutates Controller truth and never owns execution.
type homeActorEffects struct{ home *Home }

func (e homeActorEffects) PlanPoke(domain actorhost.ExecutionDomain) {
	if e.home != nil {
		e.home.pokeReconcile()
		if e.home.links != nil {
			e.home.links.PokePlan(string(domain))
		}
	}
}

// ActorsEnded is the Transition.Ended tail: Platform blind-calls each store's
// narrow ForgetActors release port. It is plain process resource hygiene —
// idempotent, unclassified (no store is ever asked which kind of record an id
// was), never retried, no tombstone. Durable rows belonging to the dead are
// inert data and are deliberately left alone; the fork replay table is NOT
// released here (it is Controller ledger state and is never pruned, §5.2).
func (e homeActorEffects) ActorsEnded(ids []actor.ActorID) {
	h := e.home
	if h == nil {
		return
	}
	if h.stateHandles != nil {
		h.stateHandles.ForgetActors(ids)
	}
	if h.engine != nil {
		h.engine.ForgetActors(ids)
	}
	for _, id := range ids {
		if h.presenceFold != nil {
			h.presenceFold.Forget(id)
		}
	}
	h.pokeReconcile()
}

func (e homeActorEffects) Fatal(err error) {
	h := e.home
	if h == nil {
		return
	}
	h.logger.Error("platform.home.system_kernel_failed", "err", err)
	go func() { _ = h.closeInternal("system_kernel_failed") }()
}

// notifyMembership is the membership-change tail. The command layer carries its
// own principals; nothing is echoed back from the store.
func (h *Home) notifyMembership(principals ...string) {
	if h == nil || h.onMembershipChange == nil {
		return
	}
	for _, principal := range principals {
		if principal != "" {
			h.onMembershipChange(principal)
		}
	}
}

// ---------------------------------------------------------------------------
// lifecycle narration (best effort, never machine truth)
// ---------------------------------------------------------------------------

// announceRegistered and announceEnded write the "so-and-so joined / left the
// channel" narration into the conversation stream with the system pen. They are
// best effort: a crash window may drop one, and nothing ever back-fills or
// reconciles them. No machine ever derives actor truth from message history.
func (h *Home) announceRegistered(ctx context.Context, record storespec.ActorRecord) {
	if h == nil || h.systemPen == nil || record.ID == "" {
		return
	}
	h.writeNarration(ctx, &message.Envelope{
		ID:   message.ID("actor-registered:" + string(record.ID)),
		Kind: message.KindEvent, Type: actor.ReservedSystemActorRegistered,
		Payload: jsonPayload(map[string]any{
			"actor_id": record.ID, "actor_kind": record.Kind,
			"registered_at": record.CreatedAt,
		}),
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{actor.SystemActorID},
	})
}

func (h *Home) announceEnded(
	ctx context.Context,
	ids []actor.ActorID,
	reason string,
	endedBy actor.ActorID,
) {
	if h == nil || h.systemPen == nil || len(ids) == 0 {
		return
	}
	if reason == "" {
		reason = "ended"
	}
	at := h.nowMs()
	for _, id := range ids {
		h.writeNarration(ctx, &message.Envelope{
			ID:   message.ID("actor-ended:" + string(id)),
			Kind: message.KindEvent, Type: actor.ReservedSystemActorEnded,
			Payload: jsonPayload(map[string]any{
				"target_id": id, "reason": reason,
				"ended_at": at, "ended_by": endedBy,
			}),
			Visibility: message.VisibilitySystem,
			Audience:   message.Audience{actor.SystemActorID},
		})
	}
}

// announceAudit narrates one completed management operation. Same discipline:
// narration, never a ledger a machine reads.
func (h *Home) announceAudit(ctx context.Context, operation string, detail map[string]any) {
	if h == nil || h.systemPen == nil {
		return
	}
	payload := map[string]any{"operation": operation}
	for k, v := range detail {
		payload[k] = v
	}
	h.writeNarration(ctx, &message.Envelope{
		ID:   message.ID("sysop:" + operation + ":" + uuid.NewString()),
		Kind: message.KindEvent, Type: sysOpAuditType, Payload: jsonPayload(payload),
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{actor.SystemActorID},
	})
}

const sysOpAuditType = "sysop_completed"

func (h *Home) writeNarration(ctx context.Context, env *message.Envelope) {
	if _, err := h.systemPen.Write(ctx, env); err != nil {
		h.logger.Debug("platform.narration.dropped",
			"channel", h.channelID, "envelope", env.ID, "err", err)
	}
}

// homeHostEvents projects current local Body observations into presence. Host
// already exact-filters successor-stale events before this sink is called.
type homeHostEvents struct{ home *Home }

func (e homeHostEvents) OnBodyExited(
	id actor.ActorID,
	_ actorhost.AttemptKey,
	self actorrt.Incarnation,
	cause error,
) {
	if e.home != nil && e.home.presenceFold != nil {
		e.home.presenceFold.OnDown(nil, id, self, cause)
	}
}

func (e homeHostEvents) OnBodyObs(
	id actor.ActorID,
	_ actorhost.AttemptKey,
	self actorrt.Incarnation,
	kind actorrt.ObsKind,
	value actorrt.ObsValue,
) {
	if e.home != nil && e.home.presenceFold != nil {
		e.home.presenceFold.OnObs(nil, id, self, kind, value)
	}
}

var _ actorhost.HostEventSink = homeHostEvents{}
