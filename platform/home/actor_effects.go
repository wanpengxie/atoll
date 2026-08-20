package home

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// homeActorEffects is the composition-root tail of committed Controller
// transitions. It never mutates Controller truth and never owns execution.
type homeActorEffects struct{ home *Home }

func (e homeActorEffects) PlanPoke(domain actorhost.ExecutionDomain) {
	if e.home != nil {
		e.home.pokeReconcile()
		if e.home.daemonRoutes != nil {
			e.home.daemonRoutes.PokePlan(string(domain), string(e.home.channelID))
		}
	}
}

// ActorsEnded is the Transition.Ended tail: Platform blind-calls each store's
// narrow ForgetActors release port. It is plain process resource hygiene —
// idempotent, unclassified (no store is ever asked which kind of record an id
// was), never retried, no tombstone. Durable rows belonging to the dead are
// inert data and are deliberately left alone.
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

// ---------------------------------------------------------------------------
// lifecycle narration (best effort, never machine truth)
// ---------------------------------------------------------------------------

// announceRegistered and announceEnded write the "so-and-so joined / left the
// channel" narration into the conversation stream with the system pen. They are
// best effort: a crash window may drop one, and nothing ever back-fills or
// reconciles them. No machine ever derives actor truth from message history.
func (h *Home) announceRegistered(ctx context.Context, cause message.Cause, id actor.ActorID, fields map[string]any) {
	if h == nil || h.systemPen == nil || id == "" {
		return
	}
	payload := map[string]any{"member": id}
	for name, value := range fields {
		payload[name] = value
	}
	h.writeNarration(ctx, cause, message.TypeSystemMemberCreated, payload)
}

func (h *Home) announceEnded(
	ctx context.Context,
	cause message.Cause,
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
	for _, id := range ids {
		h.writeNarration(ctx, cause, message.TypeSystemMemberDeleted, map[string]any{
			"member": id, "reason": reason,
			"by": map[string]any{"caller": harness.Caller{Channel: h.channelID, Actor: endedBy}},
		})
	}
}

func (h *Home) writeNarration(ctx context.Context, cause message.Cause, typ string, payload map[string]any) {
	if err := h.emitSystemEvent(ctx, cause, typ, payload); err != nil {
		h.logger.Warn("platform.narration.dropped",
			"channel", h.channelID, "type", typ, "err", err)
	}
}

type systemEventWriteError struct {
	Reason harness.HarnessRejectReason
	Detail string
}

func (e *systemEventWriteError) Error() string {
	return fmt.Sprintf("system event rejected: %s: %s", e.Reason, e.Detail)
}

// emitSystemEvent is the one system-event construction and write mouth. The
// caller supplies only the business type and payload; identity, addressing,
// visibility, id, time, and serialization are sealed here.
func (h *Home) emitSystemEvent(
	ctx context.Context,
	cause message.Cause,
	typ string,
	payload map[string]any,
) error {
	if h == nil || h.systemPen == nil {
		return errors.New("platform: system event pen unavailable")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("platform: marshal system event %q: %w", typ, err)
	}
	// Through the shared builder rather than hand-assembled: the builder is
	// where an envelope's cause is required and its parent/correlation derived,
	// and a second assembly site here is exactly how these events came to have
	// no cause at all.
	env, err := behavior.BuildEvent(func() time.Time { return time.UnixMilli(h.nowMs()) }, behavior.EventSpec{
		Type:       typ,
		Payload:    raw,
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{},
		Cause:      cause,
	})
	if err != nil {
		return fmt.Errorf("platform: build system event %q: %w", typ, err)
	}
	result, err := h.systemPen.Write(ctx, env)
	if err != nil {
		return fmt.Errorf("platform: write system event %q: %w", typ, err)
	}
	if !result.Accepted() {
		rejected := &systemEventWriteError{
			Reason: result.RejectReason, Detail: result.RejectDetail,
		}
		h.logger.Error("platform.system_event.rejected",
			"channel", h.channelID, "type", typ, "message_id", env.ID,
			"reason", result.RejectReason, "detail", result.RejectDetail)
		return rejected
	}
	return nil
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
