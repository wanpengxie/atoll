package devicebus

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/placement"
)

type actorReadinessPayload struct {
	ActorID   actor.ActorID `json:"actor_id"`
	CheckedAt int64         `json:"checked_at"`
	ChangedAt int64         `json:"changed_at"`
	Current   struct {
		Ready             bool            `json:"ready"`
		State             string          `json:"state"`
		Reason            string          `json:"reason"`
		Detail            json.RawMessage `json:"detail"`
		LastReadyAt       int64           `json:"last_ready_at"`
		LastStateChangeAt int64           `json:"last_state_change_at"`
	} `json:"current"`
}

func (s *Service) projectActorReadinessFrame(ctx context.Context, daemonID placement.DaemonID, frame DeviceFrame) {
	payload, ok := decodeActorReadinessPayload(frame)
	if !ok {
		return
	}
	actorID := payload.ActorID
	if actorID == "" {
		actorID = actor.ActorID(frame.ActorID)
	}
	if actorID == "" {
		return
	}
	state := payload.Current.State
	if state == "" {
		if payload.Current.Ready {
			state = "ready"
		} else {
			state = "not_ready"
		}
	}
	reason := payload.Current.Reason
	if reason == "" {
		if payload.Current.Ready {
			reason = "ok"
		} else {
			reason = "not_ready"
		}
	}
	detail := payload.Current.Detail
	if len(detail) == 0 {
		detail = json.RawMessage(`{}`)
	}
	checkedAt := payload.CheckedAt
	if checkedAt == 0 {
		checkedAt = s.nowMs()
	}
	lastStateChangeAt := payload.Current.LastStateChangeAt
	if lastStateChangeAt == 0 {
		lastStateChangeAt = payload.ChangedAt
	}
	if lastStateChangeAt == 0 {
		lastStateChangeAt = checkedAt
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE daemon_hosted_actors
		   SET ready_state=?,
		       ready_reason=?,
		       ready_detail=?,
		       readiness_checked_at=?,
		       last_ready_at=?,
		       last_state_change_at=?
		 WHERE daemon_id=? AND actor_id=?`,
		state, reason, string(detail), checkedAt, payload.Current.LastReadyAt,
		lastStateChangeAt, string(daemonID), string(actorID),
	); err != nil {
		s.log.Warn("devicebus.actor_readiness_projection_failed",
			"daemon_id", string(daemonID),
			"actor_id", string(actorID),
			"err", err.Error(),
		)
	}
}

func decodeActorReadinessPayload(frame DeviceFrame) (actorReadinessPayload, bool) {
	var env struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if len(frame.Payload) == 0 {
		return actorReadinessPayload{}, false
	}
	if err := json.Unmarshal(frame.Payload, &env); err != nil {
		return actorReadinessPayload{}, false
	}
	if env.Type != "actor.readiness.changed" {
		return actorReadinessPayload{}, false
	}
	var payload actorReadinessPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return actorReadinessPayload{}, false
	}
	return payload, true
}
