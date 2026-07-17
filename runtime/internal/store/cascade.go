package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// EndCascade atomically records every identity end event, ends durable rows,
// clears actor-scoped state/timers/grants, and nulls routing that pointed at an
// ended identity. Run identities have no registry row but their end event is
// committed in the same batch as their durable relatives.
func (r *actorRegistry) EndCascade(ctx context.Context, in storespec.CascadeBundle) (storespec.CascadeResult, error) {
	if in.EndedAt <= 0 || (len(in.IDs) == 0 && len(in.Envelopes) == 0) {
		return storespec.CascadeResult{}, errors.New("store: invalid end cascade")
	}
	reasons := make(map[actor.ActorID]string, len(in.Envelopes))
	for _, e := range in.Envelopes {
		if e.Target == "" || e.Target == actor.SystemActorID || e.EndedBy == "" {
			return storespec.CascadeResult{}, errors.New("store: invalid end cascade envelope")
		}
		reasons[e.Target] = e.Reason
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.CascadeResult{}, err
	}
	defer tx.Rollback()
	protectedTargets := make(map[actor.ActorID]struct{}, len(in.IDs)+len(in.Envelopes))
	for _, id := range in.IDs {
		protectedTargets[id] = struct{}{}
	}
	for _, envelope := range in.Envelopes {
		protectedTargets[envelope.Target] = struct{}{}
	}
	for id := range protectedTargets {
		var owner int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_registry WHERE actor_id=? AND role='owner' AND deregistered_at IS NULL`, string(id)).Scan(&owner); err != nil {
			return storespec.CascadeResult{}, err
		}
		if owner != 0 {
			return storespec.CascadeResult{}, storespec.ErrChannelOwnerProtected
		}
	}

	result := storespec.CascadeResult{}
	for _, id := range in.IDs {
		if id == "" || id == actor.SystemActorID {
			return storespec.CascadeResult{}, fmt.Errorf("store: invalid end target %q", id)
		}
		res, err := tx.ExecContext(ctx, `UPDATE actor_registry SET deregistered_at=? WHERE actor_id=? AND deregistered_at IS NULL`, in.EndedAt, string(id))
		if err != nil {
			return storespec.CascadeResult{}, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return storespec.CascadeResult{}, err
		}
		if n == 1 {
			if err := clearActorScopedTx(ctx, tx, id); err != nil {
				return storespec.CascadeResult{}, err
			}
			if err := clearTimersTx(ctx, tx, id); err != nil {
				return storespec.CascadeResult{}, err
			}
			if err := clearActorGrantsTx(ctx, tx, id); err != nil {
				return storespec.CascadeResult{}, err
			}
			result.Ended = append(result.Ended, id)
		} else {
			result.Already = append(result.Already, id)
		}
	}
	for _, ended := range in.Envelopes {
		id := ended.Target
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE id=?`, "actor-ended:"+string(id)).Scan(&exists); err != nil {
			return storespec.CascadeResult{}, err
		}
		if exists != 0 {
			continue
		}
		payload, _ := json.Marshal(map[string]any{"target_id": id, "reason": reasons[id], "ended_at": in.EndedAt, "ended_by": ended.EndedBy})
		env := &message.Envelope{
			ID: message.ID(fmt.Sprintf("actor-ended:%s", id)), TS: in.EndedAt, TSReceived: in.EndedAt,
			ChannelID: r.channelID, Sender: message.Sender{ID: actor.SystemActorID, Kind: actor.KindSystem},
			Kind: message.KindEvent, Type: actor.ReservedSystemActorEnded, Payload: payload,
			Visibility: message.VisibilitySystem, Audience: message.Audience{actor.SystemActorID},
		}
		if _, err := appendTx(ctx, tx, env, false); err != nil {
			return storespec.CascadeResult{}, err
		}
	}
	for id := range reasons {
		if _, err := tx.ExecContext(ctx, `UPDATE channel_routing SET default_agent=NULL WHERE default_agent=?`, string(id)); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return storespec.CascadeResult{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return storespec.CascadeResult{}, err
	}
	if r.onCommit != nil {
		r.onCommit()
	}
	return result, nil
}

var _ storespec.CascadeStore = (*actorRegistry)(nil)
