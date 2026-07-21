package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// EndCascade atomically records every identity end event, ends durable rows,
// clears actor-scoped state/timers/grants, and nulls routing that pointed at an
// ended identity. Run identities have no registry row but their end event is
// committed in the same batch as their durable relatives.
//
// This is the self-end / system-internal path (spec-exempt from the operation
// account): it owns its transaction and carries no sysop anchor/event pair. The
// member-word remove commits the SAME cascade value rows through the sysop
// value-operation store (RemoveActor) so the anchor + started/completed pair
// land in one transaction with them; both reach the durable writes through the
// single cascadeWriteTx helper below — there is exactly one cascade write.
func (r *actorRegistry) EndCascade(ctx context.Context, in storespec.CascadeBundle) (storespec.CascadeResult, error) {
	if in.EndedAt <= 0 || (len(in.IDs) == 0 && len(in.Envelopes) == 0) {
		return storespec.CascadeResult{}, errors.New("store: invalid end cascade")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.CascadeResult{}, err
	}
	defer tx.Rollback()
	// Owner protection stays welded to THIS function (archtest chokepoint): the
	// self-end path returns the typed sentinel; the member-word path instead
	// records a decisive protected_actor verdict inside RemoveActor.
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

	result, _, err := cascadeWriteTx(ctx, tx, r.channelID, in.IDs, in.Envelopes, in.EndedAt)
	if err != nil {
		return storespec.CascadeResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return storespec.CascadeResult{}, err
	}
	if r.onCommit != nil {
		r.onCommit()
	}
	return result, nil
}

// cascadeWriteTx is the single durable-cascade write, shared by the self-end
// EndCascade (its own transaction) and the member-word RemoveActor (the sysop
// value-operation transaction). It deregisters durable rows + clears their
// actor-scoped state/timers/grants, appends the whole-tree ended events
// (idempotently skipping any already written), and nulls routing that pointed
// at an ended identity. It performs NO owner-protection check — each caller
// applies its own (the sentinel vs. a decisive verdict) before reaching here.
//
// It returns the durable Ended/Already split (EndCascade's result shape) and,
// separately, the whole-tree set that received a fresh ended event this call
// (RemoveActor's Removed reply — empty on an idempotent no-op).
func cascadeWriteTx(ctx context.Context, tx *sql.Tx, channelID channel.ID, durableIDs []actor.ActorID, envelopes []storespec.CascadeEnvelope, at int64) (storespec.CascadeResult, []actor.ActorID, error) {
	reasons := make(map[actor.ActorID]string, len(envelopes))
	for _, e := range envelopes {
		if e.Target == "" || e.Target == actor.SystemActorID || e.EndedBy == "" {
			return storespec.CascadeResult{}, nil, errors.New("store: invalid end cascade envelope")
		}
		reasons[e.Target] = e.Reason
	}
	result := storespec.CascadeResult{}
	for _, id := range durableIDs {
		if id == "" || id == actor.SystemActorID {
			return storespec.CascadeResult{}, nil, fmt.Errorf("store: invalid end target %q", id)
		}
		res, err := tx.ExecContext(ctx, `UPDATE actor_registry SET deregistered_at=? WHERE actor_id=? AND deregistered_at IS NULL`, at, string(id))
		if err != nil {
			return storespec.CascadeResult{}, nil, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return storespec.CascadeResult{}, nil, err
		}
		if n == 1 {
			if err := clearActorScopedTx(ctx, tx, id); err != nil {
				return storespec.CascadeResult{}, nil, err
			}
			if err := clearTimersTx(ctx, tx, id); err != nil {
				return storespec.CascadeResult{}, nil, err
			}
			if err := clearActorGrantsTx(ctx, tx, id); err != nil {
				return storespec.CascadeResult{}, nil, err
			}
			result.Ended = append(result.Ended, id)
		} else {
			result.Already = append(result.Already, id)
		}
	}
	var newlyEnded []actor.ActorID
	for _, ended := range envelopes {
		id := ended.Target
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE id=?`, "actor-ended:"+string(id)).Scan(&exists); err != nil {
			return storespec.CascadeResult{}, nil, err
		}
		if exists != 0 {
			continue
		}
		payload, _ := json.Marshal(map[string]any{"target_id": id, "reason": reasons[id], "ended_at": at, "ended_by": ended.EndedBy})
		env := &message.Envelope{
			ID: message.ID(fmt.Sprintf("actor-ended:%s", id)), TS: at, TSReceived: at,
			ChannelID: channelID, Sender: message.Sender{ID: actor.SystemActorID, Kind: actor.KindSystem},
			Kind: message.KindEvent, Type: actor.ReservedSystemActorEnded, Payload: payload,
			Visibility: message.VisibilitySystem, Audience: message.Audience{actor.SystemActorID},
		}
		if _, err := appendTx(ctx, tx, env, false); err != nil {
			return storespec.CascadeResult{}, nil, err
		}
		newlyEnded = append(newlyEnded, id)
	}
	for id := range reasons {
		if _, err := tx.ExecContext(ctx, `UPDATE channel_routing SET default_agent=NULL WHERE default_agent=?`, string(id)); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return storespec.CascadeResult{}, nil, err
		}
	}
	return result, newlyEnded, nil
}

var _ storespec.CascadeStore = (*actorRegistry)(nil)
