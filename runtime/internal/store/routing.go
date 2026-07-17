package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func (r *actorRegistry) DefaultAgent(ctx context.Context) (actor.ActorID, bool, error) {
	var id sql.NullString
	err := r.db.QueryRowContext(ctx, `SELECT default_agent FROM channel_routing WHERE id=1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !id.Valid) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return actor.ActorID(id.String), true, nil
}

func (r *actorRegistry) SetDefaultAgent(ctx context.Context, id actor.ActorID) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if id != "" {
		var active int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_registry WHERE actor_id=? AND deregistered_at IS NULL`, string(id)).Scan(&active); err != nil {
			return err
		}
		if active != 1 {
			return storespec.ErrMemberInactive
		}
	}
	var value any
	if id != "" {
		value = string(id)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO channel_routing(id,default_agent) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET default_agent=excluded.default_agent`, value); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *actorRegistry) MarkRestartApplied(ctx context.Context, jobID int64, id actor.ActorID, at int64) (bool, error) {
	if jobID <= 0 || id == "" || at <= 0 {
		return false, errors.New("store: invalid restart journal entry")
	}
	res, err := r.db.ExecContext(ctx, `INSERT OR IGNORE INTO restart_applied(job_id,instance_id,applied_at) VALUES(?,?,?)`, jobID, string(id), at)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

var (
	_ storespec.ChannelRouting = (*actorRegistry)(nil)
	_ storespec.RestartJournal = (*actorRegistry)(nil)
)
