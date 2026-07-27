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

// SetDefaultAgent is a purely mechanical setting write: last write wins,
// dangling pointers are legal lazy configuration (§5.7). The member verdict is
// door policy asked of the value ledger — the store never asks who is a
// member (asking actor_registry here would make the durable table a second
// membership authority and refuse entry-table members).
func (r *actorRegistry) SetDefaultAgent(ctx context.Context, id actor.ActorID) error {
	var value any
	if id != "" {
		value = string(id)
	}
	_, err := r.db.ExecContext(ctx, `INSERT INTO channel_routing(id,default_agent) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET default_agent=excluded.default_agent`, value)
	return err
}

var _ storespec.ChannelRouting = (*actorRegistry)(nil)
