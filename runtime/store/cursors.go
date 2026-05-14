package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/coagent-ai/coagent/kernel/actor"
	"github.com/coagent-ai/coagent/kernel/log"
)

// Cursors implements kernel/log.Cursors over the actor_cursors table.
type Cursors struct {
	db *sql.DB
}

// NewCursors returns a Cursors bound to the channel sqlite.
func NewCursors(db *sql.DB) *Cursors { return &Cursors{db: db} }

// Get implements log.Cursors.
func (c *Cursors) Get(ctx context.Context, actorID actor.ActorID) (log.Cursor, bool, error) {
	const q = `SELECT actor_id, last_consumed_seq, COALESCE(last_consumed_id,''), updated_at
	            FROM actor_cursors WHERE actor_id=?`
	var cur log.Cursor
	err := c.db.QueryRowContext(ctx, q, string(actorID)).Scan(
		&cur.ActorID, &cur.LastConsumedSeq, &cur.LastConsumedID, &cur.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return log.Cursor{}, false, nil
	}
	if err != nil {
		return log.Cursor{}, false, fmt.Errorf("store: cursor get %q: %w", actorID, err)
	}
	return cur, true, nil
}

// Advance implements log.Cursors. Monotonic CAS — silently no-op when
// newSeq <= current last_consumed_seq (returns ok=false, err=nil).
func (c *Cursors) Advance(
	ctx context.Context,
	actorID actor.ActorID,
	newSeq log.Seq,
	newID string,
	nowMs int64,
) (bool, error) {
	const q = `UPDATE actor_cursors
	            SET last_consumed_seq=?, last_consumed_id=?, updated_at=?
	            WHERE actor_id=? AND last_consumed_seq < ?`
	res, err := c.db.ExecContext(ctx, q,
		int64(newSeq), newID, nowMs, string(actorID), int64(newSeq))
	if err != nil {
		return false, fmt.Errorf("store: cursor advance %q: %w", actorID, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}
