package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/wanpengxie/atoll/runtime/storespec"
)

// daemonBindings owns channel_daemon_bindings and nothing else. Attaching or
// detaching a daemon is a wiring-domain setting (last write wins); it never
// touches an actor record, so it carries no dedup ceremony and no cascade.
type daemonBindings struct {
	db       *sql.DB
	onCommit func()
}

func newDaemonBindings(db *sql.DB, onCommit func()) *daemonBindings {
	return &daemonBindings{db: db, onCommit: onCommit}
}

func (b *daemonBindings) AttachDaemon(
	ctx context.Context,
	id storespec.DaemonID,
	at int64,
) (bool, error) {
	if id == "" {
		return false, fmt.Errorf("store: daemon_id required")
	}
	res, err := b.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO channel_daemon_bindings(daemon_id,attached_at) VALUES (?,?)`,
		string(id), at)
	if err != nil {
		return false, fmt.Errorf("store: attach daemon %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if b.onCommit != nil {
		b.onCommit()
	}
	return n == 1, nil
}

func (b *daemonBindings) DetachDaemon(
	ctx context.Context,
	id storespec.DaemonID,
) (bool, error) {
	if id == "" {
		return false, fmt.Errorf("store: daemon_id required")
	}
	res, err := b.db.ExecContext(ctx,
		`DELETE FROM channel_daemon_bindings WHERE daemon_id=?`, string(id))
	if err != nil {
		return false, fmt.Errorf("store: detach daemon %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if b.onCommit != nil {
		b.onCommit()
	}
	return n == 1, nil
}

func (b *daemonBindings) IsBound(ctx context.Context, id storespec.DaemonID) (bool, error) {
	var bound bool
	err := b.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM channel_daemon_bindings WHERE daemon_id=?)`,
		string(id)).Scan(&bound)
	return bound, err
}

func (b *daemonBindings) ListBoundDaemons(ctx context.Context) ([]storespec.DaemonID, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT daemon_id FROM channel_daemon_bindings ORDER BY daemon_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storespec.DaemonID
	for rows.Next() {
		var id storespec.DaemonID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

var _ storespec.DaemonBindingStore = (*daemonBindings)(nil)
