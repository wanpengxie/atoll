package framework

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"

	_ "modernc.org/sqlite"
)

// SQLiteSessionStore persists the daemon-side device-session mirror so a
// daemon restart can recover bind rows without waiting for manual revoke +
// reissue.
type SQLiteSessionStore struct {
	db *sql.DB
}

// OpenSQLiteSessionStore opens/creates a sqlite-backed SessionStore.
func OpenSQLiteSessionStore(ctx context.Context, path string) (*SQLiteSessionStore, func() error, error) {
	if path == "" {
		return nil, nil, fmt.Errorf("framework.SQLiteSessionStore: empty path")
	}
	if err := ensureParentDir(path); err != nil {
		return nil, nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, nil, fmt.Errorf("framework.SQLiteSessionStore: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("framework.SQLiteSessionStore: ping: %w", err)
	}
	store, err := NewSQLiteSessionStore(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return store, db.Close, nil
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o750)
}

// NewSQLiteSessionStore wraps an existing DB handle and ensures schema.
func NewSQLiteSessionStore(ctx context.Context, db *sql.DB) (*SQLiteSessionStore, error) {
	if db == nil {
		return nil, fmt.Errorf("framework.SQLiteSessionStore: nil db")
	}
	if _, err := db.ExecContext(ctx, sqliteSessionSchema); err != nil {
		return nil, fmt.Errorf("framework.SQLiteSessionStore: schema: %w", err)
	}
	return &SQLiteSessionStore{db: db}, nil
}

const sqliteSessionSchema = `
CREATE TABLE IF NOT EXISTS device_session_mirror (
  session_id        TEXT PRIMARY KEY,
  channel_id        TEXT NOT NULL,
  adapter_actor_id  TEXT NOT NULL,
  device_id         TEXT NOT NULL,
  device_type       TEXT NOT NULL,
  state             TEXT NOT NULL,
  bound_at          INTEGER NOT NULL,
  last_active_at    INTEGER NOT NULL DEFAULT 0,
  token_fingerprint TEXT,
  expires_at        INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_device_session_mirror_channel
  ON device_session_mirror(channel_id);
`

// Upsert implements SessionStore.
func (s *SQLiteSessionStore) Upsert(ctx context.Context, sess DeviceSession) error {
	if err := sess.Validate(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO device_session_mirror (
		  session_id, channel_id, adapter_actor_id, device_id, device_type,
		  state, bound_at, last_active_at, token_fingerprint, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(session_id) DO UPDATE SET
		  channel_id        = excluded.channel_id,
		  adapter_actor_id  = excluded.adapter_actor_id,
		  device_id         = excluded.device_id,
		  device_type       = excluded.device_type,
		  state             = excluded.state,
		  bound_at          = excluded.bound_at,
		  last_active_at    = excluded.last_active_at,
		  token_fingerprint = excluded.token_fingerprint,
		  expires_at        = excluded.expires_at`,
		string(sess.SessionID), string(sess.ChannelID), string(sess.AdapterActorID),
		sess.DeviceID, sess.DeviceType, string(sess.State), sess.BoundAt,
		sess.LastActiveAt, sess.TokenFingerprint, sess.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("framework.SQLiteSessionStore.Upsert: %w", err)
	}
	return nil
}

// Get implements SessionStore.
func (s *SQLiteSessionStore) Get(ctx context.Context, sid devicetransit.DeviceSessionID) (DeviceSession, bool, error) {
	var row DeviceSession
	var sessionID, chID, actorID, state string
	err := s.db.QueryRowContext(ctx, `
		SELECT session_id, channel_id, adapter_actor_id, device_id, device_type,
		       state, bound_at, last_active_at, COALESCE(token_fingerprint,''), expires_at
		  FROM device_session_mirror
		 WHERE session_id = ?`, string(sid)).
		Scan(&sessionID, &chID, &actorID, &row.DeviceID, &row.DeviceType,
			&state, &row.BoundAt, &row.LastActiveAt, &row.TokenFingerprint, &row.ExpiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DeviceSession{}, false, nil
		}
		return DeviceSession{}, false, fmt.Errorf("framework.SQLiteSessionStore.Get: %w", err)
	}
	row.SessionID = devicetransit.DeviceSessionID(sessionID)
	row.ChannelID = channel.ID(chID)
	row.AdapterActorID = actor.ActorID(actorID)
	row.State = DeviceState(state)
	return row, true, nil
}

// SetState implements SessionStore.
func (s *SQLiteSessionStore) SetState(ctx context.Context, sid devicetransit.DeviceSessionID, next DeviceState, at int64) error {
	row, ok, err := s.Get(ctx, sid)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("framework.SQLiteSessionStore.SetState: session %q not found", sid)
	}
	if !row.State.CanTransitionTo(next) {
		return fmt.Errorf("framework.SQLiteSessionStore.SetState: illegal transition %s → %s for session %q",
			row.State, next, sid)
	}
	if at <= 0 {
		at = row.LastActiveAt
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE device_session_mirror
		   SET state = ?, last_active_at = ?
		 WHERE session_id = ?`, string(next), at, string(sid))
	if err != nil {
		return fmt.Errorf("framework.SQLiteSessionStore.SetState: %w", err)
	}
	return nil
}

// ListByChannel implements SessionStore.
func (s *SQLiteSessionStore) ListByChannel(ctx context.Context, channelID channel.ID) ([]DeviceSession, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT session_id, channel_id, adapter_actor_id, device_id, device_type,
		       state, bound_at, last_active_at, COALESCE(token_fingerprint,''), expires_at
		  FROM device_session_mirror
		 WHERE channel_id = ?
		 ORDER BY session_id`, string(channelID))
	if err != nil {
		return nil, fmt.Errorf("framework.SQLiteSessionStore.ListByChannel: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []DeviceSession
	for rows.Next() {
		var row DeviceSession
		var sessionID, chID, actorID, state string
		if err := rows.Scan(&sessionID, &chID, &actorID, &row.DeviceID, &row.DeviceType,
			&state, &row.BoundAt, &row.LastActiveAt, &row.TokenFingerprint, &row.ExpiresAt); err != nil {
			return nil, fmt.Errorf("framework.SQLiteSessionStore.ListByChannel scan: %w", err)
		}
		row.SessionID = devicetransit.DeviceSessionID(sessionID)
		row.ChannelID = channel.ID(chID)
		row.AdapterActorID = actor.ActorID(actorID)
		row.State = DeviceState(state)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("framework.SQLiteSessionStore.ListByChannel rows: %w", err)
	}
	return out, nil
}

// Delete implements SessionStore.
func (s *SQLiteSessionStore) Delete(ctx context.Context, sid devicetransit.DeviceSessionID) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM device_session_mirror WHERE session_id = ?`, string(sid)); err != nil {
		return fmt.Errorf("framework.SQLiteSessionStore.Delete: %w", err)
	}
	return nil
}
