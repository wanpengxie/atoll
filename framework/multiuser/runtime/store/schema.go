package store

import (
	"context"
	"database/sql"
	"fmt"
)

// ChannelLocalDDL creates multiuser runtime tables inside one channel sqlite.
const ChannelLocalDDL = `
-- =============================================================
-- view_sync_outbox  (L1 §8.6 — daemon persistent outbox)
-- =============================================================
CREATE TABLE IF NOT EXISTS view_sync_outbox (
  seq                INTEGER PRIMARY KEY,
  message_id         TEXT NOT NULL UNIQUE,
  envelope_json      TEXT NOT NULL,
  enqueued_at        INTEGER NOT NULL,
  pushed_at          INTEGER,
  status             TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','pushed')),
  FOREIGN KEY (seq) REFERENCES messages(seq) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_view_sync_outbox_status_seq
  ON view_sync_outbox(status, seq);

-- =============================================================
-- channel_lock  (daemon-side fencing mirror)
-- =============================================================
CREATE TABLE IF NOT EXISTS channel_lock (
  channel_id         TEXT PRIMARY KEY,
  fencing_token      TEXT NOT NULL,
  owner_epoch        INTEGER NOT NULL,
  daemon_id          TEXT NOT NULL,
  daemon_epoch       INTEGER NOT NULL,
  acquired_at        INTEGER NOT NULL,
  refreshed_at       INTEGER NOT NULL,
  channel_type       TEXT NOT NULL DEFAULT ''
);
`

// ChannelLocalTables enumerates multiuser runtime table names.
var ChannelLocalTables = []string{
	"view_sync_outbox",
	"channel_lock",
}

// EnsureChannelTables installs the multiuser runtime tables in a channel DB.
func EnsureChannelTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, ChannelLocalDDL); err != nil {
		return fmt.Errorf("multiuser store: exec channel DDL: %w", err)
	}
	return nil
}

func ensureChannelTablesTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, ChannelLocalDDL); err != nil {
		return fmt.Errorf("multiuser store: exec channel DDL tx: %w", err)
	}
	return nil
}
