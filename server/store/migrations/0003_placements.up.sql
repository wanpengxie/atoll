-- 0003_placements.up.sql — channel_placements per T1.4 / L2 §1.4.11.

CREATE TABLE IF NOT EXISTS channel_placements (
    channel_id               TEXT PRIMARY KEY,
    daemon_id                TEXT NOT NULL,
    state                    TEXT NOT NULL CHECK (state IN ('creating','active','orphan','stale')),
    owner_epoch              INTEGER NOT NULL,
    fencing_token            INTEGER NOT NULL,
    create_request_id        TEXT NOT NULL,
    daemon_connection_epoch  INTEGER NOT NULL DEFAULT 0,
    last_heartbeat_at        INTEGER NOT NULL DEFAULT 0,
    created_at               INTEGER NOT NULL,
    activated_at             INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_placements_state  ON channel_placements(state);
CREATE INDEX IF NOT EXISTS idx_placements_daemon ON channel_placements(daemon_id);
