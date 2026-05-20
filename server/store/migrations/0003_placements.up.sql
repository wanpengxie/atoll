-- 0003_placements.up.sql — channel_placements per T1.4 / L2 §1.4.11.

CREATE TABLE IF NOT EXISTS channel_placements (
    channel_id               TEXT PRIMARY KEY,
    daemon_id                TEXT NOT NULL,
    state                    TEXT NOT NULL CHECK (state IN ('creating','active','orphan','stale')),
    owner_epoch              INTEGER NOT NULL,
    -- fencing_token is an opaque, cryptographically unguessable string
    -- (32-char hex from crypto/rand) per proto-foundation §3.6.1 +
    -- proto-layer1 §6.2. Decoupled from owner_epoch (epoch is the
    -- monotonic ordering counter; this is the unguessable gate value).
    fencing_token            TEXT NOT NULL,
    create_request_id        TEXT NOT NULL,
    daemon_connection_epoch  INTEGER NOT NULL DEFAULT 0,
    last_heartbeat_at        INTEGER NOT NULL DEFAULT 0,
    created_at               INTEGER NOT NULL,
    activated_at             INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_placements_state  ON channel_placements(state);
CREATE INDEX IF NOT EXISTS idx_placements_daemon ON channel_placements(daemon_id);
