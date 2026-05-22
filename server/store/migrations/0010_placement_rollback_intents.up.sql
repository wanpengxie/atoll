-- 0010_placement_rollback_intents.up.sql — durable reclaim rollback retry queue.

CREATE TABLE IF NOT EXISTS placement_rollback_intents (
    channel_id              TEXT NOT NULL,
    create_request_id       TEXT NOT NULL,
    owner_epoch             INTEGER NOT NULL,
    daemon_id               TEXT NOT NULL,
    daemon_connection_epoch INTEGER NOT NULL DEFAULT 0,
    reason                  TEXT NOT NULL DEFAULT '',
    attempts                INTEGER NOT NULL DEFAULT 0,
    last_attempt_at         INTEGER NOT NULL DEFAULT 0,
    created_at              INTEGER NOT NULL,
    updated_at              INTEGER NOT NULL,
    PRIMARY KEY (channel_id, create_request_id, owner_epoch)
);

CREATE INDEX IF NOT EXISTS idx_rollback_intents_daemon
    ON placement_rollback_intents(daemon_id);
