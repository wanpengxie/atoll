-- 0004_daemons.up.sql — daemon registry per T6 §daemonbus.

CREATE TABLE IF NOT EXISTS daemons (
    id                 TEXT PRIMARY KEY,
    host               TEXT NOT NULL DEFAULT '',
    version            TEXT NOT NULL DEFAULT '',
    capacity           INTEGER NOT NULL DEFAULT 0,
    key_hash           TEXT NOT NULL,         -- bcrypt(shared_key)
    connection_epoch   INTEGER NOT NULL DEFAULT 0,
    last_heartbeat_at  INTEGER NOT NULL DEFAULT 0,
    created_at         INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_daemons_heartbeat ON daemons(last_heartbeat_at);
