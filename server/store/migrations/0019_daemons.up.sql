-- 0019_daemons.up.sql — proxy daemon v2 contract skeleton.
--
-- The repository already has a daemonbus registry table named daemons from
-- migration 0004. T0 keeps that production path intact and extends the table
-- with the nullable/defaulted columns needed by the proxy daemon api-key
-- contract. Later implementation tickets own data access and routing logic.

ALTER TABLE daemons ADD COLUMN channel_id TEXT NOT NULL DEFAULT '';
ALTER TABLE daemons ADD COLUMN owner_id TEXT NOT NULL DEFAULT '';
ALTER TABLE daemons ADD COLUMN name TEXT NOT NULL DEFAULT '';
ALTER TABLE daemons ADD COLUMN api_key TEXT NOT NULL DEFAULT '';
ALTER TABLE daemons ADD COLUMN api_key_prefix TEXT NOT NULL DEFAULT '';
ALTER TABLE daemons ADD COLUMN status TEXT NOT NULL DEFAULT 'offline';
ALTER TABLE daemons ADD COLUMN hostname TEXT;
ALTER TABLE daemons ADD COLUMN proxy_version TEXT;
ALTER TABLE daemons ADD COLUMN last_heartbeat INTEGER;

CREATE INDEX IF NOT EXISTS idx_daemons_channel
    ON daemons(channel_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_daemons_api_key_nonempty
    ON daemons(api_key)
    WHERE api_key <> '';

CREATE TABLE IF NOT EXISTS daemon_active_actors (
    channel_id    TEXT NOT NULL,
    actor_id      TEXT NOT NULL,
    daemon_id     TEXT NOT NULL,
    registered_at INTEGER NOT NULL,
    last_seen_at  INTEGER NOT NULL,
    PRIMARY KEY (channel_id, actor_id),
    FOREIGN KEY (daemon_id) REFERENCES daemons(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_daemon_active_actors_daemon
    ON daemon_active_actors(daemon_id);
