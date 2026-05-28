-- 0015_device_actor_tokens.up.sql — actor registration tokens for devicebus.

CREATE TABLE IF NOT EXISTS device_actor_tokens (
    token_hash     TEXT PRIMARY KEY,
    actor_id       TEXT NOT NULL,
    channel_id     TEXT NOT NULL,
    user_id        TEXT NOT NULL,
    daemon_id      TEXT NOT NULL,
    device_id      TEXT NOT NULL,
    device_type    TEXT NOT NULL,
    expires_at     INTEGER NOT NULL,
    created_at     INTEGER NOT NULL,
    last_active_at INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_device_actor_tokens_actor_channel
    ON device_actor_tokens(actor_id, channel_id);

CREATE INDEX IF NOT EXISTS idx_device_actor_tokens_expiry
    ON device_actor_tokens(expires_at);

INSERT OR IGNORE INTO device_actor_tokens (
    token_hash, actor_id, channel_id, user_id, daemon_id,
    device_id, device_type, expires_at, created_at, last_active_at
)
SELECT
    token_hash,
    COALESCE(NULLIF(adapter_actor_id, ''), 'tool:xhs') AS actor_id,
    channel_id,
    user_id,
    daemon_id,
    device_id,
    device_type,
    expires_at,
    created_at,
    0
FROM device_sessions
WHERE token_hash IS NOT NULL
  AND token_hash != ''
  AND state IN ('ready', 'active', 'offline');
