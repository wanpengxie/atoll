-- 0005_device_sessions.up.sql — device sessions per T1.10 lifecycle.
--
-- State machine: pending → ready → active → offline / expired / revoked.

CREATE TABLE IF NOT EXISTS device_sessions (
    device_session_id  TEXT PRIMARY KEY,
    device_id          TEXT NOT NULL,
    device_type        TEXT NOT NULL DEFAULT '',
    channel_id         TEXT NOT NULL,
    user_id            TEXT NOT NULL,
    daemon_id          TEXT NOT NULL,
    token_hash         TEXT NOT NULL,         -- HMAC-SHA-256 hex of issued token
    state              TEXT NOT NULL CHECK (state IN ('pending','ready','active','offline','expired','revoked')),
    expires_at         INTEGER NOT NULL,
    created_at         INTEGER NOT NULL,
    last_state_at      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_device_sessions_channel ON device_sessions(channel_id);
CREATE INDEX IF NOT EXISTS idx_device_sessions_state   ON device_sessions(state);
