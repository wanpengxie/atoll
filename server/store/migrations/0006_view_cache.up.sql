-- 0006_view_cache.up.sql — server-side view cache per T1.1 + T1.8.
--
-- INSERT OR IGNORE on (channel_id, seq) PRIMARY KEY + (channel_id,
-- message_id) UNIQUE INDEX gives at-least-once durable idempotency
-- (T1.8 transactional boundary).

CREATE TABLE IF NOT EXISTS view_cache_messages (
    channel_id    TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    message_id    TEXT NOT NULL,
    envelope_json TEXT NOT NULL,
    received_at   INTEGER NOT NULL,
    PRIMARY KEY (channel_id, seq)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_view_cache_message_id
    ON view_cache_messages(channel_id, message_id);

CREATE TABLE IF NOT EXISTS view_cache_cursors (
    channel_id        TEXT PRIMARY KEY,
    last_received_seq INTEGER NOT NULL DEFAULT 0
);
