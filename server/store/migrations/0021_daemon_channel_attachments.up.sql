-- 0021_daemon_channel_attachments.up.sql — owner-scoped daemon refactor.
--
-- v1 (migration 0019) tied a proxy daemon row to exactly one channel via
-- daemons.channel_id NOT NULL DEFAULT ''. UX feedback (owner, 2026-05-27):
-- one user-machine install should serve N channels, not require a re-
-- install per channel. We move attachment to a join table and let
-- daemons.channel_id stay (legacy column) but no longer carry meaning for
-- routing — server-side code reads daemon_channel_attachments instead.
--
-- daemon_channel_attachments: explicit per-channel attach decisions the
-- user makes via UI. The proxy daemon's ws connect attaches itself to
-- whatever channels are currently listed here; if none, the daemon is
-- "installed but not used yet".
--
-- Existing daemons rows with non-empty channel_id get one row per
-- attachment automatically so backward-compat is preserved without a
-- code freeze.

CREATE TABLE IF NOT EXISTS daemon_channel_attachments (
    daemon_id     TEXT NOT NULL,
    channel_id    TEXT NOT NULL,
    attached_at   INTEGER NOT NULL,
    PRIMARY KEY (daemon_id, channel_id),
    FOREIGN KEY (daemon_id) REFERENCES daemons(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_daemon_channel_attachments_channel
    ON daemon_channel_attachments(channel_id);

-- Backfill: any existing daemon row that carries a non-empty channel_id
-- (the v1 1:1 model) gets a single attachment so its previously-bound
-- channel is preserved.
INSERT OR IGNORE INTO daemon_channel_attachments (daemon_id, channel_id, attached_at)
SELECT id, channel_id, COALESCE(created_at, 0)
FROM daemons
WHERE channel_id <> '';
