-- 0014_member_transition_abandon.up.sql — cap member transition retries.

ALTER TABLE member_transition_outbox
    ADD COLUMN terminal_status TEXT NOT NULL DEFAULT '';

ALTER TABLE member_transition_outbox
    ADD COLUMN abandoned_at INTEGER NOT NULL DEFAULT 0;

ALTER TABLE member_transition_outbox
    ADD COLUMN abandonment_reason TEXT NOT NULL DEFAULT '';

DROP INDEX IF EXISTS idx_member_transition_unique;
CREATE UNIQUE INDEX IF NOT EXISTS idx_member_transition_unique
    ON member_transition_outbox(channel_id, member_actor_id, transition_kind)
    WHERE terminal_status = '';

DROP INDEX IF EXISTS idx_member_transition_due;
CREATE INDEX IF NOT EXISTS idx_member_transition_due
    ON member_transition_outbox(next_attempt_at, attempts)
    WHERE terminal_status = '';

CREATE TABLE IF NOT EXISTS member_transition_audit_events (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    transition_id      INTEGER NOT NULL,
    channel_id         TEXT NOT NULL,
    member_actor_id    TEXT NOT NULL,
    transition_kind    TEXT NOT NULL,
    event              TEXT NOT NULL,
    reason             TEXT NOT NULL DEFAULT '',
    attempts           INTEGER NOT NULL DEFAULT 0,
    created_at         INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_member_transition_audit_transition
    ON member_transition_audit_events(transition_id, created_at);
