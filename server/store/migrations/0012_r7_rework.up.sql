ALTER TABLE placement_rollback_intents
    ADD COLUMN next_attempt_at INTEGER NOT NULL DEFAULT 0;

UPDATE placement_rollback_intents
   SET next_attempt_at = 0
 WHERE next_attempt_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_rollback_intents_next_attempt
    ON placement_rollback_intents(next_attempt_at, attempts);

CREATE TABLE IF NOT EXISTS member_transition_outbox (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id         TEXT NOT NULL,
    user_id            TEXT NOT NULL DEFAULT '',
    member_actor_id    TEXT NOT NULL,
    role               TEXT NOT NULL DEFAULT '',
    transition_kind    TEXT NOT NULL CHECK (transition_kind IN ('add','remove')),
    attempts           INTEGER NOT NULL DEFAULT 0,
    last_attempt_at    INTEGER NOT NULL DEFAULT 0,
    next_attempt_at    INTEGER NOT NULL DEFAULT 0,
    subscription_revoked_at INTEGER NOT NULL DEFAULT 0,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_member_transition_unique
    ON member_transition_outbox(channel_id, member_actor_id, transition_kind);

CREATE INDEX IF NOT EXISTS idx_member_transition_due
    ON member_transition_outbox(next_attempt_at, attempts);
