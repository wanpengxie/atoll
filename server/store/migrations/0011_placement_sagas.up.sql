-- 0011_placement_sagas.up.sql — explicit placement saga phase tracking.

CREATE TABLE IF NOT EXISTS placement_sagas (
    saga_id                 TEXT PRIMARY KEY,
    channel_id              TEXT NOT NULL,
    create_request_id       TEXT NOT NULL DEFAULT '',
    owner_epoch             INTEGER NOT NULL DEFAULT 0,
    daemon_id               TEXT NOT NULL DEFAULT '',
    daemon_connection_epoch INTEGER NOT NULL DEFAULT 0,
    saga_kind               TEXT NOT NULL CHECK (saga_kind IN (
                                'bootstrap_reserve',
                                'reclaim_reserve',
                                'rollback'
                              )),
    phase                   TEXT NOT NULL CHECK (phase IN (
                                'sent',
                                'awaiting_ack',
                                'partial_takeover',
                                'completed',
                                'abandoned'
                              )),
    sent_at                 INTEGER NOT NULL DEFAULT 0,
    expected_ack_frame_kind TEXT NOT NULL DEFAULT '',
    terminal_status         TEXT NOT NULL DEFAULT '',
    abandonment_reason      TEXT NOT NULL DEFAULT '',
    attempt_count           INTEGER NOT NULL DEFAULT 0,
    last_attempt_at         INTEGER NOT NULL DEFAULT 0,
    created_at              INTEGER NOT NULL,
    updated_at              INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_placement_sagas_channel
    ON placement_sagas(channel_id, saga_kind, phase);

CREATE INDEX IF NOT EXISTS idx_placement_sagas_phase
    ON placement_sagas(phase, sent_at);

INSERT OR IGNORE INTO placement_sagas (
    saga_id, channel_id, create_request_id, owner_epoch, daemon_id,
    daemon_connection_epoch, saga_kind, phase, sent_at,
    expected_ack_frame_kind, attempt_count, last_attempt_at,
    created_at, updated_at
)
SELECT
    'rollback:' || channel_id || ':' || create_request_id || ':' || owner_epoch,
    channel_id,
    create_request_id,
    owner_epoch,
    daemon_id,
    daemon_connection_epoch,
    'rollback',
    'sent',
    created_at,
    'control.unbind_channel_ack',
    attempts,
    last_attempt_at,
    created_at,
    updated_at
FROM placement_rollback_intents;
