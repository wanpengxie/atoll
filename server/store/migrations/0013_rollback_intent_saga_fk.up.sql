-- 0013_rollback_intent_saga_fk.up.sql — rollback intents are owned by placement_sagas.

ALTER TABLE placement_rollback_intents
    ADD COLUMN saga_id TEXT NOT NULL DEFAULT '';

UPDATE placement_rollback_intents
   SET saga_id = 'rollback:' || channel_id || ':' || create_request_id || ':' || owner_epoch
 WHERE saga_id = '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_rollback_intent_saga_id
    ON placement_rollback_intents(saga_id);

-- SQLite cannot add a table-level FK to an existing table without rebuilding it.
-- The relationship is enforced by idx_rollback_intent_saga_id plus the gateway
-- rollback helper, which creates/CASes placement_sagas before intent mutation.
