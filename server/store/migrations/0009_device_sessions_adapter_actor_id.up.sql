-- 0009_device_sessions_adapter_actor_id.up.sql — make device session
-- routing explicit for daemon/device transit.

ALTER TABLE device_sessions
    ADD COLUMN adapter_actor_id TEXT NOT NULL DEFAULT 'tool:xhs';

CREATE INDEX IF NOT EXISTS idx_device_sessions_adapter_actor
    ON device_sessions(adapter_actor_id);
