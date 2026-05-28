-- 0023_daemon_hosted_actor_readiness.up.sql — event-driven readiness
-- projection for per-daemon hosted actors.
--
-- daemon_hosted_actors is the UI's cheap device/adapter surface. The
-- manifest comes from ready frames; live status comes from the proxy
-- daemon's actor.readiness.changed envelope events.

ALTER TABLE daemon_hosted_actors ADD COLUMN ready_state TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE daemon_hosted_actors ADD COLUMN ready_reason TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE daemon_hosted_actors ADD COLUMN ready_detail TEXT NOT NULL DEFAULT '{}';
ALTER TABLE daemon_hosted_actors ADD COLUMN readiness_checked_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daemon_hosted_actors ADD COLUMN last_state_change_at INTEGER NOT NULL DEFAULT 0;
