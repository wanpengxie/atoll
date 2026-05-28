-- 0024_daemon_hosted_actor_facade_state.up.sql
--
-- Split proxy-daemon inventory from channel-daemon facade install state.
-- A ready frame only proves the user-machine daemon hosts an actor; it
-- does not prove the cloud channel daemon installed the proxy facade.

ALTER TABLE daemon_hosted_actors ADD COLUMN facade_state TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE daemon_hosted_actors ADD COLUMN facade_detail TEXT NOT NULL DEFAULT '';
ALTER TABLE daemon_hosted_actors ADD COLUMN facade_updated_at INTEGER NOT NULL DEFAULT 0;
