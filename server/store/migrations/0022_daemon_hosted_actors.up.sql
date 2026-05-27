-- 0022_daemon_hosted_actors.up.sql — per-daemon adapter manifest.
--
-- daemon_active_actors (0019) is per-(channel, actor) routing — only
-- the channels the daemon is currently attached to appear there.
--
-- daemon_hosted_actors is the strictly per-daemon manifest: "this
-- machine's proxy daemon advertises tool:kimi + tool:xhs (+ capability
-- payload)". Populated every ready frame so the global「我的设备」page
-- can show what each device hosts even when zero channels are attached.

CREATE TABLE IF NOT EXISTS daemon_hosted_actors (
    daemon_id      TEXT NOT NULL,
    actor_id       TEXT NOT NULL,
    capability_set TEXT NOT NULL DEFAULT '',
    last_ready_at  INTEGER NOT NULL,
    PRIMARY KEY (daemon_id, actor_id),
    FOREIGN KEY (daemon_id) REFERENCES daemons(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_daemon_hosted_actors_daemon
    ON daemon_hosted_actors(daemon_id);
