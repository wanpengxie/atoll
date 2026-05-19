-- 0002_catalog.up.sql — workspaces / channels / members (T6 §catalog).

CREATE TABLE IF NOT EXISTS workspaces (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    owner_id    TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    FOREIGN KEY (owner_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_workspaces_owner ON workspaces(owner_id);

CREATE TABLE IF NOT EXISTS workspace_members (
    workspace_id  TEXT NOT NULL,
    user_id       TEXT NOT NULL,
    role          TEXT NOT NULL,              -- 'owner' | 'member'
    joined_at     INTEGER NOT NULL,
    PRIMARY KEY (workspace_id, user_id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id),
    FOREIGN KEY (user_id)      REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS channels (
    id            TEXT PRIMARY KEY,
    workspace_id  TEXT NOT NULL,
    name          TEXT NOT NULL,
    type          TEXT NOT NULL,              -- 'direct' | 'group' | 'agent' | …
    created_at    INTEGER NOT NULL,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
);

CREATE INDEX IF NOT EXISTS idx_channels_workspace ON channels(workspace_id);

-- channel_members maps (channel_id, user_id) → member_actor_id (T1.9 spec).
-- member_actor_id is unique within a channel and travels with every
-- envelope written via the human caller token path.
CREATE TABLE IF NOT EXISTS channel_members (
    channel_id           TEXT NOT NULL,
    user_id              TEXT NOT NULL,
    member_actor_id      TEXT NOT NULL,
    role                 TEXT NOT NULL,       -- 'owner' | 'member'
    joined_at            INTEGER NOT NULL,
    PRIMARY KEY (channel_id, user_id),
    UNIQUE      (channel_id, member_actor_id),
    FOREIGN KEY (channel_id) REFERENCES channels(id),
    FOREIGN KEY (user_id)    REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_channel_members_user ON channel_members(user_id);
