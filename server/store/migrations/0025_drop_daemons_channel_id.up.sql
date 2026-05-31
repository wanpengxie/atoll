-- 0025_drop_daemons_channel_id.up.sql — drop the legacy daemons.channel_id
-- column. It was the v1 (migration 0019) 1:1 daemon→channel routing key.
-- Migration 0021 moved routing to daemon_channel_attachments and backfilled
-- existing rows; the column has carried no meaning since. Drop it (and its
-- index) so daemons routing has a single source of truth: the join table.

DROP INDEX IF EXISTS idx_daemons_channel;

ALTER TABLE daemons DROP COLUMN channel_id;
