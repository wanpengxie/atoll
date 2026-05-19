-- 0008_placements_entered_state_at.up.sql — timestamp every placement
-- state entry so reconcile grace is row-local instead of process-local.

ALTER TABLE channel_placements ADD COLUMN entered_state_at INTEGER NOT NULL DEFAULT 0;

UPDATE channel_placements
   SET entered_state_at = CASE
       WHEN state = 'active' AND activated_at > 0 THEN activated_at
       ELSE created_at
   END
 WHERE entered_state_at = 0;
