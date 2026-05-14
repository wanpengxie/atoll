-- 0007_placements_federation.up.sql — reserve federation / multi-tenant
-- columns on channel_placements per .dalek/pm/m1.5-tickets.md §T10.
--
-- All three columns are nullable; M1.5 demo flows leave them NULL.
-- They are populated by future tickets:
--   host_actor_id     — M1.4 channel-as-actor: which channel-local actor
--                       exposes this channel externally.
--   federated_origin  — M2+ federation: remote origin this channel mirrors
--                       (NULL = native, non-mirror).
--   tenant_id         — M2+ multi-tenant scope; demo deployments leave
--                       NULL or write the literal "default".
--
-- No index is created in this migration — federation / tenancy paths
-- are not active in M1.5 so an index would only add write cost without
-- a corresponding lookup pattern. Future migrations will add indices
-- when the M1.4 / federation tickets land.

ALTER TABLE channel_placements ADD COLUMN host_actor_id    TEXT;
ALTER TABLE channel_placements ADD COLUMN federated_origin TEXT;
ALTER TABLE channel_placements ADD COLUMN tenant_id        TEXT;
