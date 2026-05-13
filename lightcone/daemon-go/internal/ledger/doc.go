// Package ledger implements the action_ledger Reserve / Commit two-
// phase protocol per L2 §1.4.10.1 (M1.3 ticket T6). It backs the
// turn-replay idempotency guarantees that prevent duplicate external
// side effects after a worker crash.
//
// Two functions form the public surface:
//
//   - Reserve(ledger_key, turn_id, actor_id, now) → ReserveResult
//     On a fresh ledger_key, generates a new envelope_id (UUID v4
//     by default; tests inject deterministic counters) and INSERTs
//     status='reserved'. On a key that already exists, returns the
//     stored envelope_id with Replayed=true — the caller MUST re-emit
//     the same envelope id so the harness step 0.5 dedupes the write.
//
//   - Commit(ledger_key, now) — CAS UPDATE flipping status to
//     'committed'. Idempotent: a second Commit on the same key is a
//     no-op without rewriting committed_at.
//
// Out of scope:
//
//   - ledger_key derivation. Per L2 §1.4.10.1 the caller computes
//     `SHA-256(canonical_json({turn_id, semantic_action_key}))`; the
//     `pkg/canonical` package supplies the canonical_hash primitive
//     (T2). Different message types pick different
//     `semantic_action_key` recipes; this package stays type-agnostic.
//
//   - GC of old ledger rows. The spec calls it optional ("committed +
//     N days, reserved + N hours abandoned") and leaves the cadence to
//     channel config; a future cron job lands once observability hooks
//     report the actual row counts in practice.
//
// Authoritative spec reference: .dalek/pm/v4-layer2-spec.md §1.4.10.1
// ("Turn Replay 幂等"); M1.3 ticket T6 in .dalek/pm/m1.3-tickets.md.
package ledger
