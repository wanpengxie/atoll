package platform

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// expiry.go is the substrate's deadline-closure reaper (期12 S3, glue F1 —
// 义务归位 D3): a level-triggered sweep over declared ExpiresAt that
// materialises `failed` / `unanswered_timeout` terminals for open requests
// whose deadline passed unanswered.
//
// Deadline closure is the SUBSTRATE'S obligation: a request's ExpiresAt is a
// durable declaration in truth (the harness stamps a default TTL when none is
// declared), and the caller — whose in-memory fast-path timer is merely the
// low-latency observer of the same fact — may crash, deregister, or be
// deliberately kept down. The reaper enforces the declaration from truth
// alone: it reads NO registry, judges NO liveness, and NEVER revives anyone
// to close an account (closure lives on the message axis; the actor axis is
// untouched). Authorship is system (the home's welded systemPen — never
// mint-as-caller: a gone caller must not be impersonated), with payload
// `closed_by:system` as the provenance mark; the harness's substrateExpiry
// arm authorizes exactly this (author, word) pair.
//
// Sweep order obeys the boot-order red line: activation → closure → expiry,
// on the same reconcile cadence (startup sweep + ticker + poke).

// expirySweepBatch bounds one sweep's row count so a single tick can never
// scan unboundedly; the keyset cursor carries fairness across ticks.
const expirySweepBatch = 256

// expiredClosedBy is the provenance payload the reaper stamps on every
// terminal it authors — audit reads WHO observed the deadline fact here (and
// on env.Sender), never from the reason word (one fact, one word).
var expiredClosedBy = json.RawMessage(`{"closed_by":"system"}`)

// sweepExpired runs one bounded expiry batch: every open request with
// expires_at <= now gets a system-authored unanswered_timeout terminal.
// Failures are isolated PER ROW (one poison row must not block the batch's
// remaining closures — it is logged loud and retried next tick); a
// concurrent real answer or a racing engine fast-path timer loses benignly
// on the terminal-uniqueness index (behavior.Respond treats a terminal
// duplicate as success).
func (h *Home) sweepExpired(ctx context.Context) {
	if h == nil || h.cs == nil || h.cs.Expiry == nil {
		return
	}
	rows, next, err := h.cs.Expiry.ExpiredOpenRequests(ctx, h.nowMs(), h.expiryCursor, expirySweepBatch)
	if err != nil {
		h.logger.Error("expiry sweep: query", "channel", string(h.channelID), "err", err)
		return
	}
	for i := range rows {
		if rows[i].Err != nil {
			// Poison row: loud, skipped, retried next tick (the cursor stops
			// before it, and the wrap-to-zero below keeps the tail reachable).
			h.logger.Error("expiry sweep: poison row", "channel", string(h.channelID), "err", rows[i].Err)
			continue
		}
		env := rows[i].Row.Envelope
		clock := func() time.Time { return time.UnixMilli(h.nowMs()) }
		if _, rerr := behavior.Respond(ctx, h.systemPen, clock, &env, behavior.ResponseSpec{
			Status:  message.StatusFailed,
			Reason:  string(message.TerminalUnansweredTimeout),
			Payload: expiredClosedBy,
		}); rerr != nil {
			// Per-row isolation: log loud, keep closing the rest, retry next
			// tick (level-triggered — the row stays in the scan until closed).
			h.logger.Error("expiry sweep: close", "channel", string(h.channelID),
				"request", string(env.ID), "err", rerr)
		}
	}
	if len(rows) < expirySweepBatch {
		// Scan reached the end: wrap so the next sweep starts from the top —
		// rows skipped as poison (or failed to close) become reachable again.
		h.expiryCursor = storespec.ExpiryCursor{}
	} else {
		h.expiryCursor = next
	}
}
