package actorrt

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// obs is the substrate's OBSERVATION channel: side-effect-free, non-truth,
// out-of-band reads of a unit's state. It is orthogonal on two axes — timing
// {pull, push} × source {substrate, actor} — and the substrate is the broker for
// the three LIVE cells:
//
//	            pull (consumer-timed)         push (producer-timed)
//	substrate   Stat(id) → present+StartedAt  WatchDown → OnDown (down edge = death)
//	actor       —                             PublishObs(kind,val) → WatchObs → OnObs
//
// obs is READ-ONLY (you cannot drive work through it) and NON-TRUTH — that is the
// structural guarantee it never becomes a business read bus.
//
// Vocabulary stays out of the substrate: ObsKind/ObsValue are OPAQUE for the
// actor-source cell (the substrate forwards, the actor owns its operational-only
// obs vocabulary). Substrate-source facts (embodiment/uptime) are the only obs the
// substrate authoritatively produces, and they ride the typed Stat bundle, not an
// opaque kind.
//
// DELETED CELL — actor-source PULL (Observer/Observe), ripped 期7·S8: the fourth
// cell was a zero-consumer half-built vertical slice — an Observe pull hook welded
// into cell/port/embodiment with no producer and no consumer. That is exactly the
// substrate-purity violation a "cast the shape up front" skeleton must NOT decay
// into (a bolted-but-unwired slice misleads more than an empty cell). Its stated
// motive (keep the 2×2 from re-collapsing) is already served by the three live
// cells, and its function — a consumer pulling actor state on demand — is the
// message axis (a request-response read is work), so the cell was redundant as well
// as inert.
//
// REBUILD OBLIGATION: this rips an empty cell, not the concept. When a REAL
// consumer needs actor-source pull, refill this cell — driven by that need, wired
// across BOTH hosts (cell + port) in one batch, landed with a real consumer in the
// same batch. Rebuild = fill the cell (pure-additive), never a structural refactor.

// ObsKind is an opaque observation selector for ACTOR-source obs. The substrate
// never interprets it — it routes the kind to the actor, which owns its own
// (operational, non-business) obs vocabulary and answers or rejects.
type ObsKind string

// ObsValue is one opaque obs snapshot. The substrate stores/forwards it without
// interpretation (Prometheus-registry model: the broker serves what the producer
// published; it does not understand the metric). It is IMMUTABLE BY CONTRACT:
// the substrate forwards it by reference (no clone), so a publisher must not
// mutate a value after publishing and a watcher/reader must not write it.
type ObsValue []byte

// ObsWatcher is the consumer end of the actor-source PUSH channel: it receives
// obs snapshots an actor publishes (PublishObs). The substrate is the broker —
// the producer never knows its watchers. OnObs MUST be non-blocking and must NOT
// panic (a publish runs on the actor's goroutine; the substrate guards panics but
// a blocking watcher stalls the producer). No watcher registered → publish is a
// no-op (empty fanout).
type ObsWatcher interface {
	OnObs(ctx context.Context, id actor.ActorID, kind ObsKind, val ObsValue)
}
