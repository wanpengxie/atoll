package actorrt

import (
	"context"
	"errors"

	"github.com/wanpengxie/ActOS/protocol/actor"
)

// obs is the substrate's OBSERVATION channel: side-effect-free, non-truth,
// out-of-band reads of a unit's state. It is orthogonal on two axes — timing
// {pull, push} × source {substrate, actor} — and the substrate is the broker for
// all four cells:
//
//	            pull (consumer-timed)         push (producer-timed)
//	substrate   Stat(id) → present+StartedAt  WatchDown → OnDown (down edge = death)
//	actor       Observe(id,kind) → Observer   PublishObs(kind,val) → WatchObs → OnObs
//
// The full 2×2 surface is laid down structurally (owner-directed up-front
// skeleton, NOT痛感-driven) so the shape cannot be re-collapsed; cells with no
// consumer are no-ops (no Observer → ErrObsUnsupported; no ObsWatcher → empty
// fanout). obs is READ-ONLY (you cannot drive work through it) and NON-TRUTH —
// that is the structural guarantee it never becomes a business read bus.
//
// Vocabulary stays out of the substrate: ObsKind/ObsValue are OPAQUE for the
// actor-source cells (the substrate routes/forwards, the actor owns its
// operational-only obs vocabulary and self-answers). Substrate-source facts
// (embodiment/uptime) are the only obs the substrate authoritatively produces, and
// they ride the typed Stat bundle, not an opaque kind.
//
// RETENTION (reviewed 2026-06-12): a kernel audit flagged the ACTOR-source axis
// (ObsKind/ObsValue/Observer/Observe/PublishObs/ObsWatcher/WatchObs/OnObs) as a
// cast-but-not-yet-wired vertical slice — zero production producer/consumer, and
// the cross-wire arm is a no-op (no ipc KindObs frame; see port.observe). Owner
// decision: RETAIN, do NOT rip. A business consumer is imminent (the moment an
// agent/monitor integrates it will need actor-source obs), and completing it is
// a pure-additive wire arm. So this is a DELIBERATELY-PENDING skeleton on an
// explicit keep decision, NOT an oversight — future audits should treat it as
// settled. The substrate-source axis (Stat/WatchDown/OnDown) IS wired
// end-to-end and unaffected.

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

// ErrObsUnsupported is returned by Observe when the addressed unit does not expose
// the requested obs kind (no Observer hook / kind not declared by the actor /
// out-of-process port whose wire-obs is not yet wired).
var ErrObsUnsupported = errors.New("actorrt: obs kind unsupported")

// Observer is the OPTIONAL actor-source PULL hook: an actor implements it to
// answer obs reads about its own operational state (resource/health/queue/
// rate-limit — NEVER business content; business reads are work). Observe is
// invoked OUT-OF-BAND (not on the cell's work goroutine, not through the
// mailbox), so the implementation MUST be concurrency-safe and non-perturbing —
// it reads a snapshot of its own state, like a /metrics scrape handler. An actor
// that does not implement Observer reports ErrObsUnsupported (no-op).
type Observer interface {
	Observe(ctx context.Context, kind ObsKind) (ObsValue, error)
}

// ObsWatcher is the consumer end of the actor-source PUSH channel: it receives
// obs snapshots an actor publishes (PublishObs). The substrate is the broker —
// the producer never knows its watchers. OnObs MUST be non-blocking and must NOT
// panic (a publish runs on the actor's goroutine; the substrate guards panics but
// a blocking watcher stalls the producer). No watcher registered → publish is a
// no-op (empty fanout).
type ObsWatcher interface {
	OnObs(ctx context.Context, id actor.ActorID, kind ObsKind, val ObsValue)
}
