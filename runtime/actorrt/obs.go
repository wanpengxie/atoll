package actorrt

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// ObsKind is an opaque selector for an actor-produced operational
// observation. Unit forwards it but never interprets the vocabulary.
type ObsKind string

// ObsValue is one opaque, immutable observation snapshot. Unit forwards it by
// reference, so publishers and consumers must not mutate it after publication.
type ObsValue []byte

// ObsWatcher consumes actor-produced operational snapshots. Observation is
// side-effect-free and non-truth: it cannot drive collaboration work or
// lifecycle. OnObs must return promptly and must not panic.
type ObsWatcher interface {
	OnObs(ctx context.Context, id actor.ActorID, incarnation Incarnation, kind ObsKind, val ObsValue)
}

// Per-entity subscriptions are intentionally absent: current consumers observe
// the population, so a keyed mirror would be derived state with no owner.
