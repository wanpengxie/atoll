package actorrt

// ObsKind is an opaque selector for an actor-produced operational
// observation. Unit forwards it but never interprets the vocabulary.
type ObsKind string

// ObsValue is one opaque, immutable observation snapshot. Unit forwards it by
// reference, so publishers and consumers must not mutate it after publication.
type ObsValue []byte

// Per-entity subscriptions are intentionally absent: current consumers observe
// the population, so a keyed mirror would be derived state with no owner.
