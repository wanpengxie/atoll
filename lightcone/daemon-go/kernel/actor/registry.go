package actor

import "context"

// Registration is the in-memory shape of a single actor record.
//
// The concrete persistence (sqlite-backed channel-local actor_registry)
// lives in runtime/store per T3. kernel only owns the interface
// contract.
type Registration struct {
	ID   ActorId
	Kind SenderKind
	Name string
}

// Registry is the in-memory abstraction of the channel-local
// actor_registry table (L1 §12).
//
// All methods MUST be safe for concurrent use. Lookups return
// (Registration, true) for hits and (zero, false) for misses; no error
// is returned for "not found".
//
// Authoritative spec: L1 §12 actor_registry.
type Registry interface {
	Register(ctx context.Context, r Registration) error
	Deregister(ctx context.Context, id ActorId) error
	Lookup(ctx context.Context, id ActorId) (Registration, bool, error)
}
