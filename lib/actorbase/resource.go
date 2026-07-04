package actorbase

import (
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// ResourceHandle is sys.Resource()'s thin wrap over the access plane
// (accessdoor.AccessHandle) — the verb table's "Access 臂" row. It is a
// zero-second-semantics narrowing, not a reinterpretation: AccessHandle.Invoke
// takes one op out of the closed set plus an optional Grant operand; this
// splits that single method into its four non-grant verbs (create/read/
// write/delete) so a Proc body never hand-assembles an access.Operation or an
// access.Grant it has no legitimate use for. OpSet (grant management,
// "share") is the spec's B-P1 deferred slot: the word is reserved here in
// comment form only, no method — additive when a real consumer needs it.
type ResourceHandle interface {
	// Create allocates a NEW resource id with initial bytes — birth is
	// ownership (access.OpCreate).
	Create(id resource.ResourceID, args []byte) (accessdoor.Outcome, error)
	// Read returns the resource's current bytes (access.OpRead).
	Read(id resource.ResourceID) (accessdoor.Outcome, error)
	// Write mutates an EXISTING resource's bytes (access.OpWrite); a
	// not-yet-created id is resource_not_found, not a silent create.
	Write(id resource.ResourceID, args []byte) (accessdoor.Outcome, error)
	// Delete is the resource's explicit, non-lossy death (access.OpDelete).
	Delete(id resource.ResourceID) (accessdoor.Outcome, error)
}

// StateHandle is sys.State()'s thin wrap over the actor-scoped (collapsed)
// branch of the same access plane — the verb table's "State 臂" row. Same
// underlying accessdoor.AccessHandle contract as ResourceHandle, welded to
// this actor's own owner coordinate instead of the channel-scoped tree; its
// own KV vocabulary (Get/Put/Del) is deliberately spelled differently from
// ResourceHandle's (Create/Read/Write/Delete) because the two loci read
// differently to a Proc author even though one engine implements both. Put
// is an UPSERT — the create-or-write branch the retired 期8 StateKV facade
// carried is absorbed into the engine's stateAdapter.Put (a first write to a
// new key must not surprise the author with resource_not_found).
type StateHandle interface {
	Get(id resource.ResourceID) (accessdoor.Outcome, error)
	Put(id resource.ResourceID, args []byte) (accessdoor.Outcome, error)
	Del(id resource.ResourceID) (accessdoor.Outcome, error)
}
