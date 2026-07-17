package actorbase

import (
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// ResourceHandle is sys.Resource()'s thin wrap over the access plane's
// RESOURCE face (accessdoor.ResourceAccessHandle) — the verb table's "Access
// 臂" row. It is a zero-second-semantics narrowing, not a reinterpretation:
// AccessHandle.Invoke takes one op out of the closed set plus an optional
// Grant operand; Read/Write/Delete split that single method into their
// non-grant verbs so a Proc body never hand-assembles an access.Operation it
// has no legitimate use for.
//
// Create (期11 spec §3.1's "create 单入口") now goes through the door's OWN
// dedicated Create method under the hood — this interface keeps its
// pre-existing day-1 signature (kv inline value only, no CreateSpec exposed
// to a Proc author yet) since file-kind creation has no domain-facing sugar
// built this period (S4/S5 land the daemon/lane machinery Create's file
// branch needs first).
//
// ShareActor/ShareMembers/Stat/List are 期11 §3's "词表糖名" additions — the
// Share verbs are OpSet sugar (a Proc body never hand-assembles an
// access.Grant), Stat/List are the read face landing here with zero
// reinterpretation (same accessdoor.StatResult/ListPage/ListQuery shapes,
// just reachable without an accessdoor import in domain code).
type ResourceHandle interface {
	// Create allocates a NEW resource id with initial bytes — birth is
	// ownership (access.OpCreate, day-1 kv only).
	Create(id resource.ResourceID, args []byte) (accessdoor.Outcome, error)
	// Read returns the resource's current bytes (access.OpRead).
	Read(id resource.ResourceID) (accessdoor.Outcome, error)
	// Write mutates an EXISTING resource's bytes (access.OpWrite); a
	// not-yet-created id is resource_not_found, not a silent create.
	Write(id resource.ResourceID, args []byte) (accessdoor.Outcome, error)
	// Delete is the resource's explicit, non-lossy death (access.OpDelete).
	Delete(id resource.ResourceID) (accessdoor.Outcome, error)

	// ShareActor grants ops on id to a single actor identity (chmod-style
	// SET; an empty ops revokes) — the sugar over Invoke(OpSet) with an
	// actor-kind Grant, so a Proc body never hand-assembles one. Day-1
	// narrowed to ops⊆{read,write} by the door's existing overreach check
	// (unchanged, enforced beneath this sugar).
	ShareActor(id resource.ResourceID, actorID actor.ActorID, ops []access.Operation) (accessdoor.Outcome, error)
	// ShareMembers is ShareActor's members-grantee twin — grants ops to the
	// container channel's current membership (late-bound at check time).
	ShareMembers(id resource.ResourceID, ops []access.Operation) (accessdoor.Outcome, error)

	// Stat projects id's any-grant-visible metadata + caller's effective ops.
	Stat(id resource.ResourceID) (accessdoor.StatResult, error)
	// List enumerates channel-scoped resources this caller can see (any-grant
	// projection), paginated.
	List(q accessdoor.ListQuery) (accessdoor.ListPage, error)

	// Open is file kind's own byte-access verb (期11 spec §3.9': "file 读/写
	//口：Open(ctx, id, mode)(FileAccess, error)") — read/write for kv stay
	// on Read/Write above (Outcome.Value carries the bytes); for a file-kind
	// id, Read/Write would only ever surface an authorization Route with no
	// bytes attached (§8.1: file content never rides Outcome.Value) —
	// Open is the actual entry point a Proc author calls for file bytes,
	// redeeming that Route into a live FileAccess (a local os.Root-scoped
	// handle or a lane byte-stream) in one call. The call face is
	// unconditionally present regardless of placement (FileOpener is
	// embedded in ResourceAccessHandle, no type assertion): a daemon-hosted
	// caller has a real byte lane; a home-hosted caller gets an honest
	// capability_unavailable outcome — mechanism complete, capability
	// deferred.
	Open(id resource.ResourceID, mode access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error)

	// CreateFile is file kind's own create verb (期11 spec §1.5): dir=true
	// creates an empty directory-shaped resource; dir=false+withContent=false
	// creates an empty regular file; dir=false+withContent=true creates a
	// content-bearing file and returns the write-side FileAccess to stream
	// bytes into (dir=true+withContent=true is rejected — a directory
	// carries no content, §1.5's ingress gate). The content-less paths land
	// synchronously (FileAccess is zero, nothing left to write); the
	// with-content path's FileAccess.Local/Stream write handle must be
	// Write()'d then Commit()'d (or Abort()'d) by the caller — mirrors
	// Open's own redemption contract exactly, since both ride the SAME
	// Outcome.Route carrier (§5 item 0).
	CreateFile(id resource.ResourceID, dir bool, withContent bool) (accessdoor.FileAccess, accessdoor.Outcome, error)
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
