package actorbase

import (
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// ResourceHandle is sys.Resource()'s thin wrap over the access plane's
// RESOURCE face (accessdoor.ResourceAccessHandle) — the verb table's "Access
// 臂" row. It is a zero-second-semantics narrowing, not a reinterpretation:
// AccessHandle.Invoke takes one op out of the closed set;
// Read/Write/Delete split that single method into named verbs
// so a Proc body never hand-assembles an access.Operation it
// has no legitimate use for. There are NO share verbs: authorization is
// membrane-uniform (PM-D1) — membership itself is the read/write right, so
// there is nothing to share.
//
// Create (期11 spec §3.1's "create 单入口") now goes through the door's OWN
// dedicated Create method under the hood — this interface keeps its
// pre-existing day-1 signature (kv inline value only, no CreateSpec exposed
// to a Proc author yet) since file-kind creation has no domain-facing sugar
// built this period (S4/S5 land the daemon-side machinery Create's file
// branch needs first).
//
// Stat/List are the read face landing here with zero
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
	// Delete is the resource's explicit, non-lossy death (access.OpDelete) —
	// authorized for the creator or the channel owner (PM-D3).
	Delete(id resource.ResourceID) (accessdoor.Outcome, error)

	// Stat projects id's member-visible metadata + caller's effective ops.
	Stat(id resource.ResourceID) (accessdoor.StatResult, error)
	// List enumerates channel-scoped resources this caller can see
	// (membrane-uniform: any active member), paginated.
	List(q accessdoor.ListQuery) (accessdoor.ListPage, error)

	// Open is file kind's own byte-access verb (期11 spec §3.9': "file 读/写
	//口：Open(ctx, id, mode)(FileAccess, error)") — read/write for kv stay
	// on Read/Write above (Outcome.Value carries the bytes); for a file-kind
	// id, Read/Write would only ever surface an authorization Route with no
	// bytes attached (§8.1: file content never rides Outcome.Value) —
	// Open is the actual entry point a Proc author calls for file bytes,
	// redeeming that Route into a live FileAccess in one call. The call face is
	// unconditionally present regardless of placement: a caller on the file's
	// own daemon receives a local handle, while an actor on another daemon
	// receives the same Reader/Writer face over the lane exchange.
	Open(id resource.ResourceID, mode access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error)

	// CreateFile creates an empty file or returns the write-side FileAccess
	// used to stream initial content. The content-less path lands synchronously;
	// the with-content path's unified FileAccess writer must be
	// Write()'d then Commit()'d (or Abort()'d) by the caller — mirrors
	// Open's own redemption contract exactly, since both ride the SAME
	// Outcome.Route carrier (§5 item 0).
	CreateFile(id resource.ResourceID, withContent bool) (accessdoor.FileAccess, accessdoor.Outcome, error)
	// CreateFileDecided asks the door to authorize a file create but leaves
	// route redemption to the caller (the human HTTP byte leg uses this form).
	CreateFileDecided(id resource.ResourceID, withContent bool) (accessdoor.Outcome, error)
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
