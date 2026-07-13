package platform

import (
	"github.com/wanpengxie/atoll/protocol/actor"
)

// B′ judgement (裁决 4, platform-host-packages-build-spec.md v0.2): ActorDecl
// is the one decl-family word BOTH admission paths (home.Home.SpawnIfAbsent
// cell-side and compute.Run daemon-side) read — a cross-boundary shared
// truth, so it stays on the root membrane. Every other decl-family word
// (Builder/LocalFileOpener/StorageHost/StorageResourceCoord/
// StorageReservationCoord/StorageTombstoneCoord/StorageReclaimAckFunc) is
// spoken by compute alone and lives in platform/compute/decl.go.

// ActorDecl declares one actor the daemon will host. Factory is the ActorFactory
// (def) both admission paths share (home.Home.SpawnIfAbsent cell-side and
// compute.Run daemon-side). On the daemon the Pen + plane-2 (Access/State) + time-axis
// (Schedule) caps are all wired as relay-only proxies over the actor's port
// stream; only Spawn stays nil (the fork/despawn arm does not cross the wire
// this period). The proxies only exist after the actor's stream opens, so the
// actor cannot be pre-built: every cell that can emit needs its pen at
// construction, and in the actor model every actor can emit. There is no
// pen-less construction path. The proxies relay upward without injecting
// identity; the home side welds the actor's authenticated bound id (Mint on the
// pen, the access door minter, the schedule engine minter).
//
// The type is retained as the registry.Constructor return shape (registry/actors
// still hand back a caller-held id+kind+factory triple); compute.Run itself no
// longer takes a []ActorDecl argument — see compute.Config.
type ActorDecl struct {
	ID      actor.ActorID
	Kind    actor.Kind
	Factory ActorFactory
}
