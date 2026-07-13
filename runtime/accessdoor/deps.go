package accessdoor

import (
	"context"
	"log/slog"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// MembershipCheck is the door's narrow channel-membership seam. Two loci consult
// it: op=create's container check (member ⟹ create-own) AND the members-grant
// late-binding resolution for object ops (GranteeMembers = "resolved by the door
// AT CHECK TIME"). The implementor adapts storespec.Registry.Lookup + Record.
// IsActive (NOT Exists, which does not distinguish a deregistered actor).
type MembershipCheck interface {
	IsMember(ctx context.Context, id actor.ActorID) (bool, error)

	// Lookup answers "where is this actor" for placement routing's creator-
	// affinity locus (期11 spec §4.3 policy chain ①): Host — the compute id
	// currently hosting id if it is daemon-attached ("" for a home-placed
	// cell, a human, or an id with no active membership at all).
	// found=false when id has no active membership row (mirrors IsMember's
	// own IsActive discipline, never Exists). Host is read-time (the SAME
	// membership column the home reconcile ring's placement filter and
	// link.Acceptor.reconcileHost already consult), never cached — a daemon
	// attach/detach between two Lookup calls is not this seam's problem to
	// paper over. (A kind return was retired: every caller wants placement,
	// none ever read the kind — purity v1 S3, owner 2026-07-13.)
	Lookup(ctx context.Context, id actor.ActorID) (host string, found bool, err error)
}

// DriverTable resolves a ResourceKind to its Driver. Closed-but-additive: a plain
// map, one entry per substrate driver, populated at assembly. A kind reaching the
// tree with no entry is an assembly defect (a Go error), never a verdict.
type DriverTable map[resourcespec.ResourceKind]resourcespec.Driver

// StorageMount is one channel-attached daemon's storage-placement candidacy —
// §4.3 policy chain ③④'s raw input. OwnerUserID is day-1 UNUSED (populated ""
// by every current StorageMounts implementor): policy chain ② (owner-level
// creator affinity) is deferred whole to the human-inbound debt this field's
// consumer would need (§4.3's own text: "此数据流与human接线同源，day-1不实现"),
// so the field exists for the day ② lands, not exercised now — declaring it
// without a consumer is NOT the half-built-slice violation the project's
// substrate-purity rule warns against, because ITS OWN CONSUMER (②) is
// explicitly named and deferred by the spec text this type implements, not
// invented ahead of a real use case.
type StorageMount struct {
	DaemonID    string
	OwnerUserID string
	Online      bool
}

// StorageMounts is placement routing's mount-table Dep (期11 spec §4.3): "which
// daemons are attached to this channel, and which of those are online right
// now". The runtime tree DEFINES this contract; platform assembly FILLS it
// (late-bound, closing over the link Acceptor's attach state — §4.3's own
// injection-point discipline: "注入点契约 runtime 定,实现填充下游做"). This
// package never imports platform/app to answer it itself.
type StorageMounts interface {
	ListStorageDaemons(ctx context.Context, channelID channel.ID) ([]StorageMount, error)
}

// StorageAllocSpec is one AllocRequest's payload (期11 spec §4.7's first
// frame, home→daemon): the door already knows everything the daemon's
// Allocator needs to mkdir/touch staging — channel, the server-generated
// coord (§1.6), and whether this is a directory create — before any byte
// moves. Carried through StorageControl.AllocRequest, never over the access
// wire (§8.1: coord never leaves the door/daemon pair).
type StorageAllocSpec struct {
	ChannelID channel.ID
	Coord     string
	Dir       bool
}

// ResourceOutbox / ReservationRow / TombstoneRow / ErrReservationLost
// re-export resourcespec's create/delete-outbox completion contract for the
// §4.7 daemon control-RPC handler platform assembly builds
// (homeStorageHostControl over runtime.ChannelStores.Outbox) — the SAME
// purity-wall pattern CreateSpec/KindKV already draw (handle.go): nothing
// outside the runtime tree may import resourcespec directly
// (archtest.TestResourcespecImportedOnlyWithinRuntime), so platform reaches
// this contract only through these aliases.
type (
	ResourceOutbox = resourcespec.ResourceOutbox
	ReservationRow = resourcespec.ReservationRow
	TombstoneRow   = resourcespec.TombstoneRow
)

// ErrReservationLost re-exports resourcespec.ErrReservationLost — see its
// doc for the CommitReservation found/err(ErrReservationLost) contract the
// §4.7 Committed handler must preserve.
var ErrReservationLost = resourcespec.ErrReservationLost

// StorageControl is the door's send-half of the daemon control-RPC plane
// (期11 spec §4.7): having chosen a placement daemon (via StorageMounts +
// Membership.Lookup) and generated a coord (resourcespec.GenerateCoord), the
// door hands the ALLOCATION intent to whichever party owns the live
// connection to that daemon — platform assembly, never this package, which
// has no notion of a link/wire. AllocRequest blocks until the daemon's
// AllocAck lands (or ctx/timeout/daemon-unreachable), so the door's Create
// call stays synchronous for the content-less path (§1.5's "空 create").
// Not itself named in spec §4.3's own Dep list (that item only covers
// PLACEMENT CHOICE) — it is this section's own addition, the minimal
// injection point letting door.create actually ISSUE the chosen placement's
// AllocRequest rather than stopping at "here is a daemon id".
type StorageControl interface {
	AllocRequest(ctx context.Context, daemonID string, spec StorageAllocSpec) error
	// ReclaimRequest collects an orphaned coord's already-allocated bytes on
	// daemonID (期11 review §2.5 #B). It is the content-less create loser's
	// synchronous reclaim: a content-less create AllocRequest's its coord up
	// front (an empty live/<coord>) but moves no bytes, so a loser has no
	// Committed round trip on which the with-content path's
	// CommittedReply.Lost→ReclaimCoord signal could ride — this is that signal
	// for the synchronous path. Best-effort from the door's view: a returned
	// error is logged (query.go's reclaim-loser branch, nil-safe Deps.Logger),
	// never propagated into the caller-facing verdict (the create already
	// resolved AlreadyExists; a missed reclaim leaves at worst an empty
	// directory, never a correctness fault).
	ReclaimRequest(ctx context.Context, daemonID string, coord string) error
}

// Deps bundles the collaborators the door needs. The channel-scoped tree uses the
// Registry (R + existence), the Drivers (bytes per kind), and the Membership seam
// (create locus + members late-binding). The actor-scoped (collapsed) branch uses
// only State — the owner-keyed byte realizer for the second, structurally separate
// storage locus (no R, no membership, no kind routing: that absence is the scope
// law). Registry/Drivers/Membership/State are required — New fail-fasts on any
// missing. StorageMounts/StorageControl/ChannelID are file-kind placement's OWN
// deps (期11 §4.3): nil-safe absent — a channel whose assembly never wires them
// (or a kv-only test rig) simply cannot route a file-kind create, honestly
// (ErrNoStoragePlacement-shaped Go error), never a silent kv-shaped placement.
type Deps struct {
	Registry   resourcespec.Registry
	Drivers    DriverTable
	Membership MembershipCheck
	State      resourcespec.StateStore

	// ChannelID is this door's own channel scope (§4.3: "门缺...Deps增ChannelID
	// 字段, channel-scoped门绑单频道") — StorageMounts.ListStorageDaemons's
	// argument, and StorageAllocSpec's own channel stamp.
	ChannelID channel.ID
	// StorageMounts answers placement chain ③④'s "which daemons, which online".
	StorageMounts StorageMounts
	// StorageControl issues the chosen placement's AllocRequest.
	StorageControl StorageControl
	// LaneControl mints the file byte-route Token for OpRead/OpWrite(file)
	// and Create(file, with_content=true) — §5's own Dep, nil-safe absent
	// (a kv-only test rig or an assembly that never wires the lane simply
	// cannot route file bytes, honestly, never a silent kv-shaped route).
	LaneControl LaneControl

	// Logger is the door's oplog seam (telemetry-completion spec A5/C4):
	// purely a self-report channel, never a decision input — nil-safe
	// absent (a test rig that never wires one simply gets no slog, never a
	// panic). Currently used by the reclaim-loser Warn (query.go) and the
	// OpDelete landed-Info (door.go).
	Logger *slog.Logger
}
