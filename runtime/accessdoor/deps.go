package accessdoor

import (
	"context"
	"errors"
	"log/slog"

	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// DriverTable resolves a ResourceKind to its Driver. Closed-but-additive: a plain
// map, one entry per substrate driver, populated at assembly. A kind reaching the
// tree with no entry is an assembly defect (a Go error), never a verdict.
type DriverTable map[resourcespec.ResourceKind]resourcespec.Driver

// StorageMount is one channel-ready daemon's storage-placement candidacy —
// §4.3 policy chain ③④'s raw input.
type StorageMount struct {
	DaemonID string
	Online   bool
}

// StorageMounts is placement routing's mount-table Dep (期11 spec §4.3): "which
// daemons are bound to this channel and currently have a ready service lane".
// The runtime tree DEFINES this contract; platform assembly FILLS it
// (injected from the space daemon host's positive-ready lane view —
// injection-point discipline: "注入点契约 runtime 定,实现填充下游做"). This
// package never imports a composition root to answer it itself.
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
// (homeStorageHostControl over the Platform-owned resource outbox) — the SAME
// purity-wall pattern CreateSpec/KindKV already draw (handle.go): nothing
// outside the runtime tree may import resourcespec directly, so platform
// reaches this contract only through these aliases.
type (
	ResourceOutbox = resourcespec.ResourceOutbox
	ReservationRow = resourcespec.ReservationRow
	TombstoneRow   = resourcespec.TombstoneRow
)

// ErrReservationLost re-exports resourcespec.ErrReservationLost — see its
// doc for the CommitReservation found/err(ErrReservationLost) contract the
// §4.7 Committed handler must preserve.
var ErrReservationLost = resourcespec.ErrReservationLost

// ErrStorageNotReady is StorageControl's "nothing was attempted" answer: the
// placement daemon holds a live lane but has not built the compartment behind
// it yet. It is NOT a failure verdict — the daemon formed no opinion about
// this coord, so the door must not report the create as refused, and the same
// call may succeed once the daemon's compartment is up.
//
// It reaches the door's caller as a Go error rather than an Outcome verdict,
// on the same footing as an unreachable daemon: the door's verdict set covers
// what the resolve→authorize→execute pipeline DECIDED, and this is the case
// where no decision was reached. The caller owns the retry — the door does not
// wait on it, because a compartment build retries with backoff and can take
// minutes, and burying that inside one synchronous create would turn a
// distributed wait into an unbounded one.
//
// The reservation is left standing either way (the Scrubber's timeout sweep
// owns it), so a retried create is safe.
var ErrStorageNotReady = errors.New("accessdoor: storage daemon not ready for this channel")

// StorageControl is the door's send-half of the daemon control-RPC plane
// (期11 spec §4.7): having chosen a placement daemon from StorageMounts plus
// ActorAuthority placement and generated a coord (resourcespec.GenerateCoord), the
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
	// AllocRequest returns ErrStorageNotReady when the placement daemon is
	// reachable but has not built its compartment for this channel yet — see
	// that sentinel's doc for why it is not a failure verdict.
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
// Registry (R + existence), the Drivers (bytes per kind), and ActorAuthority
// (create locus + members late-binding). The actor-scoped (collapsed) branch uses
// only State — the owner-keyed byte realizer for the second, structurally separate
// storage locus (no R, no membership, no kind routing: that absence is the scope
// law). Registry/Drivers/Authority/State are required — New fail-fasts on any
// missing. StorageMounts/StorageControl/ChannelID are file-kind placement's OWN
// deps (期11 §4.3): nil-safe absent — a channel whose assembly never wires them
// (or a kv-only test rig) simply cannot route a file-kind create, honestly
// (ErrNoStoragePlacement-shaped Go error), never a silent kv-shaped placement.
type Deps struct {
	Registry  resourcespec.Registry
	Drivers   DriverTable
	Authority storespec.ResourceActorAuthority
	State     resourcespec.StateStore

	// ChannelID is this door's own channel scope (§4.3: "门缺...Deps增ChannelID
	// 字段, channel-scoped门绑单频道") — StorageMounts.ListStorageDaemons's
	// argument, and StorageAllocSpec's own channel stamp.
	ChannelID channel.ID
	// StorageMounts answers placement chain ③④'s "which daemons, which online".
	StorageMounts StorageMounts
	// StorageControl issues the chosen placement's AllocRequest.
	StorageControl StorageControl
	// TransferControl mints the file byte-route ticket for OpRead/OpWrite(file)
	// and Create(file, with_content=true) — §5's own Dep, nil-safe absent
	// (a kv-only test rig or an assembly that never wires the byte plane
	// simply cannot route file bytes, honestly, never a silent kv-shaped route).
	TransferControl TransferControl

	// Logger is the door's oplog seam (telemetry-completion spec A5/C4):
	// purely a self-report channel, never a decision input — nil-safe
	// absent (a test rig that never wires one simply gets no slog, never a
	// panic). Currently used by the reclaim-loser Warn (query.go) and the
	// OpDelete landed-Info (door.go).
	Logger *slog.Logger
}
