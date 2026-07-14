package resourcespec

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
)

// ErrAlreadyExists is the atomic-create collision sentinel; the door maps it to
// access.AlreadyExists. create is a test-and-set on existence, so the collision
// is decided INSIDE Registry.Create's transaction (within the race window) —
// the door never resolves-then-creates in two steps.
var ErrAlreadyExists = errors.New("resourcespec: resource already exists")

// ErrOwnerInactive means an actor-scoped resource could not be born because
// its owning actor was missing or already deregistered. StateStore.Create
// decides this in the same transaction as its conditional insert, so callers
// never observe a successful create that outlives an inactive owner.
var ErrOwnerInactive = errors.New("resourcespec: actor-scoped resource owner inactive")

// ErrReservationLost is CommitReservation's same-transaction race sentinel
// (期11 spec §1.7's "并发败者"): landing lost the same-resource_id race
// against another reservation's earlier-committed row. The transaction still
// deletes THIS reservation row before returning the sentinel — the loser is
// never left dangling — so callers use it only to signal the daemon to clean
// up its staged bytes, never to retry the write (the resource id is already
// owned by the winner).
var ErrReservationLost = errors.New("resourcespec: reservation lost the create race")

// ErrMalformedCursor is List's cursor-decode sentinel (期11 spec §3.7's own
// words: "A malformed cursor is a Go error [...] verdict mapping is the
// door's job"): a syntactically bad or corrupted cursor token is still a Go
// error at THIS layer (List does no verdict mapping of its own), but it must
// be a DISTINGUISHABLE one — errors.Is-able — so the door can map exactly
// this failure (and only this one) to the QueryBadCursor verdict rather than
// an infra failure surfacing as a bad_cursor lie or a genuine cursor typo
// surfacing as an opaque Go error the caller cannot recover from.
var ErrMalformedCursor = errors.New("resourcespec: malformed list cursor")

// ResourceMeta is what the registry knows about an existing resource besides
// its bytes. There is NO Controller field: control is a full-rights entry in R,
// not a separate owner column. There is NO Scope field — and never will be:
// actor-scoped objects live in a SEPARATE storage locus (an actor_state-shaped
// table, keyed by owner), so scope is expressed by the STRUCTURE an object
// lives in, not by a column (the Unix calibration: an anonymous mapping is not
// a file tagged "anonymous", it simply is not in the fs namespace). This
// Registry and its table hold only channel-scoped objects.
type ResourceMeta struct {
	Kind      ResourceKind
	CreatedAt int64

	// PlacementDaemonID is the explicit routing column: which daemon's
	// Streamer holds the bytes ("" for kv). A durable daemon identity (§4),
	// never re-derived from PlacementCoord.
	PlacementDaemonID string

	// PlacementCoord is the opaque storage handle ("" for kv) — the SAME
	// field the design doc calls "storage handle": server-registry-generated
	// (§1.6), interpreted ONLY by the daemon named in PlacementDaemonID.
	// LOAD-BEARING CONFINEMENT: this field must NEVER cross the door's
	// Stat/List projection to a caller (§3.6 red line) — that boundary is
	// drawn one layer up (StatMeta is a SEPARATE, coord-less projection
	// type, §3), not here; ResourceMeta is the registry's full internal
	// truth, not a caller-facing shape.
	PlacementCoord string

	// CreatedBy is the durable creator identity — a PURE AUDIT column, not
	// an authorization predicate: the creator's authority is the full-rights
	// grant Create writes into R (an actor entry via SetGrant's shape), and
	// that grant — never a read of this field — is what "out-lives kind
	// checks" as the ownership predicate (the design doc's "出生满权 grant
	// 才是所有权谓词" — birth-time full grant is the ownership predicate).
	CreatedBy actor.ActorID

	// Dir is the file BYTE-SHAPE bit (the inode's S_IFDIR analogue, 期11 丁12):
	// true = this KindFile resource is directory-shaped (a workspace — its
	// bytes are a WHOLE TREE委托 the real fs, one ResourceID / one户口行, R
	// granularity = the whole tree), false = a regular single-blob file (or
	// kv, always false). This is the door's Open-ROUTING truth: a dir Open
	// hands out an os.Root subtree lease句柄 (free os.Create/Mkdir/Open/Remove,
	// NO Commit boundary — every op lands immediately), a regular-file Open
	// hands out the single-file staging→rename write句柄 (§3.9'). Declared by
	// the creator at birth (CreateSpec.Dir), stored here, read at Resolve —
	// NEVER re-derived by the daemon statting the disk (daemon holds no truth).
	Dir bool
}

// ReservationRow is one create-outbox reservation (期11 spec §1.3's
// resource_reservations, §4's daemon-facing projection): the durable
// write-ahead half of a with-content file create, before Committed lands the
// row. Consumed by the daemon's Scrubber via ListReservationsByDaemon
// (§4.7's ReconcilePull "挂起 reservation" half) so it can decide whether a
// staged-but-uncommitted byte set should resume or be swept as an orphan.
type ReservationRow struct {
	ReservationID     string
	ResourceID        resource.ResourceID
	Kind              ResourceKind
	PlacementDaemonID string
	PlacementCoord    string
	CreatedBy         actor.ActorID
	ReservedAt        int64

	// LastProgressAt is 期11 S1's "在途登记" liveness stamp (schema.go's
	// resource_reservations.last_progress_at): seeded to ReservedAt at
	// ReserveCreate, bumped by TouchReservationsByCoords. SweepExpiredReservations
	// ages on this field, never ReservedAt.
	LastProgressAt int64
}

// TombstoneRow is one delete-outbox tombstone (§1.8's resource_tombstones,
// §4's daemon-facing projection): a file resource whose row is already gone
// but whose bytes the Reclaimer has not yet confirmed collected. Consumed by
// ListTombstonesByDaemon (§4.7 ReconcilePull's "待收 tombstone" half).
type TombstoneRow struct {
	TombstoneID    string
	ResourceID     resource.ResourceID
	DaemonID       string
	PlacementCoord string
	Kind           ResourceKind
	DeletedAt      int64
}

// ResourceRow is one List row: an id + its full meta + its COMPLETE grant
// projection (every persisted (grantee_kind, grantee) entry on it, with
// ops). The full grant projection lets the door's effectiveOps(caller) — the
// union ActorAllows(caller) ∪ (MembersAllow ∧ IsMember(caller)) — be computed
// per row from data ALREADY fetched here, not a second per-op round trip
// back into the registry (期11 spec §1 item 9⑤: "避免 per-op N 次查询，一次
// 返回整行 grant 投影"). List does NOT grant-filter — that projection is the
// door's job (§3.7); List only scans and returns raw rows.
type ResourceRow struct {
	ID     resource.ResourceID
	Meta   ResourceMeta
	Grants []access.Grant
}

// Registry is the R (authorization relation) + resource-existence contract —
// the object-lifecycle truth the door consults and mutates. One per channel
// (access is channel-scoped). Implemented by runtime/internal/store.
type Registry interface {
	// Resolve reports whether id exists and its meta. This is the door's
	// authoritative RESOLVE stage; it never asks the driver.
	Resolve(ctx context.Context, id resource.ResourceID) (ResourceMeta, bool, error)

	// Create is op=create's ATOMIC birth event (the IMMEDIATE-landing half —
	// kv, and empty/dir file creates that carry no byte stream, §1.5): the
	// existence row (kind + routing/audit columns) + the
	// creator's full-rights grant (an actor entry, ops = read/write/set/delete)
	// + the initial bytes, all in ONE transaction. The atomicity is a
	// door-visible contract, not an implementation coincidence: create is the
	// single event that spans existence, R, and bytes, so splitting it would
	// open a half-built window (a visible row with no grant / no bytes). A
	// colliding id returns ErrAlreadyExists. The byte realizer participates as
	// store-internal collaboration (day-1: same DB, same transaction, free); a
	// future external-byte driver orders its own internals as "bytes first,
	// existence row last", leaving at worst an invisible orphan byte, never a
	// visible half-built resource (Resolve only sees the existence row).
	//
	// placementDaemonID/placementCoord are door-computed (§4 placement
	// routing / §1.6 coord generation), NOT client input — "" for kv (no
	// placement axis). initial is per-kind (§1 item 4):
	// kv's inline value; always nil for file (its bytes never ride this
	// param — a with-content file create lands via ReserveCreate +
	// CommitReservation instead, never this method).
	Create(ctx context.Context, id resource.ResourceID, kind ResourceKind, creator actor.ActorID, placementDaemonID string, placementCoord string, initial []byte) error

	// ReserveCreate is the create-outbox's SERVER-side write-ahead half
	// (§1.3/§1.7, for a with-content file create ONLY — kv and
	// content-less creates always use Create directly): the door,
	// authorizing OpCreate, writes this durable resource_reservations row
	// BEFORE any byte moves, carrying the door-AUTHENTICATED creator and the
	// server-registry-generated coord (§1.6) — so a later CommitReservation
	// can land a row with a creator the daemon never gets to self-report
	// (daemon has no truth, §1.3). Returns a fresh reservation_id the caller
	// threads through AllocRequest (§4.7) and expects back on Committed.
	//
	// dir carries the file's byte-shape bit (ResourceMeta.Dir) write-ahead: a
	// content-less dir=true create routes through ReserveCreate just like a
	// content-less empty file, and the shape must survive to CommitReservation
	// so the landed resources row carries it (the door's later Open routing
	// reads it, §丁12). Always false for a with-content create (dir+with_content
	// is an ingress-rejected combination — a directory carries no byte stream).
	ReserveCreate(ctx context.Context, id resource.ResourceID, kind ResourceKind, creator actor.ActorID, placementDaemonID string, placementCoord string, dir bool) (reservationID string, err error)

	// CommitReservation is create-outbox's landing half (driven by the
	// daemon's Committed(reservation_id) RPC, §4.7): looks up reservationID,
	// then performs the SAME atomic birth Create does — using the
	// RESERVATION's door-authenticated creator/coord, never a daemon-
	// reported value, with Initial always nil (file bytes live at
	// placementCoord, never inline)
	// — then deletes the reservation row, all in ONE transaction (§1.7's
	// "server 用 reservation_id 查表落户口行").
	//
	// found=false, err=nil: no such reservation (already committed by an
	// earlier replay, or never existed) — a CLEAN NO-OP; Committed is
	// level-triggered and MUST be replay-safe.
	// found=true, err=nil: this reservation landed the resource row.
	// found=true, err=ErrReservationLost: this reservation existed and was
	// deleted, but ANOTHER reservation already landed the same resource_id
	// first (§1.7's "并发败者") — the caller (§4) signals the daemon to
	// clean its staged bytes, never retries the write.
	CommitReservation(ctx context.Context, reservationID string) (found bool, err error)

	// ActorAllows is the actor-entry half of R.allows for OBJECT ops: whether
	// caller's direct actor entry grants op. members late-binding is NOT here —
	// that is the door's job: the door unions this with MembersAllow gated by a
	// membership check, resolved at check time (grant.go: "resolved by the door
	// AT CHECK TIME").
	ActorAllows(ctx context.Context, caller actor.ActorID, id resource.ResourceID, op access.Operation) (bool, error)

	// MembersAllow reports whether a members-kind entry on id grants op. It does
	// NOT look at caller: whether caller is a current member is decided by the
	// door's membership check, and the two halves are unioned at the door
	// (allow-only, no precedence).
	MembersAllow(ctx context.Context, id resource.ResourceID, op access.Operation) (bool, error)

	// SetGrant implements op=set: REPLACE the grantee's entry with g (chmod/
	// setfacl SET semantics; g.Ops == ∅ REVOKES = deletes the row). g has
	// already passed the door's ingress ValidateGrant, so the Registry trusts
	// the caller and only stores (mirrors storespec's store-not-validate
	// discipline). The entry key is (id, g.GranteeKind, g.Grantee) — the sum
	// form persisted in full.
	SetGrant(ctx context.Context, id resource.ResourceID, g access.Grant) error

	// Delete removes the resource row + ALL its grants in one transaction.
	// Non-lossy is guaranteed by the door only reaching here after Allows
	// passes. Delete is idempotent and retryable (a repeat delete on an
	// already-gone id is a clean no-op), needing no cross-call atomicity
	// (only create does, for its controller-grab window).
	//
	// Time-order is KIND-DEPENDENT (期11 spec §1 item 8 — the current
	// universal "bytes first, existence row last" contract is being flipped
	// for file):
	//   - kv: bytes live INLINE in the row itself, so removing the row IS
	//     removing the bytes — same as before, no tombstone.
	//   - file: ROW-FIRST-BYTES-LAST — this call reads the row (kind,
	//     placement_daemon_id, placement_coord) inside its own
	//     transaction, writes a resource_tombstones row from those values,
	//     THEN deletes the resource row + grants — all ONE transaction. The
	//     daemon-side Reclaimer (§4, a later addition) consumes the
	//     tombstone asynchronously and only then removes the bytes,
	//     confirming via ReclaimAck (§4.7) so the caller can ClearTombstone.
	//     The invariant "a visible row always points at valid bytes" holds
	//     throughout: the row goes invisible FIRST, so a stranded byte is
	//     always invisible-but-present, never the reverse.
	Delete(ctx context.Context, id resource.ResourceID) error

	// ClearTombstone deletes one resource_tombstones row after the
	// Reclaimer's ReclaimAck(tombstone_id) confirms the bytes are collected
	// (§4.7's fourth frame — delete's outbox closure, the tombstone
	// mirror of CommitReservation). found=false, err=nil when the tombstone
	// is already gone (a clean no-op, replay-safe like CommitReservation).
	ClearTombstone(ctx context.Context, tombstoneID string) (found bool, err error)

	// List enumerates channel-scoped resources in stable (created_at,
	// resource_id) order — a RAW range scan: it projects rows, it does NOT
	// grant-filter (§3.7's any-grant projection is the door's job, one layer
	// up). prefix is a plain string prefix over resource_id (no glob
	// semantics); limit bounds the number of rows SCANNED (not the number
	// returned after any later filtering — there is none here, so scanned ==
	// returned at this layer); cursor is this Registry's OWN opaque
	// pagination token (round-trips only through this method — the door's
	// Query-layer cursor, §3.7, wraps a DIFFERENT concern, prefix-fingerprint
	// + bad_cursor verdict mapping, on top of this one). A malformed cursor
	// is a Go error (verdict mapping is the door's job). nextCursor=="" means
	// the scan reached the end.
	List(ctx context.Context, prefix string, limit int, cursor string) (rows []ResourceRow, nextCursor string, err error)

	// ReservationDaemon reads back ONLY the placement_daemon_id of one
	// reservation (期11 spec §4.7's mechanical sender-auth assertion: a
	// Committed(reservation_id) RPC's handler must verify the attach-
	// authenticated sender daemon == this value BEFORE calling
	// CommitReservation, or daemon B could land daemon A's reservation with
	// bytes B never actually staged). found=false when the reservation is
	// already gone (committed by an earlier replay, lost a race, or swept by
	// timeout) — the caller's own no-op/lost handling takes over from there,
	// this method draws no verdict of its own.
	ReservationDaemon(ctx context.Context, reservationID string) (daemonID string, found bool, err error)

	// TombstoneDaemon is ReservationDaemon's delete-side mirror: the
	// placement_daemon_id a ReclaimAck(tombstone_id) RPC's handler checks the
	// attach-authenticated sender against before calling ClearTombstone (same
	// §4.7 mechanical assertion, delete direction — daemon B must not be able
	// to clear daemon A's tombstone and let A's bytes leak unreclaimed).
	TombstoneDaemon(ctx context.Context, tombstoneID string) (daemonID string, found bool, err error)

	// ListReservationsByDaemon returns every pending reservation whose
	// placement_daemon_id == daemonID — the daemon-scoped slice of §4.7's
	// ReconcilePull "挂起 reservation" answer. A daemon's ReconcilePull must
	// see ONLY its own rows (§4.7's sender-auth discipline extended to reads:
	// "不泄他 daemon 的 coord 清单"), so this is filtered server-side, never a
	// global list the caller narrows itself.
	ListReservationsByDaemon(ctx context.Context, daemonID string) ([]ReservationRow, error)

	// ListTombstonesByDaemon is ListReservationsByDaemon's delete-side mirror
	// — §4.7's ReconcilePull "待收 tombstone" answer, filtered to daemonID.
	ListTombstonesByDaemon(ctx context.Context, daemonID string) ([]TombstoneRow, error)

	// ListByPlacementDaemon returns every LANDED resource row whose
	// placement_daemon_id == daemonID — §4.7's ReconcilePull "应有资源清单"
	// 答案, the Scrubber's registry-side half of its registry↔directory
	// reconciliation (a coord present on disk with no matching row here is an
	// orphan; a row here with no matching coord on disk is a lost-byte
	// anomaly — both the Scrubber's concern, not this method's). Same
	// per-daemon confinement as the two List*ByDaemon methods above.
	ListByPlacementDaemon(ctx context.Context, daemonID string) ([]ResourceRow, error)

	// SweepExpiredReservations deletes — and returns — every reservation
	// belonging to daemonID whose last_progress_at is strictly older than
	// cutoffMs (unix millis; 期11 S1 additive-narrowed this from reserved_at —
	// see ReservationRow.LastProgressAt's doc — a slow-but-alive create must
	// not be judged by its BIRTH time). This is §1.7's THIRD reservation-
	// deletion trigger ("超时未Committed：server 侧按存活戳判超时，
	// ReconcilePull 响应时 level-triggered 删，不依赖 daemon 主动上报") — the
	// server ages out a reservation whose placement daemon has gone
	// unreachable long enough that nothing is bumping its liveness stamp
	// (so the daemon has nothing to resend Committed for and would
	// otherwise see the SAME stale row forever). Called from
	// ReconcilePull's handler, BEFORE ListReservationsByDaemon, so a swept
	// row never appears in that same response. A reservation whose
	// last_progress_at is younger than cutoffMs is left untouched — still
	// legitimately in flight (§1's "纯按龄扫降级为兜底": this age check now
	// only ever catches a row with NO recent activity, never a merely-slow
	// one). This is the server-side mirror of Delete's tombstone reclaim:
	// reservations never grow unbounded from an abandoned/lost create,
	// matching §1.7's "三触发全在 server 侧收口" account (①success ②loser
	// ③this one).
	SweepExpiredReservations(ctx context.Context, daemonID string, cutoffMs int64) ([]ReservationRow, error)

	// TouchReservationsByCoords bumps last_progress_at = atMs for every
	// currently-pending reservation whose placement_daemon_id == daemonID
	// AND whose PlacementCoord is IN coords (期11 S1's "在途登记" liveness
	// bump, transfer-lifecycle-spec.md §2 item 1 — "daemon 有字节活动就
	// bump" — narrowed by 期11 review: 活性 is the RESERVATION's own coord
	// having a live WriteHandle, not merely "this daemon is still polling").
	// coords is the caller's (ReconcilePull handler's) per-request
	// activeCoords list — the daemon's own snapshot of coords with a
	// currently-open local WriteHandle (cmd/daemon/internal/storagehost.
	// Host.ActiveWriteCoords). An EMPTY coords touches ZERO rows — this is
	// the honest answer when the daemon has no active writes at all, not a
	// no-filter fallback to "touch everything this daemon owns" (that
	// blanket form is exactly the bug this narrowed method replaces: an
	// abandoned reservation whose daemon merely stays online forever gets
	// touched by every ReconcilePull and never ages out). A no-op, never an
	// error, when coords is empty or daemonID owns no matching pending
	// reservation.
	TouchReservationsByCoords(ctx context.Context, daemonID string, coords []string, atMs int64) error
}

// ResourceOutbox is the NARROW slice of Registry the home-side daemon
// control-RPC handler needs (期11 spec §4.7's Committed/ReclaimAck/
// ReconcilePull handlers) — deliberately excluding ActorAllows/MembersAllow/
// SetGrant/Create/Delete/Resolve (the general authorization-relation surface
// the access door alone may drive, the anti-bypass wall runtime.ChannelStores
// already draws by not re-exporting the raw Registry). Outbox completion is a
// DIFFERENT concern from a caller-facing access decision: the door already
// authorized the create/delete at reservation/tombstone WRITE time; these
// methods only let the control-plane's daemon-driven RPCs finish an
// ALREADY-authorized transaction. Any concrete Registry satisfies this
// automatically (Go structural typing) — runtime/storeopen.go re-exports the
// SAME value under this narrower type, never a second implementation.
type ResourceOutbox interface {
	CommitReservation(ctx context.Context, reservationID string) (found bool, err error)
	ClearTombstone(ctx context.Context, tombstoneID string) (found bool, err error)
	ReservationDaemon(ctx context.Context, reservationID string) (daemonID string, found bool, err error)
	TombstoneDaemon(ctx context.Context, tombstoneID string) (daemonID string, found bool, err error)
	ListReservationsByDaemon(ctx context.Context, daemonID string) ([]ReservationRow, error)
	ListTombstonesByDaemon(ctx context.Context, daemonID string) ([]TombstoneRow, error)
	ListByPlacementDaemon(ctx context.Context, daemonID string) ([]ResourceRow, error)
	SweepExpiredReservations(ctx context.Context, daemonID string, cutoffMs int64) ([]ReservationRow, error)
	TouchReservationsByCoords(ctx context.Context, daemonID string, coords []string, atMs int64) error
}
