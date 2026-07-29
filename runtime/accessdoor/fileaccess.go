package accessdoor

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
)

// FileRoute is the file-kind byte-access authorization PRODUCT (期11 spec §5
// item 0: "门单方裁决产物") carried on an accepted Outcome for
// OpRead/OpWrite(file) and Create(file, with_content=true) — NEVER bytes,
// NEVER a coord (§8.1/§8.9 red lines: the door hands out an authorization, not
// a storage handle). Every route is same-daemon: the caller redeems Token into
// a local handle via the daemon-side control-RPC (ResolveCoord). A caller whose
// storage host is not the file's placement daemon gets no route at all — the
// door refuses with ErrFileCapabilityUnavailable (see resolveFileRoute), since
// this deployment has no daemon-to-daemon byte transport. Mode echoes the
// requested direction so a generic caller need not re-derive it. ReservationID
// is set ONLY for Create(with_content=true)'s write route — the daemon side
// must fire Committed(ReservationID) after its local fsync+rename (§1.7), never
// for a plain OpWrite on an already-existing row (§3.5: "不走create-outbox、不发
// Committed").
type FileRoute struct {
	Token         string
	Mode          access.Operation
	ReservationID string
	// Dir is the byte-shape bit the door lifts off ResourceMeta.Dir (期11
	// 丁12): the resource being opened is directory-shaped (a workspace), so
	// its redemption hands out an os.Root SUBTREE lease (LocalDirHandle), NOT
	// the single-file staging→rename write handle (§3.9').
	Dir bool
}

// LocalDirHandle is the write句柄's directory sibling (期11 丁12): a chroot-
// confined subtree lease over a dir=true file resource's coord — the切线
// 定理's "字节面委托真fs" on a workspace. The door hands this out for
// Open(dir资源) instead of the single-file LocalWriteHandle; the daemon-side
// implementor is an *os.Root confined to live/<coord>, so every method here is
// structurally satisfied by *os.Root without a wrapper (mirroring how
// LocalWriteHandle/io.ReadSeekCloser are satisfied by the storagehost handles
// directly). Unlike a single-file write there is NO Commit boundary: each
// os.* call lands IMMEDIATELY in the real subtree (the design's "无 Commit
// 边界——每个 os 操作立即生效"), because a directory is not staged-then-renamed
// as one atomic blob. Deliberately spelled in os.File / os.FileMode /
// os.FileInfo (NOT os.Root): the os.Root TYPE token is confined to cmd/daemon
// by the server-zero-storage archtest, so the interface a同信任域 caller holds
// must not name it — only its methods, satisfied structurally.
type LocalDirHandle interface {
	Create(name string) (*os.File, error)
	Open(name string) (*os.File, error)
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
	Mkdir(name string, perm os.FileMode) error
	MkdirAll(name string, perm os.FileMode) error
	Remove(name string) error
	RemoveAll(name string) error
	Stat(name string) (os.FileInfo, error)
	ReadFile(name string) ([]byte, error)
	WriteFile(name string, data []byte, perm os.FileMode) error
	Close() error
}

// LocalWriteHandle is the write-side local handle's shape (期11 spec §3.9'
// write句柄形): Write then exactly one of Commit (daemon fsync+rename to the
// live coord, firing Committed(ReservationID) too when set) or Abort
// (discard staging). Never a裸 handle to the final path — coord/path visibility
// stays daemon-internal even for a same-machine caller (§3.4 coord-confinement
// red line).
type LocalWriteHandle interface {
	io.Writer
	Commit() error
	Abort() error
}

// LocalFile is FileAccess's same-daemon arm, split by shape/mode (§3.9' P1
// precision + 丁12 dir arm): a REGULAR file → Read (mode=read: 裸 read-only
// seekable handle, no commit boundary) OR Write (mode=write: staging handle +
// Commit/Abort boundary); a DIRECTORY-shaped file (route.Dir) → Dir populated
// (an os.Root subtree lease, no commit boundary — regardless of read/write
// mode, since a dir lease is inherently both). Exactly one of Read/Write/Dir
// is non-nil.
//
// Release posture (S6 account item ③, best-effort申报, no active tracking
// built this period): a LocalFile is plain caller-held Go state — this
// package keeps no registry of outstanding handles. The well-behaved path is
// the Proc's own code Close()ing Read / Commit()ing-or-Abort()ing Write
// before returning. If a Proc's worker instead dies mid-hold (panic,
// abnormal Receive error), the handle is reclaimed only INDIRECTLY: the
// runtime's actorrt.Stopper contract calls the actorbase engine's Stop once
// the worker has fully drained (runtime/actorrt's own Stopper doc — "after
// the mailbox is closed and the last in-flight Receive has returned"), but
// the engine holds no reference to any FileAccess a Proc body opened, so
// Stop cannot close it FOR the Proc — the underlying os.File only closes via
// Go's GC finalizer (eventually) or process exit, same as any other leaked
// os.File in this codebase. This is the SAME best-effort posture §3.9/§4.6
// already accept for a daemon-local handle generally (day-1 no active
// leak-tracking); a future per-cell handle registry (Stop actively closing
// every handle a dying Proc still held) is additive, not built here.
type LocalFile struct {
	Read  io.ReadSeekCloser
	Write LocalWriteHandle
	Dir   LocalDirHandle
}

// FileAccess is the file byte-access product of a successfully-redeemed route
// (期11 spec §3.9'). Local is populated on success; nil means the redemption
// itself failed (see the error return alongside it). It stays a struct rather
// than a bare *LocalFile so a second arm — a remote byte pipe, if a
// multi-daemon deployment ever needs one — is an additive field, not a
// signature change across every Proc author's call site.
type FileAccess struct {
	Local *LocalFile
}

// FileOpener is the file byte-access capability's OWN interface — deliberately
// SEPARATE from ResourceAccessHandle's pinned four methods (期11 spec §3.1:
// "Open…" is 词表层糖名, not a fifth resource-face method; growing the four-
// method table would violate §3.1's own closed-set pin and the three-avatar-
// parity red line those four specifically bind), but EMBEDDED into
// ResourceAccessHandle unconditionally (see that interface, above) — every
// avatar structurally implements it, day-1 included. There is no optional
// type-assertion double-wrap (a "does this avatar have a file face" runtime
// check) anywhere on the call path: S10's "能力随放置、调用面无条件统一"
// decision made the FACE universal and pushed the day-1 gap onto CAPABILITY
// instead — boundHandle (a home-hosted caller: human/sysactor day-1, no
// daemon-local redemption path) implements both methods honestly by
// returning ErrFileCapabilityUnavailable (below boundHandle.Open/Redeem in
// handle.go), while the daemon-hosted wire proxy
// (platform/internal/link's remoteResourceHandle) actually redeems bytes.
// lib/actorbase's Open/CreateFile sugar therefore calls straight through
// (no assertion, no nil-arm dance beyond "is there a resource handle at
// all") — see lib/actorbase/engine.go's resourceAdapter.Open/CreateFile.
type FileOpener interface {
	// Open runs OpRead/OpWrite(file) via Invoke and redeems the resulting
	// accepted Outcome's Route into a live FileAccess in one call — the
	// read/write byte-access entry point a Proc author actually calls.
	Open(ctx context.Context, id resource.ResourceID, mode access.Operation) (FileAccess, Outcome, error)
	// Redeem turns an ALREADY-obtained accepted FileRoute (e.g. from
	// Create(with_content=true)'s own Outcome, whose row does not exist yet
	// so Open cannot re-derive the route via Invoke) into a live FileAccess.
	// Open itself is exactly Invoke(mode) followed by Redeem(outcome.Route).
	Redeem(ctx context.Context, route FileRoute) (FileAccess, error)
}

// ErrFileCapabilityUnavailable means the file call face exists on this host,
// but no byte-plane implementation is installed there.
var ErrFileCapabilityUnavailable = errors.New("accessdoor: capability_unavailable")

// TransferControl is the door's file-byte-route minting Dep (期11 spec §5 item
// 0's "门单方裁决产物"): having authorized a file OpRead/OpWrite or
// with_content create, the door mints one opaque ticket — never a coord — and
// hands the transport mechanics to whichever party owns the live connections
// (platform assembly, mirroring StorageControl's own "this package has no
// notion of a link/wire" doc). The ticket is read-only until expiry, so the
// target daemon can retry a lost ResolveCoord reply; only the daemon the
// transfer targets may resolve it (the platform-side sender-auth check).
type TransferControl interface {
	OpenTransfer(ctx context.Context, targetDaemonID, coord string, mode access.Operation, reservationID string) (string, error)
}
