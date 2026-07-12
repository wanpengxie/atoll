package platform

// compute.go is the daemon (attached-compute) assembly root: link.Dial (connect
// to the channel home, no actors declared yet) → actorrt.Runtime (business
// cells, built once and outlives any single link) → the daemon's own reconcile
// ring (computeRing), which diffs ComputeConfig.Desired against the locally-live
// set and drives Reattach → OpenStream → Spawn → StartStream per actor.
// Cloud daemon and user-proxy daemon are the same binary; cmd selects concrete
// actors.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// cellDownWatcher is the daemon's DownWatcher: when a hosted cell dies
// abnormally, OnDown fires that actor's downHandler (close its stream UP the
// link). The daemon holds no truth, so it cannot write receiver_unavailable
// itself — the home port reads EOF and the home's closure author#3 closes
// in-flight requests.
type cellDownWatcher struct {
	mu   sync.Mutex
	down map[actor.ActorID]func(cause string)
}

// OnDown implements actorrt.DownWatcher.
func (w *cellDownWatcher) OnDown(_ context.Context, id actor.ActorID, _ actorrt.Incarnation, cause error) {
	w.mu.Lock()
	handler := w.down[id]
	w.mu.Unlock()
	if handler != nil {
		msg := ""
		if cause != nil {
			msg = cause.Error()
		}
		handler(msg)
	}
}

// obsForwardQueue bounds the daemon's async obs-forward buffer. obs is non-truth
// and best-effort, so a full queue drops the edge (superseded by the next edge /
// the home lease decay) rather than blocking the producer.
const obsForwardQueue = 64

type obsMsg struct {
	id   actor.ActorID
	kind string
	val  []byte
}

// cellObsForwarder is the daemon's ObsWatcher: when a hosted cell PublishObs's an
// opaque obs snapshot (e.g. an adapter's device-presence edge), forward it UP the
// link as a KindObs frame so the home runtime's obs consumers see it. The daemon
// holds no truth — obs is non-truth, best-effort; the home folds it into a
// volatile level + lease-decays it.
//
// OnObs runs on the PUBLISHING CELL's goroutine (runtime.publishObs fanout) and
// the ObsWatcher contract requires it be NON-BLOCKING — so OnObs only enqueues
// (best-effort, drop-on-full); a separate pump goroutine does the blocking socket
// write off the cell goroutine. This keeps the observation arm from ever back-
// pressuring the actor's work path.
//
// The obs arm is the fifth rebindable face (alongside RebindableArms' four
// capability arms, §10.13 推导3): d is swapped atomically on every (re)connect
// via Rebind, so pump — which runs for the WHOLE daemon lifetime, not one
// link's — always forwards through whichever Dialer is currently connected. A
// nil/disconnected d (no link right now) just drops the queued edge, the same
// best-effort posture a dead stream already has.
type cellObsForwarder struct {
	d       atomic.Pointer[link.Dialer]
	ch      chan obsMsg
	dropped atomic.Uint64
	logger  *slog.Logger
	// reportEvery is pump's drop-account report cadence (the rate limiter that
	// keeps OnObs's hot path log-free). Defaulted by the constructor.
	reportEvery time.Duration
}

// obsDropReportInterval paces the pump-side drop Warn — generous, a diagnostic
// backstop rather than a per-event log (the OnObs hot path only bumps an atomic).
const obsDropReportInterval = 30 * time.Second

func newCellObsForwarder(logger *slog.Logger) *cellObsForwarder {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &cellObsForwarder{ch: make(chan obsMsg, obsForwardQueue), logger: logger, reportEvery: obsDropReportInterval}
}

// Rebind swaps in the currently-connected Dialer — call once per (re)connect,
// mirroring RebindableArms.Rebind for the four capability arms.
func (f *cellObsForwarder) Rebind(d *link.Dialer) { f.d.Store(d) }

// OnObs implements actorrt.ObsWatcher — non-blocking enqueue (drop on full).
func (f *cellObsForwarder) OnObs(_ context.Context, id actor.ActorID, _ actorrt.Incarnation, kind actorrt.ObsKind, val actorrt.ObsValue) {
	m := obsMsg{id: id, kind: string(kind), val: append([]byte(nil), val...)}
	select {
	case f.ch <- m:
	default: // queue full: drop (obs is best-effort; next edge / lease decay covers it)
		f.dropped.Add(1) // S6: drop 必记账 — the account is read by pump's periodic Warn.
	}
}

// pump drains the queue onto the CURRENT link OFF the cell goroutine, for the
// life of the daemon (ctx) — it survives any number of individual link deaths
// and reconnects, reading whatever Dialer Rebind last stored. A periodic tick
// surfaces the drop account (never logged on the OnObs hot path).
func (f *cellObsForwarder) pump(ctx context.Context) {
	t := time.NewTicker(f.reportEvery)
	defer t.Stop()
	var lastReported uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := f.dropped.Load(); n != lastReported {
				f.logger.Warn("platform.compute.obs_forward_dropped", "dropped", n, "delta", n-lastReported)
				lastReported = n
			}
		case m := <-f.ch:
			if d := f.d.Load(); d != nil {
				d.SendObs(m.id, m.kind, m.val)
			}
		}
	}
}

// cellCancelForwarder is the daemon's caller-side cancel arm: when a hosted cell's
// call ledger fires its Hooks.Canceller (a caller abandoning one of its OWN
// outbound requests), forward the request id UP that caller's link stream as a
// KindCancelRequest frame so the home reverse-resolves the target and reaches the
// receiver's in-station account — the daemon-hosted parity of Home.CancelRequest.
//
// Like cellObsForwarder it is the rebindable face of a WHOLE-DAEMON-LIFETIME arm:
// d is swapped atomically on every (re)connect via Rebind (mirroring
// RebindableArms + obsForwarder), so a Canceller built once at buildOne always
// sends through whichever Dialer is currently connected — a closure that captured
// the FIRST connection's Dialer would send toward a dead stream after any reconnect
// (双线审 F5). A nil/disconnected d (no link right now) drops the frame, the same
// best-effort no-ack posture the downstream KindCancel already has.
type cellCancelForwarder struct {
	d atomic.Pointer[link.Dialer]
}

func newCellCancelForwarder() *cellCancelForwarder { return &cellCancelForwarder{} }

// Rebind swaps in the currently-connected Dialer — call once per (re)connect,
// mirroring cellObsForwarder.Rebind and RebindableArms.Rebind.
func (f *cellCancelForwarder) Rebind(d *link.Dialer) { f.d.Store(d) }

// cancellerFor builds the Hooks.Canceller closure for one hosted caller id. The
// target parameter is IGNORED on the wire (the home reverse-resolves it from the
// request in the log — the caller self-reports nothing); the frame goes up THIS
// caller's own stream, so the id is welded here. The call into SendCancelRequest
// itself is synchronous (the ledger invokes Canceller off its own lock —
// ledger_call.go — not on a fanout goroutine) but SendCancelRequest's actual
// wire write is NOT: it runs off this goroutine and is grace-bounded, so a
// stuck/dead link can never pin the ledger's own goroutine (or the actor
// worker occupying it) waiting on a write that will never drain.
func (f *cellCancelForwarder) cancellerFor(id actor.ActorID) func(target actor.ActorID, requestID message.ID) {
	return func(_ actor.ActorID, requestID message.ID) {
		if d := f.d.Load(); d != nil {
			d.SendCancelRequest(id, requestID)
		}
	}
}

// ActorDecl declares one actor the daemon will host. Factory is the ActorFactory
// (def) both admission paths share (Home.SpawnIfAbsent cell-side and RunCompute
// daemon-side). On the daemon the Pen + plane-2 (Access/State) + time-axis
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
// still hand back a caller-held id+kind+factory triple); RunCompute itself no
// longer takes a []ActorDecl argument — see ComputeConfig.
type ActorDecl struct {
	ID      actor.ActorID
	Kind    actor.Kind
	Factory ActorFactory
}

// ComputeBuilder resolves a desired member's id to its ActorFactory — the
// daemon-side counterpart of the reconcile ring's activation resolve (mirrors
// CapsFactoryBuilder.Lookup, but scoped to compute: a daemon never forks, so it
// carries no LookupByClass entry). Kind is never re-answered here — it is
// caller-held on the DesiredMember the reconcile loop already read.
type ComputeBuilder interface {
	Lookup(id actor.ActorID) (def ActorFactory, ok bool)
}

// defaultComputePoll is the reconcile ring's desired-source poll period when
// ComputeConfig.Poll is unset (S-P10).
const defaultComputePoll = 30 * time.Second

// ComputeConfig configures the attached compute. ServerWS carries any auth
// credential in its query string (the ?key= the app layer resolves on WS
// upgrade) — the link layer is auth-agnostic, so there is no separate key field.
//
// Desired and Builder are the two halves of the daemon's OWN reconcile ring
// (§10.13 推导2: the reconcile paradigm is host-neutral — every host that carries
// live embodiments runs the same diff loop over its own desired source). Neither
// may be nil: a compute with no desired source or no builder never turns the
// ring at all, so RunCompute fails fast rather than silently running an empty
// daemon (same nil discipline as HomeConfig.Builder — a structural refusal, never
// a phantom no-op).
type ComputeConfig struct {
	ServerWS  string
	ComputeID string
	Logger    *slog.Logger
	// Desired is the reconcile ring's read of intent — the same DesiredSource
	// contract the home's eager-activation ring reads (AlwaysOn members only;
	// lazy activation does not apply to a daemon, which has no delivery seam of
	// its own to activate on demand against).
	Desired actorrt.DesiredSource
	// Builder resolves each desired member's factory. nil is a fail-fast
	// misconfiguration, not "no actors" (an intentionally-empty daemon should
	// still supply a Builder that finds nothing).
	Builder ComputeBuilder
	// Poll is the ring's desired-source poll period. <=0 → defaultComputePoll.
	Poll time.Duration
	// StorageHost (optional, 期11 §4) is the daemon's file-kind storage host
	// — cmd/daemon/internal/storagehost.Host, injected by cmd/daemon/main.go
	// (this package cannot construct one itself: the concrete os.Root-touching
	// implementation lives in a cmd/daemon-confined package, §8.2's
	// server-zero-storage red line — RunCompute only ROUTES already-built
	// control-plane frames to it). nil → every AllocRequest this daemon
	// receives is answered honestly OK:false (no storage host wired), and no
	// Scrubber pass ever runs (a daemon that never hosts file-kind resources
	// needs neither).
	StorageHost StorageHost
	// ScrubberInterval overrides the storage-host reconcile bridge's periodic
	// ReconcilePull cadence (storageHostForwarder.pump). <=0 →
	// scrubberPumpInterval (60s, production default). A startup pass ALWAYS
	// runs immediately regardless of this value — only the PERIODIC ticker
	// after it is affected. Additive test-only knob (mirrors Poll's own
	// nil-safe-default shape): a walk test driving delete→tombstone→
	// ReclaimAck end-to-end needs a fast, deterministic cadence rather than
	// production's 60s backstop interval.
	ScrubberInterval time.Duration
	// LocalFileOpener (optional, 期11 §5) is the daemon-side same-machine
	// byte-access capability the resource lane's Local route and this
	// daemon's own lane-inbound (target) handler both redeem through —
	// SAME underlying storagehost.Host as StorageHost, a DIFFERENT facet of
	// it (Streamer's OpenRead/OpenWrite rather than Alloc/Reconcile). No
	// mirror type is needed here (unlike StorageHost's Reconcile shapes):
	// its signature is expressed purely in io.ReadSeekCloser +
	// accessdoor.LocalWriteHandle, both already reachable from cmd/daemon's
	// adapter without importing platform/internal/link. nil → every file
	// byte redemption on this compute answers an honest "no storage host
	// wired" error.
	LocalFileOpener LocalFileOpener
}

// LocalFileOpener mirrors platform/internal/link.LocalFileOpener's exact
// method set (期11 spec §5/§3.4's "daemon 本地颁 os.Root 子句柄") — a
// SEPARATE named interface (not an alias) purely so cmd/daemon/main.go's
// wiring code reads against platform's own public vocabulary rather than
// reaching into platform/internal/link (which it cannot import); Go's
// structural interface typing makes the two directly interchangeable at
// RunCompute's Dialer.SetLocalFileOpener call site with no adapter needed.
type LocalFileOpener interface {
	OpenRead(coord string) (io.ReadSeekCloser, error)
	OpenWrite(coord string) (accessdoor.LocalWriteHandle, error)
	// OpenDir opens coord as a directory-shaped resource's SUBTREE lease (期11
	// 丁12) — an os.Root confined to live/<coord>, surfaced behind
	// accessdoor.LocalDirHandle (the os.Root TYPE stays inside cmd/daemon per
	// the server-zero-storage archtest; this interface names only its method
	// set). Redeemed for Open(dir资源) on the same-daemon Local route only.
	OpenDir(coord string) (accessdoor.LocalDirHandle, error)
	// ReclaimCoord mirrors platform/internal/link.LocalFileOpener's own
	// ReclaimCoord (期11 S2's "非-land 终态回收") — see its doc.
	ReclaimCoord(coord string) error
}

// StorageResourceCoord / StorageReservationCoord / StorageTombstoneCoord are
// StorageHost.Reconcile's injection-point shapes — plain data, deliberately
// NOT aliases of platform/internal/link's own wire types: the implementor
// (cmd/daemon/internal/storagehost.Host) lives OUTSIDE platform/internal's
// Go-enforced visibility boundary and cannot reference those types even by
// alias-name. This mirrors the CONCEPTUAL layering resourcespec/store and
// accessdoor/resourcespec already draw — a fresh mirror type at a boundary a
// downstream package cannot import across, translated by the ONE adapter
// that can see both sides (storageHostForwarder below, for this boundary;
// StorageHost.Alloc's own two arguments are plain string/bool, needing no
// mirror struct at all).
type (
	StorageResourceCoord    struct{ Coord string }
	StorageReservationCoord struct{ ReservationID, Coord string }
	StorageTombstoneCoord   struct{ TombstoneID, Coord, Provenance string }
)

// StorageReclaimAckFunc is Reconcile's network callback — RunCompute's
// bridge (storageHostForwarder) supplies a closure bound to whichever
// *link.Dialer is CURRENTLY connected; the StorageHost implementor never
// holds a live connection reference itself.
type StorageReclaimAckFunc func(ctx context.Context, tombstoneID string) (found bool, err error)

// StorageHost is the daemon storage host's injection-point contract (期11
// §4): implemented by cmd/daemon/internal/storagehost.Host (via a thin
// cmd/daemon-side adapter — Host's own method shapes already match this
// exactly, see its doc). Every method uses only the plain types above, never
// platform/internal/link's wire types, because the implementor cannot
// import that package (outside its Go-enforced internal/ visibility).
type StorageHost interface {
	// Alloc performs the mkdir/touch for one AllocRequest.
	Alloc(coord string, dir bool) error
	// Reconcile runs one Scrubber pass against the home's ReconcilePullReply
	// (already translated to plain types by storageHostForwarder).
	Reconcile(ctx context.Context, resources []StorageResourceCoord, pendingReservations []StorageReservationCoord, pendingTombstones []StorageTombstoneCoord, ack StorageReclaimAckFunc)
	// ActiveWriteCoords snapshots every coord this daemon currently has an
	// OPEN local WriteHandle for (期11 review's own narrowing addition,
	// cmd/daemon/internal/storagehost.Host.ActiveWriteCoords's plain-typed
	// mirror) — storageHostForwarder.pass reads this BEFORE every
	// ReconcilePull round trip and forwards it as link.ReconcilePull.
	// ActiveCoords, so the home's liveness touch bumps ONLY reservations
	// this daemon can actually prove are still being written, never every
	// reservation it happens to still own.
	ActiveWriteCoords() []string
}

// storageHostForwarder is StorageHost's daemon-lifetime bridge to the link
// layer — mirrors cellObsForwarder/cellCancelForwarder's OWN shape exactly
// (built once outside the redial loop, Rebind'd on every successful
// (re)connect, its background pump reads whichever Dialer Rebind last
// stored): the ONE place both the plain StorageHost vocabulary and
// platform/internal/link's wire types are simultaneously in scope, so it is
// also where every field-by-field translation between them happens.
type storageHostForwarder struct {
	host     StorageHost
	logger   *slog.Logger
	interval time.Duration
	d        atomic.Pointer[link.Dialer]
}

func newStorageHostForwarder(host StorageHost, logger *slog.Logger, interval time.Duration) *storageHostForwarder {
	if interval <= 0 {
		interval = scrubberPumpInterval
	}
	return &storageHostForwarder{host: host, logger: logger, interval: interval}
}

// Rebind swaps in the currently-connected Dialer (obsFwd/cancelFwd's own
// pattern) and installs handleAlloc as its inbound AllocRequest handler —
// every (re)connect re-installs it, since SetAllocHandler is per-Dialer
// state that does not survive a reconnect.
func (f *storageHostForwarder) Rebind(d *link.Dialer) {
	f.d.Store(d)
}

// handleAlloc answers one inbound AllocRequest by calling the injected
// StorageHost — nil host (never wired) answers an honest OK:false rather
// than silently dropping (this RPC plane is request/response).
func (f *storageHostForwarder) handleAlloc(req link.AllocRequest) link.AllocReply {
	if f.host == nil {
		return link.AllocReply{OK: false, Reason: "platform: no storage host wired on this compute"}
	}
	if err := f.host.Alloc(req.Coord, req.Dir); err != nil {
		return link.AllocReply{OK: false, Reason: err.Error()}
	}
	return link.AllocReply{OK: true}
}

// scrubberPumpInterval is the Scrubber pass cadence this bridge drives —
// generous relative to the lease ping/TTL (10s/30s), a recovery backstop
// rather than a liveness signal (mirrors storagehost.scrubberInterval's own
// reasoning, duplicated here since that constant lives in a package this
// one cannot import).
const scrubberPumpInterval = 60 * time.Second

// pump drives the startup pass then a periodic ticker for the daemon's
// whole lifetime (ctx-bound) — the same shape cellObsForwarder.pump/
// cellCancelForwarder already have for their own daemon-lifetime arms. A
// nil host makes every pass a no-op (nothing to reconcile, no point paying
// a ReconcilePull round trip).
func (f *storageHostForwarder) pump(ctx context.Context) {
	if f.host == nil {
		return
	}
	f.pass(ctx)
	t := time.NewTicker(f.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f.pass(ctx)
		}
	}
}

// pass runs ONE ReconcilePull round trip (against whichever Dialer is
// currently connected) and, on success, translates the reply into
// StorageHost.Reconcile's plain shapes. A nil Dialer (never connected yet /
// mid-reconnect) or a failed ReconcilePull just skips this pass — the next
// tick retries, the same level-triggered discipline every reconcile ring in
// this codebase already has.
func (f *storageHostForwarder) pass(ctx context.Context) {
	d := f.d.Load()
	if d == nil {
		return
	}
	// f.host is guaranteed non-nil here — pump (this method's only caller)
	// already returns early when it is nil, before ever starting the ticker
	// loop that reaches this call.
	activeCoords := f.host.ActiveWriteCoords()
	reply, err := d.SendReconcilePull(ctx, activeCoords)
	if err != nil {
		f.logger.Warn("platform.compute.storage_reconcile_pull_failed", "err", err)
		return
	}
	if reply.Reason != "" {
		// 期11 review残余#2b: the home explicitly NAK'd this pull (no storage
		// host control wired / unattached sender) — Resources/
		// PendingReservations/PendingTombstones are all zero-value on this
		// branch (handleReconcilePull's own doc), so feeding them into
		// Host.Reconcile would run the scrubber against fabricated empty
		// truth: it could wrongly treat every locally-landed resource as an
		// orphan to scrub, and every pending reservation/tombstone this
		// daemon still owes a resend/ack for as already resolved. Skip this
		// round entirely; the next tick's pull retries.
		f.logger.Warn("platform.compute.storage_reconcile_pull_rejected", "reason", reply.Reason)
		return
	}

	resources := make([]StorageResourceCoord, 0, len(reply.Resources))
	for _, r := range reply.Resources {
		resources = append(resources, StorageResourceCoord{Coord: r.Coord})
	}
	pendingReservations := make([]StorageReservationCoord, 0, len(reply.PendingReservations))
	for _, r := range reply.PendingReservations {
		pendingReservations = append(pendingReservations, StorageReservationCoord{ReservationID: r.ReservationID, Coord: r.Coord})
	}
	pendingTombstones := make([]StorageTombstoneCoord, 0, len(reply.PendingTombstones))
	for _, t := range reply.PendingTombstones {
		pendingTombstones = append(pendingTombstones, StorageTombstoneCoord{TombstoneID: t.TombstoneID, Coord: t.Coord, Provenance: t.Provenance})
	}

	ack := func(ctx context.Context, tombstoneID string) (bool, error) {
		d := f.d.Load() // re-load: a reconnect may have happened between pass start and ack
		if d == nil {
			return false, fmt.Errorf("platform: no live connection to ack tombstone %q", tombstoneID)
		}
		reply, err := d.SendReclaimAck(ctx, tombstoneID)
		return reply.Found, err
	}
	f.host.Reconcile(ctx, resources, pendingReservations, pendingTombstones, ack)
}

// redialInitialBackoff / redialMaxBackoff bound the daemon's reconnect retry
// (§10.13 推导3: link death degrades the wire, never the hosted work — so the
// daemon just keeps trying to get the wire back, at no accelerating cost to
// the home). Exponential from the floor, capped at the ceiling; reset to the
// floor the moment a connection succeeds.
const (
	redialInitialBackoff = 1 * time.Second
	redialMaxBackoff     = 30 * time.Second
)

// nextRedialBackoff doubles cur, capped at redialMaxBackoff.
func nextRedialBackoff(cur time.Duration) time.Duration {
	next := cur * 2
	if next > redialMaxBackoff {
		return redialMaxBackoff
	}
	return next
}

// jitterBackoff randomizes d to the range [d/2, d] (AWS "equal jitter") so
// daemons that all lost the link to the same home outage don't redial in
// lockstep. It only randomizes the SLEEP; the stored backoff ladder
// (nextRedialBackoff) stays a clean unjittered doubling sequence.
func jitterBackoff(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

// RunCompute connects to the channel home and runs the daemon's own reconcile
// ring against cfg.Desired: it hosts every AlwaysOn desired member as a cell,
// declaring it to the home (Reattach) before opening its stream. The home
// dispatches envelopes down each actor's link stream into the cell's mailbox;
// the cell's emits flow UP that same stream as native ipc (blocking on the
// home's authoritative EmitAck). No local truth.
//
// The link is NOT the unit of life here — rt is (§10.13 推导3: wire-session
// death ≠ hosted-work death). RunCompute dials in a redial loop with an
// exponential backoff (1s→30s cap, reset on success); every hosted cell
// survives any number of link deaths, resuming under the SAME identity once
// the wire returns (reopen, never respawn — F6). The ONLY path that ever
// StopAll's rt is ctx cancellation (graceful shutdown, each stream KindDetach
// first) — a link death alone only re-enters the redial loop.
//
// RunCompute blocks until ctx is cancelled.
var ErrComputeForwardersLeaked = errors.New("platform: compute forwarders leaked; storage root ownership transferred to process exit")

type computeLifecycleHooks struct {
	forwarderTimeout time.Duration
	// forwarderLeaked, when non-nil, replaces the invocation-local forwarder
	// leak account so a test can assert its delta directly.
	forwarderLeaked *atomic.Int64
	obsExited       func()
	storageExited   func()
	storagePump     func(context.Context, *storageHostForwarder)
}

func RunCompute(ctx context.Context, cfg ComputeConfig) error { return runCompute(ctx, cfg, nil) }

func runCompute(ctx context.Context, cfg ComputeConfig, hooks *computeLifecycleHooks) (retErr error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Desired == nil {
		return fmt.Errorf("platform: RunCompute requires a Desired source (nil never turns the ring — fail-fast, not a silent no-op)")
	}
	if cfg.Builder == nil {
		return fmt.Errorf("platform: RunCompute requires a Builder (nil never resolves a factory — fail-fast, not a silent no-op)")
	}
	computeID := cfg.ComputeID
	if computeID == "" {
		computeID = uuid.NewString()
	}
	poll := cfg.Poll
	if poll <= 0 {
		poll = defaultComputePoll
	}

	// Cell running is the kernel: the daemon owns an actorrt.Runtime directly. It
	// is built HERE, once — OUTSIDE the redial loop below (F14) — because a
	// hosted cell's identity and in-memory state must outlive a single wire
	// session.
	rt, del := actorrt.New(actorrt.Config{})
	defer func() {
		// StopAll no longer joins (G0): it enrols every hosted cell as a zombie and
		// returns. DrainZombies is the aggregate join — a卡死 cell is reported leaked
		// into the shutdown log instead of hanging RunCompute's teardown forever.
		rt.StopAll()
		if leaked := rt.DrainZombies(0); len(leaked) > 0 {
			logger.Warn("compute.close.zombies_leaked", "compute", computeID, "count", len(leaked), "actors", leaked)
		}
	}()
	watcher := &cellDownWatcher{down: map[actor.ActorID]func(cause string){}}
	rt.WatchDown(watcher)
	// The obs arm outlives every individual link the same way rt does — built
	// once, Rebind'd per connection, pumped for the daemon's whole life.
	obsFwd := newCellObsForwarder(logger)
	rt.WatchObsAll(obsFwd)
	var forwarders sync.WaitGroup
	forwarders.Add(1)
	go func() {
		defer forwarders.Done()
		defer func() {
			if hooks != nil && hooks.obsExited != nil {
				hooks.obsExited()
			}
		}()
		obsFwd.pump(ctx)
	}()
	// The caller-side cancel arm, like the obs arm, outlives every individual link:
	// built once, Rebind'd per connection, so a hosted caller's Canceller always
	// sends up whichever Dialer is currently connected.
	cancelFwd := newCellCancelForwarder()
	// The storage host bridge (期11 §4), same daemon-lifetime shape as
	// obsFwd/cancelFwd — built once (nil-host-safe: cfg.StorageHost may be
	// nil), Rebind'd per connection, pumped for the daemon's whole life.
	storageFwd := newStorageHostForwarder(cfg.StorageHost, logger, cfg.ScrubberInterval)
	forwarders.Add(1)
	go func() {
		defer forwarders.Done()
		defer func() {
			if hooks != nil && hooks.storageExited != nil {
				hooks.storageExited()
			}
		}()
		if hooks != nil && hooks.storagePump != nil {
			hooks.storagePump(ctx, storageFwd)
			return
		}
		storageFwd.pump(ctx)
	}()
	// forwarderLeaked is this RunCompute invocation's forwarder leak account —
	// the same per-instance atomic every other lifecycle component keeps. The
	// single deferred close arbitration below is its only writer, so one
	// timeout incident counts exactly once.
	forwarderLeaked := &atomic.Int64{}
	if hooks != nil && hooks.forwarderLeaked != nil {
		forwarderLeaked = hooks.forwarderLeaked
	}
	defer func() {
		timeout := 5 * time.Second
		if hooks != nil && hooks.forwarderTimeout > 0 {
			timeout = hooks.forwarderTimeout
		}
		done := make(chan struct{})
		go func() { forwarders.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(timeout):
			// Bounded abandon proof: neither pump produces actors. A storage pump
			// may still hold the pure os.Root handle, so ownership is transferred
			// intact to process exit and daemon main must skip sh.Close.
			forwarderLeaked.Add(1)
			logger.Error("platform.compute.forwarder_join_timeout", "timeout", timeout,
				"leaked", forwarderLeaked.Load(),
				"safety", "pure os.Root handle ownership transferred to process exit; no actor production")
			retErr = errors.Join(retErr, ErrComputeForwardersLeaked)
		}
	}()

	ring := &computeRing{
		rt:          rt,
		del:         del,
		watcher:     watcher,
		obsFwd:      obsFwd,
		cancelFwd:   cancelFwd,
		builder:     cfg.Builder,
		logger:      logger,
		prevCurrent: map[actor.ActorID]actor.Kind{},
		arms:        map[actor.ActorID]*link.RebindableArms{},
	}

	backoff := redialInitialBackoff
	for {
		// Dial declares NOTHING yet: every actor this compute hosts is declared by
		// the ring's own Reattach (the full-set declaration idiom, §S-P8) inside
		// the first reconcile pass below.
		dialCfg := link.DialConfig{DespawnLocal: func(id actor.ActorID) { rt.DespawnID(id) }, LocalFileOpener: cfg.LocalFileOpener}
		if cfg.StorageHost != nil {
			dialCfg.AllocHandler = storageFwd.handleAlloc
		}
		d, err := link.Dial(ctx, cfg.ServerWS, computeID, nil, dialCfg, logger)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Warn("platform.compute.dial_failed", "err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(jitterBackoff(backoff)):
			}
			backoff = nextRedialBackoff(backoff)
			continue
		}
		backoff = redialInitialBackoff

		// Host→remote despawn hook: a KindDespawn frame from the home ends that
		// actor's arm here (§10.5) — despawn the local cell, the stream loop
		// replies KindDetach. Re-installed on every connection (it closes over
		// THIS d's stream read loops).
		obsFwd.Rebind(d)
		cancelFwd.Rebind(d)
		storageFwd.Rebind(d)
		// §5's resource lane needs no per-connection carrier setup any more (片③
		// flattened it): the Dialer accepts home-relayed inbound lane substreams
		// via its always-running accept loop, and opens its own redeem substreams
		// on demand — both live for the connection's whole life without a
		// dedicated open step here.

		graceful := ring.runLink(ctx, d, cfg.Desired, poll)
		_ = d.Close() // idempotent: a no-op if the link already tore itself down.
		if graceful {
			return nil
		}

		logger.Warn("platform.compute.link_down", "retry_in", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(jitterBackoff(backoff)):
		}
		backoff = nextRedialBackoff(backoff)
	}
}

// computeRing is the daemon's reconcile ring: it diffs cfg.Desired against the
// locally-live cells and drives the actor lifecycle (open/spawn/start, despawn/
// detach) to close the gap. It is the daemon-hosted counterpart of Home's
// reconcileActivation — same paradigm (§10.13 推导2), different host.
type computeRing struct {
	rt        *actorrt.Runtime
	del       actorrt.Deliverer
	watcher   *cellDownWatcher
	obsFwd    *cellObsForwarder
	cancelFwd *cellCancelForwarder
	builder   ComputeBuilder
	logger    *slog.Logger

	// prevCurrent is the AlwaysOn desired set the LAST reconcile pass declared —
	// mirrors Home's prevEagerDesired: the 削 diff is prevCurrent − current, never
	// live − current (LiveIDs has no other embodiment category on a daemon this
	// period, but the same never-actual-diff discipline holds so a future daemon-
	// local category never gets silently evicted). Touched only from the single
	// caller goroutine (RunCompute's own loop), so it needs no lock.
	prevCurrent map[actor.ActorID]actor.Kind
	// arms is the per-id wire-flap membrane (§10.13 推导3): populated once at
	// buildOne, Rebind'd (never re-created) on every later reopen — the cell's
	// Caps were built from these facades, so a reconnect never touches the cell.
	arms map[actor.ActorID]*link.RebindableArms
}

// runLink drives ONE connected session on d: an initial reconcile pass (which
// reopens every already-live actor's stream on this new link, F6, before it
// resolves anything freshly missing) followed by a poll loop. It returns true
// (graceful) only on ctx cancellation — having first detached every stream so
// the home's ports die QUIET — and false if the link itself goes down, so the
// caller redials. It never touches rt's cells: a link death degrades the wire,
// the hosted work is untouched (§10.13 推导3).
func (r *computeRing) runLink(ctx context.Context, d *link.Dialer, desired actorrt.DesiredSource, poll time.Duration) (graceful bool) {
	r.reconcile(ctx, d, desired)

	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// Graceful shutdown: detach each actor stream (KindDetach) so the home
			// ports die QUIET (no receiver_unavailable) instead of on a raw EOF down
			// edge. rt itself is untouched here — RunCompute's defer StopAll's it.
			d.DetachAll()
			return true
		case <-d.Done():
			return false
		case <-t.C:
			r.reconcile(ctx, d, desired)
		}
	}
}

// reconcile runs one pass of the ring against link d: read desired, 削 what
// fell out of it (despawn + detach), then 补 what is desired-and-not-fully-
// hosted on THIS link — either never built, or live but missing a stream here
// (F6: a fresh reconnect where nothing has a stream yet, or a single stream
// that died while the link stayed up). Reattach the full declared set, then
// per missing id either buildOne (never live: resolve factory, OpenStream,
// Spawn, StartStream) or reopenOne (already live: OpenStream, Rebind, Start-
// Stream — never re-Spawn). A desired-read failure leaves the prior state
// untouched and retries next tick.
func (r *computeRing) reconcile(ctx context.Context, d *link.Dialer, desired actorrt.DesiredSource) {
	members, err := desired.Members(ctx)
	if err != nil {
		r.logger.Error("platform.compute.desired_failed", "err", err)
		return
	}

	current := make(map[actor.ActorID]actor.Kind, len(members))
	for _, m := range members {
		current[m.ID] = m.Kind
	}

	// 削: previously-declared ids no longer desired. Local despawn ends the cell's
	// execution arm; DetachStream tells the home this arm is gone QUIET (§10.5/S1).
	for id := range r.prevCurrent {
		if _, ok := current[id]; ok {
			continue
		}
		r.rt.DespawnID(id)
		d.DetachStream(id)
		delete(r.arms, id)
		r.watcher.mu.Lock()
		delete(r.watcher.down, id)
		r.watcher.mu.Unlock()
		r.logger.Info("platform.compute.deactivated", "actor", string(id))
	}

	// 补: desired-and-not-fully-hosted on THIS link — live ∪ stream-existence
	// (F6), never live alone: a cell can be live while its stream on d is gone.
	live := make(map[actor.ActorID]bool)
	for _, id := range r.rt.LiveIDs() {
		live[id] = true
	}
	var missing []actor.ActorID
	for id := range current {
		if !live[id] || !d.HasStream(id) {
			missing = append(missing, id)
		}
	}

	if len(missing) > 0 {
		// Reattach the FULL current declared set (kubelet node-status idiom, never
		// an increment — §S-P8) so the home's allowed set covers every id about to
		// OpenStream, then build/reopen each missing actor.
		decls := make([]link.Declaration, 0, len(current))
		for id, kind := range current {
			decls = append(decls, link.Declaration{ActorID: id, Kind: kind, Binding: actor.BindingRuntimeInboundViaRelay})
		}
		if err := d.Reattach(ctx, decls); err != nil {
			r.logger.Warn("platform.compute.reattach_failed", "err", err, "actors", len(decls))
			// Every missing id stays not-fully-hosted, so next tick's diff retries
			// them (the ids remain in `current`, untouched below).
		} else {
			for _, id := range missing {
				if live[id] {
					r.reopenOne(id, d)
				} else {
					r.buildOne(id, current[id], d)
				}
			}
		}
	}

	r.prevCurrent = current
}

// buildOne opens the actor's link stream, resolves its factory via the builder,
// and spawns + starts it — the FIRST time this id is ever hosted. A failure at
// any step is logged; the id stays out of the live set, so next tick's diff
// retries it.
func (r *computeRing) buildOne(id actor.ActorID, kind actor.Kind, d *link.Dialer) {
	factory, ok := r.builder.Lookup(id)
	if !ok {
		r.logger.Warn("platform.compute.no_builder", "actor", string(id))
		return
	}
	// Open the actor's link stream first: the RemoteWriter (the cell's pen) must
	// exist before the cell is spawned. One stream == one actor, so the dispatch
	// handler routes every envelope on this stream into THIS actor's mailbox (the
	// stream IS the target — no audience demux on the daemon).
	arms, err := d.OpenStream(id, r.dispatchFor(id, d), r.cancelFor(id))
	if err != nil {
		r.logger.Warn("platform.compute.open_stream_failed", "actor", string(id), "err", err)
		return
	}
	// The cell's Caps are built from the REBINDABLE facades, never the raw
	// per-stream proxies directly — this is the one-time construction the wire-
	// flap membrane needs: a later reconnect Rebinds this SAME *RebindableArms,
	// the cell (built once, right below) never rebuilds (§10.13 推导3).
	rb := link.NewRebindableArms(arms)
	r.arms[id] = rb
	r.watcher.mu.Lock()
	r.watcher.down[id] = arms.Down
	r.watcher.mu.Unlock()
	// Two-phase construction, mirroring the home activation path (§10.13 推导7①/G12): the
	// build closure runs inside Spawn, BEFORE go-live, so link.NewLiveArms welds
	// the cell's caps to THIS incarnation and fences every call until it goes
	// live — a factory that writes during construction is refused here exactly
	// like a cell born at home, closing the daemon-side parity gap the raw
	// (ungated) facades used to leave open.
	_, built, buildErr := r.rt.SpawnIfAbsent(id, kind, func(inc actorrt.Incarnation) actorrt.Actor {
		// daemon Hooks.Canceller = the caller-side cancel-upstream forwarder (the
		// out-generation matrix's former known gap, now filled): a daemon-hosted
		// caller's Cancel commits its own unanswered_timeout terminal AND forwards a
		// KindCancelRequest frame up this caller's stream, so the home reverse-
		// resolves the target and reaches the receiver's in-station account — the
		// daemon-hosted parity of the cell-path Home.CancelRequest.
		return build(link.NewLiveArms(rb, inc, r.rt), actorbase.Hooks{Canceller: r.cancelFwd.cancellerFor(id)}, factory)
	})
	if buildErr != nil || !built {
		if buildErr != nil {
			r.logger.Error("platform.compute.build_failed", "actor", string(id), "error", buildErr)
		} else {
			r.logger.Error("platform.compute.build_cas_lost", "actor", string(id))
		}
		d.DetachStream(id)
		delete(r.arms, id)
		r.watcher.mu.Lock()
		delete(r.watcher.down, id)
		r.watcher.mu.Unlock()
		return
	}
	d.StartStream(id)
}

// reopenOne re-establishes an ALREADY-LIVE actor's link stream on d — a fresh
// reconnect (every stream on the new Dialer starts nonexistent) or a single
// stream that died while the link stayed up (F6). It OpenStreams again and
// Rebinds the actor's membrane to the fresh arms, but never re-Spawns: the
// cell (identity + in-memory state) outlives the wire session, only the wire
// arm is replaced. A failure here is logged/retried exactly like buildOne's
// — the cell keeps running throughout, its wire arm just stays in the
// disconnect-window (fail-closed) state a moment longer.
func (r *computeRing) reopenOne(id actor.ActorID, d *link.Dialer) {
	rb := r.arms[id]
	if rb == nil {
		// Cannot happen in practice (live ⟹ buildOne already ran and populated
		// r.arms), but fail closed rather than lose the membrane silently.
		r.logger.Error("platform.compute.reopen_missing_arms", "actor", string(id))
		return
	}
	arms, err := d.OpenStream(id, r.dispatchFor(id, d), r.cancelFor(id))
	if err != nil {
		r.logger.Warn("platform.compute.reopen_stream_failed", "actor", string(id), "err", err)
		return
	}
	rb.Rebind(arms)
	r.watcher.mu.Lock()
	r.watcher.down[id] = arms.Down
	r.watcher.mu.Unlock()
	d.StartStream(id)
}

// dispatchFor builds one actor's inbound dispatch closure over link d: deliver
// into its mailbox and, for any non-Delivered outcome, report it UP d as a pure
// observation (KindDeliverResult) — the home logs it exactly as its own delivery
// tap does. Delivered = silence. The daemon holds no truth, so this is
// observation only; closure is materialised home-side from the log. d is bound
// at OpenStream time (never read off a mutable ring field), so a dispatch fired
// from a since-superseded connection's read loop can never race a reconnect.
func (r *computeRing) dispatchFor(id actor.ActorID, d *link.Dialer) func(env *message.Envelope) error {
	return func(env *message.Envelope) error {
		res, derr := r.del.Deliver([]actor.ActorID{id}, env)
		if derr != nil {
			return derr
		}
		if outcome, ok := res.Per[id]; ok && outcome != actorrt.Delivered {
			d.SendDeliverResult(id, env.ID, outcomeString(outcome), "")
		}
		return nil
	}
}

// cancelFor builds one actor's cancel hook: fire the named request's reqCtx on
// this daemon's runtime. rt-scoped only, so it needs no per-connection binding.
func (r *computeRing) cancelFor(id actor.ActorID) func(requestID message.ID) {
	return func(requestID message.ID) { r.rt.CancelRequest(id, requestID) }
}
