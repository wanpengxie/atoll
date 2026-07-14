package actorrt

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// embodiment is one live actor's substrate-side embodiment, regardless of where
// the actor's code runs. Deliver enqueues an envelope into the actor's mailbox
// (never blocking: ErrMailboxFull / ErrCellStopped); stop tears it down. Two
// backends implement it, by transport distance:
//   - cell  — in-process actor   (mailbox = Go channel)
//   - port  — out-of-process actor (mailbox = byte-stream connection)
//
// Both report the same Outcome on Deliver and the same down edge on death —
// and the same QUIET teardown on parent-ctx collapse (self-evict, cancel,
// release, NO down edge: teardown is not an observed death; closure belongs
// to the level-scan reconciler on the next open).
//
// startedAt is the obs `uptime` source: the substrate-stamped bind instant. It is
// substrate-authoritative (only the host knows when it bound the instance — the
// actor never self-reports it), read out-of-band via Runtime.Stat.
type embodiment interface {
	Deliver(env *message.Envelope) error
	cancelRequest(id message.ID)
	startedAt() time.Time
	// kind is the substrate-stamped protocol classification the embodiment was
	// minted with (Spawn/SpawnIfAbsent/Fork's kind param, or a port's Attach-time
	// KindOf resolution) — the incarnation-level household's own copy of the same
	// fact membership stores identity-level (registry.Row.Kind), read out-of-band
	// via Runtime.Stat (UnitStat.Kind). Every embodiment form now welds a real
	// kind at birth (G11: the embodiments table is the incarnation-level birth
	// registry, and every out-generation attribute belongs in it) — a live-
	// embodiment kind read is authoritative for cell/fork/port alike.
	kind() actor.Kind
	// doneCh returns the channel closed when this embodiment's goroutine(s) have
	// FULLY exited — the join handle the zombie escort (and DrainZombies) waits on
	// bounded by grace. It is the sole exit-observation seam; nobody joins it
	// directly anymore (that unbounded join was the disease the zombie ledger cures).
	doneCh() <-chan struct{}
	// isLive reports whether this embodiment is still the live incarnation — read
	// LOCK-FREE off a per-incarnation atomic (set true at go-live, false on
	// death/stop/eviction). It is the WHEN-validity probe a liveCap (livePen)
	// consults per write to fence a dangling capability held by a goroutine that
	// outlived its incarnation. It must NOT take r.mu (churn would serialise every
	// emit behind the hot addressing lock).
	isLive() bool
	// markDead flips the per-incarnation live atomic to false. Called from the
	// death path / stop / pointer-identity eviction. Idempotent.
	markDead()
	// initiateStop is the non-blocking, idempotent SIGNAL half of teardown — it
	// triggers death (cancel + immediate onExit self-eviction) and returns at
	// once, WITHOUT joining the embodiment's own goroutine. It exists so a dying
	// parent's cascade (removeIf) can tear down owned children from
	// within its own death path without deadlocking: a child's initiateStop
	// synchronously re-enters onExit (which takes r.mu), so calling the
	// join-and-signal stop() here — from the dying goroutine — would block the
	// parent's own teardown on the child's goroutine actually exiting, which is
	// not guaranteed to be prompt. It is now also the escort's QUIET-teardown
	// signal (flavorQuiet): the escort — never the entry — is the one goroutine
	// that fires it and then watches doneCh bounded by grace.
	initiateStop()
	// beginTeardown SYNCHRONOUSLY marks the quiet-teardown intent (cell: c.closed;
	// port: p.stopping+p.closed) so a body's own racing self-death is arbitrated
	// QUIET (no down edge) — non-blocking, no cancel/closeConn/evict. Used by the
	// despawn flavour, where the actual teardown (frame write + closeConn) must run
	// async in the escort but the quiet flag must be set at judge-dead time.
	// Idempotent. (Quiet-flavour teardown fires the full initiateStop synchronously
	// instead, so it needs no separate begin.)
	beginTeardown()
	// signalDespawn is the DESPAWN-flavoured SIGNAL half (non-joining), driven by
	// the escort for a by-name termination (Despawn / DespawnID / DespawnChild): a
	// port first emits a best-effort KindDespawn frame ending the remote's
	// execution arm (§10.5) BEFORE closing the wire. Its frame write is bounded by
	// dl (the escort's shared grace budget) and MUST NOT block the caller inline
	// (ipc.Codec.Write holds wmu and has no ctx — P1-5): the write runs in a sub-
	// goroutine and closeConn breaks a stuck one. A cell has no wire, so
	// signalDespawn collapses to initiateStop for it. This is the sole difference
	// from the quiet teardown (replace / channel-teardown, which sends no frame —
	// the remote learns via EOF).
	signalDespawn(dl context.Context)
}

// ErrNotHosted is returned when no embodiment is hosted for the addressed id.
var ErrNotHosted = errors.New("actorrt: actor not hosted")

// ErrRuntimeSealed means the runtime has entered terminal shutdown and will
// never admit another embodiment.
var ErrRuntimeSealed = errors.New("actorrt: runtime sealed")

// Incarnation is the opaque handle to ONE live embodiment of an ActorID — a
// (id, embodiment-pointer) pair. Identity is single-level (one stable ActorID),
// but a capability welded to a specific incarnation can outlive it (a goroutine
// captured a pen, the cell died, a same-id successor took over): the handle
// names WHICH embodiment, so the WHEN-validity gate (IsLive) is by POINTER, not
// by id — defeating ABA. It is comparable (id + pointer) and is NEVER serialised
// into an envelope/truth; it lives only in the volatile liveness plane.
type Incarnation struct {
	id actor.ActorID
	p  embodiment
}

// ID is the read-only accessor for the incarnation's ActorID. The embodiment
// pointer stays opaque (no accessor) — only the host compares it. The platform
// link layer needs ID() to Mint a pen welded to (id, chID) when wrapping a
// livePen for an out-of-process incarnation.
func (i Incarnation) ID() actor.ActorID { return i.id }

// DownWatcher is the consumer end of the obs down-edge PUSH channel. The
// runtime invokes OnDown exactly once for each hosted unit that dies abnormally
// — death is the DELETED edge of embodiment (obs), NOT a control signal and NOT
// truth. The watcher's reaction (e.g. materialise receiver_unavailable) is work
// and lands in truth on its own. The dead unit has ALREADY self-evicted from the
// addressing map before OnDown runs, so a watcher MUST NOT despawn it.
//
// Reliability: register watchers BEFORE Spawn/Attach so no death edge is missed.
// OnDown is invoked synchronously in the reap path, so a watcher MUST be non-
// blocking (it stalls the dying goroutine's reap otherwise) — the down edge is a
// LOSSY fast-path, not the closure authority: correctness rests on the level scan
// (the reconciler), which closes any orphan a dropped/blocked edge missed. A
// watcher that must do blocking closure work hands the id to its own resident
// consumer (see channelkit) rather than doing it here.
type DownWatcher interface {
	OnDown(ctx context.Context, id actor.ActorID, incarnation Incarnation, cause error)
}

// Runtime owns the live embodiments for one channel and is the addressing seam.
// Identity is single-level: a stable ActorID names at most one live embodiment at
// a time. Spawn/Attach replaces; death is terminal (no transparent same-id
// respawn). Death and replacement are checked by POINTER IDENTITY (the map
// entry still points to this very embodiment), so a dying predecessor can never
// evict its successor — no per-instance generation is needed.
type Runtime struct {
	parent context.Context
	clock  func() time.Time
	logger *slog.Logger

	mu          sync.RWMutex
	embodiments map[actor.ActorID]embodiment
	// prepared holds at most one authenticated, ACK-in-flight port candidate per
	// id. It is deliberately separate from embodiments: the incumbent remains the
	// live/addressable truth until the candidate's ACK succeeds, so a failed ACK
	// cannot create an unhosted replacement window. Guarded by mu.
	prepared    map[actor.ActorID]*port
	watchers    []DownWatcher
	obsWatchers []ObsWatcher
	mailbox     int
	// owned tracks the fork ownership edge: parent-embodiment -> its forked
	// children's embodiments. It lives ONLY in memory (an incarnation
	// is volatile) and is pruned of already-not-live entries on every Fork
	// (amortised cleanup — no separate sweep/GC) and cleared wholesale for a key
	// when that parent embodiment itself dies (removeIf cascades initiateStop() to
	// every child still on the list at that instant).
	owned map[embodiment][]embodiment

	// zombies is the already-judged-dead-but-not-yet-reaped ledger (zombie.go) —
	// every terminated body from the instant judged dead (enrollLocked, in the
	// same critical section as the death judgement, P0-1) to the instant its
	// goroutine truly exits (reapZombie). Keyed by embodiment POINTER so a replaced
	// predecessor and its live same-id successor never collide. Guarded by r.mu.
	zombies map[embodiment]*zombie
	// grace bounds every escort's wait (and the default DrainZombies deadline). 5s
	// by default (Config.ZombieGrace injects a short value in tests — DoD never
	// really waits 5s).
	grace time.Duration
	// leakedTotal counts corpses ever declared leaked (red line ⑤: leaks counted,
	// never silent). Cumulative — survives the reap that clears the entry.
	leakedTotal atomic.Int64
	sealed      bool // guarded by mu; admission check and registration are linearized together
}

// defaultZombieGrace is the aggregate teardown grace (P0-1 = 5s, not a config
// knob — only test injection overrides it).
const defaultZombieGrace = 5 * time.Second

// Config configures a Runtime.
type Config struct {
	// Parent is the context all embodiments derive from; cancelling it tears the
	// whole channel down.
	Parent context.Context
	// Mailbox is the default bounded mailbox depth (<=0 → 64).
	Mailbox int
	// Clock stamps each embodiment's bind instant (obs uptime source). nil →
	// time.Now. Injectable so tests can pin uptime deterministically.
	Clock func() time.Time
	// Logger surfaces down-watcher faults (a watcher panic on the
	// closure-critical death path) and other cell-lifecycle edge observability.
	// nil → discard (no-op).
	Logger *slog.Logger
	// ZombieGrace overrides the per-corpse escort grace (and default DrainZombies
	// deadline). <=0 → defaultZombieGrace (5s). Injected short in tests so a
	// leak/late-reap assertion never really waits 5s.
	ZombieGrace time.Duration
}

// Deliverer is the privileged capability to enqueue an envelope into a hosted
// cell's mailbox. It is the ONLY way to feed a mailbox, and New hands it out
// EXACTLY ONCE — it is deliberately NOT a method on the broadly-shared *Runtime
// handle. The composition root routes it to the single legitimate feeder: the
// post-harness fanout (every envelope it enqueues has already committed to
// truth) and that fanout's wire-dispatch arm. So code that merely holds a
// *Runtime (to Spawn / address / query) CANNOT inject into a mailbox and thus
// cannot bypass the harness — the mailbox is the harness pipeline's private
// egress, structurally, not by convention.
type Deliverer interface {
	Deliver(audience []actor.ActorID, env *message.Envelope) (DeliverResult, error)
}

// New constructs a Runtime and its confined work-egress capability. The
// *Runtime is the broadly-shareable management/addressing handle (Spawn/Attach/
// Despawn/Stat/StopAll/WatchDown); the Deliverer is the confined WORK enqueue
// (give it ONLY to the post-harness fanout). A *Runtime holder can address and
// observe but cannot inject work — the mailbox is the harness/substrate's
// private egress, structurally.
func New(cfg Config) (*Runtime, Deliverer) {
	parent := cfg.Parent
	if parent == nil {
		parent = context.Background()
	}
	mb := cfg.Mailbox
	if mb <= 0 {
		mb = 64
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	grace := cfg.ZombieGrace
	if grace <= 0 {
		grace = defaultZombieGrace
	}
	r := &Runtime{
		parent:      parent,
		clock:       clock,
		logger:      logger,
		embodiments: make(map[actor.ActorID]embodiment),
		prepared:    make(map[actor.ActorID]*port),
		owned:       make(map[embodiment][]embodiment),
		zombies:     make(map[embodiment]*zombie),
		grace:       grace,
		mailbox:     mb,
	}
	return r, deliverer{r}
}

// deliverer is the only holder-confined implementation of Deliverer; it wraps
// the runtime and calls its unexported enqueue (unreachable from other packages
// via the shared *Runtime).
type deliverer struct{ r *Runtime }

func (d deliverer) Deliver(audience []actor.ActorID, env *message.Envelope) (DeliverResult, error) {
	return d.r.deliver(audience, env)
}

// WatchDown registers a watcher for the obs down-edge PUSH channel (currently:
// death = the DELETED edge). Register BEFORE Spawn/Attach so no edge is missed.
func (r *Runtime) WatchDown(w DownWatcher) {
	if w == nil {
		return
	}
	r.mu.Lock()
	r.watchers = append(r.watchers, w)
	r.mu.Unlock()
}

// publishDown fans the down edge out to every watcher. Invoked once per abnormal
// death, synchronously in the dying goroutine's reap path — a LOSSY fast-path, not
// the closure authority (the level scan is; a watcher that drops or defers an edge
// is closed for by the next scan). Each watcher is guarded: a watcher panic must
// not escape and crash the process through the death path — AND it is logged,
// because a swallowed fault is a silent black hole. Watchers MUST be non-blocking;
// a blocking watcher stalls the dying goroutine's reap.
func (r *Runtime) publishDown(id actor.ActorID, self embodiment, cause error) {
	if cause != nil {
		r.logger.Warn("actorrt.down", "actor", id, "cause", cause)
	} else {
		r.logger.Debug("actorrt.down", "actor", id)
	}
	r.mu.RLock()
	ws := make([]DownWatcher, len(r.watchers))
	copy(ws, r.watchers)
	r.mu.RUnlock()
	for _, w := range ws {
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					r.logger.Error("actorrt.down.watcher_panic",
						"actor", id, "cause", cause, "panic", rec)
				}
			}()
			w.OnDown(context.Background(), id, Incarnation{id: id, p: self}, cause)
		}()
	}
}

// WatchObsAll registers a population-wide watcher for actor PUSH observations.
// It has the same lifetime shape as WatchDown: watchers are expected to live as
// long as their runtime owner. A future shorter-lived consumer must add an
// explicit unsubscribe operation together with documented in-flight semantics.
func (r *Runtime) WatchObsAll(w ObsWatcher) {
	if w == nil {
		return
	}
	r.mu.Lock()
	r.obsWatchers = append(r.obsWatchers, w)
	r.mu.Unlock()
}

// publishObs fans an actor-published obs snapshot to that actor's watchers
// (obs push/actor). Invoked on the publishing embodiment's goroutine; guarded so
// a watcher panic cannot escape. No watcher → no-op.
//
// POINTER-IDENTITY gated (the ABA guard, same discipline as removeIf/Despawn):
// the fanout fires IFF the addressing map still points to this very `self`
// incarnation. A stale predecessor goroutine that outlived its incarnation (it
// was replaced, or is mid-teardown) cannot publish obs that watchers would
// mis-attribute to the live same-id successor — the snapshot is dropped. The
// check shares the one RLock that snapshots the watcher slice, so it is
// consistent with the fanout it gates.
func (r *Runtime) publishObs(id actor.ActorID, self embodiment, kind ObsKind, val ObsValue) {
	r.mu.RLock()
	cur, ok := r.embodiments[id]
	if !ok || cur != self {
		r.mu.RUnlock()
		return
	}
	ws := append([]ObsWatcher(nil), r.obsWatchers...)
	r.mu.RUnlock()
	incarnation := Incarnation{id: id, p: self}
	for _, w := range ws {
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					r.logger.Error("actorrt.obs.watcher_panic", "actor", id, "kind", kind, "panic", rec)
				}
			}()
			w.OnObs(context.Background(), id, incarnation, kind, val)
		}()
	}
}

// CancelRequest fires the request-scope of cancel(scope) for one in-flight
// request on a hosted embodiment: the reqCtx the addressed actor is currently
// running that request under is cancelled, off the work goroutine. A cell fires
// its in-flight CancelFunc directly; a port writes a KindCancel frame so the
// remote host cancels its own cell's reqCtx (in-proc and cross-wire are the same
// primitive, one scope down from Despawn). No-op if no embodiment is hosted for id
// or the request already closed — cancel is a best-effort hint; the caller's
// closure owns the terminal, so a lost cancel only costs the receiver a little
// wasted work (the ExpiresAt deadline still collapses it).
func (r *Runtime) CancelRequest(id actor.ActorID, requestID message.ID) {
	r.mu.RLock()
	p, ok := r.embodiments[id]
	r.mu.RUnlock()
	if !ok {
		return
	}
	p.cancelRequest(requestID)
}

// Outcome is the per-audience truth Deliver reports about its own action — the
// substrate knows whether it hosts an addressed actor and must not misreport it.
type Outcome int

const (
	// Delivered: the envelope was enqueued into the actor's mailbox.
	Delivered Outcome = iota
	// NotHosted: this runtime hosts no embodiment for the actor (legitimately —
	// the audience may include system/user/remote-elsewhere actors). The seam
	// can fast-fail receiver_unavailable instead of waiting for a timeout.
	NotHosted
	// MailboxFull: an embodiment is hosted but its bounded mailbox is full.
	MailboxFull
	// Stopped: an embodiment is hosted but tearing down.
	Stopped
)

// DeliverResult is the structured, per-audience truth of a Deliver call: the
// Outcome for each addressed actor.
type DeliverResult struct {
	Per map[actor.ActorID]Outcome
}

// LiveIDs returns a snapshot of every live ActorID. ACK-in-flight attach
// candidates live in Runtime.prepared, never in the authoritative embodiments
// map, so an incumbent remains visible until its replacement truly commits.
func (r *Runtime) LiveIDs() []actor.ActorID {
	r.mu.RLock()
	ids := make([]actor.ActorID, 0, len(r.embodiments))
	for id, p := range r.embodiments {
		if p.isLive() {
			ids = append(ids, id)
		}
	}
	r.mu.RUnlock()
	return ids
}

// SpawnIfAbsent mints id ONLY IF no embodiment currently occupies it — activation's
// atomic CAS mint, mirroring Fork's two-phase-with-recheck discipline.
// Unlike Attach (whose wire re-bind stops and replaces, last-go-live-wins), SpawnIfAbsent NEVER replaces an
// existing embodiment: build runs OUTSIDE the lock (same discipline as
// Spawn/Fork), then absence is RE-CHECKED inside the SAME critical section as
// the insert. If id is already occupied by the time the lock is taken —
// someone else won the race (admission via Spawn, or another reconcile tick
// via SpawnIfAbsent itself) — the freshly-built shell is discarded: never
// inserted into embodiments, never started (ok=false). This is a real CAS, not
// best-effort, because rebuild cost for agent-class actors (re-establishing
// LLM context) is a real cost, not a theoretical nicety.
func (r *Runtime) SpawnIfAbsent(id actor.ActorID, kind actor.Kind, build func(Incarnation) Actor) (Incarnation, bool, error) {
	r.mu.RLock()
	sealed := r.sealed
	_, occupied := r.embodiments[id]
	if !occupied {
		_, occupied = r.prepared[id]
	}
	r.mu.RUnlock()
	if sealed {
		return Incarnation{}, false, ErrRuntimeSealed
	}
	if occupied { // fast-path: skip the build entirely if already obviously taken.
		return Incarnation{}, false, nil
	}

	c := allocShell(r.parent, id, kind, r.mailbox, r.publishDown, r.publishObs, r.removeIf, r.reapZombie, r.clock(), r.logger)
	inc := Incarnation{id: id, p: c}
	var err error
	c.impl, err = buildActor(build, inc) // OUTSIDE the lock; IsLive(inc)==false during build.
	if err != nil {
		c.cancel()
		return Incarnation{}, false, err
	}

	r.mu.Lock()
	if r.sealed {
		r.mu.Unlock()
		abortBuild(c)
		return Incarnation{}, false, ErrRuntimeSealed
	}
	_, occupied = r.embodiments[id]
	if !occupied {
		_, occupied = r.prepared[id]
	}
	if occupied { // same critical section re-check
		r.mu.Unlock() // lost the race: discard the shell, never c.start() it.
		// Release the discarded shell's ctx node (same discard-release as
		// Fork's collision/parent-dead arms): allocShell derived it from
		// r.parent, so an uncancelled discard pins a child-context entry in
		// the parent's tree for the whole channel lifetime — and the eager
		// reconcile ring races admission Spawn here every tick.
		abortBuild(c)
		return Incarnation{}, false, nil
	}
	r.embodiments[id] = c
	c.live.Store(true) // go-live: register + liveness atomic flip are one critical section.
	r.mu.Unlock()
	c.start()
	return inc, true, nil
}

// IsLive reports whether inc is still the live incarnation, by POINTER (ABA-safe)
// and LOCK-FREE (a per-incarnation atomic, never r.mu). It is the WHEN-validity
// authority a liveCap consults per write; a nil handle is never live.
func (r *Runtime) IsLive(inc Incarnation) bool {
	if inc.p == nil {
		return false
	}
	return inc.p.isLive()
}

// Attach binds an out-of-process actor that CONNECTED IN over conn, registering
// it as a `port` embodiment. The substrate does not spawn the remote (it connects
// in); Attach performs the handshake, resolves
// the connection's credential to an ActorID via resolve, relays the remote's
// upward invocations through sinks (Emit for the message plane, Access/Schedule
// for the opaque plane-2 / time-axis arms), and returns the bound Incarnation. If
// an embodiment already exists for the resolved id it is stopped and replaced.
//
// kindOf resolves the just-resolved id's declared Kind (G11: Attach is a birth
// position in the incarnation household exactly like Spawn/SpawnIfAbsent/Fork,
// so it must weld the SAME out-generation attribute — no embodiment form may
// answer a silent zero-value Kind from Runtime.Stat). It runs AFTER resolve
// yields the id — Attach itself never learns which actor is connecting before
// the handshake decodes the lease — so kindOf is a lookup keyed by the
// resolved id, not a static value; a nil kindOf (or a false ok) welds the zero
// value, same as before this parameter existed.
//
// onCancelRequest is the injected upstream relay for a KindCancelRequest frame:
// a bound actor abandoning one of ITS OWN outbound requests (the caller-side
// upstream twin of the host→remote cancel). It is NOT a Sink (the three Sinks
// arms are all ack'd; this signal is fire-and-forget, no ack — the same reason
// obs/down are not Sinks), so it rides its own parameter. The port passes the
// connection's authenticated bound id + the request id; the injecting caller
// reverse-resolves the request's target and validates the sender from the log
// (non-self-report). nil → inbound cancel_request is dropped.
//
// hsCtx bounds ONLY the connect-in handshake (a substrate-owned protocol step
// whose time bound is a substrate invariant — a peer that connects but never
// sends a handshake must not pin this goroutine forever). It does NOT scope the
// port's LIFETIME: the bound port lives for the runtime's (r.parent), not for
// this call — a per-call lifetime ctx would wrongly tear the port down when
// Attach returns. Pass a deadline ctx to guard the handshake; a nil/background
// ctx degrades to an unbounded handshake read.
// PreparedAttach is a parsed and authenticated port handshake that has not yet
// entered the Runtime address table and has not emitted an ACK. The caller must
// invoke exactly one of Commit or Abort. This split lets an outer coordinator
// acquire its daemon/slot/actor locks after learning the authenticated actor id,
// then hold its per-link port lock across the ACK and runtime commit.
type PreparedAttach struct {
	mu       sync.Mutex
	runtime  *Runtime
	port     *port
	finished bool
}

// ID returns the actor identity resolved from the handshake. It does not imply
// that the actor is live or addressable.
func (p *PreparedAttach) ID() actor.ActorID {
	if p == nil || p.port == nil {
		return ""
	}
	return p.port.id
}

// PrepareHandshake reads, resolves, and authenticates a handshake without
// publishing it. onExit is the required pointer-identity index observer.
func (r *Runtime) PrepareHandshake(hsCtx context.Context, conn io.ReadWriteCloser, sinks Sinks, resolve ResolveFunc, kindOf KindOf, onCancelRequest func(actor.ActorID, message.ID), onExit func(Incarnation)) (*PreparedAttach, error) {
	if resolve == nil {
		return nil, errors.New("actorrt: port requires ResolveFunc")
	}
	if onExit == nil {
		return nil, errors.New("actorrt: port requires exit observer")
	}
	exit := func(id actor.ActorID, self embodiment) {
		r.removeIf(id, self)
		onExit(Incarnation{id: id, p: self})
	}
	p, err := newPort(r.parent, hsCtx, conn, sinks, resolve, kindOf, r.publishDown, r.publishObs, onCancelRequest, exit, r.reapZombie, r.clock(), r.logger)
	if err != nil {
		return nil, err
	}
	return &PreparedAttach{runtime: r, port: p}, nil
}

// Commit registers an invisible candidate, emits the handshake ACK, and only
// then atomically replaces the incumbent. The old embodiment remains live and
// addressable until that final commit; failures remove and close only the
// candidate.
// Commit uses a required lock-free outer-generation fence.
// valid is evaluated while r.mu is held both before prepared installation and
// at the final commit point; it must not acquire locks. A false verdict aborts
// without making the port live.
func (p *PreparedAttach) Commit(valid func() bool) (Incarnation, error) {
	if p == nil || p.runtime == nil || p.port == nil {
		return Incarnation{}, errors.New("actorrt: nil prepared attach")
	}
	if valid == nil {
		return Incarnation{}, errors.New("actorrt: prepared attach requires generation validator")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return Incarnation{}, errors.New("actorrt: prepared attach already finished")
	}
	p.finished = true
	r := p.runtime
	port := p.port
	r.mu.Lock()
	sealed := r.sealed
	if sealed || !valid() {
		r.mu.Unlock()
		port.prepared.Store(true)
		port.initiateStop()
		if !sealed {
			return Incarnation{}, errors.New("actorrt: prepared attach generation is no longer current")
		}
		return Incarnation{}, ErrRuntimeSealed
	}
	port.prepared.Store(true)
	previousCandidate := r.prepared[port.id]
	r.prepared[port.id] = port
	r.mu.Unlock()
	// Only one ACK-in-flight candidate per id. A later candidate deposes the
	// earlier one without touching the still-live incumbent. initiateStop can
	// synchronously re-enter Runtime through onExit, so it stays outside r.mu.
	if previousCandidate != nil && previousCandidate != port {
		previousCandidate.initiateStop()
	}
	// ACK is sent while the incumbent remains authoritative. Any failure
	// conditionally aborts this exact candidate; the old body is untouched.
	if err := port.writeHandshakeAck(); err != nil {
		r.abortPrepared(port)
		return Incarnation{}, err
	}
	r.mu.Lock()
	cur, stillCurrent := r.prepared[port.id]
	if r.sealed || !stillCurrent || cur != port || !port.prepared.Load() || !valid() {
		r.mu.Unlock()
		r.abortPrepared(port)
		if r.sealed {
			return Incarnation{}, ErrRuntimeSealed
		}
		return Incarnation{}, errors.New("actorrt: prepared attach was deposed before commit")
	}
	delete(r.prepared, port.id)
	old, existed := r.embodiments[port.id]
	var retirement retirementWork
	if existed {
		// REPLACEMENT-LIVE-FLIP INVARIANT (same as Spawn): the predecessor dies
		// in the SAME critical section as the authoritative map swap. The only
		// difference from the old sequence is timing: this happens after ACK, so
		// an ACK failure leaves the predecessor fully intact.
		retirement = r.retireCurrentLocked(port.id, old, flavorQuiet)
	}
	r.embodiments[port.id] = port
	port.prepared.Store(false)
	port.live.Store(true)
	port.start()
	r.mu.Unlock()
	runRetirement(retirement)
	// Return the Incarnation (id + this embodiment pointer): the home-side port
	// death-write gate welds a livePen to it and gates each cross-wire emit
	// on IsLive, so a replaced/torn-down port's in-flight emit is fenced by
	// pointer identity — the message-plane parity of the cell path's livePen.
	return Incarnation{id: port.id, p: port}, nil
}

// Abort closes an uncommitted handshake without publishing it or enrolling a
// zombie. It is idempotent; after a successful or failed Commit it is a no-op.
func (p *PreparedAttach) Abort() {
	if p == nil || p.port == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return
	}
	p.finished = true
	p.port.prepared.Store(true)
	p.port.initiateStop()
}

func (r *Runtime) abortPrepared(p *port) {
	r.mu.Lock()
	if cur, ok := r.prepared[p.id]; ok && cur == p {
		delete(r.prepared, p.id)
	}
	r.mu.Unlock()
	p.initiateStop()
}

// takePreparedLocked removes the ACK-in-flight candidate for id. Callers use it
// when an explicit by-id/by-incarnation teardown wins, so that candidate cannot
// resurrect the just-terminated identity after its ACK completes. r.mu held.
func (r *Runtime) takePreparedLocked(id actor.ActorID) *port {
	p := r.prepared[id]
	delete(r.prepared, id)
	return p
}

// removeIf is the pointer-identity-checked self-eviction hook handed to each
// embodiment. It deletes the map entry IFF it still points to this very instance —
// so a late-dying predecessor (already replaced) cannot evict its successor.
func (r *Runtime) removeIf(id actor.ActorID, self embodiment) {
	r.mu.Lock()
	if cur, ok := r.embodiments[id]; ok && cur == self {
		delete(r.embodiments, id)
	}
	// Ownership-edge cascade: this embodiment may itself be a fork parent.
	// Take its children list and drop the r.owned entry in the SAME critical
	// section as the eviction above (so a concurrent Fork sees either the full
	// list-to-be-cascaded or nothing — never a half-updated slice).
	children := r.owned[self]
	delete(r.owned, self)
	// P0-1 (natural-death half): enrol this self-terminating body BEFORE it
	// proceeds to close its done channel, so account⇔residue holds even for a
	// body no external entry ever named. Idempotent — if an external entry already
	// enrolled it (flavorQuiet/flavorDespawn), this neither re-enrols nor launches
	// a second escort (its flavor is not downgraded to natural).
	launch := r.retireLocked(self, id, flavorNatural)
	r.mu.Unlock()
	// This embodiment is dying — flip its liveness atomic so any capability welded
	// to it (livePen) fails the WHEN gate from here on. Idempotent.
	self.markDead()
	// Launch the (natural-flavour, watch-only) escort OUTSIDE the lock. It fires
	// no teardown signal — the body is already unwinding — it only guards against
	// a stuck exit defer (a worker-joining Stop that never returns → leaked).
	launch()
	// Cascade OUTSIDE the lock, signal-only: initiateStop() must never be
	// called while holding r.mu — a child's initiateStop synchronously re-enters
	// onExit/removeIf, which takes r.mu again (deadlock if still locked here).
	// Each child's own death path recurses this same removeIf on ITS children —
	// depth-first, no level of the cascade waits on the next.
	for _, child := range children {
		child.initiateStop()
	}
}

// retirementWork is the outside-r.mu half of an external stopping transition.
// retireCurrentLocked performs the entire durable in-memory judgement under one
// lock: pointer-confirmed map removal, dead/stopping mark, zombie enrollment,
// and ownership-edge take. Only re-entrant signals and escort launch remain.
type retirementWork struct {
	children []embodiment
	launch   func()
}

func (r *Runtime) retireCurrentLocked(id actor.ActorID, body embodiment, flavor deathFlavor) retirementWork {
	delete(r.embodiments, id)
	body.markDead()
	body.beginTeardown()
	children := r.owned[body]
	delete(r.owned, body)
	return retirementWork{children: children, launch: r.retireLocked(body, id, flavor)}
}

func runRetirement(work retirementWork) {
	// Signals can synchronously re-enter removeIf, so none may run under r.mu.
	// Children are signalled before the parent's escort starts, matching the
	// ownership cascade's parent-death total order.
	for _, child := range work.children {
		child.initiateStop()
	}
	if work.launch != nil {
		work.launch()
	}
}

// Despawn stops and removes the incarnation inc (no-op unless the map still
// points to this very incarnation). This is the external deregister path. It
// carries NO caller obligation to collapse
// in-flight requests first: after despawn the id is absent from embodiment, and
// the closure reconciler (a level scan over open-request × receiver-absent)
// closes every orphan in-flight request with receiver_unavailable. The death
// edge would only fire on abnormal exit; closure is geometry (the level scan),
// not a despawn-caller convention.
func (r *Runtime) Despawn(inc Incarnation) {
	r.mu.Lock()
	cur, ok := r.embodiments[inc.id]
	matched := ok && cur == inc.p
	var retirement retirementWork
	var candidate *port
	if matched {
		// REPLACEMENT-LIVE-FLIP INVARIANT: flip dead in the SAME critical section as
		// the map delete, so no window opens where the entry is gone but IsLive still
		// reads true for a stale welded capability. markDead is idempotent + never
		// re-enters r.mu, so it is deadlock-safe in-lock. Enrol as a zombie in the
		// SAME critical section (P0-1); the escort drives the despawn teardown +
		// bounded join, so Despawn returns in O(judge-dead) not O(goroutine-exit).
		retirement = r.retireCurrentLocked(inc.id, inc.p, flavorDespawn)
		candidate = r.takePreparedLocked(inc.id)
	}
	r.mu.Unlock()
	// Guarded by POINTER IDENTITY: only despawn IFF the map still points to this
	// very incarnation, so despawning a stale handle (a replaced predecessor, or
	// an id never hosted) is a safe no-op and can never evict a same-id successor.
	runRetirement(retirement)
	if candidate != nil {
		candidate.initiateStop()
	}
}

// DespawnQuiet is Despawn's QUIET twin: same pointer-identity guard, same map
// eviction, but the teardown sends NO wire frame (stop(), not stopDespawn()). It
// is the home's graceful-teardown entry for a specific port — the host is going
// away, so its ports must fall silent (no down edge → no receiver_unavailable for
// in-flight requests) WITHOUT telling the remote to despawn its cell (a quiet close
// is a link drop the remote can redial, not a by-name arm termination). A cell's
// stop() is already quiet, so DespawnQuiet and Despawn coincide for cells.
func (r *Runtime) DespawnQuiet(inc Incarnation) {
	r.mu.Lock()
	cur, ok := r.embodiments[inc.id]
	matched := ok && cur == inc.p
	var retirement retirementWork
	var candidate *port
	if matched {
		// Quiet flavour: no KindDespawn frame (a graceful host teardown is a link
		// drop the remote can redial, not a by-name arm termination). Enrol in-lock
		// (P0-1); the escort fires the quiet teardown + bounded join.
		retirement = r.retireCurrentLocked(inc.id, inc.p, flavorQuiet)
		candidate = r.takePreparedLocked(inc.id)
	}
	r.mu.Unlock()
	runRetirement(retirement)
	if candidate != nil {
		candidate.initiateStop()
	}
}

// DespawnID stops and removes whatever embodiment CURRENTLY occupies id, if any
// (activation's scoped deactivation — the caller only ever holds an id
// there, never an Incarnation handle: the eager-managed set is tracked
// across MULTIPLE Reconcile ticks as a plain id set, and retaining stale
// Incarnation pointers across ticks would reintroduce exactly the ABA risk
// pointer-identity discipline exists to avoid). It looks the embodiment up
// fresh and evicts it under the SAME critical section — not weaker than
// Despawn(inc), just keyed differently: Despawn requires a caller-held
// pointer proving WHICH embodiment to kill, DespawnID kills "whoever is live
// for id right now" (mirrors Linux kill(pid, sig) — by-name, not by-handle).
// Returns false if id has no live embodiment (a no-op, not an error — the
// scoped deactivation diff can legitimately name an id that already died on
// its own between two ticks).
func (r *Runtime) DespawnID(id actor.ActorID) bool {
	r.mu.Lock()
	p, ok := r.embodiments[id]
	var retirement retirementWork
	if ok {
		// REPLACEMENT-LIVE-FLIP INVARIANT: dead-flip in the same critical section as
		// the map delete (idempotent atomic, no r.mu re-entry) — no live-but-unmapped
		// window for a stale welded cap to slip through IsLive. Enrol in-lock (P0-1);
		// the escort drives the despawn teardown + bounded join.
		retirement = r.retireCurrentLocked(id, p, flavorDespawn)
	}
	candidate := r.takePreparedLocked(id)
	r.mu.Unlock()
	runRetirement(retirement)
	if candidate != nil {
		candidate.initiateStop()
	}
	// The bool remains the documented "live embodiment existed" verdict. A
	// candidate is cancelled to prevent resurrection, but was never live and must
	// not be reported as one.
	return ok
}

// deliver routes env to every audience embodiment hosted by this Runtime by
// enqueueing into each mailbox, returning the per-audience Outcome. An audience
// member with no local embodiment is reported NotHosted (not silently skipped) —
// the substrate reports truthfully what it did so the seam can fast-fail. error
// is reserved for a true exception (nil envelope), not for delivery conditions.
//
// Unexported: the enqueue is reachable ONLY through the Deliverer capability New
// hands out (a *Runtime holder cannot call it), so the mailbox stays the harness
// pipeline's private egress.
//
// No ctx: the enqueue is a non-blocking mailbox post (cell.Deliver never blocks
// — a full mailbox returns MailboxFull at once), so there is no cancelable wait
// for a ctx to act on. A per-call ctx would be pure decoration.
func (r *Runtime) deliver(audience []actor.ActorID, env *message.Envelope) (DeliverResult, error) {
	if env == nil {
		return DeliverResult{}, errors.New("actorrt: deliver nil envelope")
	}
	res := DeliverResult{Per: make(map[actor.ActorID]Outcome, len(audience))}
	r.mu.RLock()
	matched := make(map[actor.ActorID]embodiment, len(audience))
	for _, id := range audience {
		p, ok := r.embodiments[id]
		if !ok {
			res.Per[id] = NotHosted
			continue
		}
		matched[id] = p
	}
	r.mu.RUnlock()

	for id, p := range matched {
		switch err := p.Deliver(env); err {
		case nil:
			res.Per[id] = Delivered
		case ErrMailboxFull:
			res.Per[id] = MailboxFull
		case ErrCellStopped:
			res.Per[id] = Stopped
		default:
			res.Per[id] = Stopped
		}
	}
	return res, nil
}

// StopAll judges every embodiment dead and enrols each as a zombie — it does NOT
// wait for any goroutine to exit. Used at channel teardown: it returns in
// O(judge-dead), and the all-kill wait is DrainZombies's job (the sole legitimate
// aggregate join). Each body's escort drives its own quiet teardown + bounded join
// in the background; a caller that wants the leaked list calls DrainZombies next.
func (r *Runtime) StopAll() {
	r.mu.Lock()
	r.sealLocked()
	retirements := make([]retirementWork, 0, len(r.embodiments))
	for id, p := range r.embodiments {
		// REPLACEMENT-LIVE-FLIP INVARIANT: dead-flip every embodiment in the SAME
		// critical section that clears the map, so no live-but-unmapped window opens
		// for a stale welded cap. markDead is an idempotent atomic and never re-enters
		// r.mu — deadlock-safe in-lock. Enrol each in-lock (P0-1) with the quiet
		// flavour (channel teardown sends no KindDespawn — the remote learns via EOF).
		retirements = append(retirements, r.retireCurrentLocked(id, p, flavorQuiet))
	}
	candidates := make([]*port, 0, len(r.prepared))
	for id, p := range r.prepared {
		delete(r.prepared, id)
		candidates = append(candidates, p)
	}
	r.mu.Unlock()
	for _, retirement := range retirements {
		runRetirement(retirement)
	}
	for _, candidate := range candidates {
		candidate.initiateStop()
	}
}

// Seal permanently closes embodiment admission. It is idempotent.
func (r *Runtime) Seal() {
	r.mu.Lock()
	r.sealLocked()
	r.mu.Unlock()
}

func (r *Runtime) sealLocked() { r.sealed = true }

// UnitStat is the substrate-owned obs facts about one hosted unit, read
// out-of-band (side-effect-free, never through the mailbox, never truth). It
// carries ONLY facts the substrate authoritatively owns; an actor's own
// operational state (quota, queue depth, …) is NOT here — that is actor-owned
// obs, answered by the actor. New substrate-owned facts are additive fields.
type UnitStat struct {
	// StartedAt is the bind instant. uptime = now - StartedAt, derived by the
	// consumer (the substrate stores the instant, not the elapsed duration —
	// registry-stores-membership / readiness-is-outcome model).
	StartedAt time.Time
	// Kind is the embodiment's own copy of the out-generation-attribute the
	// incarnation household must carry (G11: the embodiments table is the
	// incarnation-level birth registry — every out-generation attribute welded
	// at mint time belongs in it). Set by Spawn/SpawnIfAbsent/Fork's kind param,
	// or by Attach's kindOf resolution for a port — every birth position welds
	// it, none answers a silent zero value.
	Kind actor.Kind
}

// Stat reads the substrate-owned obs facts for id. The bool reports embodiment
// (is a live instance hosted here right now) — the `kill -0` / is_process_alive
// authority: only the substrate can answer it, so it never asks the actor and
// never blocks. It replaces the old boolean Has: present = the second return,
// uptime = now - StartedAt.
func (r *Runtime) Stat(id actor.ActorID) (UnitStat, bool) {
	r.mu.RLock()
	p, ok := r.embodiments[id]
	r.mu.RUnlock()
	if !ok || !p.isLive() {
		return UnitStat{}, false
	}
	return UnitStat{StartedAt: p.startedAt(), Kind: p.kind()}, true
}

// CurrentIncarnation is the authoritative self-read of an actor's live
// embodiment handle — the schedule engine's ATTACH seam: welding an
// incarnation-bind timer to "whichever embodiment is live for id right now"
// requires reading that fact FROM the runtime itself (an incarnation is never
// caller-reported and never serialised) — the same addressing-map authority
// Stat/deliver consult. It is the pidfd analogue: a HELD pointer to one
// specific live instance, not a re-resolvable name.
//
// Scope (deliberately narrow — DO NOT widen without re-reading this note):
// this only guards "no embodiment at all" (ok=false). It structurally CANNOT
// guard "the caller's mental model of WHICH embodiment is live is stale" — a
// goroutine that raced ahead of a same-id successor taking over
// (Despawn+respawn between the caller's last observation and this call) gets
// the SUCCESSOR's handle, not an error; this method reports "whoever is live
// right now", exactly like Stat/deliver's map lookup. That leaked-goroutine
// misuse class is fenced by the downstream liveSchedule membrane at the
// platform link layer, not here. The drop check at
// fire time still works correctly regardless: it compares the handle captured
// HERE at Schedule time by POINTER identity (IsLive), so a since-replaced
// incarnation is still caught even though this call itself does not detect
// staleness up front.
func (r *Runtime) CurrentIncarnation(id actor.ActorID) (Incarnation, bool) {
	r.mu.RLock()
	p, ok := r.embodiments[id]
	r.mu.RUnlock()
	if !ok || !p.isLive() {
		return Incarnation{}, false
	}
	return Incarnation{id: id, p: p}, true
}
