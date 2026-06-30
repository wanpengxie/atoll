package actorrt

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// presence is one live actor's substrate-side presence, regardless of where
// the actor's code runs. Deliver enqueues an envelope into the actor's mailbox
// (never blocking: ErrMailboxFull / ErrCellStopped); stop tears it down. Two
// backends implement it, by transport distance:
//   - cell  — in-process actor   (mailbox = Go channel)
//   - port  — out-of-process actor (mailbox = byte-stream connection)
//
// Both report the same Outcome on Deliver and the same presence-down edge on death.
//
// startedAt is the obs `uptime` source: the substrate-stamped bind instant. It is
// substrate-authoritative (only the host knows when it bound the instance — the
// actor never self-reports it), read out-of-band via Runtime.Stat.
type presence interface {
	Deliver(env *message.Envelope) error
	observe(ctx context.Context, kind ObsKind) (ObsValue, error)
	cancelRequest(id message.ID)
	startedAt() time.Time
	stop()
	// isLive reports whether this presence is still the live incarnation — read
	// LOCK-FREE off a per-incarnation atomic (set true at go-live, false on
	// death/stop/eviction). It is the WHEN-validity probe a liveCap (livePen)
	// consults per write to fence a dangling capability held by a goroutine that
	// outlived its incarnation. It must NOT take r.mu (churn would serialise every
	// emit behind the hot addressing lock).
	isLive() bool
	// markDead flips the per-incarnation live atomic to false. Called from the
	// death path / stop / pointer-identity eviction. Idempotent.
	markDead()
}

// ErrNotHosted is returned when no presence is hosted for the addressed id.
var ErrNotHosted = errors.New("actorrt: actor not hosted")

// Incarnation is the opaque handle to ONE live embodiment of an ActorID — a
// (id, presence-pointer) pair. Identity is single-level (one stable ActorID),
// but a capability welded to a specific incarnation can outlive it (a goroutine
// captured a pen, the cell died, a same-id successor took over): the handle
// names WHICH embodiment, so the WHEN-validity gate (IsLive) is by POINTER, not
// by id — defeating ABA. It is comparable (id + pointer) and is NEVER serialised
// into an envelope/truth; it lives only in the volatile liveness plane.
type Incarnation struct {
	id actor.ActorID
	p  presence
}

// ID is the read-only accessor for the incarnation's ActorID. The presence
// pointer stays opaque (no accessor) — only the host compares it. The platform
// link layer needs ID() to Mint a pen welded to (id, chID) when wrapping a
// livePen for an out-of-process incarnation.
func (i Incarnation) ID() actor.ActorID { return i.id }

// PresenceWatcher is the consumer end of the obs presence-PUSH channel. The
// runtime invokes OnDown exactly once for each hosted unit that dies abnormally
// — death is the DELETED edge of presence (obs), NOT a control signal and NOT
// truth. The watcher's reaction (e.g. materialise receiver_unavailable) is work
// and lands in truth on its own. The dead unit has ALREADY self-evicted from the
// addressing map before OnDown runs, so a watcher MUST NOT despawn it.
//
// Reliability (closure-critical path): register watchers BEFORE Spawn/Attach so
// no death edge is missed; OnDown is invoked synchronously in the reap path and
// is not droppable.
type PresenceWatcher interface {
	OnDown(ctx context.Context, id actor.ActorID, cause error)
}

// Runtime owns the live presences for one channel and is the addressing seam.
// Identity is single-level: a stable ActorID names at most one live presence at
// a time. Spawn/Attach replaces; death is terminal (no transparent same-id
// respawn). Death and replacement are checked by POINTER IDENTITY (the map
// entry still points to this very presence), so a dying predecessor can never
// evict its successor — no per-instance generation is needed.
type Runtime struct {
	parent context.Context
	clock  func() time.Time
	logger *slog.Logger

	mu        sync.RWMutex
	presences map[actor.ActorID]presence
	watchers  []PresenceWatcher
	obsWatch  map[actor.ActorID][]ObsWatcher
	mailbox   int
}

// Config configures a Runtime.
type Config struct {
	// Parent is the context all presences derive from; cancelling it tears the
	// whole channel down.
	Parent context.Context
	// Mailbox is the default bounded mailbox depth (<=0 → 64).
	Mailbox int
	// Clock stamps each presence's bind instant (obs uptime source). nil →
	// time.Now. Injectable so tests can pin uptime deterministically.
	Clock func() time.Time
	// Logger surfaces presence-watch faults (a watcher panic on the
	// closure-critical death path) and other cell-lifecycle edge observability.
	// nil → discard (no-op).
	Logger *slog.Logger
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
// Despawn/Stat/StopAll/WatchPresence); the Deliverer is the confined WORK enqueue
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
	r := &Runtime{
		parent:    parent,
		clock:     clock,
		logger:    logger,
		presences: make(map[actor.ActorID]presence),
		obsWatch:  make(map[actor.ActorID][]ObsWatcher),
		mailbox:   mb,
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

// WatchPresence registers a watcher for the obs presence-PUSH channel (currently:
// death = the DELETED edge). Register BEFORE Spawn/Attach so no edge is missed.
func (r *Runtime) WatchPresence(w PresenceWatcher) {
	if w == nil {
		return
	}
	r.mu.Lock()
	r.watchers = append(r.watchers, w)
	r.mu.Unlock()
}

// publishDown fans the presence DELETED edge out to every watcher. Invoked once
// per abnormal death, synchronously in the dying goroutine's reap path (the
// edge is not droppable — closure depends on it). Each watcher is guarded: a
// watcher panic must not escape and crash the process through the death path —
// AND it is logged, because a swallowed fault on the closure-critical path is a
// silent black hole (the caller stays unclosed). Watchers MUST be non-blocking;
// a blocking watcher stalls the dying goroutine's reap.
func (r *Runtime) publishDown(id actor.ActorID, cause error) {
	r.mu.RLock()
	ws := make([]PresenceWatcher, len(r.watchers))
	copy(ws, r.watchers)
	r.mu.RUnlock()
	for _, w := range ws {
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					r.logger.Error("actorrt.presence.watcher_panic",
						"actor", id, "cause", cause, "panic", rec)
				}
			}()
			w.OnDown(context.Background(), id, cause)
		}()
	}
}

// WatchObs registers a watcher for an actor's PUSH obs (snapshots the actor
// publishes via PublishObs). No-op fanout until the actor publishes. Part of the
// obs push/actor skeleton — registered consumers are domain (e.g. a monitor).
func (r *Runtime) WatchObs(id actor.ActorID, w ObsWatcher) {
	if w == nil {
		return
	}
	r.mu.Lock()
	r.obsWatch[id] = append(r.obsWatch[id], w)
	r.mu.Unlock()
}

// publishObs fans an actor-published obs snapshot to that actor's watchers
// (obs push/actor). Invoked on the publishing presence's goroutine; guarded so
// a watcher panic cannot escape. No watcher → no-op.
//
// POINTER-IDENTITY gated (the ABA guard, same discipline as removeIf/Despawn):
// the fanout fires IFF the addressing map still points to this very `self`
// incarnation. A stale predecessor goroutine that outlived its incarnation (it
// was replaced, or is mid-teardown) cannot publish obs that watchers would
// mis-attribute to the live same-id successor — the snapshot is dropped. The
// check shares the one RLock that snapshots the watcher slice, so it is
// consistent with the fanout it gates.
func (r *Runtime) publishObs(id actor.ActorID, self presence, kind ObsKind, val ObsValue) {
	r.mu.RLock()
	cur, ok := r.presences[id]
	if !ok || cur != self {
		r.mu.RUnlock()
		return
	}
	ws := append([]ObsWatcher(nil), r.obsWatch[id]...)
	r.mu.RUnlock()
	for _, w := range ws {
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					r.logger.Error("actorrt.obs.watcher_panic", "actor", id, "kind", kind, "panic", rec)
				}
			}()
			w.OnObs(context.Background(), id, kind, val)
		}()
	}
}

// Observe is the obs PULL resolver for ACTOR-source obs: it routes the opaque
// kind to the hosted unit, which self-answers from its own operational state or
// reports ErrObsUnsupported. Substrate-source facts (presence/uptime) do NOT
// come here — they ride the typed Stat bundle. Read-only, non-truth, out-of-band
// (never enqueued into the mailbox); the actor's Observer impl answers
// concurrently and must be non-perturbing.
func (r *Runtime) Observe(ctx context.Context, id actor.ActorID, kind ObsKind) (ObsValue, error) {
	r.mu.RLock()
	p, ok := r.presences[id]
	r.mu.RUnlock()
	if !ok {
		return nil, ErrNotHosted
	}
	return p.observe(ctx, kind)
}

// CancelRequest fires the request-scope of cancel(scope) for one in-flight
// request on a hosted presence: the reqCtx the addressed actor is currently
// running that request under is cancelled, off the work goroutine. A cell fires
// its in-flight CancelFunc directly; a port writes a KindCancel frame so the
// remote host cancels its own cell's reqCtx (in-proc and cross-wire are the same
// primitive, one scope down from Despawn). No-op if no presence is hosted for id
// or the request already closed — cancel is a best-effort hint; the caller's
// closure owns the terminal, so a lost cancel only costs the receiver a little
// wasted work (the ExpiresAt deadline still collapses it).
func (r *Runtime) CancelRequest(id actor.ActorID, requestID message.ID) {
	r.mu.RLock()
	p, ok := r.presences[id]
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
	// NotHosted: this runtime hosts no presence for the actor (legitimately —
	// the audience may include system/user/remote-elsewhere actors). The seam
	// can fast-fail receiver_unavailable instead of waiting for a timeout.
	NotHosted
	// MailboxFull: a presence is hosted but its bounded mailbox is full.
	MailboxFull
	// Stopped: a presence is hosted but tearing down.
	Stopped
)

// DeliverResult is the structured, per-audience truth of a Deliver call: the
// Outcome for each addressed actor.
type DeliverResult struct {
	Per map[actor.ActorID]Outcome
}

// Spawn creates and starts an IN-PROCESS cell for id, returning its Incarnation
// handle. If a presence already exists for id it is stopped and replaced (one
// actor, one owner).
//
// Two-phase construction (resolves the cap/cell chicken-and-egg):
//
//  1. allocShell — allocate the cell shell (impl=nil, live=false). Its pointer
//     IS inc.p (stable for the incarnation's life).
//  2. build(inc) — run the platform build closure OUTSIDE the lock to make the
//     impl. The closure may weld a livePen{pen, inc, host}; because the shell is
//     not yet in the addressing map, IsLive(inc)==false, so any write attempted
//     DURING construction is structurally rejected (the "factory must not write"
//     rule is enforced, not a soft convention).
//  3. go-live — atomically register the shell and flip its live atomic true.
//     Concurrent Spawn of the same id is LAST-GO-LIVE-WINS: the prior map entry
//     is stopped (pointer-identity discipline), and the loser's shell — though it
//     briefly set live=true — is stopped and marked dead, so the slack is bounded
//     by the already-accepted in-flight window.
func (r *Runtime) Spawn(id actor.ActorID, build func(Incarnation) Actor) Incarnation {
	c := allocShell(r.parent, id, r.mailbox, r.publishDown, r.publishObs, r.removeIf, r.clock(), r.logger)
	inc := Incarnation{id: id, p: c}
	c.impl = build(inc) // OUTSIDE the lock; IsLive(inc)==false during build.

	r.mu.Lock()
	old := r.presences[id]
	r.presences[id] = c
	c.live.Store(true) // go-live: register + liveness atomic flip are one critical section.
	r.mu.Unlock()
	// stop()/start() run OUTSIDE the lock: stop() joins the old goroutine,
	// which may itself call back into the runtime.
	if old != nil {
		old.stop()
	}
	c.start()
	return inc
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
// it as a `port` presence. The substrate does not spawn the remote (it connects
// in); Attach performs the handshake, resolves
// the connection's credential to an ActorID via resolve, relays the remote's
// emits through emit, and returns the bound id. If a presence already exists for
// the resolved id it is stopped and replaced.
//
// hsCtx bounds ONLY the connect-in handshake (a substrate-owned protocol step
// whose time bound is a substrate invariant — a peer that connects but never
// sends a handshake must not pin this goroutine forever). It does NOT scope the
// port's LIFETIME: the bound port lives for the runtime's (r.parent), not for
// this call — a per-call lifetime ctx would wrongly tear the port down when
// Attach returns. Pass a deadline ctx to guard the handshake; a nil/background
// ctx degrades to an unbounded handshake read.
func (r *Runtime) Attach(hsCtx context.Context, conn io.ReadWriteCloser, emit EmitSink, resolve ResolveFunc) (actor.ActorID, error) {
	p, err := newPort(r.parent, hsCtx, conn, emit, resolve, r.publishDown, r.publishObs, r.removeIf, r.clock(), r.logger)
	if err != nil {
		return "", err
	}
	r.mu.Lock()
	old, existed := r.presences[p.id]
	r.presences[p.id] = p
	p.live.Store(true) // go-live (port path; the home-side incarnation gate is §3.6, deferred)
	r.mu.Unlock()
	if existed {
		old.stop()
	}
	p.start()
	return p.id, nil
}

// removeIf is the pointer-identity-checked self-eviction hook handed to each
// presence. It deletes the map entry IFF it still points to this very instance —
// so a late-dying predecessor (already replaced) cannot evict its successor.
func (r *Runtime) removeIf(id actor.ActorID, self presence) {
	r.mu.Lock()
	if cur, ok := r.presences[id]; ok && cur == self {
		delete(r.presences, id)
	}
	r.mu.Unlock()
	// This presence is dying — flip its liveness atomic so any capability welded
	// to it (livePen) fails the WHEN gate from here on. Idempotent.
	self.markDead()
}

// Despawn stops and removes the incarnation inc (no-op unless the map still
// points to this very incarnation). This is the external deregister path. It
// carries NO caller obligation to collapse
// in-flight requests first: after despawn the id is absent from presence, and
// the closure reconciler (a level scan over open-request × receiver-absent)
// closes every orphan in-flight request with receiver_unavailable. The death
// edge would only fire on abnormal exit; closure is geometry (the level scan),
// not a despawn-caller convention.
func (r *Runtime) Despawn(inc Incarnation) {
	r.mu.Lock()
	cur, ok := r.presences[inc.id]
	matched := ok && cur == inc.p
	if matched {
		delete(r.presences, inc.id)
	}
	r.mu.Unlock()
	// Guarded by POINTER IDENTITY: only despawn IFF the map still points to this
	// very incarnation, so despawning a stale handle (a replaced predecessor, or
	// an id never hosted) is a safe no-op and can never evict a same-id successor.
	if matched {
		inc.p.markDead()
		inc.p.stop()
	}
}

// deliver routes env to every audience presence hosted by this Runtime by
// enqueueing into each mailbox, returning the per-audience Outcome. An audience
// member with no local presence is reported NotHosted (not silently skipped) —
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
	matched := make(map[actor.ActorID]presence, len(audience))
	for _, id := range audience {
		if p, ok := r.presences[id]; ok {
			matched[id] = p
		} else {
			res.Per[id] = NotHosted
		}
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

// StopAll stops every presence. Used at channel teardown.
func (r *Runtime) StopAll() {
	r.mu.Lock()
	ps := make([]presence, 0, len(r.presences))
	for _, p := range r.presences {
		ps = append(ps, p)
	}
	r.presences = make(map[actor.ActorID]presence)
	r.mu.Unlock()
	for _, p := range ps {
		p.stop()
	}
}

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
}

// Stat reads the substrate-owned obs facts for id. The bool reports presence
// (is a live instance hosted here right now) — the `kill -0` / is_process_alive
// authority: only the substrate can answer it, so it never asks the actor and
// never blocks. It replaces the old boolean Has: present = the second return,
// uptime = now - StartedAt.
func (r *Runtime) Stat(id actor.ActorID) (UnitStat, bool) {
	r.mu.RLock()
	p, ok := r.presences[id]
	r.mu.RUnlock()
	if !ok {
		return UnitStat{}, false
	}
	return UnitStat{StartedAt: p.startedAt()}, true
}
