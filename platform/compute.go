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
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
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
func (w *cellDownWatcher) OnDown(_ context.Context, id actor.ActorID, cause error) {
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
	d  atomic.Pointer[link.Dialer]
	ch chan obsMsg
}

func newCellObsForwarder() *cellObsForwarder {
	return &cellObsForwarder{ch: make(chan obsMsg, obsForwardQueue)}
}

// Rebind swaps in the currently-connected Dialer — call once per (re)connect,
// mirroring RebindableArms.Rebind for the four capability arms.
func (f *cellObsForwarder) Rebind(d *link.Dialer) { f.d.Store(d) }

// OnObs implements actorrt.ObsWatcher — non-blocking enqueue (drop on full).
func (f *cellObsForwarder) OnObs(_ context.Context, id actor.ActorID, kind actorrt.ObsKind, val actorrt.ObsValue) {
	m := obsMsg{id: id, kind: string(kind), val: append([]byte(nil), val...)}
	select {
	case f.ch <- m:
	default: // queue full: drop (obs is best-effort; next edge / lease decay covers it)
	}
}

// pump drains the queue onto the CURRENT link OFF the cell goroutine, for the
// life of the daemon (ctx) — it survives any number of individual link deaths
// and reconnects, reading whatever Dialer Rebind last stored.
func (f *cellObsForwarder) pump(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-f.ch:
			if d := f.d.Load(); d != nil {
				d.SendObs(m.id, m.kind, m.val)
			}
		}
	}
}

// ActorDecl declares one actor the daemon will host. Factory constructs the
// actorrt.Actor given its Caps bundle — the same unified factory signature both
// admission paths share (Home.Spawn cell-side and RunCompute daemon-side). On the
// daemon the Pen + plane-2 (Access/State) + time-axis (Schedule) caps are all
// wired as relay-only proxies over the actor's port stream; only Spawn stays nil
// (the fork/despawn arm does not cross the wire this period). The proxies only
// exist after the actor's stream opens, so the actor cannot be pre-built: every
// cell that can emit needs its pen at construction, and in the actor model every
// actor can emit. There is no pen-less construction path. The proxies relay upward
// without injecting identity; the home side welds the actor's authenticated bound
// id (Mint on the pen, the access door minter, the schedule engine minter).
//
// The type is retained as the registry.Constructor return shape (registry/actors
// still hand back a caller-held id+kind+factory triple); RunCompute itself no
// longer takes a []ActorDecl argument — see ComputeConfig.
type ActorDecl struct {
	ID      actor.ActorID
	Kind    actor.Kind
	Binding actor.Binding
	Factory func(actorcaps.Caps) actorrt.Actor
}

// ComputeBuilder resolves a desired member's id to its caps-taking factory — the
// daemon-side counterpart of the reconcile ring's activation resolve (mirrors
// CapsFactoryBuilder.Lookup, but scoped to compute: a daemon never forks, so it
// carries no LookupByClass entry). Kind is never re-answered here — it is
// caller-held on the DesiredMember the reconcile loop already read.
type ComputeBuilder interface {
	Lookup(id actor.ActorID) (factory func(actorcaps.Caps) actorrt.Actor, ok bool)
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
func RunCompute(ctx context.Context, cfg ComputeConfig) error {
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
	defer rt.StopAll()
	watcher := &cellDownWatcher{down: map[actor.ActorID]func(cause string){}}
	rt.WatchDown(watcher)
	// The obs arm outlives every individual link the same way rt does — built
	// once, Rebind'd per connection, pumped for the daemon's whole life.
	obsFwd := newCellObsForwarder()
	go obsFwd.pump(ctx)

	ring := &computeRing{
		rt:          rt,
		del:         del,
		watcher:     watcher,
		obsFwd:      obsFwd,
		builder:     cfg.Builder,
		logger:      logger,
		prevCurrent: map[actor.ActorID]actor.Kind{},
		infeasible:  map[actor.ActorID]error{},
		arms:        map[actor.ActorID]*link.RebindableArms{},
	}

	backoff := redialInitialBackoff
	for {
		// Dial declares NOTHING yet: every actor this compute hosts is declared by
		// the ring's own Reattach (the full-set declaration idiom, §S-P8) inside
		// the first reconcile pass below.
		d, err := link.Dial(ctx, cfg.ServerWS, computeID, nil, logger)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Warn("platform.compute.dial_failed", "err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			backoff = nextRedialBackoff(backoff)
			continue
		}
		backoff = redialInitialBackoff

		// Host→remote despawn hook: a KindDespawn frame from the home ends that
		// actor's arm here (§10.5) — despawn the local cell, the stream loop
		// replies KindDetach. Re-installed on every connection (it closes over
		// THIS d's stream read loops).
		d.SetDespawnLocal(func(id actor.ActorID) { rt.DespawnID(id) })
		obsFwd.Rebind(d)

		graceful := ring.runLink(ctx, d, cfg.Desired, poll)
		_ = d.Close() // idempotent: a no-op if the link already tore itself down.
		if graceful {
			return nil
		}

		logger.Warn("platform.compute.link_down", "retry_in", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = nextRedialBackoff(backoff)
	}
}

// computeRing is the daemon's reconcile ring: it diffs cfg.Desired against the
// locally-live cells and drives the actor lifecycle (open/spawn/start, despawn/
// detach) to close the gap. It is the daemon-hosted counterpart of Home's
// reconcileActivation — same paradigm (§10.13 推导2), different host.
type computeRing struct {
	rt      *actorrt.Runtime
	del     actorrt.Deliverer
	watcher *cellDownWatcher
	obsFwd  *cellObsForwarder
	builder ComputeBuilder
	logger  *slog.Logger

	// prevCurrent is the AlwaysOn desired set the LAST reconcile pass declared —
	// mirrors Home's prevEagerDesired: the 削 diff is prevCurrent − current, never
	// live − current (LiveIDs has no other embodiment category on a daemon this
	// period, but the same never-actual-diff discipline holds so a future daemon-
	// local category never gets silently evicted). Touched only from the single
	// caller goroutine (RunCompute's own loop), so it needs no lock.
	prevCurrent map[actor.ActorID]actor.Kind
	// infeasible records the last build/declare/open failure per id — surfaced via
	// structured logging each pass; the diff itself retries every tick (a failed id
	// stays "not live", so it is always back in next tick's missing set).
	infeasible map[actor.ActorID]error
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
		delete(r.infeasible, id)
		delete(r.arms, id)
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
			// them (the ids remain in `current`, untouched below); record the reason.
			for _, id := range missing {
				r.infeasible[id] = err
			}
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
// any step is recorded in r.infeasible and logged; the id stays out of the live
// set, so next tick's diff retries it.
func (r *computeRing) buildOne(id actor.ActorID, kind actor.Kind, d *link.Dialer) {
	factory, ok := r.builder.Lookup(id)
	if !ok {
		err := fmt.Errorf("no factory for %q", id)
		r.infeasible[id] = err
		r.logger.Warn("platform.compute.no_builder", "actor", string(id))
		return
	}
	// Open the actor's link stream first: the RemoteWriter (the cell's pen) must
	// exist before the cell is spawned. One stream == one actor, so the dispatch
	// handler routes every envelope on this stream into THIS actor's mailbox (the
	// stream IS the target — no audience demux on the daemon).
	arms, err := d.OpenStream(id, r.dispatchFor(id, d), r.cancelFor(id))
	if err != nil {
		r.infeasible[id] = err
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
	// Register the obs forwarder BEFORE Spawn so no early obs edge is missed
	// (same discipline as WatchDown).
	r.rt.WatchObs(id, r.obsFwd)
	// Two-phase Spawn, port-path mirror of Home.Spawn (§10.13 推导7①/G12): the
	// build closure runs inside Spawn, BEFORE go-live, so link.NewLiveArms welds
	// the cell's caps to THIS incarnation and fences every call until it goes
	// live — a factory that writes during construction is refused here exactly
	// like a cell born at home, closing the daemon-side parity gap the raw
	// (ungated) facades used to leave open.
	r.rt.Spawn(id, kind, func(inc actorrt.Incarnation) actorrt.Actor {
		return factory(link.NewLiveArms(rb, inc, r.rt))
	})
	d.StartStream(id)
	delete(r.infeasible, id)
}

// reopenOne re-establishes an ALREADY-LIVE actor's link stream on d — a fresh
// reconnect (every stream on the new Dialer starts nonexistent) or a single
// stream that died while the link stayed up (F6). It OpenStreams again and
// Rebinds the actor's membrane to the fresh arms, but never re-Spawns: the
// cell (identity + in-memory state) outlives the wire session, only the wire
// arm is replaced. A failure here is recorded/retried exactly like buildOne's
// — the cell keeps running throughout, its wire arm just stays in the
// disconnect-window (fail-closed) state a moment longer.
func (r *computeRing) reopenOne(id actor.ActorID, d *link.Dialer) {
	rb := r.arms[id]
	if rb == nil {
		// Cannot happen in practice (live ⟹ buildOne already ran and populated
		// r.arms), but fail closed rather than lose the membrane silently.
		err := fmt.Errorf("no rebindable arms for live actor %q", id)
		r.infeasible[id] = err
		r.logger.Error("platform.compute.reopen_missing_arms", "actor", string(id))
		return
	}
	arms, err := d.OpenStream(id, r.dispatchFor(id, d), r.cancelFor(id))
	if err != nil {
		r.infeasible[id] = err
		r.logger.Warn("platform.compute.reopen_stream_failed", "actor", string(id), "err", err)
		return
	}
	rb.Rebind(arms)
	r.watcher.mu.Lock()
	r.watcher.down[id] = arms.Down
	r.watcher.mu.Unlock()
	d.StartStream(id)
	delete(r.infeasible, id)
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
