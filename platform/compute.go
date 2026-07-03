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
type cellObsForwarder struct {
	d  *link.Dialer
	ch chan obsMsg
}

func newCellObsForwarder(d *link.Dialer) *cellObsForwarder {
	return &cellObsForwarder{d: d, ch: make(chan obsMsg, obsForwardQueue)}
}

// OnObs implements actorrt.ObsWatcher — non-blocking enqueue (drop on full).
func (f *cellObsForwarder) OnObs(_ context.Context, id actor.ActorID, kind actorrt.ObsKind, val actorrt.ObsValue) {
	m := obsMsg{id: id, kind: string(kind), val: append([]byte(nil), val...)}
	select {
	case f.ch <- m:
	default: // queue full: drop (obs is best-effort; next edge / lease decay covers it)
	}
}

// pump drains the queue onto the link OFF the cell goroutine. Exits when ctx is
// cancelled or the link tears down.
func (f *cellObsForwarder) pump(ctx context.Context, linkDone <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-linkDone:
			return
		case m := <-f.ch:
			f.d.SendObs(m.id, m.kind, m.val)
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

// RunCompute connects to the channel home and runs the daemon's own reconcile
// ring against cfg.Desired: it hosts every AlwaysOn desired member as a cell,
// declaring it to the home (Reattach) before opening its stream. The home
// dispatches envelopes down each actor's link stream into the cell's mailbox;
// the cell's emits flow UP that same stream as native ipc (blocking on the
// home's authoritative EmitAck). No local truth.
//
// RunCompute blocks until ctx is cancelled or the link disconnects.
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
	// is built HERE, once — never inside a reconnect attempt (F14) — because a
	// hosted cell's identity and in-memory state must outlive a single wire
	// session (§10.13 推导3: wire-session death ≠ hosted-work death). The
	// reconnect loop itself is a later slice; this shape is the structural
	// precondition it needs.
	rt, del := actorrt.New(actorrt.Config{})
	defer rt.StopAll()
	watcher := &cellDownWatcher{down: map[actor.ActorID]func(cause string){}}
	rt.WatchDown(watcher)

	// Dial declares NOTHING yet: every actor this compute hosts is declared by the
	// ring's own Reattach below (the full-set declaration idiom, §S-P8), including
	// the very first batch — the initial reconcile pass runs before the poll
	// ticker starts, so there is no dead startup window.
	d, err := link.Dial(ctx, cfg.ServerWS, computeID, nil, logger)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	// Host→remote despawn hook: a KindDespawn frame from the home ends that actor's
	// arm here (§10.5) — despawn the local cell and the stream loop replies KindDetach.
	d.SetDespawnLocal(func(id actor.ActorID) { rt.DespawnID(id) })
	obsFwd := newCellObsForwarder(d)
	go obsFwd.pump(ctx, d.Done())

	ring := &computeRing{
		rt:          rt,
		del:         del,
		d:           d,
		watcher:     watcher,
		obsFwd:      obsFwd,
		builder:     cfg.Builder,
		logger:      logger,
		prevCurrent: map[actor.ActorID]actor.Kind{},
		infeasible:  map[actor.ActorID]error{},
	}
	ring.reconcile(ctx, cfg.Desired)

	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// Graceful shutdown: detach each actor stream (KindDetach) so the home ports
			// die QUIET (no receiver_unavailable) instead of on a raw EOF down edge.
			d.DetachAll()
			return nil
		case <-d.Done():
			return nil
		case <-t.C:
			ring.reconcile(ctx, cfg.Desired)
		}
	}
}

// computeRing is the daemon's reconcile ring: it diffs cfg.Desired against the
// locally-live cells and drives the actor lifecycle (open/spawn/start, despawn/
// detach) to close the gap. It is the daemon-hosted counterpart of Home's
// reconcileActivation — same paradigm (§10.13 推导2), different host.
type computeRing struct {
	rt      *actorrt.Runtime
	del     actorrt.Deliverer
	d       *link.Dialer
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
}

// reconcile runs one pass of the ring: read desired, 削 what fell out of it
// (despawn + detach), then 补 what is desired-and-not-live (Reattach the full
// declared set, then per-id OpenStream → Spawn → StartStream). A desired-read
// failure leaves the prior state untouched and retries next tick.
func (r *computeRing) reconcile(ctx context.Context, desired actorrt.DesiredSource) {
	members, err := desired.Members(ctx)
	if err != nil {
		r.logger.Error("platform.compute.desired_failed", "err", err)
		return
	}

	current := make(map[actor.ActorID]actor.Kind, len(members))
	for _, m := range members {
		if m.Lifecycle != actorrt.LifecycleAlwaysOn {
			continue // lazy has no delivery-seam analogue on a daemon this period.
		}
		current[m.ID] = m.Kind
	}

	// 削: previously-declared ids no longer desired. Local despawn ends the cell's
	// execution arm; DetachStream tells the home this arm is gone QUIET (§10.5/S1).
	for id := range r.prevCurrent {
		if _, ok := current[id]; ok {
			continue
		}
		r.rt.DespawnID(id)
		r.d.DetachStream(id)
		delete(r.infeasible, id)
		r.logger.Info("platform.compute.deactivated", "actor", string(id))
	}

	// 补: desired-and-not-live.
	live := make(map[actor.ActorID]bool)
	for _, id := range r.rt.LiveIDs() {
		live[id] = true
	}
	var missing []actor.ActorID
	for id := range current {
		if !live[id] {
			missing = append(missing, id)
		}
	}

	if len(missing) > 0 {
		// Reattach the FULL current declared set (kubelet node-status idiom, never
		// an increment — §S-P8) so the home's allowed set covers every id about to
		// OpenStream, then build each missing actor.
		decls := make([]link.Declaration, 0, len(current))
		for id, kind := range current {
			decls = append(decls, link.Declaration{ActorID: id, Kind: kind, Binding: actor.BindingRuntimeInboundViaRelay})
		}
		if err := r.d.Reattach(ctx, decls); err != nil {
			r.logger.Warn("platform.compute.reattach_failed", "err", err, "actors", len(decls))
			// Every missing id stays not-live, so next tick's diff retries them
			// (the ids remain in `current`, untouched below); record the reason.
			for _, id := range missing {
				r.infeasible[id] = err
			}
		} else {
			for _, id := range missing {
				r.buildOne(id)
			}
		}
	}

	r.prevCurrent = current
}

// buildOne opens the actor's link stream, resolves its factory via the builder,
// and spawns + starts it. A failure at any step is recorded in r.infeasible and
// logged; the id stays out of the live set, so next tick's diff retries it.
func (r *computeRing) buildOne(id actor.ActorID) {
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
	arms, err := r.d.OpenStream(id, r.dispatchFor(id), r.cancelFor(id))
	if err != nil {
		r.infeasible[id] = err
		r.logger.Warn("platform.compute.open_stream_failed", "actor", string(id), "err", err)
		return
	}
	// Daemon-side caps: Pen + Access/State + Schedule are all wired as relay
	// proxies over the actor's port stream, so a daemon-hosted cell reaches for
	// them exactly as a local cell does (transport neutrality — the home welds
	// identity and runs the death gate over the home's port incarnation). Only
	// Spawn stays nil: the fork/despawn arm does not cross the wire this period.
	impl := factory(actorcaps.Caps{
		Pen:      arms.Pen,
		Access:   arms.Access,
		State:    arms.State,
		Schedule: arms.Schedule,
	})
	r.watcher.mu.Lock()
	r.watcher.down[id] = arms.Down
	r.watcher.mu.Unlock()
	// Register the obs forwarder BEFORE Spawn so no early obs edge is missed
	// (same discipline as WatchDown).
	r.rt.WatchObs(id, r.obsFwd)
	// The daemon cell's pen is the link's relay-only RemoteWriter; it is NOT
	// wrapped in a livePen HERE — the death gate for this actor's writes is
	// enforced HOME-SIDE (the Acceptor's emitSink wraps a livePen per emit,
	// welded to the home's port incarnation). What stays deferred is only the
	// DAEMON-side gate on this local relay (federation trigger): a daemon is its
	// owner's own host, so a local stale-relay write still crosses the home gate
	// before reaching truth. The build closure returns the prebuilt impl.
	r.rt.Spawn(id, func(actorrt.Incarnation) actorrt.Actor { return impl })
	r.d.StartStream(id)
	delete(r.infeasible, id)
}

// dispatchFor builds one actor's inbound dispatch closure: deliver into its
// mailbox and, for any non-Delivered outcome, report it UP the wire as a pure
// observation (KindDeliverResult) — the home logs it exactly as its own delivery
// tap does. Delivered = silence. The daemon holds no truth, so this is
// observation only; closure is materialised home-side from the log.
func (r *computeRing) dispatchFor(id actor.ActorID) func(env *message.Envelope) error {
	return func(env *message.Envelope) error {
		res, derr := r.del.Deliver([]actor.ActorID{id}, env)
		if derr != nil {
			return derr
		}
		if outcome, ok := res.Per[id]; ok && outcome != actorrt.Delivered {
			r.d.SendDeliverResult(id, env.ID, outcomeString(outcome), "")
		}
		return nil
	}
}

// cancelFor builds one actor's cancel hook: fire the named request's reqCtx on
// this daemon's runtime.
func (r *computeRing) cancelFor(id actor.ActorID) func(requestID message.ID) {
	return func(requestID message.ID) { r.rt.CancelRequest(id, requestID) }
}
