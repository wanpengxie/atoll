package platform

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/lib/channelkit"
	"github.com/wanpengxie/atoll/platform/internal/devicepresence"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/platform/internal/tap"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// HomeConfig configures the channel-home assembly.
type HomeConfig struct {
	ChannelID channelpkg.ID
	DBPath    string
	Logger    *slog.Logger
	// ReconcileInterval tunes the closure reconciler's level safety-net sweep
	// period (the backstop for lost death edges). <=0 → the default. The death
	// edge closes the common case immediately; this sweep is a rare backstop.
	ReconcileInterval time.Duration
}

// Home is the assembled channel-home. Its public surface is the capability set in
// the package doc — the bare Runtime/Deliverer/Membership/Registry never escape
// it; assembly only hands out capabilities. The app layer owns HTTP/transport;
// Home is pure Go.
type Home struct {
	channelID  channelpkg.ID
	minter     harness.Minter
	channel    *channelkit.Channel
	cs         *runtime.ChannelStores
	signal     *tap.Signal
	delivery   *tap.Pump
	links      *link.Acceptor
	deviceFold *devicepresence.Fold
	logger     *slog.Logger
	nowMs      func() int64

	// placement decides which host a new activity (fork/activation) starts on.
	// Single fixed-home identity this period (SinglePlacement); shaped now so
	// fork/activation route through Place() and multi-home swaps additively.
	placement actorrt.Placement
	// builder is the platform-layer class → caps-factory table (the fork
	// injection-point contract, CapsFactoryBuilder). nil until the domain's
	// factory table is injected — a nil builder makes SpawnHandle.Fork fail-fast
	// with ErrNoBuilder rather than fabricate a child. The table hands back the RAW
	// factory (func(Caps) Actor); the caps weld happens at the platform assembler
	// (buildCaps) when a fork child is born, so a child gets the identical membrane
	// set as a top-level admission (purity: the domain fills WHAT to build, the
	// platform seam owns HOW caps are welded — actorrt never touches harness/link).
	builder CapsFactoryBuilder

	// reconcileStop tears down the closure reconciler ticker (level backstop).
	reconcileStop context.CancelFunc
	reconcileDone chan struct{}
}

// reconcileInterval is the closure reconciler's low-frequency safety-net sweep
// period. The death edge closes the common case immediately; this level sweep
// catches lost edges (clean despawn / ctx-cancel / open requests predating a
// restart). It is a backstop, not the primary path, so it runs rarely.
const reconcileInterval = 30 * time.Second

// Open assembles the channel home. Assembly is linearised by the tap seam (no
// construction cycle, no back-fill):
//
//	signal -> stores(OnCommit=signal.Notify) -> harness -> channelkit(spawns
//	sysactor against the live runtime) -> delivery tap -> link acceptor.
func Open(cfg HomeConfig) (*Home, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if cfg.ChannelID == "" {
		return nil, fmt.Errorf("platform: ChannelID required")
	}
	ctx := context.Background()
	nowMs := func() int64 { return time.Now().UnixMilli() }

	// 1. Build the commit Signal (tap fan-out). It has NO dependencies, so it is
	//    built first and handed to the store as its post-commit source. The
	//    commit signal belongs to the log append chokepoint (Postgres WAL / Kafka
	//    offset), not to any one writer — so BOTH write paths (request-path Append
	//    and the control-plane membership mirror) fire it through the store,
	//    instead of only the harness path being wrapped.
	signal := tap.NewSignal()

	// 2. Open channel stores (substrate), wiring the commit signal as the store's
	//    OnCommit. Now any durable append — request or control-plane — wakes the
	//    tap identically.
	cs, err := runtime.OpenChannel(ctx, cfg.ChannelID, cfg.DBPath, runtime.OpenChannelOptions{
		OnCommit: signal.Notify,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: open channel store: %w", err)
	}

	// 3. Build the harness Minter (the substrate mint machine). New returns a Minter, never a
	//    bare chain — the bare writer's visibility is compile-time capped inside the
	//    harness package. Every admission point (Spawn / attach / system closure)
	//    Mints a Pen welded to (actorID, chID); the welded identity is unforgeable
	//    by the holder. The post-commit Notify lives at the store append chokepoint,
	//    so there is no write-gate wrapper layer.
	minter, err := harness.New(harness.Deps{
		ChannelID: cfg.ChannelID,
		Log:       cs.Log,
		Logger:    logger,
	})
	if err != nil {
		_ = cs.Close()
		return nil, fmt.Errorf("platform: build harness: %w", err)
	}
	// The system Pen: every system-authored write (sysactor serve responses,
	// channelkit's author#3 closure terminals) commits through this welded pen, so
	// sender==SystemActorID rides each write by construction — no caller stamping
	// at the call sites.
	systemPen := minter.Mint(actor.SystemActorID, actor.KindSystem, cfg.ChannelID)

	// 5. Bootstrap: register the intrinsic system actor so its substrate-death
	//    terminals pass harness sender validation. Idempotent SEED: on a home
	//    restart over a persistent channel DB the row already exists, and a raw
	//    re-Insert would PK-conflict (actor_id is the table key) — failing Open
	//    before the restart-recovery reconciler below can even run. Insert itself
	//    stays strict (a duplicate is an error, locked by the store's
	//    coverage test); the idempotent seed lives here at the genesis call site
	//    (guard the idempotency at the platform bootstrap call site — do not relax substrate).
	if exists, err := cs.Registry.Exists(ctx, actor.SystemActorID); err != nil {
		_ = cs.Close()
		return nil, fmt.Errorf("platform: check system actor: %w", err)
	} else if !exists {
		if err := cs.Membership.Insert(ctx, storespec.Record{
			ID: actor.SystemActorID, Kind: actor.KindSystem, CreatedAt: nowMs(),
		}); err != nil {
			_ = cs.Close()
			return nil, fmt.Errorf("platform: register system actor: %w", err)
		}
	}

	// 5.5 Device-presence fold (L3): folds actor-source obs PUSH edges
	//     into a volatile per-actor level (in-memory; never persisted), decays to
	//     unknown on the actor's death edge (link-down cascade). Built BEFORE
	//     channelkit so the system cell can read it (sysactor observes L1/L2/L3);
	//     registered as a down watcher + per-actor obs watcher below.
	deviceFold := devicepresence.New(logger)

	// 6. channelkit: actorrt runtime + sysactor + death-edge wiring. The system
	//    cell is built against the LIVE runtime (factory) — its liveness Stat seam
	//    reads the real runtime at construction, no back-filled pointer.
	clock := func() time.Time { return time.UnixMilli(nowMs()) }
	channel := channelkit.New(channelkit.Config{
		ChannelID: cfg.ChannelID,
		System: func(rt *actorrt.Runtime) actorrt.Actor {
			return sysactor.New(sysactor.Deps{
				Registry: cs.Registry,
				Writer:   systemPen,
				Lookup:   cs.Requests,
				Clock:    clock,
				Stat:     &runtimeLivenessAdapter{rt: rt},
				Device:   deviceFold,
			})
		},
		SystemPen:    systemPen,
		OpenRequests: cs.Query,
		Clock:        clock,
		Logger:       logger,
	})

	// 7. Build the delivery tap: a Pump over the Signal-fed Deliverer. cursor start
	//    = current MaxSeq (mailbox semantics: only new commits). DeliverResult
	//    lands here as structured per-audience logs.
	from, err := cs.Query.MaxSeq(ctx)
	if err != nil {
		channel.Cells().StopAll()
		_ = cs.Close()
		return nil, fmt.Errorf("platform: read max seq: %w", err)
	}
	deliver := deliveryHandle(channel.Deliverer(), cfg.ChannelID, logger)
	delivery := tap.NewPump(signal, cs.Query, from, deliver, logger)
	delivery.Start()

	// 7.5 Register the device-presence fold as a global down watcher (so an actor's
	//     death edge decays its L3 to unknown — the link-down cascade). Per-actor
	//     obs registration happens at attach (Acceptor below).
	channel.Cells().WatchDown(deviceFold)

	// 8. Build the link acceptor (physical layer: WS mux + per-actor ipc streams
	//    + lease judgement for attached computes). It folds each attached actor's
	//    obs PUSH into the device-presence fold (per-actor WatchObs at attach).
	links := link.NewAcceptor(link.Config{
		Minter:     minter,
		Runtime:    channel.Cells(),
		Membership: cs.Membership,
		ChannelID:  cfg.ChannelID,
		Logger:     logger,
		ObsWatcher: deviceFold,
	})

	// 9. Closure reconciler (level backstop). Run one sweep at startup — this is
	//    the home-restart recovery path (#5): an open request whose receiver is
	//    absent because its embodiment predates this process gets closed now, not
	//    held forever. Then a low-frequency ticker keeps it as a safety net for
	//    any lost death edge. The death edge (OnDown) remains the lossy fast-path.
	channel.Reconcile(ctx)
	sweepEvery := cfg.ReconcileInterval
	if sweepEvery <= 0 {
		sweepEvery = reconcileInterval
	}
	reconcileCtx, reconcileStop := context.WithCancel(context.Background())
	reconcileDone := make(chan struct{})
	go func() {
		defer close(reconcileDone)
		t := time.NewTicker(sweepEvery)
		defer t.Stop()
		for {
			select {
			case <-reconcileCtx.Done():
				return
			case <-t.C:
				channel.Reconcile(reconcileCtx)
			}
		}
	}()

	logger.Info("platform.home.ready", "channel", string(cfg.ChannelID))
	return &Home{
		channelID:     cfg.ChannelID,
		minter:        minter,
		channel:       channel,
		cs:            cs,
		signal:        signal,
		delivery:      delivery,
		links:         links,
		deviceFold:    deviceFold,
		logger:        logger,
		nowMs:         nowMs,
		placement:     SinglePlacement{},
		builder:       nil, // injected by the domain's factory-migration path; nil → Fork fail-fasts (ErrNoBuilder).
		reconcileStop: reconcileStop,
		reconcileDone: reconcileDone,
	}, nil
}

// View returns the read-only observation set (ReadAfterSeq / MaxSeq /
// ListActors / daemon attachment). It carries no write capability — observation
// only. The host (app) reads these projections OUT-OF-BAND (no message, no
// truth-log write) — UI status polling must not pollute the log; in-universe
// actors instead ask the system actor by message (that path is logged).
func (h *Home) View() View {
	return View{query: h.cs.Query, registry: h.cs.Registry, links: h.links, deviceFold: h.deviceFold}
}

// Spawn admits one actor into the channel as durable membership truth and, when
// factory is non-nil, places it as a live in-process cell (binding=embedded) with
// a Pen welded to its own (id, channelID).
//
// Identity weld at the admission membrane: the app supplies the id (domain authority —
// "this id may be admitted") and a factory; the substrate Mints the welded Pen
// and hands it to the factory, so the cell is born with a pen welded to its own id
// (the substrate's "actorID and write capability are welded inseparably" invariant). The app never sees a
// bare writer or a Minter — it only chooses WHAT to place; Home decides HOW.
//
// Order invariant (security-critical): membership apply -> build caps (Mint pen +
// access/state/spawn) -> factory(caps) -> spawn cell. Membership must be durable
// truth BEFORE the embodiment goes live — this is the birth mirror of the
// death-side "despawn before deregister" (creation/destruction
// symmetry): the sender gate no longer queries the registry (kind is welded into
// the pen at Mint), so the old "else a cell that writes on construction
// hits sender_deregistered" reason no longer holds; the invariant remains because
// membership ≠ embodiment (membership is the durable identity-level truth the
// live cell layers on top), and a construction-time plane-2 access invoke needing
// a member-grant resolves against durable membership. A live cell must never
// precede its own committed membership row — "welded at birth" must not become
// "pen first, then register".
//
// nil-guard: factory == nil = membership-ONLY (a cell-less member, e.g. a
// human user who is a member but has no cell). No Pen is Minted and no cell is
// placed — Minting/placing a cell for a cell-less member would be wasted (and
// passing nil to Mint→factory would NPE). Membership ≠ embodiment is the substrate
// truth; the cell, if any, is the embodiment layer on top. A pre-existing row (server
// restart) is reused — the live instance rebinds.
func (h *Home) Spawn(ctx context.Context, id actor.ActorID, kind actor.Kind, factory func(actorcaps.Caps) actorrt.Actor) error {
	if id == "" {
		return fmt.Errorf("platform: Spawn id required")
	}
	binding := actor.Binding("")
	if factory != nil {
		binding = actor.BindingEmbedded
	}
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{{
		ID: id, Kind: kind, Binding: binding, At: h.nowMs(),
	}}, nil); err != nil {
		return fmt.Errorf("platform: Spawn membership: %w", err)
	}
	if factory != nil {
		// Two-phase Spawn: the build closure runs inside Spawn (after membership is
		// durable, before go-live). It welds the whole caps bundle bound to THIS
		// incarnation (livePen + liveAccess membranes, spawn handle) and hands the
		// gated bundle to the factory in ONE step — no bare handle escapes. A
		// participant is a gated cap holder; substrate anchors are not (see
		// channelkit/compute).
		rt := h.channel.Cells()
		rt.Spawn(id, func(inc actorrt.Incarnation) actorrt.Actor {
			return factory(h.buildCaps(id, kind, inc))
		})
	}
	h.logger.Info("platform.member.spawned", "channel", string(h.channelID),
		"actor", string(id), "kind", string(kind), "cell", factory != nil)
	return nil
}

// buildCaps assembles the caps bundle — the five-capability bundle welded to (id,
// inc). Handing out the handle and wrapping the live membrane happen in the same
// step (invariant: no bare handle escapes). It is the SINGLE
// caps assembler, shared by admission (Home.Spawn) and by fork (spawnHandle.Fork
// holds this method value as its capsAssembler and re-runs it against each child's
// incarnation) — so a fork child is born with the IDENTICAL membrane set as a
// top-level admission (recursive assembly), never a raw un-membraned closure.
//
// Wired this period: Pen (livePen over the harness pen), Access + State
// (liveAccess over the channel-scoped Mint and actor-scoped MintState handles —
// cs.Access is already assembled by storeopen, drawn directly), Spawn (the
// by-incarnation fork/despawn handle; builder may be nil → Fork fail-fasts).
//
// Schedule is DELIBERATELY nil this period: the ScheduleHandle requires the
// schedule engine, which OpenScheduler assembles only once FireSink (mirroring
// the pen mint) and Reviver (SpawnIfAbsent+Builder) exist — that assembly path
// is not wired yet. Forcing a placeholder here would be a half-built piece (a
// minted handle over an engine that never Start()s), so the field is left nil
// until that assembly wires it — no actor consumes it yet.
func (h *Home) buildCaps(id actor.ActorID, kind actor.Kind, inc actorrt.Incarnation) actorcaps.Caps {
	rt := h.channel.Cells()
	return actorcaps.Caps{
		Pen:      link.NewLivePen(h.minter.Mint(id, kind, h.channelID), inc, rt),
		Access:   link.NewLiveAccess(h.cs.Access.Mint(id), inc, rt),
		State:    link.NewLiveAccess(h.cs.Access.MintState(id), inc, rt),
		Schedule: nil, // not wired yet (OpenScheduler assembly) — see doc above.
		Spawn:    newSpawnHandle(inc, rt, h.builder, h.buildCaps, h.placement),
	}
}

// ServeAttach is the attach admission surface: the app hands an upgraded WS request here so a
// daemon can attach its actor streams. Home keeps the internal link acceptor and
// only exposes this capability — the acceptor object never escapes.
func (h *Home) ServeAttach(w http.ResponseWriter, r *http.Request, daemonID string) {
	h.links.Serve(w, r, daemonID)
}

// Subscribe is the subscription registration surface (client push): a client stream subscribes to
// the commit Signal and reads forward from its own seq cursor. It returns the
// wake channel and the unsubscribe func — the internal Signal never escapes.
func (h *Home) Subscribe() (<-chan struct{}, func()) {
	return h.signal.Subscribe()
}

// Close tears down the channel home in order: link acceptor (WS connections +
// per-actor streams) -> delivery tap -> cells -> channel stores (DB).
func (h *Home) Close() error {
	// 0. Reconciler ticker first: stop the level sweep and join it, so no
	//    Reconcile runs against the writer/runtime/stores being torn down below.
	if h.reconcileStop != nil {
		h.reconcileStop()
		<-h.reconcileDone
	}
	// 1. Link acceptor first: close all WS links, tear down every actor stream,
	//    wait for Serve goroutines. Stops external compute traffic before the
	//    runtime/stores underneath shut down.
	linkErr := h.links.Close()
	// 2. Delivery tap: stop the pump before tearing the runtime down.
	h.delivery.Close()
	// 3. Cells: stop actor cells (system actors included).
	h.channel.Cells().StopAll()
	// 4. Channel stores (DB) last.
	csErr := h.cs.Close()

	if linkErr != nil {
		return linkErr
	}
	return csErr
}

// ---------------------------------------------------------------------------
// View -- the read-only observation capability
// ---------------------------------------------------------------------------

// View is the channel-home's read-only observation set: committed message tail
// (ReadAfterSeq), head cursor (MaxSeq), and active actor roster (ListActors). It
// holds only read interfaces — there is no write path through a View.
type View struct {
	query      storespec.MessageQuery
	registry   storespec.Registry
	links      *link.Acceptor
	deviceFold *devicepresence.Fold
}

// DevicePresence returns the latest opaque L3 device-presence snapshot an actor
// pushed (via the obs axis), folded read-time. known=false = UNKNOWN (the actor
// never reported, or its link dropped and the fold decayed it) — NOT offline.
// The caller decodes the bytes via introspect.ParseDevicePresence. Advisory only;
// authoritative reachability is send→terminal.
func (v View) DevicePresence(id actor.ActorID) (snapshot []byte, known bool) {
	if v.deviceFold == nil {
		return nil, false
	}
	return v.deviceFold.Device(id)
}

// IsAttached reports whether daemon (compute) id has a live attach right now
// (L0 attachment) — read-time, derived from the link acceptor, never stored.
func (v View) IsAttached(daemonID string) bool {
	if v.links == nil {
		return false
	}
	return v.links.IsAttached(daemonID)
}

// AttachedDaemons returns the currently-attached compute ids (L1 snapshot).
func (v View) AttachedDaemons() []string {
	if v.links == nil {
		return nil
	}
	return v.links.AttachedDaemons()
}

// ReadAfterSeq returns committed envelopes with seq > afterSeq (client tail).
func (v View) ReadAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]storespec.StoredRow, error) {
	return v.query.ReadAfterSeq(ctx, afterSeq, limit)
}

// MaxSeq returns the channel's current head seq (client cursor anchor).
func (v View) MaxSeq(ctx context.Context) (int64, error) {
	return v.query.MaxSeq(ctx)
}

// ListActors returns all active actors from the membership registry.
func (v View) ListActors(ctx context.Context) ([]storespec.Record, error) {
	return v.registry.ListActive(ctx)
}

// ---------------------------------------------------------------------------
// runtimeLivenessAdapter -- bridges actorrt.Runtime.Stat -> sysactor.LivenessStat
// ---------------------------------------------------------------------------

type runtimeLivenessAdapter struct {
	rt *actorrt.Runtime
}

func (a *runtimeLivenessAdapter) Stat(id actor.ActorID) (startedAt time.Time, present bool) {
	if a.rt == nil {
		return time.Time{}, false
	}
	stat, ok := a.rt.Stat(id)
	if !ok {
		return time.Time{}, false
	}
	return stat.StartedAt, true
}

// ---------------------------------------------------------------------------
// delivery tap handle -- the cell-delivery Pump's per-row work
// ---------------------------------------------------------------------------

// deliveryHandle is the delivery tap's per-row work: deliver the committed
// envelope to its audience cells and OBSERVE the per-audience Outcome (the
// substrate's structured DeliverResult lands here — NotHosted / MailboxFull /
// Stopped are logged, never silently dropped). It is best-effort (push-mailbox
// semantics): a not-hosted / full mailbox is observed, not retried, so the
// handle always returns nil and the pump cursor always advances.
func deliveryHandle(d actorrt.Deliverer, chID channelpkg.ID, logger *slog.Logger) func(storespec.StoredRow) error {
	return func(row storespec.StoredRow) error {
		env := row.Envelope
		res, err := d.Deliver(env.Audience, &env)
		if err != nil {
			logger.Error("platform.delivery.error",
				"channel", string(chID), "seq", row.Seq, "envelope", string(env.ID), "err", err)
			return nil
		}
		for id, outcome := range res.Per {
			if outcome == actorrt.Delivered {
				continue
			}
			logger.Warn("platform.delivery.outcome",
				"channel", string(chID), "seq", row.Seq, "envelope", string(env.ID),
				"audience", string(id), "outcome", outcomeString(outcome))
		}
		return nil
	}
}

// outcomeString names an actorrt.Outcome for structured logging (an observation
// label, not a semantic branch — the handle does not act differently per kind).
func outcomeString(o actorrt.Outcome) string {
	switch o {
	case actorrt.Delivered:
		return "delivered"
	case actorrt.NotHosted:
		return "not_hosted"
	case actorrt.MailboxFull:
		return "mailbox_full"
	case actorrt.Stopped:
		return "stopped"
	default:
		return "unknown"
	}
}
