package platform

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/wanpengxie/ActOS/lib/channelkit"
	"github.com/wanpengxie/ActOS/platform/internal/link"
	presencefold "github.com/wanpengxie/ActOS/platform/internal/presence"
	"github.com/wanpengxie/ActOS/platform/internal/sysactor"
	"github.com/wanpengxie/ActOS/platform/internal/tap"
	"github.com/wanpengxie/ActOS/protocol/actor"
	channelpkg "github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
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
// the package doc —裸 Runtime/Deliverer/Membership/Registry never escape it (装配
// 只交钥匙). The app layer owns HTTP/transport; Home is pure Go.
type Home struct {
	channelID channelpkg.ID
	minter    harness.Minter
	channel   *channelkit.Channel
	cs        *runtime.ChannelStores
	signal    *tap.Signal
	delivery  *tap.Pump
	links     *link.Acceptor
	presence  *presencefold.Fold
	logger    *slog.Logger
	nowMs     func() int64

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
		ChannelID:     cfg.ChannelID,
		ActorRegistry: cs.Registry,
		Log:           cs.Log,
		Logger:        logger,
	})
	if err != nil {
		_ = cs.Close()
		return nil, fmt.Errorf("platform: build harness: %w", err)
	}
	// The system Pen: every system-authored write (sysactor serve responses,
	// channelkit's author#3 closure terminals) commits through this welded pen, so
	// sender==SystemActorID rides each write by construction — no caller stamping
	// at the call sites.
	systemPen := minter.Mint(actor.SystemActorID, cfg.ChannelID)

	// 5. Bootstrap: register the intrinsic system actor so its substrate-death
	//    terminals pass harness sender validation. Idempotent SEED: on a home
	//    restart over a persistent channel DB the row already exists, and a raw
	//    re-Insert would PK-conflict (actor_id is the table key) — failing Open
	//    before the restart-recovery reconciler below can even run. Insert itself
	//    stays strict (a duplicate is an error, locked by the store's
	//    coverage test); the idempotent seed lives here at the genesis call site
	//    (audit ④-a: guard at the platform bootstrap, do not relax substrate).
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

	// 5.5 Presence fold (L3 device presence): folds actor-source obs PUSH edges
	//     into a volatile per-actor level (in-memory; never persisted), decays to
	//     unknown on the actor's death edge (link-down cascade). Built BEFORE
	//     channelkit so the system cell can read it (sysactor observes L1/L2/L3);
	//     registered as a presence watcher + per-actor obs watcher below.
	presence := presencefold.New(logger)

	// 6. channelkit: actorrt runtime + sysactor + death-edge wiring. The system
	//    cell is built against the LIVE runtime (factory) — its presence Stat seam
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
				Stat:     &runtimePresenceAdapter{rt: rt},
				Device:   presence,
			})
		},
		SystemPen:    systemPen,
		OpenRequests: cs.Query,
		Clock:        clock,
		Logger:       logger,
	})

	// 7. Build the delivery tap: a Pump over the Signal持 Deliverer. cursor start
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

	// 7.5 Register the presence fold as a global presence watcher (so an actor's
	//     death edge decays its L3 to unknown — the link-down cascade). Per-actor
	//     obs registration happens at attach (Acceptor below).
	channel.Cells().WatchPresence(presence)

	// 8. Build the link acceptor (physical layer: WS mux + per-actor ipc streams
	//    + lease judgement for attached computes). It folds each attached actor's
	//    obs PUSH into the presence fold (per-actor WatchObs at attach).
	links := link.NewAcceptor(link.Config{
		Minter:     minter,
		Runtime:    channel.Cells(),
		Membership: cs.Membership,
		ChannelID:  cfg.ChannelID,
		Logger:     logger,
		ObsWatcher: presence,
	})

	// 9. Closure reconciler (level backstop). Run one sweep at startup — this is
	//    the home-restart recovery path (#5): an open request whose receiver is
	//    absent because its presence predates this process gets closed now, not
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
		presence:      presence,
		logger:        logger,
		nowMs:         nowMs,
		reconcileStop: reconcileStop,
		reconcileDone: reconcileDone,
	}, nil
}

// View returns the read-only observation set (ReadAfterSeq / MaxSeq /
// ListActors / daemon link-presence). It carries no写 capability — observation
// only. The host (app) reads presence through here OUT-OF-BAND (no message, no
// truth-log write) — UI status polling must not pollute the log; in-universe
// actors instead ask the system actor by message (that path is logged).
func (h *Home) View() View {
	return View{query: h.cs.Query, registry: h.cs.Registry, links: h.links, presence: h.presence}
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
// Order invariant (security-critical): membership apply -> Mint pen -> factory(pen) ->
// spawn cell. Membership must be durable truth BEFORE the factory gets a pen,
// else a cell that writes on construction hits step 4 (sender not yet registered).
// "welded at birth" must not become "pen first, then register".
//
// nil-guard: factory == nil = membership-ONLY (a presence-less member, e.g. a
// human user who is a member but has no cell). No Pen is Minted and no cell is
// placed — Minting/placing a cell for a presence-less member would be wasted (and
// passing nil to Mint→factory would NPE). Membership ≠ presence is the substrate
// truth; the cell, if any, is the presence layer on top. A pre-existing row (server
// restart) is reused — the live instance rebinds.
func (h *Home) Spawn(ctx context.Context, id actor.ActorID, kind actor.Kind, factory func(harness.Pen) actorrt.Actor) error {
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
		// durable, before go-live). It Mints the welded raw pen, wraps it in a
		// livePen bound to THIS incarnation (death-after-write fence), and hands the
		// gated pen to the factory. A participant is a gated cap holder; substrate
		// anchors are not (see channelkit/compute).
		rt := h.channel.Cells()
		rt.Spawn(id, func(inc actorrt.Incarnation) actorrt.Actor {
			pen := link.NewLivePen(h.minter.Mint(id, h.channelID), inc, rt)
			return factory(pen)
		})
	}
	h.logger.Info("platform.member.spawned", "channel", string(h.channelID),
		"actor", string(id), "kind", string(kind), "cell", factory != nil)
	return nil
}

// ServeAttach is the attach受理面: the app hands an upgraded WS request here so a
// daemon can attach its actor streams. Home keeps the internal link acceptor and
// only exposes this capability — the acceptor object never escapes.
func (h *Home) ServeAttach(w http.ResponseWriter, r *http.Request, daemonID string) {
	h.links.Serve(w, r, daemonID)
}

// Subscribe is the subscription注册面 (client push): a client stream subscribes to
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
	query    storespec.MessageQuery
	registry storespec.Registry
	links    *link.Acceptor
	presence *presencefold.Fold
}

// DevicePresence returns the latest opaque L3 device-presence snapshot an actor
// pushed (via the obs axis), folded read-time. known=false = UNKNOWN (the actor
// never reported, or its link dropped and the fold decayed it) — NOT offline.
// The caller decodes the bytes via introspect.ParsePresence. Advisory only;
// authoritative reachability is send→terminal.
func (v View) DevicePresence(id actor.ActorID) (snapshot []byte, known bool) {
	if v.presence == nil {
		return nil, false
	}
	return v.presence.Device(id)
}

// IsAttached reports whether daemon (compute) id has a live attach right now
// (L1 link presence) — read-time, derived from the link acceptor, never stored.
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
// runtimePresenceAdapter -- bridges actorrt.Runtime.Stat -> sysactor.PresenceStat
// ---------------------------------------------------------------------------

type runtimePresenceAdapter struct {
	rt *actorrt.Runtime
}

func (a *runtimePresenceAdapter) Stat(id actor.ActorID) (startedAt time.Time, present bool) {
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
