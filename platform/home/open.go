package home

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/lib/channelkit"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/platform/internal/sysactor"
	"github.com/wanpengxie/atoll/platform/internal/tap"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// reconcileInterval is the closure reconciler's low-frequency safety-net sweep
// period. The death edge closes the common case immediately; this level sweep
// catches lost edges (clean despawn / ctx-cancel / open requests predating a
// restart). It is a backstop, not the primary path, so it runs rarely.
const reconcileInterval = 30 * time.Second

// Open assembles the channel home. Assembly is linearised by the tap seam (no
// construction cycle, no back-fill):
//
//	signal -> stores(OnCommit=signal.Notify) -> harness -> channelkit(registers
//	the sysactor factory; Start births it against the live runtime) ->
//	delivery tap -> link acceptor.
func Open(cfg Config) (_ *Home, retErr error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if cfg.ChannelID == "" {
		return nil, fmt.Errorf("platform: ChannelID required")
	}
	if cfg.CompositionResolver == nil {
		return nil, fmt.Errorf("platform: CompositionResolver required")
	}
	if cfg.DaemonAuthority == nil {
		return nil, fmt.Errorf("platform: DaemonAuthority required")
	}
	if cfg.Bootstrap && cfg.MustExistDB {
		return nil, errors.New("platform: Bootstrap and MustExistDB are mutually exclusive")
	}
	h := &Home{channelID: cfg.ChannelID, logger: logger, closeDone: make(chan struct{})}
	defer func() {
		if p := recover(); p != nil {
			func() {
				defer func() {
					if q := recover(); q != nil {
						logger.Error("home.rollback.panic", "panic", q)
					}
				}()
				_ = h.closeInternal("panic")
			}()
			panic(p)
		}
		if retErr != nil {
			logger.Error("platform.home.rollback", "channel", cfg.ChannelID, "cause", retErr)
			retErr = errors.Join(retErr, h.closeInternal("rollback"))
		}
	}()
	ctx := context.Background()
	nowMs := func() int64 { return time.Now().UnixMilli() }
	clock := func() time.Time { return time.UnixMilli(nowMs()) }
	sweepEvery := cfg.ReconcileInterval
	if sweepEvery <= 0 {
		sweepEvery = reconcileInterval
	}

	// 1. Build the commit Signal (tap fan-out). It has NO dependencies, so it is
	//    built first and handed to the store as its post-commit source. The
	//    commit signal belongs to the log append chokepoint (Postgres WAL / Kafka
	//    offset), not to any one writer — so BOTH write paths (request-path Append
	//    and the control-plane membership mirror) fire it through the store,
	//    instead of only the harness path being wrapped.
	signal := tap.NewSignal()
	h.signal = signal

	// 2. Open channel stores (substrate), wiring the commit signal as the store's
	//    OnCommit. Now any durable append — request or control-plane — wakes the
	//    tap identically. StorageMounts/StorageControl (期11 §4.3) are LATE-BOUND
	//    here (storagehost.go's lateAcceptor): the ONLY thing that can actually
	//    answer them — the link Acceptor's attach state — is not built until
	//    step 11 below. bindLateAcceptor closes that gap once it exists; any
	//    file-kind Create landing before then sees an honest empty mount list
	//    (§4.3's own "late-bound...延迟解析,调用时才读在线态" escape hatch).
	lateAcc := &lateAcceptor{}
	cs, err := runtime.OpenChannel(ctx, cfg.ChannelID, cfg.DBPath, runtime.OpenChannelOptions{
		MustExist:      cfg.MustExistDB,
		OnCommit:       signal.Notify,
		StorageMounts:  lateStorageMounts{acc: lateAcc},
		StorageControl: lateStorageControl{acc: lateAcc},
		LaneControl:    lateLaneControl{acc: lateAcc},
	})
	if err != nil {
		return nil, fmt.Errorf("platform: open channel store: %w", err)
	}
	h.cs = cs
	if cfg.Bootstrap && cfg.Genesis != nil {
		if err := cs.Genesis.CreateGenesis(ctx, *cfg.Genesis); err != nil {
			return nil, fmt.Errorf("platform: write channel genesis: %w", err)
		}
	}
	var storedGenesis *storespec.ChannelGenesis
	if cfg.ExpectedGenesis != nil {
		got, found, err := cs.Genesis.ReadGenesis(ctx)
		if err != nil {
			return nil, fmt.Errorf("platform: read channel genesis: %w", err)
		}
		if !found || got.ChannelID != cfg.ExpectedGenesis.ChannelID || got.Type != cfg.ExpectedGenesis.Type {
			return nil, fmt.Errorf("platform: schema incompatible: channel genesis mismatch")
		}
		storedGenesis = &got
	}

	// 3. Durable genesis and authority boot. Every durable identity, including
	// the intrinsic system actor, enters through the same registry+decl+event
	// transaction. The complete durable image is then swapped into the Home
	// index and the one shared authority slot is bound exactly once. No effect
	// consumer or goroutine exists yet, so no query can race the bind.
	systemAdmission, err := cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
		ID:        actor.SystemActorID,
		Kind:      actor.KindSystem,
		Class:     "system",
		Placement: storespec.NewServerPlacement(),
		CreatedAt: nowMs(),
	})
	if err != nil {
		return nil, fmt.Errorf("platform: admit system actor: %w", err)
	}
	bootRows, err := cs.Declared.ListDeclaredActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("platform: load actor authority: %w", err)
	}
	if !cfg.Bootstrap {
		owners := 0
		ownerPrincipal := ""
		for _, row := range bootRows {
			if row.Role == storespec.RoleOwner {
				owners++
				ownerPrincipal = row.Principal
			}
		}
		if owners != 1 {
			return nil, fmt.Errorf("platform: normal open requires exactly one active channel owner (got %d)", owners)
		}
		if storedGenesis != nil && ownerPrincipal != storedGenesis.OwnerPrincipal {
			return nil, fmt.Errorf("platform: owner invariant: registry owner does not match genesis")
		}
	}
	bootEntries := make([]controlEntry, 0, len(bootRows))
	bootIDs := make([]actor.ActorID, 0, len(bootRows))
	for _, row := range bootRows {
		bootEntries = append(bootEntries, controlEntry{Row: row, World: storespec.WorldDurable})
		bootIDs = append(bootIDs, row.ID)
	}
	controlIndex := newActorControlIndex()
	if !controlIndex.ReplaceAll(bootEntries) {
		return nil, errors.New("platform: invalid actor authority boot image")
	}
	if err := cs.BindActorAuthority(controlIndex); err != nil {
		return nil, fmt.Errorf("platform: bind actor authority: %w", err)
	}
	grantOverlay := newActorGrantOverlay()
	if err := cs.BindGrantOverlay(grantOverlay); err != nil {
		return nil, fmt.Errorf("platform: bind actor grant overlay: %w", err)
	}
	h.controlIndex = controlIndex
	h.liveness = newLivenessLedger(func(id actor.ActorID, env *message.Envelope, reason deliveryDropReason, err error) {
		logger.Warn("platform.delivery.dropped",
			"channel", string(cfg.ChannelID), "actor", string(id), "envelope", string(env.ID),
			"kind", string(env.Kind), "reason", string(reason), "err", err)
	})
	if h.liveness.Bootstrap(bootIDs) != transitionApplied {
		return nil, errors.New("platform: invalid liveness boot image")
	}
	h.grantOverlay = grantOverlay
	stateHandles, err := accessdoor.NewStateHandleResolver(cs.Authority, cs.Access)
	if err != nil {
		return nil, fmt.Errorf("platform: build state handle resolver: %w", err)
	}
	h.stateHandles = stateHandles
	if systemAdmission.Created {
		logger.Info("platform.member.admitted", "channel", string(cfg.ChannelID),
			"actor", string(actor.SystemActorID), "kind", string(actor.KindSystem), "principal", "")
	}

	// 4. Build the harness Minter (the substrate mint machine). New returns a Minter, never a
	//    bare chain — the bare writer's visibility is compile-time capped inside the
	//    harness package. Every admission point (Spawn / attach / system closure)
	//    Mints a Pen welded to (actorID, chID); the welded identity is unforgeable
	//    by the holder. The post-commit Notify lives at the store append chokepoint,
	//    so there is no write-gate wrapper layer.
	minter, err := harness.New(harness.Deps{
		ChannelID: cfg.ChannelID,
		Log:       cs.Log,
		Authority: cs.Authority,
		Logger:    logger,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: build harness: %w", err)
	}
	// The system Pen: every system-authored write (sysactor serve responses,
	// channelkit's author#3 closure terminals) commits through this welded pen, so
	// sender==SystemActorID rides each write by construction — no caller stamping
	// at the call sites.
	systemPen := minter.Mint(actor.SystemActorID, actor.KindSystem, cfg.ChannelID, 1)

	// 5. Presence fold: mechanism-only latest-value cache. The level-kind set is
	// an assembly concern; non-level diagnostics are not accumulated here.
	presenceFold := presence.New(logger, clock,
		[]actorrt.ObsKind{actorrt.ObsKind(introspect.ObsDevicePresence)}, sweepEvery)

	// 6. channelkit: actorrt runtime + sysactor + death-edge wiring. The system
	//    cell is built against the LIVE runtime (factory) — its liveness Stat seam
	//    reads the real runtime at construction, no back-filled pointer.
	//
	//    h is predeclared (nil) here and assigned below (step 9): sysactor is a
	//    ring0 special Proc (spec §3's out-generation matrix) that still enters
	//    through actorbase.New like every other actor, so its Hooks.Canceller
	//    wants Home.CancelRequest — but the system cell's factory is registered
	//    at channelkit.New (and the cell birthed at channel.Start), before Home
	//    is assigned. The closure captures the h VARIABLE (not its zero value);
	//    by the time a cancel actually fires (long after Open returns), h has
	//    been assigned.
	channel, err := channelkit.New(channelkit.Config{
		ChannelID: cfg.ChannelID,
		System: func(rt *actorrt.Runtime, inc actorrt.Incarnation) actorrt.Actor {
			// S6 Q5: the ring0 anchor's four caps arms装真 — all RAW (no
			// incarnation membrane), the anchor姿势 the system pen already wears
			// (权威自身不设 incarnation 门). Access/State are eager (the access
			// door is assembled by storeopen, before channelkit); Schedule/Spawn
			// are late-bound (their engines are assembled after this cell is born
			// — see sysanchorcaps.go), captured through the same h variable
			// Hooks.Canceller uses.
			homeOf := func() *Home { return h }
			caps := actorcaps.Caps{
				Pen:    systemPen,
				Access: cs.Access.Mint(storespec.AuthorStamp{ID: actor.SystemActorID, BirthVersion: 1}),
				State: func() accessdoor.AccessHandle {
					handle, resolveErr := h.stateHandles.Resolve(context.Background(), storespec.AuthorStamp{ID: actor.SystemActorID, BirthVersion: 1})
					if resolveErr != nil {
						panic(resolveErr)
					}
					return handle
				}(),
				Schedule:  systemScheduleHandle{home: homeOf},
				Lifecycle: systemSpawnHandle{inc: inc, home: homeOf},
			}
			hooks := actorbase.Hooks{
				Canceller: func(target actor.ActorID, requestID message.ID) {
					if h != nil {
						h.CancelRequest(target, requestID)
					}
				},
			}
			return actorbase.New(caps, hooks, sysactor.Def(sysactor.Deps{
				Authority: cs.Authority,
				Clock:     clock,
				Presence:  presence.NewView(presenceFold, rt, cs.Authority),
				Logger:    logger,
				Operate:   cfg.Operate,
			}))
		},
		SystemPen:    systemPen,
		OpenRequests: cs.Query,
		// ClosedForever is closure's monotone predicate (拔根 #14/#15): a receiver's
		// callers are closed with receiver_unavailable ONLY when it is deregistered
		// or never a member — the irreversible facts (an actor id is minted once and
		// never reused; dereg never reverts). Same classification as the scheduler's
		// not_a_member reject. A crashed-but-still-registered receiver returns false
		// here and is left to the expiry reaper (its callers wait for the request
		// deadline) — no liveness snapshot is ever a terminal-write dependency.
		ClosedForever: func(ctx context.Context, id actor.ActorID) (bool, error) {
			_, ok, err := cs.Authority.LookupActive(ctx, id)
			if err != nil {
				return false, err // transient: skip this round, the reconciler retries next tick.
			}
			return !ok, nil
		},
		Clock:  clock,
		Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: construct channel: %w", err)
	}
	h.channel = channel

	// 7. Build the delivery tap: a Pump over the Signal-fed Deliverer. cursor start
	//    = current MaxSeq (mailbox semantics: only new commits). DeliverResult
	//    lands here as structured per-audience logs.
	from, err := cs.Query.MaxSeq(ctx)
	if err != nil {
		return nil, fmt.Errorf("platform: read max seq: %w", err)
	}
	// 8. Register the device-presence fold once for the runtime population. Every
	//    actor's obs wire naturally feeds this single fanout subscription; attach
	//    churn therefore needs no per-actor watcher bookkeeping.
	channel.Cells().WatchDown(presenceFold)
	channel.Cells().WatchObsAll(presenceFold)

	// 9. Assemble the Home shell now: the scheduler's Reviver and the eager
	//    reconcile arm both close over it (buildCaps, builder, cells), so it must
	//    exist before those are wired. schedMinter/engine/links are filled below —
	//    the link acceptor is built AFTER the scheduler because it welds a remote
	//    port's incarnation onto the schedule Minter (the time-axis wire arm), which
	//    only exists once OpenScheduler has run.
	h.channelID = cfg.ChannelID
	h.minter = minter
	h.channel = channel
	h.cs = cs
	h.signal = signal
	h.delivery = nil
	h.presenceFold = presenceFold
	h.logger = logger
	h.nowMs = nowMs
	view := &compositionView{h: h, resolver: cfg.CompositionResolver}
	h.factories = view
	h.noFactoryWarned = map[actor.ActorID]struct{}{}
	h.portIndex = map[actor.ActorID]homePortEntry{}
	h.forkReceipts = map[forkReceiptKey]forkReceipt{}
	h.usedForkIDs = map[actor.ActorID]struct{}{}
	h.systemPen = systemPen
	h.systemEnd = lifecycleEndHandle{home: h, author: storespec.AuthorStamp{ID: actor.SystemActorID, BirthVersion: 1}}
	h.reviveLogAt = map[actor.ActorID]time.Time{}
	h.reviveBackoff = map[actor.ActorID]reviveBackoffEntry{}
	h.pokeCh = make(chan struct{}, 1)
	h.onMembershipChange = cfg.OnMembershipChange
	// 装配链 step① (gateway 期 S2): the slot registry exists BEFORE any cell
	// construction path (human cells are born at the reconcile sweep below), so
	// the factory's step③ slot lookup never races an absent registry.
	h.subjectgate = subjectgate.NewRegistry()
	channel.Cells().WatchDown(livenessDownWatcher{h: h})

	// 10. Time axis (OpenScheduler). FireSink mints a pen per fire (author-welded);
	//     Reviver activates an absent identity-timer author via SpawnIfAbsent. The
	//     engine is Started here and Closed in Home.Close (minting a handle without
	//     Start would be a cast-but-unwired half-piece). BOOT-ORDER红线: the Reviver
	//     is wired and the engine is Started BEFORE the first reconcile sweep below,
	//     because an overdue fire on Start can precede the eager ring re-minting the
	//     always-on set — and append has no backfill, so the wake must be revivable
	//     from the first instant.
	rt := channel.Cells()
	schedMinter, engine, err := runtime.OpenScheduler(cs, schedule.AssemblyDeps{
		Fire:   fireSink{minter: minter, authority: cs.Authority, rt: rt, chID: cfg.ChannelID},
		Host:   rt,
		Revive: homeReviver{h: h},
		Clock:  cfg.Clock,
		Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: open scheduler: %w", err)
	}
	h.schedMinter = schedMinter
	h.engine = engine

	// 11. Build the link acceptor (physical layer: WS mux + per-actor ipc streams
	//      + lease judgement for attached computes). Still Construct — pure
	//      fallible preparation, zero goroutines: NewAcceptor only allocates its
	//      tables (Serve is what runs links, and nothing serves until the app
	//      binds an HTTP route after Open returns). It welds an attaching remote
	//      port's incarnation onto the same three minters a local cell's Caps draw
	//      from — the harness pen Minter, the access door (cs.Access), and the
	//      schedule engine Minter (which is why it must follow OpenScheduler) —
	//      so a daemon-hosted cell's message / off-log / time-axis capability is
	//      behaviourally identical to a local one (transport neutrality).
	//      Attached-port obs enters the runtime's one population subscription
	//      just like local-cell obs.
	links, err := link.NewAcceptor(link.Config{
		Minter:             minter,
		Access:             cs.Access,
		StateHandles:       stateHandles,
		Schedule:           schedMinter,
		Runtime:            rt,
		Authority:          cs.Authority,
		Declarations:       homeDeclarationCoordinator{h: h},
		ChannelID:          cfg.ChannelID,
		Logger:             logger,
		CancelRequest:      h.handleCancelUpstream,
		StorageHostControl: homeStorageHostControl{outbox: cs.Outbox, timeout: cfg.ReservationTimeout, logger: logger},
		PlanProvider:       boundPlanProvider{channelID: cfg.ChannelID, provider: cfg.PlanProvider},
		DaemonAuthority:    daemonAuthorityAdapter{inner: cfg.DaemonAuthority},
		ActorLock:          h.actorGates.lock,
		PortIndex:          homePortIndex{h: h},
		IdleRequest:        h.approveRemoteIdle,
		SpawnRequest:       h.handleRemoteSpawn,
		EndRequest:         h.handleRemoteEnd,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: construct link acceptor: %w", err)
	}
	h.links = links

	// 12. Activate: construct is complete (every fallible preparation done, all
	//     ownership already in h) — start the components: channel cells, the
	//     schedule engine, then the delivery pump.
	if err := channel.Start(); err != nil {
		return nil, fmt.Errorf("platform: start channel: %w", err)
	}
	sysInc, sysLive := channel.Cells().CurrentIncarnation(actor.SystemActorID)
	if !sysLive {
		return nil, errors.New("platform: system cell not live at publish")
	}
	if ticket, verdict := h.liveness.BeginEnsure(actor.SystemActorID, 1); verdict != transitionApplied ||
		h.liveness.PublishLocal(actor.SystemActorID, ticket, sysInc, runtimeDeliveryCarrier{id: actor.SystemActorID, deliverer: channel.Deliverer()}) != transitionApplied {
		return nil, errors.New("platform: publish system liveness")
	}
	h.redeliverOpenRequests(ctx, actor.SystemActorID)
	engine.Start()
	deliver := deliveryHandle(h, cfg.ChannelID, logger)
	h.delivery = tap.OpenPump(signal, cs.Query, from, deliver, logger)

	// Close the late-binding window (see step 2's lateAcc note): every
	// file-kind placement decision from this instant on can actually route an
	// AllocRequest / see attached daemons as storage-mount candidates.
	lateAcc.bind(links)

	// 12b. Boot wake-debt rebuild (S6 write-path ⑥, the open-request predicate
	//     half): boot loads every declared identity dormant with dirty=false,
	//     so an open request that predates this process must re-create its
	//     receiver's wake debt here — without this a finite-idle receiver is
	//     never re-activated to finish pre-restart work until its caller's
	//     deadline. One truth-derived scan per receiver, same anchor machinery
	//     the handoff path uses; the system anchor already ran above.
	if receivers, rerr := cs.Query.DistinctOpenRequestReceivers(ctx); rerr != nil {
		logger.Warn("platform.boot.open_request_scan_failed", "err", rerr)
	} else {
		for _, id := range receivers {
			if id == actor.SystemActorID {
				continue
			}
			h.redeliverOpenRequests(ctx, id)
		}
	}

	// 13. Reconcilers (level backstops). Run one sweep of EACH at startup —
	//     activation re-mints the always-on desired set; closure closes orphan open
	//     requests whose receiver is closed forever (deregistered / never a member)
	//     — a lost dereg edge, or a dereg that predates this process. Then a
	//     low-frequency ticker keeps both as the safety net for any lost death edge
	//     / intent change. The death edge (OnDown) remains the lossy fast-path for
	//     closure. Step order is fixed inside reconcileSweep (see its head).
	h.reconcileSweep(ctx)
	reconcileCtx, reconcileStop := context.WithCancel(context.Background())
	reconcileDone := make(chan struct{})
	h.reconcileStop = reconcileStop
	h.reconcileDone = reconcileDone
	go func() {
		defer close(reconcileDone)
		t := time.NewTicker(sweepEvery)
		defer t.Stop()
		for {
			select {
			case <-reconcileCtx.Done():
				return
			case <-t.C:
				h.reconcileSweep(reconcileCtx)
			case <-h.pokeCh:
				// Admit poke: run the same sweep off-tick so a freshly-admitted member
				// embodies without the ≤30s wait.
				h.reconcileSweep(reconcileCtx)
			}
		}
	}()
	logger.Info("platform.home.ready", "channel", string(cfg.ChannelID))
	return h, nil
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
type runtimeDeliveryCarrier struct {
	id        actor.ActorID
	deliverer actorrt.Deliverer
}

func (c runtimeDeliveryCarrier) Enqueue(env *message.Envelope) error {
	res, err := c.deliverer.Deliver([]actor.ActorID{c.id}, env)
	if err != nil {
		return err
	}
	switch res.Per[c.id] {
	case actorrt.Delivered:
		return nil
	case actorrt.MailboxFull:
		return actorrt.ErrMailboxFull
	case actorrt.Stopped:
		return actorrt.ErrCellStopped
	default:
		return actorrt.ErrNotHosted
	}
}

func deliveryHandle(h *Home, chID channelpkg.ID, logger *slog.Logger) func(storespec.StoredRow) error {
	return func(row storespec.StoredRow) error {
		env := row.Envelope
		for _, id := range env.Audience {
			verdict, err := h.liveness.AcceptDelivery(id, &env)
			if err != nil || verdict != transitionApplied {
				logger.Warn("platform.delivery.outcome",
					"channel", string(chID), "seq", row.Seq, "envelope", string(env.ID),
					"audience", string(id), "outcome", "not_accepted", "err", err)
			}
			if env.Kind == message.KindRequest {
				h.pokeReconcile()
			}
		}
		return nil
	}
}
