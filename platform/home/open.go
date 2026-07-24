package home

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/internal/hostcommon"
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
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const reconcileInterval = 30 * time.Second

// Open assembles one channel in dependency order. Managed actor truth is
// published exactly once by ChannelActors.Start; no old Runtime/control index
// is built or shadow-written.
func Open(cfg Config) (_ *Home, retErr error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if cfg.ChannelID == "" {
		return nil, errors.New("platform: ChannelID required")
	}
	if cfg.CompositionResolver == nil {
		return nil, errors.New("platform: CompositionResolver required")
	}
	if cfg.IntroductionResolver == nil {
		return nil, errors.New("platform: IntroductionResolver required")
	}
	if cfg.Bootstrap && cfg.MustExistDB {
		return nil, errors.New("platform: Bootstrap and MustExistDB are mutually exclusive")
	}

	h := &Home{
		channelID: cfg.ChannelID, logger: logger, closeDone: make(chan struct{}),
		nowMs:              func() int64 { return time.Now().UnixMilli() },
		onMembershipChange: cfg.OnMembershipChange,
		subjectgate:        subjectgate.NewRegistry(),
		pokeCh:             make(chan struct{}, 1),
	}
	defer func() {
		if p := recover(); p != nil {
			_ = h.closeInternal("panic")
			panic(p)
		}
		if retErr != nil {
			retErr = errors.Join(retErr, h.closeInternal("rollback"))
		}
	}()
	ctx := context.Background()
	clock := func() time.Time { return time.UnixMilli(h.nowMs()) }
	sweepEvery := cfg.ReconcileInterval
	if sweepEvery <= 0 {
		sweepEvery = reconcileInterval
	}

	h.signal = tap.NewSignal()
	lateAcc := &lateAcceptor{}
	cs, err := runtime.OpenChannel(ctx, cfg.ChannelID, cfg.DBPath, runtime.OpenChannelOptions{
		MustExist: cfg.MustExistDB, OnCommit: h.signal.Notify,
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
	if err := validateGenesis(ctx, cs, cfg); err != nil {
		return nil, err
	}

	systemAdmission, err := cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
		ID: actor.SystemActorID, Kind: actor.KindSystem, Class: "system",
		Placement: storespec.NewServerPlacement(), CreatedAt: h.nowMs(),
	})
	if err != nil {
		return nil, fmt.Errorf("platform: admit system actor: %w", err)
	}
	if cfg.Bootstrap {
		if err := seedBootstrap(ctx, cs, cfg, h.nowMs); err != nil {
			return nil, err
		}
	}
	if err := validateOwnerInvariant(ctx, cs, cfg); err != nil {
		return nil, err
	}

	h.grantOverlay = newActorGrantOverlay()
	if err := cs.BindGrantOverlay(h.grantOverlay); err != nil {
		return nil, fmt.Errorf("platform: bind actor grant overlay: %w", err)
	}
	stateHandles, err := accessdoor.NewStateHandleResolver(cs.Authority, cs.Access)
	if err != nil {
		return nil, fmt.Errorf("platform: build state handle resolver: %w", err)
	}
	h.stateHandles = stateHandles

	h.minter, err = harness.New(harness.Deps{
		ChannelID: cfg.ChannelID, Log: cs.Log, Authority: cs.Authority,
		ResolveAudience: h.resolveAudience, Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: build harness: %w", err)
	}
	h.systemPen = h.minter.Mint(actor.SystemActorID, actor.KindSystem, cfg.ChannelID, 1)
	h.presenceFold = presence.New(logger, clock,
		[]actorrt.ObsKind{actorrt.ObsKind(introspect.ObsDevicePresence)}, sweepEvery)

	actorStore := newHomeActorStore(cfg.ChannelID, cs, cfg.IntroductionResolver, time.Now)
	h.actorStore = actorStore
	h.factories = &compositionView{h: h, resolver: cfg.CompositionResolver}

	// Scheduler construction precedes System Unit construction, but its run
	// loop starts only after ChannelActors is Running.
	h.schedMinter, h.engine, err = runtime.OpenScheduler(cs, schedule.AssemblyDeps{
		Fire: fireSink{
			minter: h.minter, authority: cs.Authority,
			actors: func() *actorctl.ChannelActors { return h.actors },
			chID:   cfg.ChannelID,
		},
		Clock: cfg.Clock, Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: open scheduler: %w", err)
	}

	actors, err := actorctl.NewChannelActors(actorctl.Config{
		Store: actorStore, Effects: homeActorEffects{home: h},
		ServerDomain: actorhost.ExecutionDomain("server"),
		ServerHost: actorhost.Config{
			Logger: logger, Events: homeHostEvents{home: h},
		},
		BuildManagedBody: func(
			input actorhost.BodyBuildInput,
			lifecycle actorcaps.LifecycleHandle,
		) actorrt.Actor {
			caps, capErr := h.buildManagedCaps(input, lifecycle)
			if capErr != nil {
				logger.Warn("platform.actor_caps_failed", "actor", input.ActorID, "err", capErr)
				return nil
			}
			def, ok := h.factories.Lookup(input.ActorID)
			if input.ExecutionSpec.Kind == actor.KindHuman {
				// Controller publication is already authoritative at this
				// composition seam. Ensure the stable subject slot before the
				// actor goroutine can start, so an initial Host build cannot
				// race the later maintenance sweep and become permanently
				// mailbox-only.
				h.ensureSubjectSlot(input.ActorID)
				def, ok = humanCellFactory(h, input.ActorID), true
			}
			if !ok {
				logger.Warn("platform.actor_factory_missing",
					"actor", input.ActorID, "class", input.ExecutionSpec.Class)
				return nil
			}
			return hostcommon.Build(
				caps, h.hooks(), def,
				actorbase.Options{IdleTimeout: input.ExecutionSpec.IdleTimeout},
			)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("platform: construct actor control: %w", err)
	}
	h.actors = actors
	actorStore.bindAuthority(actors)
	if err := cs.BindActorAuthority(actors); err != nil {
		return nil, fmt.Errorf("platform: bind actor authority: %w", err)
	}
	h.opEntry = &opEntry{home: h}

	systemAuthor := storespec.AuthorStamp{ID: actor.SystemActorID, BirthVersion: 1}
	systemUnit, err := actorrt.Prepare(actorrt.UnitConfig{
		ActorID: actor.SystemActorID, Kind: actor.KindSystem, Logger: logger,
	}, func(actorrt.Incarnation) actorrt.Actor {
		caps := actorcaps.Caps{
			Pen:       h.systemPen,
			Access:    cs.Access.Mint(systemAuthor),
			State:     cs.Access.MintState(systemAuthor),
			Schedule:  h.schedMinter.Mint(systemAuthor),
			Lifecycle: nil,
		}
		return actorbase.New(caps, h.hooks(), sysactor.Def(sysactor.Deps{
			Authority: actors, Clock: clock,
			Presence: presence.NewView(h.presenceFold, actors, actors),
			Logger:   logger, Operate: h.opEntry,
		}))
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("platform: prepare system unit: %w", err)
	}
	if err := actors.Start(ctx, systemUnit); err != nil {
		return nil, fmt.Errorf("platform: start actor control: %w", err)
	}
	h.sweepSubjectSlots()

	links, err := link.NewAcceptor(link.Config{
		Minter: h.minter, Access: cs.Access, StateHandles: stateHandles,
		Schedule: h.schedMinter, Authority: actors,
		ChannelID: cfg.ChannelID, Logger: logger,
		AuthorizeAttach: actors.AuthorizeAttach,
		AttachBinding:   actors.AttachBinding,
		BindingDown:     actors.BindingDown,
		Fork:            actors.RemoteFork,
		RequestIdle:     actors.RemoteRequestIdle,
		EndSelf:         actors.RemoteEndSelf,
		Observe: func(
			id actor.ActorID,
			key actorhost.AttemptKey,
			kind actorrt.ObsKind,
			value actorrt.ObsValue,
		) {
			h.presenceFold.OnRemoteObs(id, key, kind, value)
		},
		ObserveDown:   h.presenceFold.OnRemoteDown,
		CancelRequest: h.handleCancelUpstream,
		StorageHostControl: homeStorageHostControl{
			outbox: cs.Outbox, timeout: cfg.ReservationTimeout, logger: logger,
		},
		Plan: h.planForDaemon,
		CanAttach: func(ctx context.Context, daemonID string) error {
			bound, err := cs.Bindings.IsBound(ctx, storespec.DaemonID(daemonID))
			if err != nil {
				return err
			}
			if !bound {
				return errors.New("link: daemon_binding_stale")
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("platform: construct link acceptor: %w", err)
	}
	h.links = links
	lateAcc.bind(links)

	from, err := cs.Query.MaxSeq(ctx)
	if err != nil {
		return nil, fmt.Errorf("platform: read max seq: %w", err)
	}
	h.engine.Start()
	h.deliveryCtx, h.deliveryStop = context.WithCancel(context.Background())
	h.delivery = tap.OpenPump(h.signal, cs.Query, from, deliveryHandle(h, cfg.ChannelID, logger), logger)

	h.reconcileSweep(ctx)
	reconcileCtx, stop := context.WithCancel(context.Background())
	h.reconcileStop = stop
	h.reconcileDone = make(chan struct{})
	go func() {
		defer close(h.reconcileDone)
		ticker := time.NewTicker(sweepEvery)
		defer ticker.Stop()
		for {
			select {
			case <-reconcileCtx.Done():
				return
			case <-ticker.C:
				h.reconcileSweep(reconcileCtx)
			case <-h.pokeCh:
				h.reconcileSweep(reconcileCtx)
			}
		}
	}()

	if systemAdmission.Created {
		logger.Info("platform.system.admitted", "channel", cfg.ChannelID)
	}
	logger.Info("platform.home.ready", "channel", cfg.ChannelID)
	return h, nil
}

func validateGenesis(ctx context.Context, cs *runtime.ChannelStores, cfg Config) error {
	if cfg.ExpectedGenesis == nil {
		return nil
	}
	got, found, err := cs.Genesis.ReadGenesis(ctx)
	if err != nil {
		return fmt.Errorf("platform: read channel genesis: %w", err)
	}
	if !found || got.ChannelID != cfg.ExpectedGenesis.ChannelID || got.Type != cfg.ExpectedGenesis.Type {
		return errors.New("platform: schema incompatible: channel genesis mismatch")
	}
	return nil
}

func seedBootstrap(
	ctx context.Context,
	cs *runtime.ChannelStores,
	cfg Config,
	nowMs func() int64,
) error {
	if cfg.BootstrapOwnerPrincipal != "" {
		if _, err := cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
			Kind: actor.KindHuman, Principal: cfg.BootstrapOwnerPrincipal,
			Class: "human", Role: storespec.RoleOwner,
			Placement: storespec.NewServerPlacement(), CreatedAt: nowMs(),
		}); err != nil {
			return fmt.Errorf("platform: seed owner: %w", err)
		}
	}
	for _, declaration := range cfg.BootstrapDeclarations {
		if _, err := admitBootstrapDeclaration(ctx, cs, declaration); err != nil {
			return err
		}
	}
	return nil
}

func admitBootstrapDeclaration(
	ctx context.Context,
	cs *runtime.ChannelStores,
	in DeclareRequest,
) (storespec.ActorControlRow, error) {
	if err := validateDeclareRequest(in); err != nil {
		return storespec.ActorControlRow{}, err
	}
	var config []byte
	if in.Config != nil {
		config = append(config, (*in.Config)...)
	}
	binding := actor.Binding("")
	if in.Placement.Kind == storespec.PlacementDaemon {
		binding = actor.BindingRuntimeInboundViaRelay
	}
	admitted, err := cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
		Kind: in.Kind, Binding: binding, Class: in.Class, Config: config,
		Placement: in.Placement, TIdle: durationMillis(in.TIdle),
		SourceDeclID: in.SourceDeclID, CreatedAt: in.CreatedAt,
	})
	if err != nil {
		return storespec.ActorControlRow{}, err
	}
	row, ok, err := cs.Declared.LookupDeclaredActive(ctx, admitted.ID)
	if err != nil {
		return storespec.ActorControlRow{}, err
	}
	if !ok {
		return storespec.ActorControlRow{}, errors.New("platform: bootstrap declaration missing")
	}
	if in.MakeDefault {
		if err := cs.Routing.SetDefaultAgent(ctx, row.ID); err != nil {
			return storespec.ActorControlRow{}, err
		}
	}
	return row, nil
}

func validateOwnerInvariant(
	ctx context.Context,
	cs *runtime.ChannelStores,
	cfg Config,
) error {
	if cfg.Bootstrap {
		return nil
	}
	rows, err := cs.Declared.ListDeclaredActive(ctx)
	if err != nil {
		return err
	}
	owners := 0
	ownerPrincipal := ""
	for _, row := range rows {
		if row.Role == storespec.RoleOwner {
			owners++
			ownerPrincipal = row.Principal
		}
	}
	if owners != 1 {
		return fmt.Errorf("platform: normal open requires exactly one active channel owner (got %d)", owners)
	}
	if cfg.ExpectedGenesis != nil {
		expectedOwner := cfg.ExpectedGenesis.OwnerPrincipal
		if expectedOwner == "" {
			genesis, found, err := cs.Genesis.ReadGenesis(ctx)
			if err != nil {
				return err
			}
			if !found {
				return errors.New("platform: owner invariant: channel genesis missing")
			}
			expectedOwner = genesis.OwnerPrincipal
		}
		if ownerPrincipal != expectedOwner {
			return errors.New("platform: owner invariant: registry owner does not match genesis")
		}
	}
	return nil
}

func deliveryHandle(
	h *Home,
	chID channelpkg.ID,
	logger *slog.Logger,
) func(storespec.StoredRow) error {
	return func(row storespec.StoredRow) error {
		env := row.Envelope
		for _, id := range env.Audience {
			if !message.ShouldDeliver(id, &env) {
				continue
			}
			err := h.actors.DeliverCommitted(h.deliveryCtx, id, &env)
			if err != nil {
				logger.Warn("platform.delivery.outcome",
					"channel", chID, "seq", row.Seq, "envelope", env.ID,
					"audience", id, "outcome", "not_delivered", "err", err)
			}
		}
		return nil
	}
}
