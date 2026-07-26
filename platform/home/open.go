package home

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
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
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/managedcaps"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/systemcaps"
	"github.com/wanpengxie/atoll/runtime/systemkernel"
)

const reconcileInterval = 30 * time.Second

// Open assembles one channel in dependency order. Managed actor truth is
// published exactly once by Controller.Start; no old Runtime/control index
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

	// The system kernel is an internal constant, not a member: it gets no
	// registry row, no admission and no record. Its identity reaches the kernel
	// as a construction constant.
	if cfg.Bootstrap {
		if err := seedBootstrap(ctx, cs, cfg, h.nowMs); err != nil {
			return nil, err
		}
	}
	owner, err := readOwnerPrincipal(ctx, cs)
	if err != nil {
		return nil, err
	}
	h.ownerPrincipal = owner

	h.presenceFold = presence.New(logger, clock,
		[]actorrt.ObsKind{actorrt.ObsKind(introspect.ObsDevicePresence)}, sweepEvery)

	h.resolver = cfg.IntroductionResolver
	h.factories = &compositionView{h: h, resolver: cfg.CompositionResolver}
	organ, err := newActorOrgan(cs, h.nowMs)
	if err != nil {
		return nil, fmt.Errorf("platform: construct actor organ: %w", err)
	}
	h.controller = organ.controller
	h.systemKernel = systemkernel.New()
	h.actors = newActorSystem(h, logger)

	var completion accessdoor.ResourceCompletion
	h.access, completion, err = accessdoor.NewAssembly(accessdoor.Deps{
		Registry:       cs.Assembly.Resources,
		Drivers:        accessdoor.DriverTable{resourcespec.KindKV: cs.Assembly.KV},
		Authority:      h.actors,
		State:          cs.Assembly.State,
		ChannelID:      cfg.ChannelID,
		StorageMounts:  lateStorageMounts{acc: lateAcc},
		StorageControl: lateStorageControl{acc: lateAcc},
		LaneControl:    lateLaneControl{acc: lateAcc},
	})
	if err != nil {
		return nil, fmt.Errorf("platform: build access door: %w", err)
	}
	h.outbox = resourceOutbox{
		ResourceOutbox: cs.Assembly.Resources,
		completion:     completion,
	}
	h.minter, err = harness.New(harness.Deps{
		ChannelID: cfg.ChannelID, Log: cs.Log, Presence: h.actors,
		ResolveAudience: h.resolveAudience, Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: build harness: %w", err)
	}
	stateHandles, err := accessdoor.NewStateHandleResolver(organ.entries, h.access)
	if err != nil {
		return nil, fmt.Errorf("platform: build state handle resolver: %w", err)
	}
	h.stateHandles = stateHandles

	// Scheduler construction precedes System Unit construction, but its run
	// loop starts only after Controller is Running.
	schedulerClock := cfg.Clock
	if schedulerClock == nil {
		schedulerClock = schedule.NewSystemClock()
	}
	h.schedMinter, h.engine, err = schedule.New(schedule.Deps{
		Store: cs.Assembly.Timers,
		Fire: fireSink{
			minter: h.minter, authority: h.actors,
			chID: cfg.ChannelID,
		},
		Clock: schedulerClock, Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: open scheduler: %w", err)
	}

	h.managedCaps, err = managedcaps.New(
		cfg.ChannelID,
		h.minter,
		h.access,
		h.stateHandles,
		h.schedMinter,
		h.actors,
	)
	if err != nil {
		return nil, fmt.Errorf("platform: construct managed caps minter: %w", err)
	}
	h.systemCaps, err = systemcaps.New(
		cfg.ChannelID,
		h.minter,
		h.access,
		h.schedMinter,
	)
	if err != nil {
		return nil, fmt.Errorf("platform: construct system caps minter: %w", err)
	}
	h.serverHost, err = actorhost.New(actorhost.Config{
		Domain: actorhost.ExecutionDomain("server"),
		Logger: logger,
		Events: homeHostEvents{home: h},
		BodyBuilder: func(input actorhost.BodyBuildInput) actorrt.Actor {
			prepared, err := h.controller.PrepareRun(
				input.ActorID,
				input.AttemptKey,
				input.ExecutionSpec,
			)
			if err != nil {
				logger.Warn("platform.actor_prepare_run_failed", "actor", input.ActorID, "err", err)
				return nil
			}
			caps, err := h.managedCaps.Mint(context.Background(), prepared)
			if err != nil {
				logger.Warn("platform.actor_caps_mint_failed", "actor", input.ActorID, "err", err)
				return nil
			}
			def, ok := h.factories.LookupByClass(
				input.ActorID,
				input.ExecutionSpec.Class,
				input.ExecutionSpec.Config,
			)
			if input.ExecutionSpec.Kind == actor.KindHuman {
				h.ensureSubjectSlot(input.ActorID)
				def, ok = humanCellFactory(h, input.ActorID), true
			}
			if !ok {
				logger.Warn("platform.actor_factory_missing",
					"actor", input.ActorID, "class", input.ExecutionSpec.Class)
				return nil
			}
			return hostcommon.Build(caps, h.hooks(), def)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("platform: construct server actor host: %w", err)
	}
	h.opEntry = &opEntry{home: h}

	systemCaps, err := h.systemCaps.Mint(ctx)
	if err != nil {
		return nil, fmt.Errorf("platform: mint system caps: %w", err)
	}
	h.systemPen = systemCaps.Pen
	systemUnit, err := actorrt.Prepare(actorrt.UnitConfig{
		ActorID: actor.SystemActorID, Kind: actor.KindSystem, Logger: logger,
	}, func(actorrt.Incarnation) actorrt.Actor {
		return actorbase.New(systemCaps, h.hooks(), sysactor.Def(sysactor.Deps{
			Authority: h.actors, Clock: clock,
			Presence: presence.NewView(h.presenceFold, h.actors, h.actors),
			Logger:   logger, Operate: h.opEntry,
		}))
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("platform: prepare system unit: %w", err)
	}
	if err := h.actors.start(ctx, systemUnit); err != nil {
		return nil, fmt.Errorf("platform: start actor control: %w", err)
	}
	h.sweepSubjectSlots(ctx)

	links, err := link.NewAcceptor(link.Config{
		Minter: h.minter, Access: h.access, StateHandles: stateHandles,
		Schedule: h.schedMinter, Authority: h.actors,
		ChannelID: cfg.ChannelID, Logger: logger,
		AuthorizeAttach: h.actors.AuthorizeAttach,
		AttachBinding:   h.actors.AttachBinding,
		BindingDown:     h.actors.BindingDown,
		Fork:            h.actors.RemoteFork,
		EndSelf:         h.actors.RemoteEndSelf,
		Observe: func(
			id actor.ActorID,
			key actorhost.AttemptKey,
			route actorhost.Binding,
			kind actorrt.ObsKind,
			value actorrt.ObsValue,
		) {
			h.presenceFold.OnRemoteObs(id, key, route, kind, value)
		},
		ObserveDown:   h.presenceFold.OnRemoteDown,
		CancelRequest: h.handleCancelUpstream,
		StorageHostControl: homeStorageHostControl{
			outbox: h.outbox, timeout: cfg.ReservationTimeout, logger: logger,
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

// seedBootstrap commits the bootstrap records before Controller.Start so the
// Controller publishes one complete durable image. The owner's human record is
// an ordinary human admission: no marker is seeded at the door, because owner
// lives on the genesis pointer alone.
func seedBootstrap(
	ctx context.Context,
	cs *runtime.ChannelStores,
	cfg Config,
	nowMs func() int64,
) error {
	if cfg.BootstrapOwnerPrincipal != "" {
		if _, err := cs.Actors.Insert(ctx, storespec.ActorDraft{
			Kind: actor.KindHuman, Principal: cfg.BootstrapOwnerPrincipal,
			Definition: storespec.ActorDefinition{Class: "human"},
			Placement:  storespec.NewServerPlacement(), CreatedAt: nowMs(),
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
) (storespec.ActorRecord, error) {
	if err := validateDeclareRequest(in); err != nil {
		return storespec.ActorRecord{}, err
	}
	var config []byte
	if in.Config != nil {
		config = append(config, (*in.Config)...)
	}
	record, err := cs.Actors.Insert(ctx, storespec.ActorDraft{
		Kind:         in.Kind,
		SourceDeclID: in.SourceDeclID,
		CreatedAt:    in.CreatedAt,
		Definition:   storespec.ActorDefinition{Class: in.Class, Config: config},
		Placement:    in.Placement,
	})
	if err != nil {
		return storespec.ActorRecord{}, err
	}
	if in.MakeDefault {
		if err := cs.Routing.SetDefaultAgent(ctx, record.ID); err != nil {
			return storespec.ActorRecord{}, err
		}
	}
	return record, nil
}

// readOwnerPrincipal reads the channel's one owner pointer from genesis. Owner
// is immutable channel self-truth, so one read at open is the whole story —
// there is no second account to cross-check it against, and the open check
// degenerates to "if this channel has a genesis, its owner is non-empty".
func readOwnerPrincipal(ctx context.Context, cs *runtime.ChannelStores) (string, error) {
	genesis, found, err := cs.Genesis.ReadGenesis(ctx)
	if err != nil {
		return "", fmt.Errorf("platform: read channel genesis: %w", err)
	}
	if !found {
		return "", nil
	}
	if genesis.OwnerPrincipal == "" {
		return "", errors.New("platform: channel genesis carries no owner principal")
	}
	return genesis.OwnerPrincipal, nil
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
			err := h.actors.Deliver(id, &env)
			if err != nil {
				logger.Warn("platform.delivery.outcome",
					"channel", chID, "seq", row.Seq, "envelope", env.ID,
					"audience", id, "outcome", "not_delivered", "err", err)
			}
		}
		return nil
	}
}
