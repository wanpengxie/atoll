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
	"github.com/wanpengxie/atoll/runtime/remoteingress"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/systemcaps"
	"github.com/wanpengxie/atoll/runtime/systemkernel"
	"github.com/wanpengxie/atoll/runtime/timerfire"
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
	// cs is a LOCAL: the assembly surface lives for the length of this function
	// and no longer. Home keeps only the faces below — every raw write face
	// (Log, Actors, Genesis, the Assembly leaf ports) is handed to the organ
	// that owns it further down and then goes out of scope with cs. closeStore
	// is taken immediately so the rollback defer above can release the store
	// from any later failure point.
	h.closeStore = cs.Close
	h.query = cs.Query
	h.visible = cs.Visible
	h.expiry = cs.Expiry
	h.requests = cs.Requests
	h.bindings = cs.Bindings
	h.resourceRead = cs.ResourceRead
	h.defaultAgent = openDefaultAgentFold(ctx, h, cs.Query, logger)

	if cfg.Bootstrap && cfg.Genesis != nil {
		if err := cs.Genesis.CreateGenesis(ctx, *cfg.Genesis); err != nil {
			return nil, fmt.Errorf("platform: write channel genesis: %w", err)
		}
	}
	if err := validateGenesis(ctx, cs.Genesis, cfg); err != nil {
		return nil, err
	}

	// The system kernel is an internal constant, not a member: it gets no
	// registry row, no admission and no record. Its identity reaches the kernel
	// as a construction constant.
	if cfg.Bootstrap {
		if err := seedBootstrap(ctx, cs.Actors, cfg, h.nowMs); err != nil {
			return nil, err
		}
	}
	owner, err := readOwnerPrincipal(ctx, cs.Genesis)
	if err != nil {
		return nil, err
	}
	h.ownerPrincipal = owner

	h.presenceFold = presence.New(logger, clock,
		[]actorrt.ObsKind{actorrt.ObsKind(introspect.ObsDevicePresence)}, sweepEvery)

	h.resolver = cfg.IntroductionResolver
	h.factories = &compositionView{h: h, resolver: cfg.CompositionResolver}
	organ, err := newActorOrgan(cs.Actors, h.nowMs)
	if err != nil {
		return nil, fmt.Errorf("platform: construct actor organ: %w", err)
	}
	h.controller = organ.controller
	h.systemKernel = systemkernel.New()
	h.actors = newActorSystem(h, logger)

	// access and schedMinter below are the organ doors: assembly ingredients for
	// the capability bundles and the remote ingress, and nothing else. They are
	// locals for the same reason cs is — a door kept on Home is a door every
	// method in this package can knock on.
	access, completion, err := accessdoor.NewAssembly(accessdoor.Deps{
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
		Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: build harness: %w", err)
	}
	stateHandles, err := accessdoor.NewStateHandleResolver(organ.entries, access)
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
	// The fire sink is substrate glue (schedule exit → ledger admission →
	// harness write) defined in runtime; assembly here only constructs it.
	// The authority rides the Controller directly — no facade forwarding.
	fire, err := timerfire.New(h.controller, h.minter)
	if err != nil {
		return nil, fmt.Errorf("platform: construct fire sink: %w", err)
	}
	schedMinter, engine, err := schedule.New(schedule.Deps{
		Store: cs.Assembly.Timers,
		Fire:  fire,
		Clock: schedulerClock, Logger: logger,
	})
	if err != nil {
		return nil, fmt.Errorf("platform: open scheduler: %w", err)
	}
	// The engine IS kept: Home starts it, closes it, and hands it forgotten ids.
	// The minter beside it is not — that one is assembly ingredient only.
	h.engine = engine

	h.managedCaps, err = managedcaps.New(
		h.minter,
		access,
		h.stateHandles,
		schedMinter,
		h.actors,
	)
	if err != nil {
		return nil, fmt.Errorf("platform: construct managed caps minter: %w", err)
	}
	// A local, unlike its managed twin above: this mint fires once, below, and
	// Home's running-period need is the pen it yields (h.systemPen), never the
	// power to mint another root bundle — Mint takes no argument and checks no
	// precondition, so a kept field would be a standing mint-system-root door.
	systemCapsMinter, err := systemcaps.New(
		h.minter,
		access,
		schedMinter,
	)
	if err != nil {
		return nil, fmt.Errorf("platform: construct system caps minter: %w", err)
	}
	// The remote ingress is this channel's standard part, built beside the
	// managed minter from the same Controller and the same four organ doors:
	// one instance, no channel id, no actor, no state. It is the only thing the
	// link is given. Authority ingredients come straight from the Controller —
	// Platform keeps zero capability-coordinate surface; only the completed
	// lifecycle command face (tails included) rides the actorSystem.
	// It is handed to the link acceptor whole and never touched again, so it is
	// a local: the acceptor is its owner, Home was only the courier.
	remoteIngress, err := remoteingress.New(
		h.controller,
		h.actors,
		h.minter,
		access,
		h.stateHandles,
		schedMinter,
	)
	if err != nil {
		return nil, fmt.Errorf("platform: construct remote ingress: %w", err)
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

	systemCaps, err := systemCapsMinter.Mint(ctx)
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
		Ingress:         remoteIngress,
		ChannelID:       cfg.ChannelID,
		Logger:          logger,
		AuthorizeAttach: h.actors.AuthorizeAttach,
		AttachBinding:   h.actors.AttachBinding,
		BindingDown:     h.actors.BindingDown,
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
			bound, err := h.bindings.IsBound(ctx, storespec.DaemonID(daemonID))
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

	from, err := h.query.MaxSeq(ctx)
	if err != nil {
		return nil, fmt.Errorf("platform: read max seq: %w", err)
	}
	h.engine.Start()
	h.delivery = tap.OpenPump(h.signal, h.query, from, deliveryHandle(h, cfg.ChannelID, logger), logger)

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

func validateGenesis(ctx context.Context, genesis storespec.GenesisStore, cfg Config) error {
	if cfg.ExpectedGenesis == nil {
		return nil
	}
	got, found, err := genesis.ReadGenesis(ctx)
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
	actors storespec.ActorRegistryStore,
	cfg Config,
	nowMs func() int64,
) error {
	if cfg.BootstrapOwnerPrincipal != "" {
		if _, err := actors.Insert(ctx, storespec.ActorDraft{
			Kind: actor.KindHuman, Principal: cfg.BootstrapOwnerPrincipal,
			Definition: storespec.ActorDefinition{Class: "human"},
			Placement:  storespec.NewServerPlacement(), CreatedAt: nowMs(),
		}); err != nil {
			return fmt.Errorf("platform: seed owner: %w", err)
		}
	}
	for _, declaration := range cfg.BootstrapDeclarations {
		if err := admitBootstrapDeclaration(ctx, actors, declaration); err != nil {
			return err
		}
	}
	return nil
}

// admitBootstrapDeclaration is the bootstrap-only seed write, before any
// Controller exists. It returns no record: nothing above it may hold one.
func admitBootstrapDeclaration(
	ctx context.Context,
	actors storespec.ActorRegistryStore,
	in DeclareRequest,
) error {
	if err := validateDeclareRequest(in); err != nil {
		return err
	}
	var config []byte
	if in.Config != nil {
		config = append(config, (*in.Config)...)
	}
	_, err := actors.Insert(ctx, storespec.ActorDraft{
		Kind:         in.Kind,
		SourceDeclID: in.SourceDeclID,
		CreatedAt:    in.CreatedAt,
		Definition:   storespec.ActorDefinition{Class: in.Class, Config: config},
		Placement:    in.Placement,
	})
	if err != nil {
		return err
	}
	return nil
}

// readOwnerPrincipal reads the channel's one owner pointer from genesis. Owner
// is immutable channel self-truth, so one read at open is the whole story —
// there is no second account to cross-check it against, and the open check
// degenerates to "if this channel has a genesis, its owner is non-empty".
func readOwnerPrincipal(ctx context.Context, store storespec.GenesisStore) (string, error) {
	genesis, found, err := store.ReadGenesis(ctx)
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
				// Two very different findings share this line, so name which
				// one it was: "not_hosted" is a wrong audience (this host has
				// no state for the actor), "no_endpoint_yet" is a timing window
				// (the host knows the actor but it has no body or attached
				// route at this instant — starting, link down, or in backoff).
				// Either way the row is observed and skipped, never retried.
				outcome := "not_delivered"
				switch {
				case errors.Is(err, actorhost.ErrNotHosted):
					outcome = "not_hosted"
				case errors.Is(err, actorhost.ErrNoEndpointYet):
					outcome = "no_endpoint_yet"
				}
				logger.Warn("platform.delivery.outcome",
					"channel", chID, "seq", row.Seq, "envelope", env.ID,
					"audience", id, "outcome", outcome, "err", err)
			}
		}
		return nil
	}
}
