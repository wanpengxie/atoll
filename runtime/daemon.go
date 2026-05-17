package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	kadapter "github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/bootstrap"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/lifecycle"
	"github.com/wanpengxie/ActOS/runtime/scheduler"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/transit"
	"github.com/wanpengxie/ActOS/runtime/trigger"
	"github.com/wanpengxie/ActOS/runtime/workerhost"
)

// DaemonConfig is the cmd/daemon assembly knobs.
type DaemonConfig struct {
	DataDir     string
	ChannelsDir string
	DaemonID    string
	DaemonEpoch int64
	UseMockBus  bool

	// WSConfig is the production daemonbus WS connection knobs. Required
	// when UseMockBus is false. Ignored when UseMockBus is true.
	WSConfig *transit.WSClientConfig

	// HumanCallerSecret is the shared HMAC key matching server.gateway.
	// App.cfg.HumanCallerSecret. Required when the daemon should accept
	// control.write_message frames (production wiring); may be empty in
	// tests that don't drive the write path.
	HumanCallerSecret []byte

	// ReplayWindow caps |now - human_caller.ts|. Zero disables the
	// check (M1.5 default).
	ReplayWindow time.Duration

	// NowFn / FrameIDGen optional — production injects time.Now and uuid.
	NowFn      func() int64
	FrameIDGen func() string

	// SchedulerPeriod overrides the long-pending scheduler tick period.
	// Defaults to 1s (L2 §3.7).
	SchedulerPeriod time.Duration

	// HeartbeatPeriod overrides the daemon → server control.heartbeat
	// cadence. Defaults to transit.DefaultHeartbeatPeriod (15s). Without
	// the sender, server placements drift to `stale` 90s after boot even
	// when the daemon process is alive (M1.6-T1 acceptance #1).
	HeartbeatPeriod time.Duration

	// WorkerSpawner, when non-nil, swaps the bootChannel deliverer
	// handler from the P2 counter stub to a full WorkerManager that
	// spawns + reuses worker subprocesses (M1.6-T1 P4). Tests that
	// don't care about workers leave this nil and keep the stub —
	// runtime/daemon_test relies on the counter probe to assert
	// trigger fan-out without paying the spawn cost.
	WorkerSpawner workerhost.Spawner

	// PostBoot is invoked once phase 4 starts. May be nil. Used by
	// tests to inspect state without racing with shutdown.
	PostBoot func(ctx context.Context, d *Daemon) error

	// OnChannelBoot is invoked at the tail of every per-channel boot
	// (both phase-3 cold start and the hot OnCreateChannel path). cmd/
	// daemon wires this to a framework.Manager constructor so adapters
	// install per channel without the runtime package taking a
	// dependency on adapters/**.
	//
	// May be nil — channels boot without adapter framework hosting.
	// The returned teardown closure (when non-nil) runs on channel
	// unload / daemon shutdown. Returning an error fails the boot and
	// the channel is not added to the active map.
	OnChannelBoot func(ctx context.Context, h ChannelHooks) (teardown func(context.Context) error, err error)

	// ChannelTemplate is the static channel template hosted by this
	// daemon. Currently consumed by bootstrap.Saga to seed extra
	// actor_registry rows (e.g. tool:xhs-adapter for the xhs-creator
	// template). T2 wires a fixed template; L4 will swap this for a
	// template snapshot lookup.
	ChannelTemplate ChannelTemplate
}

// ChannelTemplate is the daemon-side projection of an L4 channel
// template snapshot. For M1.6 it carries only the actor_registry seeds
// the bootstrap saga MUST insert before adapter framework Install runs.
//
// Empty template = no extra actors (channel still gets system +
// initial members seeded by the saga base path).
type ChannelTemplate struct {
	// AdapterActorSeeds lists the tool actor rows the saga inserts in
	// addition to system + initial members. Each row supplies enough
	// fields for kernel/adapter.Manager.Install to find the actor with
	// the right binding.
	AdapterActorSeeds []actor.Record
}

// ChannelAgentID is the well-known actor id every per-channel runtime
// registers on boot. It is the single L2 worker target for trigger
// fan-out: scheduler.Deliverer routes every audience-resolved envelope
// addressed to this id into the per-channel WorkerManager (M1.6-T1 P4).
//
// We use the flat form rather than the spec'd `<channel_id>:<agent>`
// shape because the per-channel sqlite already scopes the id; the L2
// runtime today is single-worker-per-channel so a stable constant is
// sufficient. Multi-agent / sub-agent ids land with M1.4 Part 2.
const ChannelAgentID actor.ActorID = "agent:channel-agent"

// channelAgentDisplayName is the human-readable label seeded alongside
// ChannelAgentID. Surfaces in view-sync rows that join actor_registry
// (display_name nullable, callers fall back to id when empty).
const channelAgentDisplayName = "channel agent"

// channelRuntime is the per-owned-channel set of seams the daemon
// operational loop drives during phase 3+: harness chain (for writes),
// outbox pusher (for view-sync), actor registry (for write_message
// caller-id resolution), message log (for long-pending scans),
// trigger gateway (post-harness fan-out + future-message scheduler
// scan), scheduler.Deliverer (per-actor handler registry — M1.6-T1
// registers a channel-agent handler for ChannelAgentID; M1.6-T2 adds
// adapter framework Manager registering Dispatch handlers via the
// OnChannelBoot hook).
type channelRuntime struct {
	channelID     channel.ID
	db            *sql.DB
	registry      *store.ActorRegistry
	messages      *store.Messages
	ledger        *store.Ledger
	lock          *store.ChannelLock
	outbox        *store.ViewSyncOutbox
	chain         *harness.Chain
	wrappedChain  *postHarnessChain
	pusher        *transit.Pusher
	deliverer     *scheduler.Deliverer
	gateway       *trigger.Gateway
	typeRegistry  *store.TypeRegistry
	requestLookup *store.RequestLookup
	teardown      func(context.Context) error

	// channelAgentID is always the constant ChannelAgentID; cached here
	// so the deliverer handler closure does not need to import the
	// package-level symbol. (M1.6-T1)
	channelAgentID actor.ActorID

	// channelAgentTriggers counts every envelope dispatched to the
	// channel-agent handler. (M1.6-T1)
	channelAgentTriggers atomic.Int64

	// workerManager is non-nil when DaemonConfig.WorkerSpawner is set
	// and bootChannel successfully built the per-channel manager. The
	// channel teardown calls manager.Close so worker subprocesses do
	// not leak past placement reclaim. (M1.6-T1)
	workerManager *workerhost.Manager

	// deviceTransit is the per-channel adapter.DeviceTransit (T147 §A).
	// One instance per channel sharing the daemon's *transit.Client so
	// daemon→server device_transit.send frames carry the SendFrame's
	// channel_id verbatim; inbound device_transit.recv frames are
	// routed back here by handleDeviceTransitFrame (see daemon.go).
	// Nil only when the daemon is constructed without a transit client
	// (defensive — every production wiring sets one up).
	deviceTransit *transit.DeviceTransit

	// deviceCallback receives the decoded SendFrame the framework
	// Manager turns into Module.OnExternalCallback. OnChannelBoot sets
	// it (atomic.Value enables late binding after buildChannelRuntime
	// has already wired the *transit.DeviceTransit). Reads are atomic;
	// "" / nil means "no adapter wired yet — drop silently".
	deviceCallback atomic.Value // func(ctx context.Context, frame kadapter.SendFrame) error
}

// ChannelHooks bundles the per-channel seams exposed to a daemon
// composition root (cmd/daemon) via DaemonConfig.OnChannelBoot. Lets the
// caller construct an adapter framework Manager + module set per
// channel WITHOUT runtime/** taking a dependency on adapters/** (go-
// arch-lint enforces runtime ↛ adapters).
//
// The composition root MAY return a teardown closure that the daemon
// invokes on channel unload / shutdown — typically to call
// framework.Manager.Shutdown.
type ChannelHooks struct {
	// ChannelID is the channel this hook is wiring.
	ChannelID channel.ID

	// DB is the channel-local sqlite handle. Same handle backing every
	// store in this struct — exposed for callers that need to construct
	// new per-channel stores not already in this struct.
	DB *sql.DB

	// ActorRegistry is the channel-local actor.Registry (sqlite-backed).
	ActorRegistry *store.ActorRegistry

	// Messages is the channel-local message log.
	Messages *store.Messages

	// TypeRegistry is the channel-local sqlite type_registry. Implements
	// both kernel/adapter.TypeRegistry (for framework.Manager.Install)
	// AND runtime/harness.TypeRegistry (for chain step 4-8) — daemon
	// already wired the HarnessView projection into HarnessChain.
	TypeRegistry *store.TypeRegistry

	// RequestLookup is the channel-local request lookup over Messages,
	// satisfying kernel/adapter.RequestLookup.
	RequestLookup *store.RequestLookup

	// HarnessChain is the per-channel wrapped chain (kernel/harness.Chain)
	// that stamps fencing + runs the post-harness trigger.Gateway fan-out
	// + messages.MarkDelivered. Adapter framework uses this so its
	// response writes share the same invariants the WriteMessage entry
	// point enforces.
	HarnessChain khar.Chain

	// Deliverer is the per-channel scheduler.Deliverer — the
	// composition root registers a HandlerFn per adapter actor id that
	// calls framework.Manager.Dispatch.
	Deliverer *scheduler.Deliverer

	// NowFn returns unix-ms; same clock the daemon stamps writes with.
	NowFn func() int64

	// DeviceTransit is the per-channel adapter.DeviceTransit handed to
	// `via_server_transit` adapter modules (T147 §A). When the channel
	// boots without a daemonbus transit client (e.g. tests that only
	// exercise the harness write path), this is nil — the composition
	// root MUST skip via_server_transit factories or use a fake. When
	// non-nil, pass it verbatim into framework.ManagerConfig.DeviceTransit
	// so the framework can satisfy adapter modules that declare
	// Binding=via_server_transit at Install time.
	DeviceTransit kadapter.DeviceTransit

	// SetDeviceCallback wires the inbound device→daemon callback for
	// this channel. The composition root calls it after constructing the
	// framework.Manager so device_transit.recv frames routed back from
	// the server land on Manager.OnExternalCallback(adapter, payload).
	// The closure shape mirrors transit.DeviceTransitConfig.OnRecv
	// (frame body, not raw bytes) so callers don't need to parse the
	// adapter.SendFrame envelope themselves. Calling SetDeviceCallback
	// more than once REPLACES the previous binding — that's the M1.6
	// hot-swap escape hatch when a channel's adapter set changes.
	// Passing nil clears the binding (frames are dropped silently).
	SetDeviceCallback func(func(ctx context.Context, frame kadapter.SendFrame) error)
}

// Daemon is the assembled cmd/daemon process. Exposed so tests can
// drive the phases manually.
type Daemon struct {
	cfg        DaemonConfig
	daemonDB   *sql.DB
	channelDBs map[string]*sql.DB
	bootRes    lifecycle.BootResult
	transit    *transit.Client
	bus        *transit.MockBus
	wsClient   *transit.WSClient
	booter     *lifecycle.Bootstrapper
	reconciler *bootstrap.Reconciler
	saga       *bootstrap.Saga
	unloader   *lifecycle.Unloader
	heartbeat  *transit.HeartbeatTracker

	channels   map[channel.ID]*channelRuntime
	cursors    *transit.CursorTracker
	dispatcher *transit.Dispatcher
	schedTimer *scheduler.Timer
	hbSender   *transit.HeartbeatSender

	runCtx    context.Context
	runCancel context.CancelFunc
	wg        sync.WaitGroup

	// owned-channels barrier — phase 3 closes this once per-channel
	// seams are wired so OnWriteMessage can return channel_unbound for
	// any frame that arrives before bootstrap completes.
	ready atomic.Bool
}

// RunDaemon is the cmd/daemon entry point body. Blocks until ctx is
// cancelled or a fatal phase fails.
func RunDaemon(ctx context.Context, cfg DaemonConfig) error {
	d, err := AssembleDaemon(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()

	if err := d.RunPhases(ctx); err != nil {
		return err
	}

	if cfg.PostBoot != nil {
		if err := cfg.PostBoot(ctx, d); err != nil {
			return err
		}
	}

	<-ctx.Done()
	return nil
}

// AssembleDaemon wires the daemon dependencies without starting the
// phases. Returns a Daemon that the caller drives via RunPhases.
func AssembleDaemon(ctx context.Context, cfg DaemonConfig) (*Daemon, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("runtime: DaemonConfig.DataDir empty")
	}
	if cfg.ChannelsDir == "" {
		cfg.ChannelsDir = filepath.Join(cfg.DataDir, "channels")
	}
	if cfg.DaemonID == "" {
		cfg.DaemonID = "daemon-local"
	}
	if cfg.DaemonEpoch == 0 {
		cfg.DaemonEpoch = time.Now().Unix()
	}
	if cfg.NowFn == nil {
		cfg.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.FrameIDGen == nil {
		var n atomic.Int64
		cfg.FrameIDGen = func() string {
			return fmt.Sprintf("f-%d", n.Add(1))
		}
	}
	if cfg.SchedulerPeriod <= 0 {
		cfg.SchedulerPeriod = time.Second
	}
	if !cfg.UseMockBus && cfg.WSConfig == nil {
		return nil, errors.New("runtime: DaemonConfig.WSConfig required when UseMockBus=false")
	}

	daemonDB, err := store.OpenDaemon(ctx, filepath.Join(cfg.DataDir, "daemon.sqlite"), store.OpenOptions{})
	if err != nil {
		return nil, fmt.Errorf("runtime: open daemon sqlite: %w", err)
	}

	reconciler, err := bootstrap.NewReconciler(daemonDB, cfg.NowFn)
	if err != nil {
		_ = daemonDB.Close()
		return nil, err
	}
	saga, err := bootstrap.NewSaga(bootstrap.SagaConfig{
		DaemonDB:          daemonDB,
		ChannelsDir:       cfg.ChannelsDir,
		NowFn:             cfg.NowFn,
		AdapterActorSeeds: cfg.ChannelTemplate.AdapterActorSeeds,
	})
	if err != nil {
		_ = daemonDB.Close()
		return nil, err
	}

	channelDBs := make(map[string]*sql.DB)
	openLock := func(ctx context.Context, sqlitePath string) (*store.ChannelLock, error) {
		if db, ok := channelDBs[sqlitePath]; ok {
			return store.NewChannelLock(db), nil
		}
		db, err := store.OpenChannel(ctx, sqlitePath, store.OpenOptions{SkipDDL: true})
		if err != nil {
			return nil, err
		}
		channelDBs[sqlitePath] = db
		return store.NewChannelLock(db), nil
	}

	booter, err := lifecycle.NewBootstrapper(lifecycle.BootConfig{
		DaemonID:    placement.DaemonID(cfg.DaemonID),
		DaemonEpoch: placement.DaemonEpoch(cfg.DaemonEpoch),
		NowFn:       cfg.NowFn,
		ChannelsDir: cfg.ChannelsDir,
		LockOpener:  openLock,
		// EmitReclaim left nil — T4 wires the WS client; until then the
		// offline path treats all locally-owned channels as still ours.
	})
	if err != nil {
		_ = daemonDB.Close()
		return nil, err
	}

	d := &Daemon{
		cfg:        cfg,
		daemonDB:   daemonDB,
		channelDBs: channelDBs,
		booter:     booter,
		reconciler: reconciler,
		saga:       saga,
		unloader:   lifecycle.NewUnloader(),
		heartbeat:  transit.NewHeartbeatTracker(),
		channels:   make(map[channel.ID]*channelRuntime),
		cursors:    transit.NewCursorTracker(),
	}

	if cfg.UseMockBus {
		d.bus = transit.NewMockBus(64)
		client, err := transit.NewClient(transit.ClientConfig{
			DaemonID: cfg.DaemonID, Transport: d.bus, NowFn: cfg.NowFn,
		})
		if err != nil {
			_ = daemonDB.Close()
			return nil, err
		}
		if _, err := client.Connect(ctx); err != nil {
			_ = daemonDB.Close()
			return nil, err
		}
		d.transit = client
	} else {
		ws, err := transit.NewWSClient(*cfg.WSConfig)
		if err != nil {
			_ = daemonDB.Close()
			return nil, err
		}
		client, err := transit.NewClient(transit.ClientConfig{
			DaemonID: cfg.DaemonID, Transport: ws, NowFn: cfg.NowFn,
		})
		if err != nil {
			_ = ws.Close()
			_ = daemonDB.Close()
			return nil, err
		}
		if _, err := client.Connect(ctx); err != nil {
			_ = ws.Close()
			_ = daemonDB.Close()
			return nil, err
		}
		d.wsClient = ws
		d.transit = client
	}
	return d, nil
}

// RunPhases executes T1.6 phases 1→4 in order.
func (d *Daemon) RunPhases(ctx context.Context) error {
	// Phase 0 (implicit): reconcile crashed sagas.
	if _, err := d.reconciler.Run(ctx); err != nil {
		return fmt.Errorf("runtime: reconcile: %w", err)
	}

	// Phase 1: scan channels/, refresh daemon_epoch.
	if _, err := d.booter.LoadLocal(ctx); err != nil {
		return fmt.Errorf("runtime: phase1 LoadLocal: %w", err)
	}

	// Phase 2: report reclaim (offline path = all owned accepted).
	res, err := d.booter.ReportReclaim(ctx)
	if err != nil {
		return fmt.Errorf("runtime: phase2 ReportReclaim: %w", err)
	}
	d.bootRes = res

	// Phase 3: operational loop — boot per-channel seams, then start
	// the transit dispatcher / pushers / scheduler goroutines.
	if err := d.startPhase3(ctx); err != nil {
		return fmt.Errorf("runtime: phase3 start: %w", err)
	}
	d.booter.MarkRecovering()

	// Phase 4: accept new control.create_channel frames.
	d.booter.MarkAcceptingNew()
	return nil
}

// startPhase3 wires the daemon's operational goroutines: per-channel
// outbox pushers, the transit dispatcher, and the long-pending
// scheduler. All goroutines share a child context cancelled by Close
// so shutdown is deterministic.
func (d *Daemon) startPhase3(ctx context.Context) error {
	if d.runCtx != nil {
		return errors.New("runtime: phase 3 already started")
	}
	d.runCtx, d.runCancel = context.WithCancel(context.Background())

	// 3.1 — per-channel runtime + outbox push.
	acceptedSet := make(map[channel.ID]struct{}, len(d.bootRes.ReclaimAccepted))
	for _, id := range d.bootRes.ReclaimAccepted {
		acceptedSet[id] = struct{}{}
	}
	for _, lc := range d.bootRes.Local {
		if _, ok := acceptedSet[lc.ChannelID]; !ok {
			continue
		}
		if err := d.bootChannel(ctx, lc); err != nil {
			return fmt.Errorf("runtime: channel %s: %w", lc.ChannelID, err)
		}
	}

	// 3.2 — transit dispatcher (one goroutine drains the recv side).
	handler, err := transit.NewWriteMessageHandler(transit.WriteMessageHandlerConfig{
		Secret:       d.cfg.HumanCallerSecret,
		Router:       d.routeWrite,
		NowMs:        d.cfg.NowFn,
		ReplayWindow: d.cfg.ReplayWindow,
	})
	switch {
	case err == nil:
		// proceed
	case len(d.cfg.HumanCallerSecret) == 0:
		// Allow the daemon to boot without the secret — write_message
		// frames will surface as OnWriteMessage=nil (silent drop).
		handler = nil
	default:
		return err
	}

	ackHandler, err := transit.NewAckHandlerForChannels(d.cursors, func(id channel.ID) (transit.OutboxAcker, bool) {
		cr, ok := d.channels[id]
		if !ok {
			return nil, false
		}
		return cr.outbox, true
	})
	if err != nil {
		return fmt.Errorf("runtime: build ack handler: %w", err)
	}

	resyncRouter := func(id channel.ID) (transit.ResyncSource, bool) {
		cr, ok := d.channels[id]
		if !ok {
			return nil, false
		}
		return cr.outbox, true
	}
	resyncServer, err := transit.NewMultiResyncServer(resyncRouter)
	if err != nil {
		return fmt.Errorf("runtime: build resync server: %w", err)
	}

	handlers := transit.ControlHandlers{
		OnViewsyncAck:           ackHandler.Handle,
		OnViewsyncResyncRequest: resyncServer.ServeResync,
		OnCreateChannel:         d.handleCreateChannel,
		OnUnbindChannel:         d.handleUnbindChannel,
		OnHeartbeatAck:          d.heartbeat.Handle(d.cfg.NowFn),
		OnReclaimAccepted:       d.handleReclaimAccepted,
		OnReclaimRejected:       d.handleReclaimRejected,
		// T147 §A — central router for device_transit.* frames. Decodes
		// the SendFrame, looks up the per-channel DeviceTransit by
		// frame.ChannelID, and forwards via DispatchIncoming so the
		// channel's framework.Manager fans out to Module.OnExternalCallback.
		OnDeviceTransit: d.handleDeviceTransitFrame,
	}
	if handler != nil {
		handlers.OnWriteMessage = func(ctx context.Context, _ daemonbus.Frame, body transit.WriteMessageBody) transit.WriteMessageAckBody {
			return handler.Handle(ctx, body)
		}
	}

	dispatcher, err := transit.NewDispatcher(transit.DispatcherConfig{
		Client:   d.transit,
		FrameID:  d.cfg.FrameIDGen,
		Handlers: handlers,
	})
	if err != nil {
		return fmt.Errorf("runtime: build dispatcher: %w", err)
	}
	d.dispatcher = dispatcher
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		if err := dispatcher.Loop(d.runCtx); err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, transit.ErrBusClosed) {
				fmt.Printf("runtime: dispatcher exited: %v\n", err)
			}
		}
	}()

	// 3.3 — long-pending scheduler. M1.5 has no concrete fallback path;
	// the scan callback iterates per-channel hooks (currently a no-op
	// until trigger gateway / T3 fills in the fallback emit). The
	// goroutine still ticks so cmd/daemon proves graceful shutdown.
	timer, err := scheduler.NewTimer(scheduler.TimerConfig{
		Period: d.cfg.SchedulerPeriod,
		NowFn:  d.cfg.NowFn,
		Scan:   d.scanLongPending,
	})
	if err != nil {
		return fmt.Errorf("runtime: build scheduler timer: %w", err)
	}
	d.schedTimer = timer
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		if err := timer.Run(d.runCtx); err != nil {
			if !errors.Is(err, context.Canceled) {
				fmt.Printf("runtime: scheduler exited: %v\n", err)
			}
		}
	}()

	// 3.4 — control.heartbeat sender (M1.6-T1 part A). Without this
	// ticker the daemon's OnHeartbeatAck receipt watermark works fine,
	// but the server never sees a heartbeat → after 90s server.placements
	// flips active → stale even though the daemon process is alive. The
	// sender snapshot-reads d.channels each tick; that's the same
	// lock-free pattern routeWrite uses, see daemon.go:848.
	hb, err := transit.NewHeartbeatSender(transit.HeartbeatSenderConfig{
		Client:   d.transit,
		Period:   d.cfg.HeartbeatPeriod,
		FrameID:  d.cfg.FrameIDGen,
		Channels: d.snapshotOwnedChannels,
	})
	if err != nil {
		return fmt.Errorf("runtime: build heartbeat sender: %w", err)
	}
	d.hbSender = hb
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		if err := hb.Run(d.runCtx); err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, transit.ErrBusClosed) {
				fmt.Printf("runtime: heartbeat sender exited: %v\n", err)
			}
		}
	}()

	d.ready.Store(true)
	return nil
}

// snapshotOwnedChannels returns the current owned-channel ids. Used by
// the heartbeat sender to populate HeartbeatBody.Channels each tick.
// Same lock-free read pattern as routeWrite — the map is mutated only
// by the dispatcher / phase-3 boot goroutines, never concurrently with
// itself.
func (d *Daemon) snapshotOwnedChannels() []channel.ID {
	if len(d.channels) == 0 {
		return nil
	}
	out := make([]channel.ID, 0, len(d.channels))
	for id := range d.channels {
		out = append(out, id)
	}
	return out
}

// buildChannelRuntime opens the per-channel sqlite, constructs every
// seam phase 3 needs (registry / messages / outbox / chain / pusher /
// type_registry / request_lookup / wrapped post-harness chain).
func (d *Daemon) buildChannelRuntime(ctx context.Context, lc lifecycle.LocalChannel) (*channelRuntime, error) {
	db, err := d.openChannelDB(ctx, lc.SQLitePath)
	if err != nil {
		return nil, err
	}
	registry := store.NewActorRegistry(db)
	// FIX-T6 — every channel-local mutation MUST validate (fencing_token,
	// daemon_epoch) inside its sqlite tx. Wire one shared *ChannelLock
	// per channel into both Messages and Ledger so harness.Append and
	// workerhost.handleReserve/Commit go through the gate.
	lock := store.NewChannelLock(db)
	messages := store.NewMessagesWithLock(db, lock)
	ledger := store.NewLedgerWithLock(db, lock)
	outbox := store.NewViewSyncOutbox(db, lc.ChannelID)

	// T2 — sqlite type_registry doubles as the framework.TypeRegistry
	// (adapter install path) and harness.TypeRegistry (harness step 4-8
	// read path). Building both off the same row keeps Install + Write
	// consistent.
	typeRegistry := store.NewTypeRegistry(db, d.cfg.NowFn)
	requestLookup := store.NewRequestLookup(messages, lc.ChannelID)

	chain, err := harness.New(harness.Deps{
		ChannelID:     lc.ChannelID,
		ActorRegistry: registry,
		TypeRegistry:  typeRegistry.HarnessView(),
		Log:           messages,
		NowMs:         d.cfg.NowFn,
	})
	if err != nil {
		return nil, fmt.Errorf("harness for %s: %w", lc.ChannelID, err)
	}

	pusher, err := transit.NewPusher(transit.PusherConfig{
		Outbox:  outbox,
		Client:  d.transit,
		Cursors: d.cursors,
		FrameID: d.cfg.FrameIDGen,
		NowFn:   d.cfg.NowFn,
	})
	if err != nil {
		return nil, fmt.Errorf("pusher for %s: %w", lc.ChannelID, err)
	}

	// Trigger gateway — post-harness fan-out seam (FIX-T3 / L1 §5.1).
	// Deliverer starts empty; OnChannelBoot wires per-adapter Dispatch
	// handlers below.
	deliverer := scheduler.NewDeliverer()
	gw, err := trigger.New(trigger.Config{
		Registry:  registry,
		Deliverer: deliverer,
		NowFn:     d.cfg.NowFn,
	})
	if err != nil {
		return nil, fmt.Errorf("trigger gateway for %s: %w", lc.ChannelID, err)
	}

	// Wrapped chain — same fencing-stamping + post-harness gateway dispatch
	// + MarkDelivered behavior the daemon's WriteMessage handler uses
	// (FIX-T3 / FIX-T6). Shared across the WriteMessage entrypoint AND the
	// adapter framework so adapter responses (and timer-fired failed
	// terminals) flow through the same invariants.
	wrappedChain := &postHarnessChain{
		chain:    chain,
		gateway:  gw,
		messages: messages,
		lock:     lock,
		nowFn:    d.cfg.NowFn,
	}

	cr := &channelRuntime{
		channelID:      lc.ChannelID,
		db:             db,
		registry:       registry,
		messages:       messages,
		ledger:         ledger,
		lock:           lock,
		outbox:         outbox,
		chain:          chain,
		wrappedChain:   wrappedChain,
		pusher:         pusher,
		deliverer:      deliverer,
		gateway:        gw,
		typeRegistry:   typeRegistry,
		requestLookup:  requestLookup,
		channelAgentID: ChannelAgentID,
	}

	// T147 §A — per-channel adapter.DeviceTransit. The instance shares
	// the daemon's *transit.Client (single WS connection to the server
	// daemonbus mux) and owns the inbound SendFrame fan-out. We MUST
	// have a transit client at this point — phase 3 cold-start runs
	// AFTER AssembleDaemon connected it. The OnRecv closure reads
	// cr.deviceCallback at frame-receive time so OnChannelBoot can
	// late-bind the callback after constructing its framework.Manager.
	if d.transit != nil {
		dt, err := transit.NewDeviceTransit(transit.DeviceTransitConfig{
			Client:  d.transit,
			FrameID: d.cfg.FrameIDGen,
			OnRecv: func(ctx context.Context, frame kadapter.SendFrame) error {
				cb, _ := cr.deviceCallback.Load().(func(context.Context, kadapter.SendFrame) error)
				if cb == nil {
					// No adapter wired yet — drop. Production wiring is
					// expected to call SetDeviceCallback during
					// OnChannelBoot, well before the first inbound recv.
					return nil
				}
				return cb(ctx, frame)
			},
		})
		if err != nil {
			return nil, fmt.Errorf("device transit for %s: %w", lc.ChannelID, err)
		}
		cr.deviceTransit = dt
	}

	return cr, nil
}

// ensureChannelAgent guarantees the per-channel actor_registry contains
// the L2 worker target (ChannelAgentID) and registers a deliverer
// handler that the trigger gateway can route post-harness envelopes to.
//
// Idempotency: bootChannel runs on every cold-start (RunPhases) AND every
// hot OnCreateChannel; a daemon restart re-loads existing channel
// sqlite, so we Lookup first and only Insert when missing. Re-running
// Insert would fail the actor_registry PRIMARY KEY.
//
// Handler wiring depends on whether DaemonConfig.WorkerSpawner is set:
//   - nil (P2 default, runtime/daemon_test) → stub handler increments
//     channelAgentTriggers and returns. No worker subprocess.
//   - non-nil (P4 wiring, cmd/daemon) → builds a workerhost.Manager and
//     registers a handler that ticks the counter AND calls
//     manager.OnTrigger so the trigger envelope reaches a real worker.
func (d *Daemon) ensureChannelAgent(ctx context.Context, cr *channelRuntime) error {
	_, ok, err := cr.registry.Lookup(ctx, cr.channelAgentID)
	if err != nil {
		return fmt.Errorf("runtime: ensure channel-agent lookup %s: %w", cr.channelID, err)
	}
	if !ok {
		if err := cr.registry.Insert(ctx, actor.Record{
			ID:          cr.channelAgentID,
			Kind:        message.SenderAgent,
			DisplayName: channelAgentDisplayName,
			CreatedAt:   d.cfg.NowFn(),
		}); err != nil {
			return fmt.Errorf("runtime: ensure channel-agent insert %s: %w", cr.channelID, err)
		}
	}

	// Lock snapshot for fencing. Use the in-process channel lock row;
	// bootChannel guarantees this row is current (cold-start phase 2
	// already refreshed daemon_epoch; hot OnCreateChannel inserts the
	// row in the same tx as the saga).
	lockRow, lockOK, err := cr.lock.Get(ctx)
	if err != nil {
		return fmt.Errorf("runtime: ensure channel-agent lock get %s: %w", cr.channelID, err)
	}
	if !lockOK {
		return fmt.Errorf("runtime: ensure channel-agent missing lock %s", cr.channelID)
	}

	// P4 wire: build manager when a spawner is configured. Otherwise
	// fall back to the P2 counter stub so tests don't pay the spawn cost.
	if d.cfg.WorkerSpawner != nil {
		leaseStore := workerhost.NewLeaseStore(cr.db)
		mgr, err := workerhost.NewManager(workerhost.ManagerConfig{
			ChannelID:     cr.channelID,
			AgentID:       cr.channelAgentID,
			WorkerActorID: cr.channelAgentID,
			Spawner:       d.cfg.WorkerSpawner,
			LeaseStore:    leaseStore,
			Chain:         cr.chain,
			Ledger:        cr.ledger,
			NowFn:         d.cfg.NowFn,
			FencingToken:  lockRow.FencingToken,
			DaemonEpoch:   lockRow.DaemonEpoch,
			ServeCtx:      d.runCtx,
		})
		if err != nil {
			return fmt.Errorf("runtime: ensure channel-agent manager %s: %w", cr.channelID, err)
		}
		cr.workerManager = mgr
		cr.deliverer.Register(cr.channelAgentID, func(ctx context.Context, id actor.ActorID, env *message.Envelope) error {
			cr.channelAgentTriggers.Add(1)
			return mgr.OnTrigger(ctx, id, env)
		})
		return nil
	}

	// P2 fallback — counter-only handler.
	cr.deliverer.Register(cr.channelAgentID, func(_ context.Context, _ actor.ActorID, _ *message.Envelope) error {
		cr.channelAgentTriggers.Add(1)
		return nil
	})
	return nil
}

// ChannelAgentTriggerCount returns the number of times the per-channel
// agent handler has been invoked since boot. Exposed for daemon_test to
// assert M1.6-T1 P2 wiring. Returns -1 when the channel is not owned by
// this daemon (caller can distinguish "no channel" from "no triggers").
func (d *Daemon) ChannelAgentTriggerCount(chID channel.ID) int64 {
	cr, ok := d.channels[chID]
	if !ok {
		return -1
	}
	return cr.channelAgentTriggers.Load()
}

// CurrentWorkerIDFor returns the id of the worker subprocess currently
// alive for the channel, or "" when no worker is alive (manager not
// configured, channel not owned, worker crashed). Test accessor for
// the M1.6-T1 e2e reuse check.
func (d *Daemon) CurrentWorkerIDFor(chID channel.ID) string {
	cr, ok := d.channels[chID]
	if !ok || cr.workerManager == nil {
		return ""
	}
	return cr.workerManager.CurrentWorkerID()
}

// bootChannel wires a per-channel runtime + outbox pusher goroutine
// and registers a teardown function with lifecycle.Unloader. Used by
// both phase-3 cold-start AND the hot OnCreateChannel handler so the
// two paths stay symmetric (single source of "what does it take to
// bring a channel up").
func (d *Daemon) bootChannel(ctx context.Context, lc lifecycle.LocalChannel) error {
	cr, err := d.buildChannelRuntime(ctx, lc)
	if err != nil {
		return err
	}
	d.channels[lc.ChannelID] = cr

	// M1.6-T1 — register the per-channel agent target so trigger gateway
	// fan-out has somewhere to route audience=['*']. Done before the
	// pusher goroutine starts so a write that immediately follows boot
	// still observes the handler.
	if err := d.ensureChannelAgent(ctx, cr); err != nil {
		delete(d.channels, lc.ChannelID)
		return err
	}

	// M1.6-T2 — OnChannelBoot hook lets cmd/daemon wire the adapter
	// framework Manager + register Dispatch handlers on the Deliverer.
	// Run BEFORE starting the pusher goroutine so a failing hook unwinds
	// cleanly (no orphan goroutine).
	if d.cfg.OnChannelBoot != nil {
		hooks := ChannelHooks{
			ChannelID:     lc.ChannelID,
			DB:            cr.db,
			ActorRegistry: cr.registry,
			Messages:      cr.messages,
			TypeRegistry:  cr.typeRegistry,
			RequestLookup: cr.requestLookup,
			// HarnessChain is the post-harness wrapper (fencing + gateway
			// dispatch + MarkDelivered) so adapter responses + framework
			// timer-fired terminals flow through the same invariants the
			// WriteMessage handler enforces.
			HarnessChain: cr.wrappedChain,
			Deliverer:    cr.deliverer,
			NowFn:        d.cfg.NowFn,
		}
		// T147 §A — expose the per-channel DeviceTransit + an inbound
		// callback hook so the composition root can build a
		// via_server_transit-capable framework.Manager.
		if cr.deviceTransit != nil {
			hooks.DeviceTransit = cr.deviceTransit
			// Capture cr in the closure (atomic.Value publishes the new
			// callback to the OnRecv reader without taking a lock).
			capturedCR := cr
			hooks.SetDeviceCallback = func(cb func(context.Context, kadapter.SendFrame) error) {
				if cb == nil {
					// atomic.Value cannot store a typed nil — publish a
					// non-nil sentinel that resolves to "drop" on the
					// reader side.
					capturedCR.deviceCallback.Store(func(context.Context, kadapter.SendFrame) error { return nil })
					return
				}
				capturedCR.deviceCallback.Store(cb)
			}
		}
		teardown, hookErr := d.cfg.OnChannelBoot(ctx, hooks)
		if hookErr != nil {
			delete(d.channels, lc.ChannelID)
			return fmt.Errorf("runtime: channel %s on_boot hook: %w", lc.ChannelID, hookErr)
		}
		cr.teardown = teardown
	}

	// Per-channel context so Unload can stop the pusher independently
	// of the global d.runCtx.
	pusherCtx, pusherCancel := context.WithCancel(d.runCtx)
	d.wg.Add(1)
	go func(p *transit.Pusher, id channel.ID) {
		defer d.wg.Done()
		if err := p.Pump(pusherCtx); err != nil {
			if !errors.Is(err, context.Canceled) {
				// Log via stderr — tests assert on graceful exit so we
				// keep this best-effort. cmd/daemon swaps in structured
				// logging via DaemonConfig in a later ticket.
				fmt.Printf("runtime: pusher %s exited: %v\n", id, err)
			}
		}
	}(cr.pusher, lc.ChannelID)

	// Register the teardown: shut down worker manager (T1) and adapter
	// framework (T2 via cr.teardown), cancel pusher ctx, drop the
	// runtime entry. Sqlite handle stays open — Close() reaps it at
	// shutdown (unbind is not wipe; channels/<id>/ stays on disk).
	chID := lc.ChannelID
	d.unloader.Register(chID, func() error {
		if cr.workerManager != nil {
			closeCtx, cc := context.WithTimeout(context.Background(), 3*time.Second)
			_ = cr.workerManager.Close(closeCtx)
			cc()
		}
		if cr.teardown != nil {
			tctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := cr.teardown(tctx); err != nil {
				fmt.Printf("runtime: channel %s teardown: %v\n", chID, err)
			}
			cancel()
		}
		pusherCancel()
		delete(d.channels, chID)
		d.booter.Unload(chID)
		return nil
	})
	return nil
}

// handleCreateChannel is the OnCreateChannel handler — T0.1. It runs
// the bootstrap saga, writes the channel_lock row (saga step 6
// historically lived in lifecycle.Creator), wires the new channel into
// the daemon's runtime map via lifecycle.Bootstrapper.LoadOne +
// buildChannelRuntime, and emits a control.create_channel_ack with
// every match-field populated so the server's step-5 CAS succeeds
// (kernel/placement.CreateChannelAck.Match).
//
// Failure semantics:
//   - saga.Bootstrap fails → AckRejected (reason=saga error). bootstrap_registry
//     row stays 'in_progress'; reconcile loop rolls it back on next restart.
//   - channel_lock.Insert race (idempotent replay with matching tuple) →
//     branch on existing row: matching tuple ⇒ AckBound idempotent; any
//     mismatch ⇒ AckRejected per lifecycle.Creator branch 2/3/4.
//   - LoadOne / buildChannelRuntime fail → AckRejected; channel_lock row
//     remains so reconciler can drive recovery.
func (d *Daemon) handleCreateChannel(
	ctx context.Context,
	frame daemonbus.Frame,
	req placement.CreateChannelRequest,
) error {
	ack := placement.CreateChannelAck{
		FrameID:         frame.FrameID,
		ChannelID:       req.ChannelID,
		CreateRequestID: req.CreateRequestID,
		OwnerEpoch:      req.OwnerEpoch,
		FencingToken:    req.FencingToken,
		DaemonID:        placement.DaemonID(d.cfg.DaemonID),
		DaemonEpoch:     placement.DaemonEpoch(d.cfg.DaemonEpoch),
	}

	if req.ChannelID == "" {
		ack.Status = placement.AckRejected
		ack.Reason = "empty_channel_id"
		return d.sendCreateAck(ctx, ack)
	}
	if req.CreateRequestID == "" {
		ack.Status = placement.AckRejected
		ack.Reason = "empty_create_request_id"
		return d.sendCreateAck(ctx, ack)
	}

	sqlitePath := filepath.Join(d.cfg.ChannelsDir, string(req.ChannelID), "channel.sqlite")

	// Idempotency / conflict pre-check: if a channel.sqlite already
	// exists on disk and carries a channel_lock row, branch the same
	// way lifecycle.Creator does (FIX-T4 ladder). When the sqlite is
	// absent, fall through to fresh-bootstrap below — OpenChannelLock
	// would otherwise CREATE an empty sqlite file with no DDL, leaving
	// a half-baked channel dir behind.
	sqliteExists := false
	if _, err := os.Stat(sqlitePath); err == nil {
		sqliteExists = true
	}
	if sqliteExists {
		existingLock, err := d.OpenChannelLock(ctx, sqlitePath)
		if err != nil {
			ack.Status = placement.AckRejected
			ack.Reason = fmt.Sprintf("lock open: %v", err)
			return d.sendCreateAck(ctx, ack)
		}
		row, ok, getErr := existingLock.Get(ctx)
		if getErr != nil {
			ack.Status = placement.AckRejected
			ack.Reason = fmt.Sprintf("lock get: %v", getErr)
			return d.sendCreateAck(ctx, ack)
		}
		if ok {
			switch {
			case row.FencingToken < req.FencingToken:
				ack.Status = placement.AckRejected
				ack.Reason = "local_lock_stale_higher_token_received"
				return d.sendCreateAck(ctx, ack)
			case row.FencingToken == req.FencingToken:
				if row.OwnerEpoch != req.OwnerEpoch {
					ack.Status = placement.AckRejected
					ack.Reason = "owner_epoch_mismatch"
					return d.sendCreateAck(ctx, ack)
				}
				if row.DaemonID != placement.DaemonID(d.cfg.DaemonID) {
					ack.Status = placement.AckRejected
					ack.Reason = "daemon_id_mismatch"
					return d.sendCreateAck(ctx, ack)
				}
				// Idempotent replay — make sure the runtime is mounted
				// (re-issued create after a restart with the same tuple).
				if !d.HasChannel(req.ChannelID) {
					if err := d.mountExistingChannel(ctx, req.ChannelID, sqlitePath); err != nil {
						ack.Status = placement.AckRejected
						ack.Reason = fmt.Sprintf("mount existing: %v", err)
						return d.sendCreateAck(ctx, ack)
					}
				}
				ack.Status = placement.AckBound
				return d.sendCreateAck(ctx, ack)
			case row.FencingToken > req.FencingToken:
				ack.Status = placement.AckRejected
				ack.Reason = "stale_request"
				return d.sendCreateAck(ctx, ack)
			}
		}
	}

	// Fresh bootstrap path: run the saga (steps 1-5,7) then insert
	// channel_lock (step 6 — daemon-side equivalent of lifecycle.Creator).
	if _, err := d.saga.Bootstrap(ctx, req.ChannelID, req); err != nil {
		ack.Status = placement.AckRejected
		ack.Reason = fmt.Sprintf("saga: %v", err)
		return d.sendCreateAck(ctx, ack)
	}
	lockStore, err := d.OpenChannelLock(ctx, sqlitePath)
	if err != nil {
		ack.Status = placement.AckRejected
		ack.Reason = fmt.Sprintf("lock open: %v", err)
		return d.sendCreateAck(ctx, ack)
	}
	now := d.cfg.NowFn()
	if err := lockStore.Insert(ctx, store.ChannelLockRow{
		ChannelID:    req.ChannelID,
		FencingToken: req.FencingToken,
		OwnerEpoch:   req.OwnerEpoch,
		DaemonID:     placement.DaemonID(d.cfg.DaemonID),
		DaemonEpoch:  placement.DaemonEpoch(d.cfg.DaemonEpoch),
		AcquiredAt:   now,
		RefreshedAt:  now,
	}); err != nil {
		ack.Status = placement.AckRejected
		ack.Reason = fmt.Sprintf("lock insert: %v", err)
		return d.sendCreateAck(ctx, ack)
	}

	if err := d.mountExistingChannel(ctx, req.ChannelID, sqlitePath); err != nil {
		ack.Status = placement.AckRejected
		ack.Reason = fmt.Sprintf("mount: %v", err)
		return d.sendCreateAck(ctx, ack)
	}
	ack.Status = placement.AckBound
	return d.sendCreateAck(ctx, ack)
}

// mountExistingChannel hot-loads a channel that has its bootstrap saga
// + channel_lock already on disk: it runs lifecycle.Bootstrapper.LoadOne
// + bootChannel so the new channel id becomes routable for
// OnWriteMessage / viewsync without restarting the daemon.
func (d *Daemon) mountExistingChannel(ctx context.Context, id channel.ID, sqlitePath string) error {
	lc, err := d.booter.LoadOne(ctx, id, sqlitePath)
	if err != nil {
		return err
	}
	return d.bootChannel(ctx, lc)
}

// sendCreateAck wraps the transit Send for a CreateChannelAck. Pulled
// out so handleCreateChannel doesn't repeat the boilerplate.
func (d *Daemon) sendCreateAck(ctx context.Context, ack placement.CreateChannelAck) error {
	return d.transit.Send(ctx, d.cfg.FrameIDGen(),
		daemonbus.FrameTypeControlCreateChannelAck, ack)
}

// unbindChannelBody is the minimal payload shape this daemon
// understands for a control.unbind_channel frame. The kernel package
// does not yet define a typed payload for this frame (M1.6 scope), so
// we decode an inline JSON struct. The server only needs frame_id +
// channel_id round-tripped on the ack.
type unbindChannelBody struct {
	FrameID   string     `json:"frame_id"`
	ChannelID channel.ID `json:"channel_id"`
}

// unbindChannelAckBody is the response payload sent back as
// control.unbind_channel_ack.
type unbindChannelAckBody struct {
	FrameID   string     `json:"frame_id"`
	ChannelID channel.ID `json:"channel_id"`
	Status    string     `json:"status"` // "unbound" | "not_found"
	Reason    string     `json:"reason,omitempty"`
}

// handleUnbindChannel — T0.2. Closes the per-channel pusher
// goroutine (via lifecycle.Unloader teardown), drops the runtime
// entry from d.channels, and emits control.unbind_channel_ack. Does
// NOT delete channels/<id>/ on disk — that's the GC schedule, not the
// unbind path.
func (d *Daemon) handleUnbindChannel(ctx context.Context, frame daemonbus.Frame) error {
	var body unbindChannelBody
	if len(frame.Payload) > 0 {
		if err := json.Unmarshal(frame.Payload, &body); err != nil {
			return fmt.Errorf("runtime: decode unbind_channel: %w", err)
		}
	}
	if body.FrameID == "" {
		body.FrameID = frame.FrameID
	}
	ack := unbindChannelAckBody{FrameID: body.FrameID, ChannelID: body.ChannelID}
	if body.ChannelID == "" {
		ack.Status = "not_found"
		ack.Reason = "empty_channel_id"
		return d.transit.Send(ctx, d.cfg.FrameIDGen(),
			daemonbus.FrameTypeControlUnbindChannelAck, ack)
	}
	if !d.HasChannel(body.ChannelID) {
		ack.Status = "not_found"
		return d.transit.Send(ctx, d.cfg.FrameIDGen(),
			daemonbus.FrameTypeControlUnbindChannelAck, ack)
	}
	if err := d.unloader.Unload(ctx, body.ChannelID, lifecycle.UnloadOrphan); err != nil {
		ack.Status = "not_found"
		ack.Reason = err.Error()
		return d.transit.Send(ctx, d.cfg.FrameIDGen(),
			daemonbus.FrameTypeControlUnbindChannelAck, ack)
	}
	ack.Status = "unbound"
	return d.transit.Send(ctx, d.cfg.FrameIDGen(),
		daemonbus.FrameTypeControlUnbindChannelAck, ack)
}

// reclaimAcceptedBody mirrors the wire payload server/gateway emits:
// {"daemon_id":..., "decisions":[ReclaimDecision...]}
type reclaimAcceptedBody struct {
	DaemonID  string                      `json:"daemon_id"`
	Decisions []placement.ReclaimDecision `json:"decisions"`
}

// handleReclaimAccepted — T0.4. The server bundles per-channel
// reclaim decisions inside a single control.reclaim_accepted frame
// (server/gateway/handlers.go), so this handler iterates each decision
// and updates the bootstrap.Reconciler watermark; rejected entries
// trigger the per-channel unload path to prevent zombie writes.
func (d *Daemon) handleReclaimAccepted(ctx context.Context, frame daemonbus.Frame) error {
	var body reclaimAcceptedBody
	if len(frame.Payload) > 0 {
		if err := json.Unmarshal(frame.Payload, &body); err != nil {
			return fmt.Errorf("runtime: decode reclaim_accepted: %w", err)
		}
	}
	for _, dec := range body.Decisions {
		if dec.Accepted {
			d.reconciler.AcceptReclaim(dec.ChannelID)
			continue
		}
		d.reconciler.RejectReclaim(dec.ChannelID, dec.Reason)
		// Trigger per-channel unload — zombie writes are forbidden once
		// the server has revoked our ownership claim.
		if d.HasChannel(dec.ChannelID) {
			if err := d.unloader.Unload(ctx, dec.ChannelID, lifecycle.UnloadStale); err != nil {
				fmt.Printf("runtime: reclaim reject unload %s: %v\n", dec.ChannelID, err)
			}
		}
	}
	return nil
}

// reclaimRejectedBody mirrors the alternative wire payload some servers
// may emit as a dedicated frame_type. Currently the M1.5 server bundles
// rejections inside control.reclaim_accepted, so this handler is a
// future-proof companion that drives the same Reject path per channel.
type reclaimRejectedBody struct {
	DaemonID  string                      `json:"daemon_id"`
	Decisions []placement.ReclaimDecision `json:"decisions"`
	ChannelID channel.ID                  `json:"channel_id,omitempty"`
	Reason    string                      `json:"reason,omitempty"`
}

// handleReclaimRejected — T0.4 companion. Accepts both the
// per-channel single-shot shape ({channel_id, reason}) and the
// bundled decisions shape so the handler is tolerant to either server
// revision.
func (d *Daemon) handleReclaimRejected(ctx context.Context, frame daemonbus.Frame) error {
	var body reclaimRejectedBody
	if len(frame.Payload) > 0 {
		if err := json.Unmarshal(frame.Payload, &body); err != nil {
			return fmt.Errorf("runtime: decode reclaim_rejected: %w", err)
		}
	}
	rejected := body.Decisions
	if len(rejected) == 0 && body.ChannelID != "" {
		rejected = []placement.ReclaimDecision{
			{ChannelID: body.ChannelID, Accepted: false, Reason: body.Reason},
		}
	}
	for _, dec := range rejected {
		d.reconciler.RejectReclaim(dec.ChannelID, dec.Reason)
		if d.HasChannel(dec.ChannelID) {
			if err := d.unloader.Unload(ctx, dec.ChannelID, lifecycle.UnloadStale); err != nil {
				fmt.Printf("runtime: reclaim_rejected unload %s: %v\n", dec.ChannelID, err)
			}
		}
	}
	return nil
}

// openChannelDB reuses the cache populated by lifecycle.LockOpener.
// Subsequent calls return the same *sql.DB so the channel sqlite
// stays single-handle (sqlite WAL works best that way).
func (d *Daemon) openChannelDB(ctx context.Context, sqlitePath string) (*sql.DB, error) {
	if db, ok := d.channelDBs[sqlitePath]; ok {
		return db, nil
	}
	db, err := store.OpenChannel(ctx, sqlitePath, store.OpenOptions{SkipDDL: true})
	if err != nil {
		return nil, err
	}
	d.channelDBs[sqlitePath] = db
	return db, nil
}

// routeWrite returns the per-channel harness chain + registry +
// caller-context stamper for the WriteMessage handler. ok=false means
// the daemon does not currently own the channel.
//
// The returned HarnessChain is `transitChainBridge` wrapping the
// channel's postHarnessChain — after a successful chain.Write it
// invokes trigger.Gateway.Dispatch (L1 §5.1 post-harness fan-out) +
// messages.MarkDelivered. This is the FIX-T3 post-harness wiring seam
// — dedupe / deferred / reject paths skip dispatch so we honor
// at-least-once-by-message.id (§6.2).
func (d *Daemon) routeWrite(_ context.Context, ch channel.ID) (transit.HarnessChain, actor.Registry, transit.CallerStamper, bool) {
	cr, ok := d.channels[ch]
	if !ok {
		return nil, nil, nil, false
	}
	stamp := func(ctx context.Context, actorID actor.ActorID, chID channel.ID) context.Context {
		return harness.CtxWithCaller(ctx, harness.CallerContext{
			ActorID:                 actorID,
			ChannelID:               chID,
			AllowProvidedSenderKind: false,
		})
	}
	return transitChainBridge{inner: cr.wrappedChain}, cr.registry, stamp, true
}

// handleDeviceTransitFrame is the central daemonbus dispatcher hook for
// device_transit.* frames (T147 §A). The transit.Dispatcher routes any
// CategoryDeviceTransit frame here; we decode the SendFrame envelope,
// look up the per-channel runtime by frame.ChannelID, and hand the
// frame to the channel's *transit.DeviceTransit for the recv-side
// fan-out. Ack / Error frames don't carry channel_id at the kernel
// level — for the M1.6 baseline (1 channel ↔ 1 device), we fan them
// out to EVERY channel's DeviceTransit; each channel's correlation
// tracker decides whether the frame matches an outstanding request.
//
// Unknown channels / disabled hooks are dropped silently — at-least-
// once delivery means callers will retry if the daemon is not ready.
func (d *Daemon) handleDeviceTransitFrame(ctx context.Context, frame daemonbus.Frame) error {
	switch frame.FrameType {
	case daemonbus.FrameTypeDeviceTransitSend, daemonbus.FrameTypeDeviceTransitRecv:
		// SendFrame / recv share the same payload — decode once to
		// extract the routing key (channel_id).
		var payload kadapter.SendFrame
		if err := transit.DecodePayload(frame, &payload); err != nil {
			return fmt.Errorf("runtime: decode device_transit %s: %w", frame.FrameType, err)
		}
		cr, ok := d.channels[payload.ChannelID]
		if !ok || cr.deviceTransit == nil {
			// Unknown channel or no device transit wired — drop.
			return nil
		}
		return cr.deviceTransit.DispatchIncoming(ctx, frame)
	case daemonbus.FrameTypeDeviceTransitAck, daemonbus.FrameTypeDeviceTransitError:
		// No channel_id on ack/error — fan out to every channel; each
		// channel's DeviceTransit checks frame_id correlation locally.
		// Returns the first non-nil error so observability still surfaces
		// the failure; downstream channels are best-effort.
		var firstErr error
		for _, cr := range d.channels {
			if cr.deviceTransit == nil {
				continue
			}
			if err := cr.deviceTransit.DispatchIncoming(ctx, frame); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
	return nil
}

// transitChainBridge adapts a kernel/harness.Chain to the transit
// package's HarnessChain shape (transit.HarnessWriteResult). Keeps the
// transit package decoupled from kernel/harness while letting the
// daemon share one wrapped chain instance across the WriteMessage
// handler AND the adapter framework hook.
type transitChainBridge struct {
	inner khar.Chain
}

func (b transitChainBridge) Write(ctx context.Context, env *message.Envelope) (transit.HarnessWriteResult, error) {
	res, err := b.inner.Write(ctx, env)
	if err != nil {
		return transit.HarnessWriteResult{}, err
	}
	return transit.HarnessWriteResult{
		MessageID:        res.MessageID,
		Seq:              res.Seq,
		Deduped:          res.Deduped,
		RejectReason:     string(res.RejectReason),
		RejectDetail:     res.RejectDetail,
		PartialMessageID: res.PartialMessageID,
	}, nil
}

// scanLongPending is the per-tick callback the long-pending scheduler
// invokes (registered as TimerConfig.Scan). It runs TWO passes per
// owned channel — each pass is independent and per-channel errors are
// logged-and-skipped so a single bad row cannot abort the daemon loop
// (at-least-once contract, L1 §6.1):
//
//  1. FIX-T3 future-message drain (L1 §5.3) — every row with
//     `not_before <= now AND delivered_at IS NULL` is fed through
//     trigger.Gateway.Dispatch + messages.MarkDelivered. This makes
//     `emit not_before=future` honour its delayed-delivery contract
//     across daemon restarts (the persisted row remains scannable
//     until MarkDelivered stamps it).
//
//  2. L1 §6.4 long-pending fallback (T147) — every request that has
//     blown past its expires_at deadline without earning a terminal
//     response gets a synthesised failed terminal whose reason depends
//     on the receiver:
//
//     - audience[0] resolves to an active `agent` / `system` actor →
//     reason='unanswered_timeout' (the agent silently dropped it).
//     - audience[0] resolves to a `tool` actor → SKIP: the adapter
//     framework F3 timer is the authoritative source for tool-
//     receiver timeouts. Emitting from both paths would race the
//     `ux_terminal_response_per_request` UNIQUE constraint and
//     bombard the loser with a harness `terminal_duplicate` reject
//     even though business semantics are identical. This is the
//     ticket's MUTUAL-EXCLUSION rule.
//     - audience[0] resolves to a `human` actor → SKIP: humans do not
//     have an SLA in the baseline spec (channel-config-driven human
//     timeouts are M1.7 scope).
//     - audience[0] is missing from actor_registry, OR is soft-
//     deregistered → reason='receiver_unavailable' (the request
//     can never be answered because nobody is listening). NOTE: per
//     L1 §6.4 this should fire IMMEDIATELY rather than waiting for
//     expires_at. M1.6 simplifies by piggybacking on the expires_at
//     scan; the standalone "deregister-time immediate emit" scan is
//     deferred to a later ticket to keep T147 surface tight.
//
// The synthesised response uses the system actor as sender (the canonical
// "harness wrote this on the channel's behalf" identity per L1 §3.2) and
// a deterministic id derived from the request id + reason so re-firing
// the scan idempotently dedupes via harness step 0.5.
func (d *Daemon) scanLongPending(ctx context.Context, nowMs int64) error {
	for chID, cr := range d.channels {
		if cr.gateway == nil || cr.messages == nil {
			continue
		}

		// Pass 1 — §5.3 future-message drain.
		due, err := cr.messages.PendingDue(ctx, nowMs, 64)
		if err != nil {
			fmt.Printf("runtime: scheduler scan %s: %v\n", chID, err)
		}
		for i := range due {
			env := due[i]
			// scheduler is the dispatch-path upstream — bypass §5.1
			// step 3 self-trigger ban (L1 §5.3 explicit semantics).
			if _, err := cr.gateway.Dispatch(ctx, &env, trigger.Options{BypassSelfTriggerBan: true}); err != nil {
				fmt.Printf("runtime: scheduler dispatch %s/%s: %v\n", chID, env.ID, err)
				continue
			}
			if err := cr.messages.MarkDelivered(ctx, env.ID, nowMs); err != nil {
				fmt.Printf("runtime: scheduler mark delivered %s/%s: %v\n", chID, env.ID, err)
			}
		}

		// Pass 2 — §6.4 long-pending fallback emit.
		overdue, err := cr.messages.LongPendingRequests(ctx, nowMs, 64)
		if err != nil {
			fmt.Printf("runtime: scheduler long-pending %s: %v\n", chID, err)
			continue
		}
		for i := range overdue {
			req := overdue[i]
			if err := d.emitLongPendingFallback(ctx, cr, &req, nowMs); err != nil {
				fmt.Printf("runtime: scheduler long-pending emit %s/%s: %v\n", chID, req.ID, err)
				continue
			}
		}
	}
	return nil
}

// longPendingPayload is the synthesised response payload the scheduler
// writes for L1 §6.4 fallback. The shape MUST match adapter framework's
// terminalPayload (status/reason/detail) so consumers reading either
// path see one schema.
type longPendingPayload struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

// emitLongPendingFallback synthesises one failed terminal response for
// an overdue request. Classification (skip vs. emit + which reason) is
// driven by the receiver actor record. The write goes through the
// channel's wrappedChain so the post-harness fan-out + MarkDelivered +
// fencing-stamp invariants are honoured identically to the WriteMessage
// entrypoint AND the adapter framework's Respond path.
func (d *Daemon) emitLongPendingFallback(
	ctx context.Context,
	cr *channelRuntime,
	req *message.Envelope,
	nowMs int64,
) error {
	if len(req.Audience) == 0 {
		// L1 §5.1 invariant: every request lands on at least one resolved
		// audience entry by the time it's persisted. A zero-length slice
		// here would be a harness bug — log and skip rather than fabricate
		// a recipient.
		return fmt.Errorf("request %s has empty audience", req.ID)
	}
	// M1.6 baseline: 1 channel ↔ 1 device, 1 receiver per request. The
	// audience[0] entry is the canonical receiver for the SLA decision.
	// Multi-receiver fan-out (audience=['*'] expansions) is M1.7 scope.
	receiverID := actor.ActorID(req.Audience[0])

	reason, emit, err := d.classifyLongPendingReason(ctx, cr, receiverID)
	if err != nil {
		return fmt.Errorf("classify %s: %w", req.ID, err)
	}
	if !emit {
		return nil
	}

	payload, err := json.Marshal(longPendingPayload{Status: "failed", Reason: string(reason)})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	// Deterministic id keeps the scan idempotent across ticks: every
	// re-fire builds the same id, hits harness step 0.5 dedupe, and
	// short-circuits (canonical-hash-equal → Deduped=true).
	envID := "response:" + req.ID + ":sys-" + string(reason)

	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = req.ID
	}

	env := &message.Envelope{
		ID:            envID,
		TS:            nowMs,
		ChannelID:     req.ChannelID,
		Sender:        message.Sender{Kind: message.SenderSystem, ID: string(actor.SystemActorID)},
		Kind:          message.KindResponse,
		Type:          req.Type,
		Payload:       payload,
		ParentID:      req.ID,
		CorrelationID: correlationID,
		Visibility:    req.Visibility,
		Audience:      []string{req.Sender.ID},
	}

	// The scheduler is a system caller; stamp the harness context with
	// the system actor + permit kind pre-fill (we set Kind=SenderSystem
	// above so the registry-truth overwrite path remains exact-match).
	chainCtx := harness.CtxWithCaller(ctx, harness.CallerContext{
		ActorID:                 actor.SystemActorID,
		ChannelID:               channel.ID(req.ChannelID),
		AllowProvidedSenderKind: true,
	})

	res, err := cr.wrappedChain.Write(chainCtx, env)
	if err != nil {
		return fmt.Errorf("chain write: %w", err)
	}
	// terminal_duplicate is benign — another path (adapter framework
	// timer racing the scheduler) won the One-Law-uniqueness; the
	// scheduler's job is done either way.
	if res.RejectReason != "" && res.RejectReason != message.HarnessTerminalDuplicate {
		return fmt.Errorf("rejected: %s (%s)", res.RejectReason, res.RejectDetail)
	}
	return nil
}

// classifyLongPendingReason inspects the receiver's actor_registry row
// and returns (reason, emit?, err) per the L1 §6.4 dispatch matrix.
// See scanLongPending's pass-2 docstring for the rule set.
func (d *Daemon) classifyLongPendingReason(
	ctx context.Context,
	cr *channelRuntime,
	receiverID actor.ActorID,
) (message.TerminalFailureReason, bool, error) {
	rec, ok, err := cr.registry.Lookup(ctx, receiverID)
	if err != nil {
		return "", false, err
	}
	if !ok {
		// Receiver never registered → request will never be answered.
		return message.TerminalReceiverUnavailable, true, nil
	}
	if rec.DeregisteredAt != 0 {
		// Receiver soft-deregistered → request will never be answered.
		return message.TerminalReceiverUnavailable, true, nil
	}
	switch rec.Kind {
	case message.SenderTool:
		// Adapter framework F3 timer owns this case — MUTUAL EXCLUSION.
		return "", false, nil
	case message.SenderHuman:
		// Baseline: humans do not have an SLA in M1.6.
		return "", false, nil
	case message.SenderAgent, message.SenderSystem:
		return message.TerminalUnansweredTimeout, true, nil
	default:
		// Unknown kind — defensive log + skip. The CHECK constraint on
		// actor_registry.actor_kind makes this branch unreachable in
		// production but we don't want a future kind expansion to
		// accidentally emit the wrong reason.
		return "", false, nil
	}
}

// postHarnessChain wraps the bare runtime/harness.Chain with the per-
// channel post-harness fan-out invariants every daemon-side caller
// must obey (WriteMessage handler + adapter framework + future write-
// path callers):
//
//   - FIX-T6: stamp the channel's current (fencing_token, daemon_epoch)
//     tuple before delegating so the messages-insert tx validates the
//     fencing pair (a silent placement reclaim under us surfaces as
//     HarnessWorkerFencingStale instead of corrupting outbox).
//   - FIX-T3: after a successful, non-deduped, non-rejected write call
//     trigger.Gateway.Dispatch (L1 §5.1 fan-out) + messages.MarkDelivered.
//     Dedupe / deferred / reject paths skip dispatch.
//
// Implements kernel/harness.Chain so the framework Manager can consume
// the same wrapper without taking a dependency on the daemon package.
type postHarnessChain struct {
	chain    *harness.Chain
	gateway  *trigger.Gateway
	messages *store.Messages
	lock     *store.ChannelLock
	nowFn    func() int64
}

// Write implements kernel/harness.Chain.
func (a *postHarnessChain) Write(ctx context.Context, env *message.Envelope) (khar.WriteResult, error) {
	if a.lock != nil {
		if row, ok, err := a.lock.Get(ctx); err == nil && ok {
			ctx = store.CtxWithFencing(ctx, row.FencingToken, row.DaemonEpoch)
		}
	}
	res, err := a.chain.Write(ctx, env)
	if err != nil {
		return khar.WriteResult{}, err
	}
	if res.RejectReason == "" && !res.Deduped && a.gateway != nil {
		dr, derr := a.gateway.Dispatch(ctx, env, trigger.Options{})
		if derr != nil {
			fmt.Printf("runtime: gateway dispatch %s: %v\n", env.ID, derr)
		} else if !dr.Deferred && a.messages != nil {
			if err := a.messages.MarkDelivered(ctx, env.ID, a.nowFn()); err != nil {
				fmt.Printf("runtime: mark delivered %s: %v\n", env.ID, err)
			}
		}
	}
	return res, nil
}

// compile-time interface checks.
var (
	_ khar.Chain = (*harness.Chain)(nil)
	_ khar.Chain = (*postHarnessChain)(nil)
)

// Phase returns the current boot phase (for tests / observability).
func (d *Daemon) Phase() lifecycle.Phase { return d.booter.Phase() }

// BootResult returns the phase-2 reclaim outcome.
func (d *Daemon) BootResult() lifecycle.BootResult { return d.bootRes }

// Saga exposes the channel bootstrap saga (used by lifecycle.Creator).
func (d *Daemon) Saga() *bootstrap.Saga { return d.saga }

// Reconciler exposes the bootstrap reconciler — used by tests + the
// future M1-FIX-T4 reclaim wiring.
func (d *Daemon) Reconciler() *bootstrap.Reconciler { return d.reconciler }

// Heartbeat exposes the heartbeat-ack tracker.
func (d *Daemon) Heartbeat() *transit.HeartbeatTracker { return d.heartbeat }

// Unloader exposes the lifecycle unloader (used by tests + future
// adapter manager teardown paths).
func (d *Daemon) Unloader() *lifecycle.Unloader { return d.unloader }

// DaemonDB exposes the daemon-level sqlite (tests / future server.go).
func (d *Daemon) DaemonDB() *sql.DB { return d.daemonDB }

// Transit returns the daemonbus client. Non-nil after AssembleDaemon
// regardless of UseMockBus.
func (d *Daemon) Transit() *transit.Client { return d.transit }

// Bus returns the underlying MockBus (nil unless UseMockBus).
func (d *Daemon) Bus() *transit.MockBus { return d.bus }

// Dispatcher returns the transit dispatcher started in phase 3.
func (d *Daemon) Dispatcher() *transit.Dispatcher { return d.dispatcher }

// FrameIDGen returns the configured frame id generator.
func (d *Daemon) FrameIDGen() func() string { return d.cfg.FrameIDGen }

// HasChannel reports whether channelID is currently owned by this
// daemon (phase 3 booted it).
func (d *Daemon) HasChannel(id channel.ID) bool {
	_, ok := d.channels[id]
	return ok
}

// OpenChannelLock returns the channel_lock store for a channel sqlite
// path (cached). Useful for lifecycle.Creator / FencingChecker wiring
// in tests.
func (d *Daemon) OpenChannelLock(ctx context.Context, sqlitePath string) (*store.ChannelLock, error) {
	if db, ok := d.channelDBs[sqlitePath]; ok {
		return store.NewChannelLock(db), nil
	}
	db, err := store.OpenChannel(ctx, sqlitePath, store.OpenOptions{SkipDDL: true})
	if err != nil {
		return nil, err
	}
	d.channelDBs[sqlitePath] = db
	return store.NewChannelLock(db), nil
}

// Close cancels phase-3 goroutines and releases all open sqlite
// handles + the transit bus.
func (d *Daemon) Close() error {
	if d.runCancel != nil {
		d.runCancel()
	}
	if d.bus != nil {
		_ = d.bus.Close()
	}
	if d.wsClient != nil {
		_ = d.wsClient.Close()
	}
	// Wait for phase 3 goroutines to finish BEFORE closing the
	// per-channel sqlite handles — sqlite "database is locked" can
	// otherwise leak.
	d.wg.Wait()
	for _, db := range d.channelDBs {
		_ = db.Close()
	}
	if d.daemonDB != nil {
		_ = d.daemonDB.Close()
	}
	return nil
}
