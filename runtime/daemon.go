package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
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

	// PostBoot is invoked once phase 4 starts. May be nil. Used by
	// tests to inspect state without racing with shutdown.
	PostBoot func(ctx context.Context, d *Daemon) error
}

// channelRuntime is the per-owned-channel set of seams the daemon
// operational loop drives during phase 3+: harness chain (for writes),
// outbox pusher (for view-sync), actor registry (for write_message
// caller-id resolution), message log (for long-pending scans),
// trigger gateway (post-harness fan-out + future-message scheduler
// scan), scheduler.Deliverer (per-actor handler registry — the actual
// handler wiring lands with T4/T5 adapter framework).
type channelRuntime struct {
	channelID channel.ID
	db        *sql.DB
	registry  *store.ActorRegistry
	messages  *store.Messages
	outbox    *store.ViewSyncOutbox
	chain     *harness.Chain
	pusher    *transit.Pusher
	deliverer *scheduler.Deliverer
	gateway   *trigger.Gateway
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

	channels   map[channel.ID]*channelRuntime
	cursors    *transit.CursorTracker
	dispatcher *transit.Dispatcher
	schedTimer *scheduler.Timer

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
		DaemonDB: daemonDB, ChannelsDir: cfg.ChannelsDir, NowFn: cfg.NowFn,
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
		cr, err := d.buildChannelRuntime(ctx, lc)
		if err != nil {
			return fmt.Errorf("runtime: channel %s: %w", lc.ChannelID, err)
		}
		d.channels[lc.ChannelID] = cr
		d.wg.Add(1)
		go func(p *transit.Pusher, id channel.ID) {
			defer d.wg.Done()
			if err := p.Pump(d.runCtx); err != nil {
				if !errors.Is(err, context.Canceled) {
					// Log via stderr — tests assert on graceful exit so we
					// keep this best-effort. cmd/daemon swaps in structured
					// logging via DaemonConfig in a later ticket.
					fmt.Printf("runtime: pusher %s exited: %v\n", id, err)
				}
			}
		}(cr.pusher, lc.ChannelID)
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

	d.ready.Store(true)
	return nil
}

// buildChannelRuntime opens the per-channel sqlite, constructs every
// seam phase 3 needs (registry / messages / outbox / chain / pusher).
func (d *Daemon) buildChannelRuntime(ctx context.Context, lc lifecycle.LocalChannel) (*channelRuntime, error) {
	db, err := d.openChannelDB(ctx, lc.SQLitePath)
	if err != nil {
		return nil, err
	}
	registry := store.NewActorRegistry(db)
	messages := store.NewMessages(db)
	outbox := store.NewViewSyncOutbox(db, lc.ChannelID)

	chain, err := harness.New(harness.Deps{
		ChannelID:     lc.ChannelID,
		ActorRegistry: registry,
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
	// Deliverer starts empty; T4/T5 adapter framework will register per-
	// actor handlers as agents/tools spawn.
	deliverer := scheduler.NewDeliverer()
	gw, err := trigger.New(trigger.Config{
		Registry:  registry,
		Deliverer: deliverer,
		NowFn:     d.cfg.NowFn,
	})
	if err != nil {
		return nil, fmt.Errorf("trigger gateway for %s: %w", lc.ChannelID, err)
	}

	return &channelRuntime{
		channelID: lc.ChannelID,
		db:        db,
		registry:  registry,
		messages:  messages,
		outbox:    outbox,
		chain:     chain,
		pusher:    pusher,
		deliverer: deliverer,
		gateway:   gw,
	}, nil
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
// The returned HarnessChain is a `harnessChainAdapter` that, after a
// successful chain.Write, invokes trigger.Gateway.Dispatch (L1 §5.1
// post-harness fan-out) + messages.MarkDelivered. This is the FIX-T3
// post-harness wiring seam — dedupe / deferred / reject paths skip
// dispatch so we honor at-least-once-by-message.id (§6.2).
func (d *Daemon) routeWrite(_ context.Context, ch channel.ID) (transit.HarnessChain, actor.Registry, transit.CallerStamper, bool) {
	cr, ok := d.channels[ch]
	if !ok {
		return nil, nil, nil, false
	}
	chainAdapter := harnessChainAdapter{
		chain:    cr.chain,
		gateway:  cr.gateway,
		messages: cr.messages,
		nowFn:    d.cfg.NowFn,
	}
	stamp := func(ctx context.Context, actorID actor.ActorID, chID channel.ID) context.Context {
		return harness.CtxWithCaller(ctx, harness.CallerContext{
			ActorID:                 actorID,
			ChannelID:               chID,
			AllowProvidedSenderKind: false,
		})
	}
	return chainAdapter, cr.registry, stamp, true
}

// scanLongPending is the per-tick callback the long-pending scheduler
// invokes (registered as TimerConfig.Scan). It performs the FIX-T3
// future-message drain (L1 §5.3): every row with `not_before <= now AND
// delivered_at IS NULL` is fed through trigger.Gateway.Dispatch +
// messages.MarkDelivered. Each per-channel error is logged and skipped
// — the scheduler tick MUST NOT abort the daemon loop on a single bad
// row (at-least-once contract, L1 §6.1).
//
// M1.5 long-pending fallback emit (§6.4) is still no-op: the trigger
// Deliverer has no per-actor handlers registered, so Dispatch only
// records the resolved audience. A later ticket replaces this no-op
// with the system-side terminal_response emit path.
func (d *Daemon) scanLongPending(ctx context.Context, nowMs int64) error {
	for chID, cr := range d.channels {
		if cr.gateway == nil || cr.messages == nil {
			continue
		}
		due, err := cr.messages.PendingDue(ctx, nowMs, 64)
		if err != nil {
			fmt.Printf("runtime: scheduler scan %s: %v\n", chID, err)
			continue
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
	}
	return nil
}

// harnessChainAdapter bridges runtime/harness.Chain (kernel
// WriteResult) to the transit.HarnessChain (transit WriteResult). It
// avoids leaking kernel/harness into the transit package — the bridge
// stays inside runtime where both packages are already imported.
//
// FIX-T3 post-harness wiring: after a successful, non-deduped chain
// write the adapter calls trigger.Gateway.Dispatch + messages.MarkDelivered.
// The dedupe path is skipped (the original write already dispatched —
// at-least-once-by-message.id, §6.2). The deferred future-message path
// is skipped too: scheduler.scanLongPending will pick it up after
// not_before passes.
type harnessChainAdapter struct {
	chain    *harness.Chain
	gateway  *trigger.Gateway
	messages *store.Messages
	nowFn    func() int64
}

// Write implements transit.HarnessChain.
func (a harnessChainAdapter) Write(ctx context.Context, env *message.Envelope) (transit.HarnessWriteResult, error) {
	res, err := a.chain.Write(ctx, env)
	if err != nil {
		return transit.HarnessWriteResult{}, err
	}
	// Post-harness fan-out: only on a fresh accept. Reject / dedupe /
	// internal-error paths skip dispatch — dedupe because the prior
	// successful write already dispatched (§6.2); reject because no row
	// landed; chain-error already returned to caller above.
	if res.RejectReason == "" && !res.Deduped && a.gateway != nil {
		dr, derr := a.gateway.Dispatch(ctx, env, trigger.Options{})
		if derr != nil {
			fmt.Printf("runtime: gateway dispatch %s: %v\n", env.ID, derr)
		} else if !dr.Deferred && a.messages != nil {
			// Immediate-delivery path: stamp delivered_at so the
			// scheduler future-message scan ignores this row. Future-
			// message path leaves delivered_at NULL — scheduler claims it
			// later, then stamps after its own dispatch.
			if err := a.messages.MarkDelivered(ctx, env.ID, a.nowFn()); err != nil {
				fmt.Printf("runtime: mark delivered %s: %v\n", env.ID, err)
			}
		}
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

// compile-time interface check.
var _ khar.Chain = (*harness.Chain)(nil)

// Phase returns the current boot phase (for tests / observability).
func (d *Daemon) Phase() lifecycle.Phase { return d.booter.Phase() }

// BootResult returns the phase-2 reclaim outcome.
func (d *Daemon) BootResult() lifecycle.BootResult { return d.bootRes }

// Saga exposes the channel bootstrap saga (used by lifecycle.Creator).
func (d *Daemon) Saga() *bootstrap.Saga { return d.saga }

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
