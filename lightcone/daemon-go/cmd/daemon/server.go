package main

// server.go owns the M1.3 daemon composition root.
//
// The single exported entrypoint is Run(ctx, cfg). It wires every M1.3
// subsystem in dependency order, blocks until ctx is cancelled, then
// shuts everything down in reverse order. The function is invoked from
// the production binary (main.go) and from smoke_test.go in the same
// package — keeping the wiring testable without spawning a child
// process.
//
// Subsystem layering (top-down):
//
//	┌─ HTTP server (single mux, single listener)
//	│   ├─ /api/channel/{create,list}   ← bootstrap.RegisterRoutes
//	│   ├─ /api/rpc/message.send         ← internal/harness HTTP binding
//	│   ├─ /api/rpc/view.resync_channel  ← viewsync.NewResyncHandler
//	│   ├─ /api/device/{id}/callback     ← xhs.NewCallbackHandler (multi-channel fanout)
//	│   └─ /api/healthz                  ← liveness probe
//	│
//	├─ daemon-level sqlite (bootstrap_registry)
//	│   └─ bootstrap.Saga + Reconcile (boot-time recovery)
//	│
//	└─ per active channel:
//	    ├─ channel sqlite (messages.sqlite under workdir)
//	    ├─ harness deps (Store + Actors + Types + WorkerLocks)
//	    ├─ adapter.Manager  (xhs module installed + BootRecoverTimers + RunGC)
//	    ├─ supervisor.Loop  (per channel-agent; ExecSpawner; optional)
//	    ├─ long-pending scheduler
//	    └─ future scheduler  (trigger.Gateway over channel actor_registry)
//
// Reverse shutdown is driven by graceful HTTP shutdown + ctx cancel +
// WaitGroup over every goroutine spawned; channel + daemon sqlite
// handles close last.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coagent-ai/daemon-go/internal/adapters/xhs"
	"github.com/coagent-ai/daemon-go/internal/bootstrap"
	internalharness "github.com/coagent-ai/daemon-go/internal/harness"
	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/scheduler"
	"github.com/coagent-ai/daemon-go/internal/store"
	"github.com/coagent-ai/daemon-go/internal/supervisor"
	"github.com/coagent-ai/daemon-go/internal/trigger"
	"github.com/coagent-ai/daemon-go/internal/viewsync"
	"github.com/coagent-ai/daemon-go/internal/worker"
	"github.com/coagent-ai/daemon-go/pkg/adapter"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// Config is the boot-time wiring the daemon needs. main.go builds it
// from CLI flags + env; tests build it inline (HTTPListen=":0" so the
// kernel picks a free port; Ready surfaces the bound address).
type Config struct {
	// DaemonDBPath is the absolute path of the daemon-level sqlite
	// file backing bootstrap_registry. Required.
	DaemonDBPath string

	// ChannelRoot is the directory under which every channel's workdir
	// lives. Bootstrap stores the absolute workdir per channel inside
	// bootstrap_registry; this value is informational for the boot log.
	// May be empty when the daemon starts with no channels yet.
	ChannelRoot string

	// HTTPListen is the bind address for the daemon HTTP server (any
	// net.Listen("tcp", ...) syntax — ":3101", "127.0.0.1:0", etc.).
	// Required.
	HTTPListen string

	// AuthToken is the shared bearer token accepted by the daemon_rpc
	// message.send, view.resync_channel, and xhs callback endpoints.
	// Required.
	AuthToken string

	// WorkerBinaryPath is the absolute path of the worker binary the
	// supervisor exec.Cmd-spawns. When empty, the supervisor loops are
	// disabled (useful for smoke tests that exercise the HTTP write
	// path without launching workers).
	WorkerBinaryPath string

	// WorkerStdout / WorkerStderr override the destination for spawned
	// worker processes' stdout / stderr (FIX-4: ExecSpawner used to
	// drop them on the floor). Default nil → ExecSpawner uses
	// os.Stdout / os.Stderr so the daemon log aggregation pipe sees
	// every "worker.*" JSON line. Smoke tests pipe to a captured
	// buffer to assert real worker JSON logs surfaced.
	WorkerStdout io.Writer
	WorkerStderr io.Writer

	// ServerURL is the view-sync server origin. When empty, the daemon
	// runs without a Pusher (smoke / single-node mode). When non-empty,
	// pkgharness.Deps.Dispatcher gains a push hook (M1.x — wired in
	// follow-up; M1.3 baseline leaves Dispatcher = NoopDispatcher).
	ServerURL string

	// XHSDefaultDeviceID lets the xhs adapter default the payload's
	// device_id when the agent omits one. Optional.
	XHSDefaultDeviceID string

	// SchedulerPeriod overrides the long-pending + future scheduler
	// tick cadence. Defaults to 1 second. Smoke tests set 50ms so the
	// fallback path fires within the test timeout.
	SchedulerPeriod time.Duration

	// SupervisorPeriod overrides the supervisor scan cadence. Defaults
	// to 10s (supervisor.DefaultPeriod).
	SupervisorPeriod time.Duration

	// LeaseTTL is the worker_locks lease in seconds. Defaults to
	// supervisor.DefaultLeaseTTL (60s).
	LeaseTTL int64

	// Logger receives every "daemon.*" event. Defaults to slog.Default().
	Logger *slog.Logger

	// Ready receives a single ReadyInfo value once the HTTP listener is
	// up and every channel subsystem has been installed. Optional —
	// production callers leave it nil; tests use it to discover the
	// listener address (e.g. when HTTPListen=":0").
	Ready chan<- ReadyInfo
}

// ReadyInfo is the boot-completion signal Config.Ready delivers.
// Tests use HTTPAddr to dial the daemon without parsing CLI flags.
type ReadyInfo struct {
	// HTTPAddr is the actual TCP address the daemon is listening on,
	// after Listen(":0") port assignment. Format matches net.Addr.String
	// — `127.0.0.1:NNNN`.
	HTTPAddr string

	// ChannelIDs is the snapshot of active channel ids (the ones whose
	// supervisor + scheduler + adapter manager have all been wired).
	// Useful for the smoke test to assert "bootstrap_registry survived
	// the boot path".
	ChannelIDs []string
}

// Run is the daemon composition root. It blocks until ctx is cancelled
// or the HTTP listener fails to start, then performs an ordered
// shutdown of every subsystem it brought up.
//
// Errors returned by Run signal "daemon failed to boot" or
// "shutdown encountered an error" — context cancellation alone returns
// nil so signal-driven shutdowns look clean in the logs.
func Run(ctx context.Context, cfg Config) error {
	cfg = applyDefaults(cfg)
	logger := cfg.Logger

	if err := validateConfig(cfg); err != nil {
		return err
	}

	// ---- daemon-level sqlite + bootstrap saga ----------------------------
	daemonDB, err := store.OpenDaemon(ctx, cfg.DaemonDBPath, store.OpenOptions{})
	if err != nil {
		return fmt.Errorf("daemon: open daemon sqlite %q: %w", cfg.DaemonDBPath, err)
	}
	defer func() { _ = daemonDB.Close() }()

	// channelRoot is required for the bootstrap saga's containment
	// check (T102 FIX-2). Config validation already guarantees a
	// non-empty value; main.go falls back to <home>/channels when the
	// operator did not pass --channel-root.
	saga := bootstrap.New(daemonDB, bootstrap.WithChannelRoot(cfg.ChannelRoot))

	report, err := saga.Reconcile(ctx)
	if err != nil {
		return fmt.Errorf("daemon: bootstrap reconcile: %w", err)
	}
	logger.Info("daemon.reconcile.done",
		"scanned", report.Scanned,
		"completed", report.Completed,
		"rolled_back", report.RolledBack,
		"failures", len(report.Failures),
	)

	// ---- enumerate active channels --------------------------------------
	infos, err := saga.ListChannels(ctx)
	if err != nil {
		return fmt.Errorf("daemon: list channels: %w", err)
	}

	channels := make([]*channelRuntime, 0, len(infos))
	defer func() {
		// Reverse-order cleanup if Run returns early (before its own
		// shutdown block runs). The shutdown block sets channels=nil on
		// success so this defer is a no-op there.
		for i := len(channels) - 1; i >= 0; i-- {
			channels[i].close(logger)
		}
	}()

	resolverEntries := map[string]viewsync.ResyncStore{}
	managers := []*adapter.Manager{}
	channelIDs := make([]string, 0, len(infos))

	for _, info := range infos {
		runtime, rerr := installChannel(ctx, cfg, info, logger)
		if rerr != nil {
			return fmt.Errorf("daemon: install channel %q: %w", info.ChannelID, rerr)
		}
		channels = append(channels, runtime)
		resolverEntries[info.ChannelID] = viewsync.NewSQLiteResyncStore(runtime.db)
		if runtime.adapterMgr != nil {
			managers = append(managers, runtime.adapterMgr)
		}
		channelIDs = append(channelIDs, info.ChannelID)
		logger.Info("daemon.channel.installed",
			"channel_id", info.ChannelID,
			"workdir", info.WorkdirPath,
		)
	}

	// ---- HTTP routing ----------------------------------------------------
	mux := http.NewServeMux()
	mux.Handle("/api/healthz", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	// Bootstrap routes — server uses these to provision new channels.
	bootstrap.RegisterRoutes(mux, saga)

	// Daemon_rpc message.send. The auth function trusts the shared
	// daemon token and pulls sender identity from the envelope (the
	// M1.3 baseline trust model — single-machine token, sender stamped
	// by the caller).
	if len(channels) > 0 {
		// Pick the first channel's Deps for the handler — but in a
		// multi-channel daemon, the harness write path needs the right
		// Deps per envelope.channel_id. We mount a per-channel handler
		// router so each channel id dispatches to its own Deps.
		mux.Handle(internalharness.RPCPath, newMessageSendRouter(cfg.AuthToken, channels))
	}

	// Adapter callback fanout (multi-channel: try each manager; the
	// callback owner is whichever channel's correlation tracker has the
	// id — others silently no-op per L1 §6.5). Auth is enforced inside
	// the handler against cfg.AuthToken (T102 FIX-2 hardening).
	if len(managers) > 0 {
		mux.Handle("/api/device/", xhs.NewCallbackHandler(
			&multiAdapterManager{managers: managers},
			cfg.AuthToken,
		))
	}

	// View-sync resync handler — uses the channel id from the request
	// body to pick the per-channel SQLiteResyncStore.
	mux.Handle(viewsync.ResyncRPCPath, viewsync.NewResyncHandler(viewsync.ResyncHandlerOptions{
		Resolver: viewsync.ResyncStoreResolverFunc(func(_ context.Context, channelID string) (viewsync.ResyncStore, bool, error) {
			s, ok := resolverEntries[channelID]
			return s, ok, nil
		}),
		Auth: sharedTokenResyncAuth(cfg.AuthToken),
	}))

	// ---- HTTP listener ---------------------------------------------------
	listener, err := net.Listen("tcp", cfg.HTTPListen)
	if err != nil {
		return fmt.Errorf("daemon: listen %q: %w", cfg.HTTPListen, err)
	}

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// ---- spin up channel background loops --------------------------------
	loopCtx, cancelLoops := context.WithCancel(ctx)
	defer cancelLoops()
	var wg sync.WaitGroup
	for _, rt := range channels {
		rt.start(loopCtx, &wg, logger)
	}

	// ---- signal Ready ----------------------------------------------------
	if cfg.Ready != nil {
		select {
		case cfg.Ready <- ReadyInfo{
			HTTPAddr:   listener.Addr().String(),
			ChannelIDs: channelIDs,
		}:
		default:
			// Receiver may have already given up; non-blocking send is
			// fine — the listener address is observable via the logger
			// event below.
		}
	}
	logger.Info("daemon.ready",
		"http_addr", listener.Addr().String(),
		"channel_count", len(channels),
	)

	// ---- serve until ctx.Done --------------------------------------------
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		// Graceful shutdown.
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("daemon: http serve: %w", err)
		}
	}

	// ---- ordered shutdown -------------------------------------------------
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShutdown()

	logger.Info("daemon.shutdown.begin")
	_ = srv.Shutdown(shutdownCtx)

	cancelLoops()
	wg.Wait()

	for _, rt := range channels {
		rt.shutdownAdapter(shutdownCtx, logger)
	}
	// Close channel sqlite handles via the deferred close stack — replace
	// the slice with nil so the outer defer doesn't double-close.
	closingChannels := channels
	channels = nil
	for i := len(closingChannels) - 1; i >= 0; i-- {
		closingChannels[i].close(logger)
	}

	logger.Info("daemon.shutdown.done")
	return nil
}

// applyDefaults fills in zero-valued Config fields with the protocol
// baseline defaults.
func applyDefaults(cfg Config) Config {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.SchedulerPeriod <= 0 {
		cfg.SchedulerPeriod = scheduler.DefaultPeriod
	}
	if cfg.SupervisorPeriod <= 0 {
		cfg.SupervisorPeriod = supervisor.DefaultPeriod
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = supervisor.DefaultLeaseTTL
	}
	return cfg
}

// validateConfig fails fast on the required fields. Caller maps the
// error to a non-zero exit code; the smoke test reads the message
// verbatim.
func validateConfig(cfg Config) error {
	if strings.TrimSpace(cfg.DaemonDBPath) == "" {
		return errors.New("daemon: Config.DaemonDBPath is required")
	}
	if strings.TrimSpace(cfg.HTTPListen) == "" {
		return errors.New("daemon: Config.HTTPListen is required")
	}
	if strings.TrimSpace(cfg.AuthToken) == "" {
		return errors.New("daemon: Config.AuthToken is required")
	}
	if strings.TrimSpace(cfg.ChannelRoot) == "" {
		return errors.New("daemon: Config.ChannelRoot is required (T102 FIX-2 containment)")
	}
	if !filepath.IsAbs(cfg.ChannelRoot) {
		return fmt.Errorf("daemon: Config.ChannelRoot %q must be absolute", cfg.ChannelRoot)
	}
	return nil
}

// channelRuntime bundles every per-channel handle Run wires up. Stored
// in a slice so reverse-order shutdown sees the same fixtures the boot
// path constructed.
type channelRuntime struct {
	channelID  string
	workdir    string
	db         *sql.DB
	deps       pkgharness.Deps
	adapterMgr *adapter.Manager
	mgrXHS     *xhs.Module
	longSched  *scheduler.Scheduler
	futSched   *trigger.FutureScheduler
	supLoops   []*supervisor.Loop
}

// installChannel opens the channel sqlite, builds harness deps, installs
// the adapter framework, and constructs (but does NOT start) the per-
// channel scheduler / supervisor loops. Run.start() spins them up once
// the listener is bound so a boot failure tearing down half-constructed
// loops is straightforward.
func installChannel(
	ctx context.Context,
	cfg Config,
	info bootstrap.ChannelInfo,
	logger *slog.Logger,
) (*channelRuntime, error) {
	channelDB, err := store.OpenChannel(ctx, filepath.Join(info.WorkdirPath, "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		return nil, fmt.Errorf("open channel sqlite: %w", err)
	}

	storeAdapter := internalharness.NewSQLiteStore(channelDB)
	actorsAdapter := internalharness.NewSQLiteActors(channelDB)
	wlocks := internalharness.NewSQLiteWorkerLocks(channelDB)
	types, err := internalharness.LoadTypeLookup(ctx, channelDB)
	if err != nil {
		_ = channelDB.Close()
		return nil, fmt.Errorf("load type registry: %w", err)
	}

	deps := pkgharness.New(storeAdapter, actorsAdapter, types, wlocks, info.ChannelID)

	// ---- adapter framework + xhs module (when registered) --------------
	//
	// Only install adapter modules whose actor + type rows have been
	// bootstrapped into this channel. A channel that doesn't include
	// xhs (e.g. an internal-only channel) skips the xhs install entirely
	// — the adapter.Manager is built with an empty module set if no
	// adapter is registered.
	modules := map[string]adapter.Module{}
	var xhsMod *xhs.Module
	if hasActor, err := actorRegistered(ctx, channelDB, xhs.AdapterActorID); err != nil {
		_ = channelDB.Close()
		return nil, fmt.Errorf("check xhs actor: %w", err)
	} else if hasActor {
		xhsMod = xhs.New(xhs.Config{
			DeviceClient:    newNoopDeviceClient(logger),
			DefaultDeviceID: cfg.XHSDefaultDeviceID,
		})
		modules[xhs.AdapterName] = xhsMod
	}

	var mgr *adapter.Manager
	if len(modules) > 0 {
		mgr, err = adapter.NewManager(adapter.ManagerConfig{
			DB:      channelDB,
			Deps:    deps,
			Modules: modules,
			// *slog.Logger satisfies adapter.Logger directly.
			Logger: logger,
		})
		if err != nil {
			_ = channelDB.Close()
			return nil, fmt.Errorf("adapter NewManager: %w", err)
		}
		if err := mgr.Install(ctx); err != nil {
			_ = channelDB.Close()
			return nil, fmt.Errorf("adapter Install: %w", err)
		}
		if err := mgr.BootRecoverTimers(ctx); err != nil {
			_ = channelDB.Close()
			return nil, fmt.Errorf("adapter BootRecoverTimers: %w", err)
		}
	} else {
		logger.Info("daemon.channel.adapter_disabled",
			"channel_id", info.ChannelID,
			"reason", "no adapter actors registered (e.g. tool:xhs-adapter)",
		)
	}

	// ---- long-pending scheduler -----------------------------------------
	writer := scheduler.HarnessWriteFunc(func(c context.Context, env *v4types.Envelope, cctx pkgharness.CallerCtx) (*pkgharness.Result, error) {
		return pkgharness.Write(c, deps, env, cctx)
	})
	longSched, err := scheduler.NewLongPendingScheduler(channelDB, writer, info.ChannelID, scheduler.Config{
		Period: cfg.SchedulerPeriod,
		Logger: logger,
	})
	if err != nil {
		_ = channelDB.Close()
		return nil, fmt.Errorf("NewLongPendingScheduler: %w", err)
	}

	// ---- future scheduler over trigger gateway --------------------------
	gateway, err := trigger.NewGateway(&registryActorList{db: channelDB}, nil)
	if err != nil {
		_ = channelDB.Close()
		return nil, fmt.Errorf("trigger NewGateway: %w", err)
	}
	futSched, err := trigger.NewFutureScheduler(channelDB, gateway, info.ChannelID, trigger.SchedulerConfig{
		Period: cfg.SchedulerPeriod,
		Logger: logger,
	})
	if err != nil {
		_ = channelDB.Close()
		return nil, fmt.Errorf("trigger NewFutureScheduler: %w", err)
	}

	// ---- supervisor loop per active in_worker_bus agent -----------------
	var loops []*supervisor.Loop
	if cfg.WorkerBinaryPath != "" {
		actors, lerr := registry.ListActive(ctx, channelDB)
		if lerr != nil {
			_ = channelDB.Close()
			return nil, fmt.Errorf("list active actors: %w", lerr)
		}
		spawner, serr := worker.NewExecSpawner(worker.ExecSpawnerConfig{
			BinaryPath:     cfg.WorkerBinaryPath,
			ChannelWorkdir: info.WorkdirPath,
			LeaseTTL:       cfg.LeaseTTL,
			Stdout:         cfg.WorkerStdout,
			Stderr:         cfg.WorkerStderr,
		})
		if serr != nil {
			_ = channelDB.Close()
			return nil, fmt.Errorf("NewExecSpawner: %w", serr)
		}
		for _, a := range actors {
			if a.Kind != registry.KindAgent {
				continue
			}
			if a.Binding != registry.BindingInWorkerBus {
				continue
			}
			loop, lerr := supervisor.New(channelDB, info.ChannelID, a.ActorID, spawner, supervisor.LoopConfig{
				Period:    cfg.SupervisorPeriod,
				LeaseTTL:  cfg.LeaseTTL,
				Logger:    logger,
				AuthToken: cfg.AuthToken,
				// DaemonURL intentionally left empty in M1.3: the in-process
				// in_worker_bus binding doesn't need it; coagent CLI
				// fallback wiring lands in a follow-up ticket once the
				// daemon's externally-reachable URL is part of Config.
			})
			if lerr != nil {
				_ = channelDB.Close()
				return nil, fmt.Errorf("supervisor.New for agent %q: %w", a.ActorID, lerr)
			}
			loops = append(loops, loop)
		}
	} else {
		logger.Info("daemon.channel.supervisor_disabled",
			"channel_id", info.ChannelID,
			"reason", "WorkerBinaryPath empty",
		)
	}

	return &channelRuntime{
		channelID:  info.ChannelID,
		workdir:    info.WorkdirPath,
		db:         channelDB,
		deps:       deps,
		adapterMgr: mgr,
		mgrXHS:     xhsMod,
		longSched:  longSched,
		futSched:   futSched,
		supLoops:   loops,
	}, nil
}

// start kicks off every background goroutine the channel needs. wg is
// incremented for each goroutine and Done-ed on goroutine exit.
func (rt *channelRuntime) start(ctx context.Context, wg *sync.WaitGroup, logger *slog.Logger) {
	// Long-pending scheduler.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := rt.longSched.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("daemon.long_pending.run",
				"channel_id", rt.channelID, "err", err.Error())
		}
	}()

	// Future scheduler.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := rt.futSched.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("daemon.future_scheduler.run",
				"channel_id", rt.channelID, "err", err.Error())
		}
	}()

	// Adapter GC (only when an adapter framework is installed).
	if rt.adapterMgr != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rt.adapterMgr.RunGC(ctx)
		}()
	}

	// Supervisor loops.
	for _, loop := range rt.supLoops {
		l := loop
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("daemon.supervisor.run",
					"channel_id", rt.channelID, "err", err.Error())
			}
		}()
	}
}

// shutdownAdapter runs Manager.Shutdown on the adapter framework. Kept
// separate from close() so the shutdown ordering (HTTP first → ctx
// cancel → adapter shutdown → close sqlite) is explicit at the Run
// call site.
func (rt *channelRuntime) shutdownAdapter(ctx context.Context, logger *slog.Logger) {
	if rt.adapterMgr == nil {
		return
	}
	if err := rt.adapterMgr.Shutdown(ctx); err != nil {
		logger.Warn("daemon.adapter.shutdown.error",
			"channel_id", rt.channelID, "err", err.Error())
	}
}

// close releases the channel sqlite handle. Safe to call multiple times.
func (rt *channelRuntime) close(logger *slog.Logger) {
	if rt.db == nil {
		return
	}
	if err := rt.db.Close(); err != nil {
		logger.Warn("daemon.channel.close.error",
			"channel_id", rt.channelID, "err", err.Error())
	}
	rt.db = nil
}

// -----------------------------------------------------------------------------
// HTTP routing helpers
// -----------------------------------------------------------------------------

// newMessageSendRouter builds the daemon_rpc /api/rpc/message.send
// handler that routes incoming requests to the per-channel harness
// Deps. The envelope's channel_id picks the right channel; unknown
// channel ids return 404.
func newMessageSendRouter(authToken string, channels []*channelRuntime) http.Handler {
	byID := make(map[string]*channelRuntime, len(channels))
	for _, rt := range channels {
		byID[rt.channelID] = rt
	}
	handlersByChannel := make(map[string]http.Handler, len(channels))
	for id, rt := range byID {
		handlersByChannel[id] = internalharness.NewHTTPHandler(internalharness.HTTPHandlerOptions{
			Deps: rt.deps,
			Auth: sharedTokenMessageAuth(authToken),
		})
	}

	// Channel id discovery: a request body is JSON containing
	// `params.channel_id`. We peek at the body once + restore it.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "POST required")
			return
		}
		body, err := readAndRestoreBody(r, 1<<20)
		if err != nil {
			writeError(w, http.StatusBadRequest, "params_invalid", err.Error())
			return
		}
		channelID, perr := peekChannelID(body)
		if perr != nil {
			writeError(w, http.StatusBadRequest, "params_invalid", perr.Error())
			return
		}
		handler, ok := handlersByChannel[channelID]
		if !ok {
			writeError(w, http.StatusNotFound, "channel_missing",
				fmt.Sprintf("channel %q is not hosted by this daemon", channelID))
			return
		}
		handler.ServeHTTP(w, r)
	})
}

// sharedTokenMessageAuth returns an AuthFunc that accepts the shared
// daemon token and pulls sender identity from the envelope. M1.3
// trust model — single static token, per-actor identity supplied by
// the caller (the daemon trusts the envelope's sender.id).
func sharedTokenMessageAuth(token string) internalharness.AuthFunc {
	return func(_ context.Context, t string, body *internalharness.MessageSendRequest) (pkgharness.CallerCtx, error) {
		if t != token {
			return pkgharness.CallerCtx{}, errors.New("token mismatch")
		}
		if body == nil {
			return pkgharness.CallerCtx{}, errors.New("empty body")
		}
		actorID := body.Params.Sender.ID
		if strings.TrimSpace(actorID) == "" {
			return pkgharness.CallerCtx{}, errors.New("envelope.sender.id is required")
		}
		return pkgharness.CallerCtx{
			Authenticated: true,
			ActorID:       actorID,
		}, nil
	}
}

// sharedTokenResyncAuth returns a ResyncAuthFunc that accepts the shared
// daemon token. Any mismatch yields auth_failed.
func sharedTokenResyncAuth(token string) viewsync.ResyncAuthFunc {
	return func(_ context.Context, t string, _ *viewsync.ResyncRequest) error {
		if t != token {
			return errors.New("token mismatch")
		}
		return nil
	}
}

// (Auth enforcement for the xhs callback path now lives inside
// xhs.NewCallbackHandler — see T102 FIX-2. The previous placeholder
// `authMiddleware` / `authedCallbackManager` wrapper has been removed
// to avoid the appearance of double-auth that confused reviewers.)

// multiAdapterManager fans an OnExternalCallback across every channel's
// adapter.Manager. The L1 §6.5 contract says a Manager whose correlation
// tracker has no entry for the correlation_id silently no-ops, so
// fanout is safe across N channels: at most one Manager owns the
// correlation, the rest return nil.
type multiAdapterManager struct {
	managers []*adapter.Manager
}

// OnExternalCallback satisfies xhs.CallbackManager.
func (m *multiAdapterManager) OnExternalCallback(ctx context.Context, name string, payload []byte) error {
	var firstErr error
	for _, mgr := range m.managers {
		if err := mgr.OnExternalCallback(ctx, name, payload); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// actorRegistered reports whether the given actor_id exists in the
// channel's actor_registry and is active. Used to decide whether to
// install adapter modules on a channel — a daemon hosting both
// adapter-bearing channels and internal-only channels would otherwise
// crash on Install for the second category.
func actorRegistered(ctx context.Context, db *sql.DB, actorID string) (bool, error) {
	var dummy int
	err := db.QueryRowContext(ctx,
		`SELECT 1 FROM actor_registry
		  WHERE actor_id = ? AND deregistered_at IS NULL
		  LIMIT 1`,
		actorID,
	).Scan(&dummy)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// registryActorList satisfies trigger.ActorLookup over a channel sqlite,
// delegating to internal/registry.ListActive. The trigger gateway calls
// ListActive only when an envelope has audience=['*']; per-call cost is
// fine for M1.3 baseline (channels are small) — caching is a follow-up
// optimization.
type registryActorList struct {
	db *sql.DB
}

// ListActive satisfies trigger.ActorLookup.
func (r *registryActorList) ListActive(ctx context.Context) ([]string, error) {
	metas, err := registry.ListActive(ctx, r.db)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(metas))
	for _, m := range metas {
		out = append(out, m.ActorID)
	}
	return out, nil
}

// newNoopDeviceClient returns a DeviceClient that always errors with
// ErrDeviceOffline. Production wires a real WS server (Chrome extension
// transport); M1.3 baseline daemon falls back to "offline" so a missing
// device manifests as a failed-terminal response within seconds rather
// than hanging forever. Smoke tests inject a stub via cfg.XHSDeviceClient
// (M1.x — for now they target the long-pending fallback path instead).
func newNoopDeviceClient(logger *slog.Logger) xhs.DeviceClient {
	return &noopDeviceClient{logger: logger}
}

type noopDeviceClient struct {
	logger *slog.Logger
}

func (n *noopDeviceClient) PushCommand(_ context.Context, deviceID string, cmd xhs.Command) error {
	n.logger.Info("daemon.xhs.device.offline",
		"device_id", deviceID, "cmd", cmd.Cmd, "correlation_id", cmd.CorrelationID)
	return xhs.ErrDeviceOffline
}

// (adapter.Logger is satisfied by *slog.Logger directly — no shim needed.)
