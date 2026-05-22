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

	"github.com/rs/zerolog"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	khlog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
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

	// ReplayWindow caps |now - human_caller.ts|. Production callers must set
	// a positive value; zero/negative only works when
	// AllowReplayWindowDisabled is explicitly set for test/dev paths.
	ReplayWindow time.Duration

	// AllowReplayWindowDisabled permits ReplayWindow<=0 for tests/dev only.
	// Production wiring must leave this false so replay protection cannot be
	// disabled by an omitted config value.
	AllowReplayWindowDisabled bool

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
	// handler from the P2 counter stub to a full WorkerBridge that
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

	// OnBindDeviceSession / OnUnbindDeviceSession (T147 §A-S2) handle
	// the server → daemon device-session lifecycle frames. The composition
	// root (cmd/daemon) wires these to the per-process SessionStore so
	// adapter modules with binding=runtime_inbound_via_relay can mirror the
	// server's authoritative device_sessions row. Both are nil-safe — when
	// unset, the transit dispatcher synthesises a result=rejected ack with
	// the spec bind/unbind internal-error reason so the server can
	// distinguish "daemon does not implement bind" from "daemon rejected bind".
	OnBindDeviceSession   func(ctx context.Context, body transit.BindDeviceSessionBody) transit.BindDeviceSessionAckBody
	OnUnbindDeviceSession func(ctx context.Context, body transit.UnbindDeviceSessionBody) transit.UnbindDeviceSessionAckBody

	// ListDeviceSessionsForChannel snapshots the per-daemon device
	// session mirror rows for a single channel. cmd/daemon wires this
	// to DeviceSessionBinder.SessionStore so the worker spawn path can
	// fold an "active device sessions" section into the kimi system
	// prompt without runtime/** importing adapters/device/framework
	// (go-arch-lint forbids that direction). Returned slice is read-
	// only — callers must not mutate.
	//
	// Nil hook → workers spawn without the devices section, same as a
	// channel with no bound devices. Empty slice (hook present, no
	// rows) is also acceptable.
	ListDeviceSessionsForChannel func(ctx context.Context, ch channel.ID) []DeviceSessionInfo

	// Logger receives daemon lifecycle + transport supervisor events.
	// When nil a no-op zerolog.Nop() is used so legacy callers and tests
	// keep compiling unchanged. Production wiring (cmd/daemon) supplies
	// the project-stamped logger so transit events join the same JSON
	// stream as the rest of the daemon.
	Logger *zerolog.Logger

	// ReconnectInitialBackoff is the initial wait the WS reconnect
	// supervisor sleeps after a disconnect before redialing. Doubles
	// every failure up to ReconnectMaxBackoff. Defaults to 1s.
	ReconnectInitialBackoff time.Duration

	// ReconnectMaxBackoff caps exponential backoff between reconnect
	// attempts. Defaults to 30s — fast enough that a multi-minute
	// outage still gets a fresh dial attempt regularly, slow enough not
	// to hammer a struggling server.
	ReconnectMaxBackoff time.Duration

	// ChannelTemplates is the per-type template registry keyed by
	// CreateChannelRequest.ChannelType (catalog channel.type — e.g.
	// "group" / "xhs-creator"). Unknown types fall through to the entry
	// keyed by "" if one exists, otherwise to a zero ChannelTemplate (no
	// actor seeds / subdirs).
	//
	// The empty-string key is reserved for the "no template" legacy
	// path (generic group channels) so cmd/daemon can register both
	// `""` and `"xhs-creator"` in one map without losing the fallback.
	ChannelTemplates map[string]ChannelTemplate
}

// DeviceSessionInfo is the runtime-package projection of one device
// session mirror row, kept narrow so runtime/** does not need to
// import adapters/device/framework (forbidden by go-arch-lint). The
// composition root (cmd/daemon) populates this via the
// DaemonConfig.ListDeviceSessionsForChannel hook by mapping
// framework.DeviceSession → DeviceSessionInfo. Fields mirror what the
// kimi system prompt needs to surface "active device sessions" to the
// LLM (M1.6 follow-up — agent self-awareness fix).
type DeviceSessionInfo struct {
	SessionID  string
	DeviceID   string
	DeviceType string
	State      string
}

// ChannelTemplate is the daemon-side projection of an L4 channel
// template snapshot. M1.6-T2 covered actor_registry seeds; M1.6-T5
// phase-2 adds workdir subdirs (e.g. xhs-creator published-notes/) and
// the domain prompt segment (consumed by phase-3 worker spawn).
//
// Zero value = no extra actors / no extra subdirs / no domain prompt
// (channel still gets system + initial members seeded by the saga base
// path).
type ChannelTemplate struct {
	// AdapterActorSeeds lists the tool actor rows the saga inserts in
	// addition to system + initial members. Each row supplies enough
	// fields for kernel/adapter.Manager.Install to find the actor with
	// the right binding.
	AdapterActorSeeds []actorreg.Record

	// WorkdirSubdirs lists relative directory paths the bootstrap saga
	// mkdirs inside <ChannelsDir>/<channelID>/ during step 5c. The
	// xhs-creator template ships ["published-notes", "drafts", "assets"]
	// per v4-layer4-spec §2.5. Entries are treated as path components
	// joined with channelDir; "../" or absolute escapes are rejected.
	WorkdirSubdirs []string

	// DomainPrompt is the L4 §2.4 prompt segment cmd/daemon injects into
	// the worker base prompt at spawn time (M1.6-T5 phase-3 will plumb
	// this via env). Empty = no domain prompt (legacy channels).
	DomainPrompt string
}

// ChannelAgentID is the well-known actor id every per-channel runtime
// registers on boot. It is the single L2 worker target for trigger
// fan-out: scheduler.Deliverer routes every audience-resolved envelope
// addressed to this id into the per-channel WorkerBridge (M1.6-T1 P4).
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
	pushMu        sync.Mutex
	pausePush     func()
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

	// workerBridge is non-nil when DaemonConfig.WorkerSpawner is set
	// and bootChannel successfully built the per-channel bridge. The
	// channel teardown calls bridge.Close so worker subprocesses do
	// not leak past placement reclaim. (M1.6-T1)
	workerBridge *workerhost.Bridge

	// deviceTransit is the per-channel devicetransit.DeviceTransit (T147 §A).
	// One instance per channel sharing the daemon's *transit.Client so
	// daemon→server `device_transit.recv` frames (impl-layer2 §5.3.2
	// outbound) carry the SendFrame's channel_id verbatim; inbound
	// `device_transit.send` frames (§5.3.1 inbound) are routed back here
	// by handleDeviceTransitFrame (see daemon.go).
	// Nil only when the daemon is constructed without a transit client
	// (defensive — every production wiring sets one up).
	deviceTransit *transit.DeviceTransit

	// deviceCallback receives the decoded SendFrame the framework
	// Manager turns into Module.OnExternalCallback. OnChannelBoot sets
	// it (atomic.Value enables late binding after buildChannelRuntime
	// has already wired the *transit.DeviceTransit). Reads are atomic;
	// "" / nil means "no adapter wired yet — drop silently".
	deviceCallback atomic.Value // func(ctx context.Context, frame devicetransit.SendFrame) error
}

func (cr *channelRuntime) cancelPusher() {
	if cr == nil {
		return
	}
	cr.pushMu.Lock()
	cancel := cr.pausePush
	cr.pausePush = nil
	cr.pushMu.Unlock()
	if cancel != nil {
		cancel()
	}
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

	// ChannelType is the L4 channel-template key (catalog.Channel.Type;
	// e.g. "" / "group" / "xhs-creator") that this channel was created
	// with. Sourced from CreateChannelRequest on the hot path and the
	// channel_lock row on cold-start (M1.6-T5 phase-2). cmd/daemon
	// inspects this in its AdapterModuleFactory closures to decide
	// whether the factory applies for this channel (e.g. install xhs
	// adapter only when ChannelType=="xhs-creator").
	ChannelType string

	// DomainPrompt is the L4 §2.4 prompt segment associated with
	// ChannelType (M1.6-T5 phase-3 will plumb this into worker spawn
	// env so the agent's base prompt picks it up). Empty when the
	// channel has no template or the template declared no prompt.
	DomainPrompt string

	// DB is the channel-local sqlite handle. Same handle backing every
	// store in this struct — exposed for callers that need to construct
	// new per-channel stores not already in this struct.
	DB *sql.DB

	// ActorRegistry is the channel-local actorreg.Registry (sqlite-backed).
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

	// Logger is the daemon's structured log sink, scoped to the same JSON
	// stream as runtime lifecycle and transit events.
	Logger *zerolog.Logger

	// DeviceTransit is the per-channel devicetransit.DeviceTransit handed to
	// `runtime_inbound_via_relay` adapter modules (T147 §A). When the channel
	// boots without a daemonbus transit client (e.g. tests that only
	// exercise the harness write path), this is nil — the composition
	// root MUST skip runtime_inbound_via_relay factories or use a fake. When
	// non-nil, pass it verbatim into framework.ManagerConfig.DeviceTransit
	// so the framework can satisfy adapter modules that declare
	// Binding=runtime_inbound_via_relay at Install time.
	DeviceTransit devicetransit.DeviceTransit

	// SetDeviceCallback wires the inbound device→daemon callback for
	// this channel. The composition root calls it after constructing the
	// framework.Manager so `device_transit.send` frames (impl-layer2
	// §5.3.1 inbound) routed back from the server land on
	// Manager.OnExternalCallback(adapter, payload).
	// The closure shape mirrors transit.DeviceTransitConfig.OnRecv
	// (frame body, not raw bytes) so callers don't need to parse the
	// devicetransit.SendFrame envelope themselves. Calling SetDeviceCallback
	// more than once REPLACES the previous binding — that's the M1.6
	// hot-swap escape hatch when a channel's adapter set changes.
	// Passing nil clears the binding (frames are dropped silently).
	SetDeviceCallback func(func(ctx context.Context, frame devicetransit.SendFrame) error)
}

// buildTemplateResolver returns a closure that picks the ChannelTemplate
// for a given CreateChannelRequest.ChannelType. M1.6-T5 phase-2 wiring:
//
//   - When templates is non-empty: prefer an exact-match entry; fall
//     back to the entry keyed by "" if present; otherwise return the
//     zero value (no template).
func buildTemplateResolver(templates map[string]ChannelTemplate) func(channelType string) ChannelTemplate {
	if len(templates) == 0 {
		return func(string) ChannelTemplate { return ChannelTemplate{} }
	}
	// Copy the map so callers can't mutate the resolver state
	// post-construction.
	snapshot := make(map[string]ChannelTemplate, len(templates))
	for k, v := range templates {
		snapshot[k] = v
	}
	return func(channelType string) ChannelTemplate {
		if tpl, ok := snapshot[channelType]; ok {
			return tpl
		}
		if tpl, ok := snapshot[""]; ok {
			return tpl
		}
		return ChannelTemplate{}
	}
}

// Daemon is the assembled cmd/daemon process. Exposed so tests can
// drive the phases manually.
type Daemon struct {
	cfg          DaemonConfig
	log          zerolog.Logger
	daemonDB     *sql.DB
	channelDBs   map[string]*sql.DB
	channelDBsMu *sync.Mutex
	bootRes      lifecycle.BootResult
	transit      *transit.Client
	bus          *transit.MockBus
	wsClient     *transit.WSClient
	booter       *lifecycle.Bootstrapper
	reconciler   *bootstrap.Reconciler
	saga         *bootstrap.Saga
	unloader     *lifecycle.Unloader
	heartbeat    *transit.HeartbeatTracker
	// resolveTemplate returns the daemon-side ChannelTemplate for a
	// given channel type. Used by bootChannel to surface WorkdirSubdirs
	// / DomainPrompt into ChannelHooks (M1.6-T5 phase-2/3).
	resolveTemplate func(channelType string) ChannelTemplate

	channelsMu sync.RWMutex
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
	if cfg.ReconnectInitialBackoff <= 0 {
		cfg.ReconnectInitialBackoff = time.Second
	}
	if cfg.ReconnectMaxBackoff <= 0 {
		cfg.ReconnectMaxBackoff = 30 * time.Second
	}
	if !cfg.UseMockBus && cfg.WSConfig == nil {
		return nil, errors.New("runtime: DaemonConfig.WSConfig required when UseMockBus=false")
	}
	log := zerolog.Nop()
	if cfg.Logger != nil {
		log = *cfg.Logger
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
	// Build a per-type template resolver. When ChannelTemplates is wired,
	// look up by req.ChannelType, falling back to the "" entry for unknown
	// / generic types.
	resolveTemplate := buildTemplateResolver(cfg.ChannelTemplates)

	saga, err := bootstrap.NewSaga(bootstrap.SagaConfig{
		DaemonDB:    daemonDB,
		ChannelsDir: cfg.ChannelsDir,
		NowFn:       cfg.NowFn,
		ResolveTemplate: func(channelType string) bootstrap.TemplateView {
			tpl := resolveTemplate(channelType)
			return bootstrap.TemplateView{
				AdapterActorSeeds: tpl.AdapterActorSeeds,
				WorkdirSubdirs:    tpl.WorkdirSubdirs,
			}
		},
	})
	if err != nil {
		_ = daemonDB.Close()
		return nil, err
	}

	channelDBs := make(map[string]*sql.DB)
	channelDBsMu := &sync.Mutex{}
	openLock := func(ctx context.Context, sqlitePath string) (*store.ChannelLock, error) {
		channelDBsMu.Lock()
		defer channelDBsMu.Unlock()
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
		// EmitHeldChannelsReport left nil — the offline path treats all
		// locally-owned channels as still ours.
	})
	if err != nil {
		_ = daemonDB.Close()
		return nil, err
	}

	d := &Daemon{
		cfg:             cfg,
		log:             log,
		daemonDB:        daemonDB,
		channelDBs:      channelDBs,
		channelDBsMu:    channelDBsMu,
		booter:          booter,
		reconciler:      reconciler,
		saga:            saga,
		unloader:        lifecycle.NewUnloader(),
		heartbeat:       transit.NewHeartbeatTracker(),
		channels:        make(map[channel.ID]*channelRuntime),
		cursors:         transit.NewCursorTracker(),
		resolveTemplate: resolveTemplate,
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
		wsCfg := *cfg.WSConfig
		if wsCfg.Logger == nil {
			wsCfg.Logger = &log
		}
		ws, err := transit.NewWSClient(wsCfg)
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

	// Phase 2: report held channels (offline path = all owned accepted).
	res, err := d.booter.ReportHeldChannels(ctx)
	if err != nil {
		return fmt.Errorf("runtime: phase2 ReportHeldChannels: %w", err)
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
	acceptedSet := make(map[channel.ID]struct{}, len(d.bootRes.HeldAccepted))
	for _, id := range d.bootRes.HeldAccepted {
		acceptedSet[id] = struct{}{}
	}
	for _, lc := range d.bootRes.Local {
		if _, ok := acceptedSet[lc.ChannelID]; !ok {
			continue
		}
		if err := d.bootChannel(ctx, lc, true); err != nil {
			return fmt.Errorf("runtime: channel %s: %w", lc.ChannelID, err)
		}
	}

	// 3.2 — transit dispatcher (one goroutine drains the recv side).
	handler, err := transit.NewWriteMessageHandler(transit.WriteMessageHandlerConfig{
		Secret:                    d.cfg.HumanCallerSecret,
		Router:                    d.routeWrite,
		NowMs:                     d.cfg.NowFn,
		ReplayWindow:              d.cfg.ReplayWindow,
		AllowReplayWindowDisabled: d.cfg.AllowReplayWindowDisabled,
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
		cr, ok := d.getChannel(id)
		if !ok {
			return nil, false
		}
		return cr.outbox, true
	})
	if err != nil {
		return fmt.Errorf("runtime: build ack handler: %w", err)
	}

	resyncRouter := func(id channel.ID) (transit.ResyncSource, bool) {
		cr, ok := d.getChannel(id)
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
		OnViewsyncAck: func(ctx context.Context, ack viewsync.AckFrame) error {
			if ack.RejectReason == viewsync.RejectReasonMuxOwnerEpochStale {
				d.freezeChannel(ctx, ack.ChannelID, string(ack.RejectReason))
				return nil
			}
			return ackHandler.Handle(ctx, ack)
		},
		OnViewsyncResyncRequest: resyncServer.ServeResync,
		OnCreateChannel:         d.handleCreateChannel,
		OnDaemonReclaim:         d.handleDaemonReclaim,
		OnUnbindChannel:         d.handleUnbindChannel,
		OnHeartbeatAck:          d.handleHeartbeatAck,
		OnHeldChannelsAck:       d.handleHeldChannelsAck,
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
	if d.cfg.OnBindDeviceSession != nil {
		handlers.OnBindDeviceSession = func(ctx context.Context, _ daemonbus.Frame, body transit.BindDeviceSessionBody) transit.BindDeviceSessionAckBody {
			return d.cfg.OnBindDeviceSession(ctx, body)
		}
	}
	if d.cfg.OnUnbindDeviceSession != nil {
		handlers.OnUnbindDeviceSession = func(ctx context.Context, _ daemonbus.Frame, body transit.UnbindDeviceSessionBody) transit.UnbindDeviceSessionAckBody {
			return d.cfg.OnUnbindDeviceSession(ctx, body)
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
		d.runDispatcherSupervised(d.runCtx, dispatcher)
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
				d.log.Error().Err(err).Str("event", "runtime.scheduler_exited").
					Msg("scheduler exited with error")
			}
		}
	}()

	// 3.4 — control.heartbeat sender (M1.6-T1 part A). Without this
	// ticker the daemon's OnHeartbeatAck receipt watermark works fine,
	// but the server never sees a heartbeat → after 90s server.placements
	// flips active → stale even though the daemon process is alive. The
	// sender snapshot-reads d.channels each tick through the runtime
	// map lock shared with routeWrite / hot bind-unbind.
	hb, err := transit.NewHeartbeatSender(transit.HeartbeatSenderConfig{
		Client:       d.transit,
		Period:       d.cfg.HeartbeatPeriod,
		FrameID:      d.cfg.FrameIDGen,
		HeldChannels: d.snapshotHeldChannels,
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
				d.log.Error().Err(err).Str("event", "runtime.heartbeat_sender_exited").
					Msg("heartbeat sender exited with error")
			}
		}
	}()

	d.ready.Store(true)
	return nil
}

// runDispatcherSupervised drives transit.Dispatcher.Loop and, on
// non-context errors, attempts to reconnect the underlying transport
// with exponential backoff before restarting the loop. This is the
// recovery path for the production incident (2026-05-18) where a stuck
// TCP write left the WS conn dead but the daemon had no way to retry
// the dial — every Send returned "not connected" forever.
//
// Reconnect is only attempted when a wsClient is wired (production
// path). Mock-bus deployments fall through and exit on the first error
// preserving the existing test semantics.
func (d *Daemon) runDispatcherSupervised(ctx context.Context, dispatcher *transit.Dispatcher) {
	backoff := d.cfg.ReconnectInitialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		err := dispatcher.Loop(ctx)
		if err == nil {
			// Loop returned nil — defensive; treat as shutdown.
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if errors.Is(err, transit.ErrBusClosed) {
			// MockBus closed (test path) — terminate. Production
			// uses WSClient which surfaces transport errors instead.
			return
		}
		d.log.Warn().Err(err).
			Str("event", "runtime.dispatcher_disconnected").
			Dur("retry_in", backoff).
			Msg("dispatcher loop disconnected; attempting reconnect")

		if d.wsClient == nil {
			// No reconnect transport (mock bus). Exit so callers see
			// the original behavior.
			d.log.Error().Err(err).
				Str("event", "runtime.dispatcher_exited").
				Msg("dispatcher exited (no reconnect transport wired)")
			return
		}

		// Sleep with cancellation awareness.
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		if _, connectErr := d.transit.Connect(ctx); connectErr != nil {
			if errors.Is(connectErr, context.Canceled) {
				return
			}
			d.log.Warn().Err(connectErr).
				Str("event", "runtime.reconnect_failed").
				Dur("next_retry_in", nextBackoff(backoff, d.cfg.ReconnectMaxBackoff)).
				Msg("daemonbus reconnect failed; will retry")
			backoff = nextBackoff(backoff, d.cfg.ReconnectMaxBackoff)
			continue
		}
		d.log.Info().
			Str("event", "runtime.reconnected").
			Int64("connection_epoch", int64(d.transit.Epoch())).
			Msg("daemonbus reconnected; resuming dispatcher loop")
		// Reset backoff on a successful reconnect so a flaky link
		// doesn't permanently inflate.
		backoff = d.cfg.ReconnectInitialBackoff
	}
}

// nextBackoff doubles the current backoff, capped at maxBackoff.
func nextBackoff(cur, maxB time.Duration) time.Duration {
	next := cur * 2
	if next > maxB {
		next = maxB
	}
	if next <= 0 {
		next = time.Second
	}
	return next
}

// snapshotOwnedChannels returns the current owned-channel ids.
// Same lock-free read pattern as routeWrite — the map is mutated only
// by the dispatcher / phase-3 boot goroutines, never concurrently with
// itself.
func (d *Daemon) snapshotOwnedChannels() []channel.ID {
	d.channelsMu.RLock()
	defer d.channelsMu.RUnlock()
	if len(d.channels) == 0 {
		return nil
	}
	out := make([]channel.ID, 0, len(d.channels))
	for id := range d.channels {
		out = append(out, id)
	}
	return out
}

// snapshotHeldChannels returns the current owned-channel fencing tuples
// for control.heartbeat. It reads channel_lock on every tick so reclaim
// fencing rotation is reflected without rebuilding the daemon runtime.
func (d *Daemon) snapshotHeldChannels(ctx context.Context) []placement.HeartbeatHeldChannel {
	d.channelsMu.RLock()
	runtimes := make([]*channelRuntime, 0, len(d.channels))
	for _, cr := range d.channels {
		runtimes = append(runtimes, cr)
	}
	d.channelsMu.RUnlock()
	if len(runtimes) == 0 {
		return nil
	}
	out := make([]placement.HeartbeatHeldChannel, 0, len(runtimes))
	for _, cr := range runtimes {
		row, ok, err := cr.lock.Get(ctx)
		if err != nil || !ok {
			continue
		}
		out = append(out, placement.HeartbeatHeldChannel{
			ChannelID:    cr.channelID,
			OwnerEpoch:   row.OwnerEpoch,
			FencingToken: row.FencingToken,
		})
	}
	return out
}

func (d *Daemon) getChannel(id channel.ID) (*channelRuntime, bool) {
	d.channelsMu.RLock()
	defer d.channelsMu.RUnlock()
	cr, ok := d.channels[id]
	return cr, ok
}

func (d *Daemon) setChannel(id channel.ID, cr *channelRuntime) {
	d.channelsMu.Lock()
	d.channels[id] = cr
	d.channelsMu.Unlock()
}

func (d *Daemon) deleteChannel(id channel.ID) {
	d.channelsMu.Lock()
	delete(d.channels, id)
	d.channelsMu.Unlock()
}

func (d *Daemon) pauseChannelPush(id channel.ID) {
	cr, ok := d.getChannel(id)
	if !ok {
		return
	}
	cr.cancelPusher()
}

func (d *Daemon) freezeChannel(ctx context.Context, id channel.ID, reason string) {
	cr, ok := d.getChannel(id)
	if !ok {
		return
	}
	if cr.wrappedChain != nil {
		cr.wrappedChain.Freeze(reason)
	}
	cr.cancelPusher()
	if cr.workerBridge != nil {
		closeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_ = cr.workerBridge.Close(closeCtx)
		cancel()
	}
}

func (d *Daemon) channelFrozen(id channel.ID) bool {
	cr, ok := d.getChannel(id)
	return ok && cr.wrappedChain != nil && cr.wrappedChain.IsFrozen()
}

func (d *Daemon) snapshotChannelRuntimes() []*channelRuntime {
	d.channelsMu.RLock()
	defer d.channelsMu.RUnlock()
	out := make([]*channelRuntime, 0, len(d.channels))
	for _, cr := range d.channels {
		out = append(out, cr)
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
		Fencing: khlog.FencingTuple{
			Token: lc.Lock.FencingToken,
			Epoch: lc.Lock.DaemonEpoch,
		},
		NowMs: d.cfg.NowFn,
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
		Fencing: func(ctx context.Context, _ channel.ID) (placement.OwnerEpoch, placement.FencingToken, error) {
			row, ok, err := lock.Get(ctx)
			if err != nil {
				return 0, "", err
			}
			if !ok {
				return 0, "", errors.New("runtime: channel_lock missing for viewsync push")
			}
			return row.OwnerEpoch, row.FencingToken, nil
		},
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

	// Wrapped chain — same fencing + post-harness gateway dispatch
	// + MarkDelivered behavior the daemon's WriteMessage handler uses
	// (FIX-T3 / FIX-T6). Shared across the WriteMessage entrypoint AND the
	// adapter framework so adapter responses (and timer-fired failed
	// terminals) flow through the same invariants.
	wrappedChain := &postHarnessChain{
		chain:    chain,
		gateway:  gw,
		messages: messages,
		log:      &d.log,
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

	// T147 §A — per-channel devicetransit.DeviceTransit. The instance shares
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
			OnRecv: func(ctx context.Context, frame devicetransit.SendFrame) error {
				cb, _ := cr.deviceCallback.Load().(func(context.Context, devicetransit.SendFrame) error)
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
//   - non-nil (P4 wiring, cmd/daemon) → builds a workerhost.Bridge and
//     registers a handler that ticks the counter AND calls
//     bridge.OnTrigger so the trigger envelope reaches a real worker.
func (d *Daemon) ensureChannelAgent(ctx context.Context, cr *channelRuntime) error {
	_, ok, err := cr.registry.Lookup(ctx, cr.channelAgentID)
	if err != nil {
		return fmt.Errorf("runtime: ensure channel-agent lookup %s: %w", cr.channelID, err)
	}
	if !ok {
		if err := cr.registry.Insert(ctx, actorreg.Record{
			ID:          cr.channelAgentID,
			Kind:        actor.KindAgent,
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

	// P4 wire: build bridge when a spawner is configured. Otherwise
	// fall back to the P2 counter stub so tests don't pay the spawn cost.
	if d.cfg.WorkerSpawner != nil {
		leaseStore := workerhost.NewLeaseStore(cr.db)
		// M1.6-T5 phase-3 + phase-4 — pack the per-channel domain
		// prompt, channel type, channel id, and channel-sqlite path
		// into the worker spawn env. Empty values are still passed so
		// the worker can distinguish "no template" from "missing wire".
		// Order is:
		//   COAGENT_CHANNEL_TYPE=<type>     (may be "")
		//   COAGENT_DOMAIN_PROMPT=<prompt>  (may be ""; omitted entirely if empty)
		//   COAGENT_CHANNEL_ID=<id>         (always set for owned channels)
		//   COAGENT_CHANNEL_DB=<abs-path>   (always set; xhs-cli dedupe reads it ro)
		// mock_bridge / kimi_bridge read these directly via os.Getenv;
		// no extra IPC frame is introduced (the prompt is base-prompt
		// scaffolding, not a per-turn signal). coagent ask resolves
		// COAGENT_CHANNEL_ID for the gateway URL path, and
		// adapters/device/xhs/cli reads COAGENT_CHANNEL_DB to perform
		// the duplicate-publish business-invariant check.
		workerEnv := d.buildWorkerEnvForChannel(lockRow.ChannelType)
		workerEnv = append(workerEnv,
			"COAGENT_CHANNEL_ID="+string(cr.channelID),
			"COAGENT_CHANNEL_DB="+filepath.Join(d.cfg.ChannelsDir, string(cr.channelID), "channel.sqlite"),
		)
		// M1.6 follow-up — agent self-awareness fix. PreSpawn snapshots
		// the channel's actor_registry / type_registry / device_sessions
		// at every Spawner.Spawn call (i.e. fresh on first trigger after
		// boot and on every worker re-spawn after crash). Snapshot is
		// taken AFTER OnChannelBoot has installed adapter types, so the
		// type_registry list is complete by the time the worker boots.
		// File path lives inside the channel workdir so it shares the
		// same lifecycle as the channel sqlite (cleaned up on channel
		// archive). Worker reads it via COAGENT_CHANNEL_CONTEXT_FILE.
		channelCtxPath := filepath.Join(d.cfg.ChannelsDir, string(cr.channelID), "worker-context.json")
		channelIDCopy := cr.channelID
		channelTypeCopy := lockRow.ChannelType
		registryCopy := cr.registry
		typeRegistryCopy := cr.typeRegistry
		deviceListerCopy := d.cfg.ListDeviceSessionsForChannel
		preSpawn := func(spawnCtx context.Context) ([]string, error) {
			if err := writeChannelContextSnapshot(
				spawnCtx,
				channelCtxPath,
				channelIDCopy,
				channelTypeCopy,
				registryCopy,
				typeRegistryCopy,
				deviceListerCopy,
			); err != nil {
				return nil, err
			}
			return []string{"COAGENT_CHANNEL_CONTEXT_FILE=" + channelCtxPath}, nil
		}
		bridge, err := workerhost.NewBridge(workerhost.BridgeConfig{
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
			WorkerEnv:     workerEnv,
			PreSpawn:      preSpawn,
		})
		if err != nil {
			return fmt.Errorf("runtime: ensure channel-agent bridge %s: %w", cr.channelID, err)
		}
		cr.workerBridge = bridge
		cr.deliverer.Register(cr.channelAgentID, func(ctx context.Context, id actor.ActorID, env *message.Envelope) error {
			cr.channelAgentTriggers.Add(1)
			return bridge.OnTrigger(ctx, id, env)
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
	cr, ok := d.getChannel(chID)
	if !ok {
		return -1
	}
	return cr.channelAgentTriggers.Load()
}

// buildWorkerEnvForChannel assembles the per-channel "KEY=VALUE" env
// list BridgeConfig.WorkerEnv carries (M1.6-T5 phase-3). cmd/daemon
// sets DaemonConfig.ChannelTemplates so resolveTemplate returns the L4
// snapshot keyed by the lock row's ChannelType; the DomainPrompt is
// either the §2.4 segment (e.g. xhs-creator) or "" (generic group).
//
// The env always contains COAGENT_CHANNEL_TYPE so the worker bridge can
// distinguish "no template" (empty) from "template missing" (key
// absent). COAGENT_DOMAIN_PROMPT is only emitted when non-empty to
// avoid wasting the cmd.Env slot for legacy channels.
//
// Exported via tests through WorkerEnvForChannel below.
func (d *Daemon) buildWorkerEnvForChannel(channelType string) []string {
	env := make([]string, 0, 3)
	env = append(env, "COAGENT_CHANNEL_TYPE="+channelType)
	if d.resolveTemplate != nil {
		if prompt := d.resolveTemplate(channelType).DomainPrompt; prompt != "" {
			env = append(env, "COAGENT_DOMAIN_PROMPT="+prompt)
		}
	}
	return env
}

// WorkerEnvForChannel is the test-facing accessor that mirrors the env
// the Daemon would hand to workerhost.BridgeConfig.WorkerEnv for the
// supplied channel type. Returns the COAGENT_* "KEY=VALUE" slice in
// resolution order. Used by daemon_test / template_integration_test to
// assert the prompt env shape without spawning a real worker.
func (d *Daemon) WorkerEnvForChannel(channelType string) []string {
	return d.buildWorkerEnvForChannel(channelType)
}

// writeChannelContextSnapshot snapshots the channel's actor_registry +
// type_registry + (optional) device_sessions rows and persists a JSON
// file at path. The worker subprocess reads this file via the
// COAGENT_CHANNEL_CONTEXT_FILE env var and folds it into the kimi
// system prompt (M1.6 follow-up — agent self-awareness fix).
//
// The JSON shape matches adapters/llm/kimi.ChannelContext so the
// worker can json.Unmarshal directly without a re-encoding step. We
// keep the shape inline here (anonymous structs) rather than import
// adapters/** because go-arch-lint forbids runtime ↛ adapters.
// Drift between the two shapes is caught by an integration test (a
// new test in daemon_test) that round-trips a snapshot through the
// worker bridge's loader.
//
// Errors are returned unwrapped so the caller (PreSpawn closure) can
// decide whether to abort the spawn. Today every error is non-fatal:
// the worker boots without the appendix, same as a legacy channel.
func writeChannelContextSnapshot(
	ctx context.Context,
	path string,
	channelID channel.ID,
	channelType string,
	registry *store.ActorRegistry,
	types *store.TypeRegistry,
	deviceLister func(ctx context.Context, ch channel.ID) []DeviceSessionInfo,
) error {
	type actorJSON struct {
		ActorID     string `json:"actor_id"`
		Kind        string `json:"kind"`
		Binding     string `json:"binding,omitempty"`
		DisplayName string `json:"display_name,omitempty"`
	}
	type typeJSON struct {
		Type           string   `json:"type"`
		HandlerActorID string   `json:"handler_actor_id"`
		HandlerBinding string   `json:"handler_binding,omitempty"`
		AllowedKinds   []string `json:"allowed_kinds,omitempty"`
		MaxPendingMs   int64    `json:"max_pending_ms,omitempty"`
	}
	type deviceJSON struct {
		SessionID  string `json:"session_id"`
		DeviceID   string `json:"device_id,omitempty"`
		DeviceType string `json:"device_type,omitempty"`
		State      string `json:"state,omitempty"`
	}
	type snapshotJSON struct {
		ChannelID   string       `json:"channel_id,omitempty"`
		ChannelType string       `json:"channel_type,omitempty"`
		Actors      []actorJSON  `json:"actors,omitempty"`
		Types       []typeJSON   `json:"types,omitempty"`
		Devices     []deviceJSON `json:"devices,omitempty"`
	}

	snap := snapshotJSON{
		ChannelID:   string(channelID),
		ChannelType: channelType,
	}

	if registry != nil {
		actors, err := registry.ListActive(ctx)
		if err != nil {
			return fmt.Errorf("runtime: snapshot actor_registry %s: %w", channelID, err)
		}
		for _, a := range actors {
			snap.Actors = append(snap.Actors, actorJSON{
				ActorID:     string(a.ID),
				Kind:        string(a.Kind),
				Binding:     string(a.Binding),
				DisplayName: a.DisplayName,
			})
		}
	}

	if types != nil {
		rows, err := types.List(ctx)
		if err != nil {
			return fmt.Errorf("runtime: snapshot type_registry %s: %w", channelID, err)
		}
		for _, r := range rows {
			allowed := make([]string, 0, len(r.AllowedKinds))
			for _, k := range r.AllowedKinds {
				allowed = append(allowed, string(k))
			}
			snap.Types = append(snap.Types, typeJSON{
				Type:           r.Type,
				HandlerActorID: string(r.HandlerActorID),
				HandlerBinding: string(r.HandlerBinding),
				AllowedKinds:   allowed,
				MaxPendingMs:   r.MaxPendingMs,
			})
		}
	}

	if deviceLister != nil {
		for _, d := range deviceLister(ctx, channelID) {
			// deviceJSON is structurally identical to DeviceSessionInfo;
			// staticcheck S1016 prefers the explicit conversion to a
			// field-by-field literal copy.
			snap.Devices = append(snap.Devices, deviceJSON(d))
		}
	}

	// MarshalIndent for human grep-ability — the file is small (single
	// channel, typically <100 actors/types/devices) and is written at
	// worker spawn cadence (rare), so the extra bytes are cheap.
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("runtime: snapshot marshal %s: %w", channelID, err)
	}
	// Channel workdir already exists by this point (bootstrap saga
	// step 5c mkdirs it before any ensureChannelAgent runs); but be
	// defensive in case a test wires the daemon without the saga.
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("runtime: snapshot mkdir %s: %w", channelID, err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("runtime: snapshot write %s: %w", channelID, err)
	}
	return nil
}

// CurrentWorkerIDFor returns the id of the worker subprocess currently
// alive for the channel, or "" when no worker is alive (bridge not
// configured, channel not owned, worker crashed). Test accessor for
// the M1.6-T1 e2e reuse check.
func (d *Daemon) CurrentWorkerIDFor(chID channel.ID) string {
	cr, ok := d.getChannel(chID)
	if !ok || cr.workerBridge == nil {
		return ""
	}
	return cr.workerBridge.CurrentWorkerID()
}

// bootChannel wires a per-channel runtime + outbox pusher goroutine
// and registers a teardown function with lifecycle.Unloader. Used by
// both phase-3 cold-start AND the hot OnCreateChannel handler so the
// two paths stay symmetric (single source of "what does it take to
// bring a channel up").
func (d *Daemon) bootChannel(ctx context.Context, lc lifecycle.LocalChannel, startPusher bool) error {
	cr, err := d.buildChannelRuntime(ctx, lc)
	if err != nil {
		return err
	}
	d.setChannel(lc.ChannelID, cr)

	// M1.6-T1 — register the per-channel agent target so trigger gateway
	// fan-out has somewhere to route audience=['*']. Done before the
	// pusher goroutine starts so a write that immediately follows boot
	// still observes the handler.
	if err := d.ensureChannelAgent(ctx, cr); err != nil {
		d.deleteChannel(lc.ChannelID)
		return err
	}

	// M1.6-T2 — OnChannelBoot hook lets cmd/daemon wire the adapter
	// framework Manager + register Dispatch handlers on the Deliverer.
	// Run BEFORE starting the pusher goroutine so a failing hook unwinds
	// cleanly (no orphan goroutine).
	if d.cfg.OnChannelBoot != nil {
		// M1.6-T5 phase-2 — surface the L4 channel-template key into
		// hooks so cmd/daemon factories can gate themselves (e.g.
		// XHSScaffoldFactory installs ONLY for type=="xhs-creator").
		// The lock row is the single source of truth across both the
		// hot OnCreateChannel path (lock just written from req) and
		// the cold-start path (lock pre-existed on disk).
		channelType := lc.Lock.ChannelType
		var domainPrompt string
		if d.resolveTemplate != nil {
			domainPrompt = d.resolveTemplate(channelType).DomainPrompt
		}
		hooks := ChannelHooks{
			ChannelID:     lc.ChannelID,
			ChannelType:   channelType,
			DomainPrompt:  domainPrompt,
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
			Logger:       &d.log,
		}
		// T147 §A — expose the per-channel DeviceTransit + an inbound
		// callback hook so the composition root can build a
		// runtime_inbound_via_relay-capable framework.Manager.
		if cr.deviceTransit != nil {
			hooks.DeviceTransit = cr.deviceTransit
			// Capture cr in the closure (atomic.Value publishes the new
			// callback to the OnRecv reader without taking a lock).
			capturedCR := cr
			hooks.SetDeviceCallback = func(cb func(context.Context, devicetransit.SendFrame) error) {
				if cb == nil {
					// atomic.Value cannot store a typed nil — publish a
					// non-nil sentinel that resolves to "drop" on the
					// reader side.
					capturedCR.deviceCallback.Store(func(context.Context, devicetransit.SendFrame) error { return nil })
					return
				}
				capturedCR.deviceCallback.Store(cb)
			}
		}
		teardown, hookErr := d.cfg.OnChannelBoot(ctx, hooks)
		if hookErr != nil {
			d.deleteChannel(lc.ChannelID)
			return fmt.Errorf("runtime: channel %s on_boot hook: %w", lc.ChannelID, hookErr)
		}
		cr.teardown = teardown
	}

	// Register the teardown: shut down worker bridge (T1) and adapter
	// framework (T2 via cr.teardown), cancel pusher ctx, drop the
	// runtime entry. Sqlite handle stays open — Close() reaps it at
	// shutdown (unbind is not wipe; channels/<id>/ stays on disk).
	chID := lc.ChannelID
	d.unloader.Register(chID, func() error {
		if cr.workerBridge != nil {
			closeCtx, cc := context.WithTimeout(context.Background(), 3*time.Second)
			_ = cr.workerBridge.Close(closeCtx)
			cc()
		}
		if cr.teardown != nil {
			tctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := cr.teardown(tctx); err != nil {
				d.log.Warn().Err(err).
					Str("event", "runtime.channel_teardown_failed").
					Str("channel_id", string(chID)).
					Msg("channel teardown returned error")
			}
			cancel()
		}
		cr.cancelPusher()
		d.deleteChannel(chID)
		d.booter.Unload(chID)
		return nil
	})
	if startPusher {
		d.startChannelPusher(cr)
	}
	return nil
}

func (d *Daemon) startChannelPusher(cr *channelRuntime) {
	if cr == nil || cr.pusher == nil {
		return
	}
	cr.pushMu.Lock()
	if cr.pausePush != nil {
		cr.pushMu.Unlock()
		return
	}
	pusherCtx, pusherCancel := context.WithCancel(d.runCtx)
	cr.pausePush = pusherCancel
	cr.pushMu.Unlock()

	d.wg.Add(1)
	go func(p *transit.Pusher, id channel.ID) {
		defer d.wg.Done()
		if err := p.Pump(pusherCtx); err != nil {
			if !errors.Is(err, context.Canceled) {
				// Structured log so cmd/daemon JSON stream captures it
				// alongside dispatcher/heartbeat events.
				d.log.Warn().Err(err).
					Str("event", "runtime.pusher_exited").
					Str("channel_id", string(id)).
					Msg("per-channel pusher exited with error")
			}
		}
	}(cr.pusher, cr.channelID)
}

func (d *Daemon) startChannelPusherFor(id channel.ID) {
	cr, ok := d.getChannel(id)
	if !ok {
		return
	}
	d.startChannelPusher(cr)
}

// handleCreateChannel is the OnCreateChannel handler — T0.1. It is the
// daemon-side Phase 2 of the bootstrap saga (proto-foundation §3.3.3 +
// impl-layer2 §3.2.2): the daemon is the trust root for fencing.
//
// Fresh-bootstrap path:
//   - Generate a fresh unguessable fencing_token (placement.NewFencingToken).
//   - Set owner_epoch=1 (proto-foundation §3.3.3 Phase 2 fixed value).
//   - Run the bootstrap saga (steps 1-5).
//   - Insert channel_lock with the daemon-generated tuple.
//   - Append the singleton system.channel.created event as message seq=1.
//   - Mark the saga completed and mount the new channel runtime.
//   - Emit result=accepted carrying (owner_epoch, fencing_token, daemon_id) so
//     the server's Phase 3 CAS can write them into the placement row.
//
// Idempotent-replay path: when the channel already has a channel_lock
// row on disk we re-emit the daemon-stored fencing tuple. Distinguishing
// "same saga retry" from "conflicting reuse of channel_id" is done via
// bootstrap_registry.channel_id (UNIQUE) — if the existing row was created
// by req.CreateRequestID the replay is honoured, otherwise the server
// is trying to allocate the same channel_id twice and we reject.
//
// Failure semantics:
//   - saga.Bootstrap fails → control.reject_channel (reason=saga error). bootstrap_registry
//     row stays 'in_progress'; reconcile loop rolls it back on next restart.
//   - LoadOne / buildChannelRuntime fail → control.reject_channel; channel_lock row
//     remains so reconciler can drive recovery.
func (d *Daemon) handleCreateChannel(
	ctx context.Context,
	frame daemonbus.Frame,
	req placement.CreateChannelRequest,
) error {
	ack := placement.CreateChannelAck{
		FrameID:         string(frame.FrameID),
		ChannelID:       req.ChannelID,
		CreateRequestID: req.CreateRequestID,
		DaemonID:        placement.DaemonID(d.cfg.DaemonID),
		DaemonEpoch:     placement.DaemonEpoch(d.cfg.DaemonEpoch),
	}
	reject := func(reason string) error {
		return d.sendRejectChannel(ctx, frame.FrameID, placement.RejectChannel{
			FrameID:         string(frame.FrameID),
			ChannelID:       req.ChannelID,
			CreateRequestID: req.CreateRequestID,
			Reason:          reason,
		})
	}

	if req.ChannelID == "" {
		return reject("empty_channel_id")
	}
	if req.CreateRequestID == "" {
		return reject("empty_create_request_id")
	}

	sqlitePath := filepath.Join(d.cfg.ChannelsDir, string(req.ChannelID), "channel.sqlite")

	// Idempotency / conflict pre-check: if a channel.sqlite already exists
	// on disk and carries a channel_lock row, decide between idempotent
	// replay (same create_request_id) and conflicting reuse (different
	// create_request_id) using bootstrap_registry. When the sqlite is
	// absent we fall through to fresh-bootstrap below — OpenChannelLock
	// would otherwise CREATE an empty sqlite file with no DDL, leaving a
	// half-baked channel dir behind.
	sqliteExists := false
	if _, err := os.Stat(sqlitePath); err == nil {
		sqliteExists = true
	}
	if sqliteExists {
		existingLock, err := d.OpenChannelLock(ctx, sqlitePath)
		if err != nil {
			return reject(fmt.Sprintf("lock open: %v", err))
		}
		row, ok, getErr := existingLock.Get(ctx)
		if getErr != nil {
			return reject(fmt.Sprintf("lock get: %v", getErr))
		}
		if ok {
			// daemon-side ownership check — a different daemon binary
			// process is responsible for this channel, reject.
			if row.DaemonID != placement.DaemonID(d.cfg.DaemonID) {
				return reject("daemon_id_mismatch")
			}
			// Idempotency oracle: bootstrap_registry.create_request_id
			// is the saga identifier — match it against req to decide
			// whether this is a benign replay or a colliding new saga.
			originalReqID, lookupErr := d.lookupBootstrapRequestID(ctx, req.ChannelID)
			if lookupErr != nil {
				return reject(fmt.Sprintf("bootstrap registry lookup: %v", lookupErr))
			}
			if originalReqID != string(req.CreateRequestID) {
				// Server is asking us to create the same channel under a
				// new saga id — local state is the source of truth, reject.
				return reject("create_request_id_mismatch")
			}
			// Idempotent replay — first repair the Layer 0 bootstrap
			// event if the daemon crashed after writing channel_lock but
			// before appending system.channel.created, then re-emit the
			// daemon-stored fencing tuple so the server's Phase 3 CAS sees
			// consistent values across retries.
			if err := d.ensureChannelCreatedEvent(ctx, sqlitePath, existingLock, row); err != nil {
				return reject(fmt.Sprintf("channel created event: %v", err))
			}
			ack.OwnerEpoch = row.OwnerEpoch
			ack.FencingToken = row.FencingToken
			if !d.HasChannel(req.ChannelID) {
				if err := d.mountExistingChannel(ctx, req.ChannelID, sqlitePath, false); err != nil {
					return reject(fmt.Sprintf("mount existing: %v", err))
				}
			}
			ack.Result = placement.CreateChannelAccepted
			if err := d.sendCreateAck(ctx, frame.FrameID, ack); err != nil {
				return err
			}
			d.startChannelPusherFor(req.ChannelID)
			return nil
		}
	}

	// Fresh bootstrap path: generate fencing tuple FIRST (the daemon is
	// the trust root — proto-foundation §3.3.3 Phase 2), then run the
	// saga (steps 1-5), insert channel_lock (step 6), and finally mark
	// bootstrap complete (step 7).
	fencingToken, err := placement.NewFencingToken()
	if err != nil {
		return reject(fmt.Sprintf("fencing token: %v", err))
	}
	const bootstrapOwnerEpoch = placement.OwnerEpoch(1)

	if _, err := d.saga.Bootstrap(ctx, req.ChannelID, req); err != nil {
		return reject(fmt.Sprintf("saga: %v", err))
	}
	lockStore, err := d.OpenChannelLock(ctx, sqlitePath)
	if err != nil {
		return reject(fmt.Sprintf("lock open: %v", err))
	}
	now := d.cfg.NowFn()
	if err := lockStore.Insert(ctx, store.ChannelLockRow{
		ChannelID:    req.ChannelID,
		FencingToken: fencingToken,
		OwnerEpoch:   bootstrapOwnerEpoch,
		DaemonID:     placement.DaemonID(d.cfg.DaemonID),
		DaemonEpoch:  placement.DaemonEpoch(d.cfg.DaemonEpoch),
		AcquiredAt:   now,
		RefreshedAt:  now,
		// M1.6-T5 phase-2 — persist the L4 channel-template key so a
		// cold-start daemon can re-resolve the template without
		// round-tripping the server.
		ChannelType: req.ChannelType,
	}); err != nil {
		return reject(fmt.Sprintf("lock insert: %v", err))
	}
	if err := d.ensureChannelCreatedEvent(ctx, sqlitePath, lockStore, store.ChannelLockRow{
		ChannelID:    req.ChannelID,
		FencingToken: fencingToken,
		OwnerEpoch:   bootstrapOwnerEpoch,
		DaemonID:     placement.DaemonID(d.cfg.DaemonID),
		DaemonEpoch:  placement.DaemonEpoch(d.cfg.DaemonEpoch),
		ChannelType:  req.ChannelType,
	}); err != nil {
		return reject(fmt.Sprintf("channel created event: %v", err))
	}
	if err := d.saga.Complete(ctx, string(req.CreateRequestID)); err != nil {
		return reject(fmt.Sprintf("bootstrap complete: %v", err))
	}

	if err := d.mountExistingChannel(ctx, req.ChannelID, sqlitePath, false); err != nil {
		return reject(fmt.Sprintf("mount: %v", err))
	}
	ack.OwnerEpoch = bootstrapOwnerEpoch
	ack.FencingToken = fencingToken
	ack.Result = placement.CreateChannelAccepted
	if err := d.sendCreateAck(ctx, frame.FrameID, ack); err != nil {
		return err
	}
	d.startChannelPusherFor(req.ChannelID)
	return nil
}

// ensureChannelCreatedEvent appends the Layer 0 bootstrap event before
// saga completion / mount side effects. The deterministic id makes create
// retries and crash repair idempotent; the seq guard keeps the vocabulary
// invariant that system.channel.created is the first channel message.
func (d *Daemon) ensureChannelCreatedEvent(
	ctx context.Context,
	sqlitePath string,
	lock *store.ChannelLock,
	row store.ChannelLockRow,
) error {
	db, err := d.openChannelDB(ctx, sqlitePath)
	if err != nil {
		return err
	}
	now := d.cfg.NowFn()
	payloadMap := map[string]any{
		"channel_id":  row.ChannelID,
		"daemon_id":   row.DaemonID,
		"owner_epoch": row.OwnerEpoch,
		"created_at":  now,
	}
	if row.ChannelType != "" {
		payloadMap["channel_type"] = row.ChannelType
	}
	payload, err := json.Marshal(payloadMap)
	if err != nil {
		return err
	}
	env := &message.Envelope{
		ID:         message.ID("system.channel.created:" + string(row.ChannelID)),
		TS:         now,
		TSReceived: now,
		ChannelID:  row.ChannelID,
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "system.channel.created",
		Payload:    payload,
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{message.AudienceWildcard},
	}
	if env.CanonicalHash, err = message.CanonicalHash(*env); err != nil {
		return fmt.Errorf("canonical hash: %w", err)
	}
	res, err := store.NewMessagesWithLock(db, lock).Append(ctx, env, khlog.FencingTuple{
		Token: row.FencingToken,
		Epoch: row.DaemonEpoch,
	})
	if err != nil {
		return err
	}
	if res.Seq != 1 {
		return fmt.Errorf("system.channel.created seq=%d want 1", res.Seq)
	}
	return nil
}

// lookupBootstrapRequestID returns the create_request_id that originally
// bootstrapped channelID. Used by handleCreateChannel to detect
// conflicting reuse of a channel_id (the server attempting to allocate
// the same channel under a new saga id) vs benign idempotent replay
// (server retrying the original saga frame).
//
// Returns "" when no bootstrap_registry row exists — caller treats that
// as "no original saga", which falls back to the fresh-bootstrap path.
func (d *Daemon) lookupBootstrapRequestID(ctx context.Context, channelID channel.ID) (string, error) {
	const q = `SELECT create_request_id FROM bootstrap_registry WHERE channel_id = ?`
	var reqID string
	err := d.DaemonDB().QueryRowContext(ctx, q, string(channelID)).Scan(&reqID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("daemon: bootstrap_registry lookup: %w", err)
	}
	return reqID, nil
}

// mountExistingChannel hot-loads a channel that has its bootstrap saga
// + channel_lock already on disk: it runs lifecycle.Bootstrapper.LoadOne
// + bootChannel so the new channel id becomes routable for
// OnWriteMessage / viewsync without restarting the daemon.
func (d *Daemon) mountExistingChannel(ctx context.Context, id channel.ID, sqlitePath string, startPusher bool) error {
	lc, err := d.booter.LoadOne(ctx, id, sqlitePath)
	if err != nil {
		return err
	}
	return d.bootChannel(ctx, lc, startPusher)
}

// sendCreateAck wraps the transit Send for a CreateChannelAck. Pulled
// out so handleCreateChannel doesn't repeat the boilerplate.
//
// FIX-2026-05-18: the envelope frame_id we emit MUST echo the inbound
// create_channel envelope frame_id so the server-side
// daemonbus.Connection.matchAck (keyed on the SendAndAwait envelope
// frame_id) can pair the ack with the originating caller. The legacy
// create-channel HTTP path consumes the ack via the OnCreateChannelAck
// hook (no SendAndAwait), but we keep the policy uniform across all
// ack frames so future SendAndAwait callers don't re-trip the bug.
// Empty inboundFrameID falls back to a fresh generator id (test-only
// path; production always carries a frame_id).
func (d *Daemon) sendCreateAck(ctx context.Context, inboundFrameID daemonbus.FrameID, ack placement.CreateChannelAck) error {
	frameID := string(inboundFrameID)
	if frameID == "" {
		frameID = d.cfg.FrameIDGen()
	}
	return d.transit.Send(ctx, frameID,
		daemonbus.FrameTypeControlCreateChannelAck, ack)
}

func (d *Daemon) sendRejectChannel(ctx context.Context, inboundFrameID daemonbus.FrameID, rej placement.RejectChannel) error {
	frameID := string(inboundFrameID)
	if frameID == "" {
		frameID = d.cfg.FrameIDGen()
	}
	return d.transit.Send(ctx, frameID,
		daemonbus.FrameTypeControlRejectChannel, rej)
}

func (d *Daemon) handleDaemonReclaim(
	ctx context.Context,
	frame daemonbus.Frame,
	req placement.DaemonReclaimRequest,
) error {
	reject := func(reason placement.ReclaimRejectReason) error {
		return d.sendReclaimRejected(ctx, frame.FrameID, placement.ReclaimRejected{
			ChannelID:       req.ChannelID,
			CreateRequestID: req.CreateRequestID,
			Reason:          reason,
		})
	}
	if req.ChannelID == "" || req.CreateRequestID == "" || req.NewOwnerEpoch <= 0 {
		return reject(placement.ReclaimRejectInternalError)
	}
	sqlitePath := filepath.Join(d.cfg.ChannelsDir, string(req.ChannelID), "channel.sqlite")
	if _, err := os.Stat(sqlitePath); err != nil {
		return reject(placement.ReclaimRejectStoreMissing)
	}
	db, err := d.openChannelDB(ctx, sqlitePath)
	if err != nil {
		return reject(placement.ReclaimRejectStoreMissing)
	}
	if ok, err := reclaimStoreComplete(ctx, db); err != nil || !ok {
		if err != nil {
			d.log.Warn().Err(err).
				Str("event", "runtime.reclaim_completeness_check_failed").
				Str("channel_id", string(req.ChannelID)).
				Msg("failed to inspect reclaim store")
		}
		return reject(placement.ReclaimRejectCompletenessCheckFailed)
	}
	lock := store.NewChannelLock(db)
	row, ok, err := lock.Get(ctx)
	if err != nil {
		return reject(placement.ReclaimRejectCompletenessCheckFailed)
	}
	if !ok || row.ChannelID != req.ChannelID {
		return reject(placement.ReclaimRejectStoreMissing)
	}
	if row.OwnerEpoch == req.NewOwnerEpoch && row.DaemonID == placement.DaemonID(d.cfg.DaemonID) {
		previousOwnerEpoch := req.NewOwnerEpoch - 1
		if previousOwnerEpoch < 0 {
			previousOwnerEpoch = 0
		}
		if err := d.emitPlacementReclaimed(ctx, db, lock, req, previousOwnerEpoch, row.FencingToken); err != nil {
			return reject(placement.ReclaimRejectInternalError)
		}
		if !d.HasChannel(req.ChannelID) {
			if err := d.mountExistingChannel(ctx, req.ChannelID, sqlitePath, true); err != nil {
				return reject(placement.ReclaimRejectInternalError)
			}
		}
		return d.sendReclaimAccepted(ctx, frame.FrameID, placement.ReclaimAccepted{
			ChannelID:       req.ChannelID,
			CreateRequestID: req.CreateRequestID,
			NewOwnerEpoch:   req.NewOwnerEpoch,
			FencingToken:    row.FencingToken,
		})
	}
	if row.OwnerEpoch+1 != req.NewOwnerEpoch {
		return reject(placement.ReclaimRejectOwnerEpochInvalid)
	}
	fencingToken, err := placement.NewFencingToken()
	if err != nil {
		return reject(placement.ReclaimRejectInternalError)
	}
	now := d.cfg.NowFn()
	newRow := row
	newRow.FencingToken = fencingToken
	newRow.OwnerEpoch = req.NewOwnerEpoch
	newRow.DaemonID = placement.DaemonID(d.cfg.DaemonID)
	newRow.DaemonEpoch = placement.DaemonEpoch(d.cfg.DaemonEpoch)
	newRow.AcquiredAt = now
	newRow.RefreshedAt = now
	if err := lock.Takeover(ctx, newRow, row.OwnerEpoch); err != nil {
		return reject(placement.ReclaimRejectInternalError)
	}
	if err := d.emitPlacementReclaimed(ctx, db, lock, req, row.OwnerEpoch, fencingToken); err != nil {
		return reject(placement.ReclaimRejectInternalError)
	}
	if d.HasChannel(req.ChannelID) {
		if err := d.unloader.Unload(ctx, req.ChannelID, lifecycle.UnloadStale); err != nil {
			return reject(placement.ReclaimRejectInternalError)
		}
	}
	if err := d.mountExistingChannel(ctx, req.ChannelID, sqlitePath, true); err != nil {
		return reject(placement.ReclaimRejectInternalError)
	}
	return d.sendReclaimAccepted(ctx, frame.FrameID, placement.ReclaimAccepted{
		ChannelID:       req.ChannelID,
		CreateRequestID: req.CreateRequestID,
		NewOwnerEpoch:   req.NewOwnerEpoch,
		FencingToken:    fencingToken,
	})
}

func reclaimStoreComplete(ctx context.Context, db *sql.DB) (bool, error) {
	for _, table := range []string{"messages", "actor_registry", "type_registry", "channel_lock"} {
		var name string
		err := db.QueryRowContext(
			ctx,
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`,
			table,
		).Scan(&name)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	var systemActor string
	err := db.QueryRowContext(
		ctx,
		`SELECT actor_id FROM actor_registry WHERE actor_id=? AND deregistered_at IS NULL`,
		string(actor.SystemActorID),
	).Scan(&systemActor)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (d *Daemon) emitPlacementReclaimed(
	ctx context.Context,
	db *sql.DB,
	lock *store.ChannelLock,
	req placement.DaemonReclaimRequest,
	previousOwnerEpoch placement.OwnerEpoch,
	fencingToken placement.FencingToken,
) error {
	now := d.cfg.NowFn()
	payload, err := json.Marshal(map[string]any{
		"channel_id":               req.ChannelID,
		"new_owner_daemon_id":      d.cfg.DaemonID,
		"new_owner_epoch":          req.NewOwnerEpoch,
		"previous_owner_daemon_id": req.PreviousOwnerDaemon,
		"previous_owner_epoch":     previousOwnerEpoch,
		"reclaimed_from_state":     req.PreviousState,
		"reclaimed_at":             now,
	})
	if err != nil {
		return err
	}
	_, err = store.NewMessagesWithLock(db, lock).Append(ctx, &message.Envelope{
		ID:         message.ID("system.placement.reclaimed:" + string(req.ChannelID) + ":" + string(req.CreateRequestID)),
		TS:         now,
		TSReceived: now,
		ChannelID:  req.ChannelID,
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "system.placement.reclaimed",
		Payload:    payload,
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{actor.SystemActorID},
	}, khlog.FencingTuple{
		Token: fencingToken,
		Epoch: placement.DaemonEpoch(d.cfg.DaemonEpoch),
	})
	return err
}

func (d *Daemon) sendReclaimAccepted(ctx context.Context, inboundFrameID daemonbus.FrameID, ack placement.ReclaimAccepted) error {
	frameID := string(inboundFrameID)
	if frameID == "" {
		frameID = d.cfg.FrameIDGen()
	}
	return d.transit.Send(ctx, frameID, daemonbus.FrameTypeControlReclaimAccepted, ack)
}

func (d *Daemon) sendReclaimRejected(ctx context.Context, inboundFrameID daemonbus.FrameID, rej placement.ReclaimRejected) error {
	frameID := string(inboundFrameID)
	if frameID == "" {
		frameID = d.cfg.FrameIDGen()
	}
	return d.transit.Send(ctx, frameID, daemonbus.FrameTypeControlReclaimRejected, rej)
}

// handleUnbindChannel — T0.2. Closes the per-channel pusher
// goroutine (via lifecycle.Unloader teardown), drops the runtime
// entry from d.channels, and emits control.unbind_channel_ack. Does
// NOT delete channels/<id>/ on disk — that's the GC schedule, not the
// unbind path.
func (d *Daemon) handleUnbindChannel(ctx context.Context, frame daemonbus.Frame) error {
	var body daemonbus.UnbindChannelBody
	if len(frame.Payload) > 0 {
		if err := json.Unmarshal(frame.Payload, &body); err != nil {
			return fmt.Errorf("runtime: decode unbind_channel: %w", err)
		}
	}
	ack := daemonbus.UnbindChannelAckBody{
		ChannelID:  body.ChannelID,
		OwnerEpoch: body.OwnerEpoch,
	}
	// FIX-2026-05-18: echo inbound envelope frame_id (see sendCreateAck
	// for the full root-cause comment). Empty inbound id falls back to
	// the generator — only reachable under test stubs.
	replyFrameID := string(frame.FrameID)
	if replyFrameID == "" {
		replyFrameID = d.cfg.FrameIDGen()
	}
	if body.ChannelID == "" {
		ack.Result = daemonbus.UnbindChannelRejected
		ack.Reason = daemonbus.UnbindChannelRejectAlreadyReleased
		return d.transit.Send(ctx, replyFrameID,
			daemonbus.FrameTypeControlUnbindChannelAck, ack)
	}
	cr, ok := d.getChannel(body.ChannelID)
	if !ok {
		ack.Result = daemonbus.UnbindChannelRejected
		ack.Reason = daemonbus.UnbindChannelRejectAlreadyReleased
		return d.transit.Send(ctx, replyFrameID,
			daemonbus.FrameTypeControlUnbindChannelAck, ack)
	}
	row, lockOK, err := cr.lock.Get(ctx)
	if err != nil {
		ack.Result = daemonbus.UnbindChannelRejected
		ack.Reason = daemonbus.UnbindChannelRejectInternalError
		return d.transit.Send(ctx, replyFrameID,
			daemonbus.FrameTypeControlUnbindChannelAck, ack)
	}
	if !lockOK {
		ack.Result = daemonbus.UnbindChannelRejected
		ack.Reason = daemonbus.UnbindChannelRejectAlreadyReleased
		return d.transit.Send(ctx, replyFrameID,
			daemonbus.FrameTypeControlUnbindChannelAck, ack)
	}
	ack.OwnerEpoch = row.OwnerEpoch
	if body.OwnerEpoch != row.OwnerEpoch {
		ack.Result = daemonbus.UnbindChannelRejected
		ack.Reason = daemonbus.UnbindChannelRejectOwnerEpochStale
		return d.transit.Send(ctx, replyFrameID,
			daemonbus.FrameTypeControlUnbindChannelAck, ack)
	}
	if err := d.unloader.Unload(ctx, body.ChannelID, lifecycle.UnloadOrphan); err != nil {
		ack.Result = daemonbus.UnbindChannelRejected
		ack.Reason = daemonbus.UnbindChannelRejectInternalError
		return d.transit.Send(ctx, replyFrameID,
			daemonbus.FrameTypeControlUnbindChannelAck, ack)
	}
	ack.Result = daemonbus.UnbindChannelReleased
	return d.transit.Send(ctx, replyFrameID,
		daemonbus.FrameTypeControlUnbindChannelAck, ack)
}

func (d *Daemon) handleHeartbeatAck(ctx context.Context, frame daemonbus.Frame) error {
	if err := d.heartbeat.Handle(d.cfg.NowFn)(ctx, frame); err != nil {
		return err
	}
	var body placement.HeartbeatAckPayload
	if len(frame.Payload) == 0 {
		return nil
	}
	if err := transit.DecodePayload(frame, &body); err != nil {
		return fmt.Errorf("runtime: decode heartbeat_ack: %w", err)
	}
	for _, diff := range body.PlacementDiff {
		switch diff.Action {
		case placement.PlacementDiffActionOK:
			continue
		case placement.PlacementDiffActionReclaimPending:
			d.freezeChannel(ctx, diff.ChannelID, string(diff.Action))
		case placement.PlacementDiffActionUnbindPending:
			if d.HasChannel(diff.ChannelID) {
				_ = d.unloader.Unload(ctx, diff.ChannelID, lifecycle.UnloadUnbindPending)
			}
		case placement.PlacementDiffActionDirectoryMissing:
			if d.HasChannel(diff.ChannelID) {
				_ = d.unloader.Unload(ctx, diff.ChannelID, lifecycle.UnloadDirectoryMissing)
			}
		}
	}
	return nil
}

// heldChannelsAckBody mirrors the control.held_channels_ack payload
// server/gateway emits.
type heldChannelsAckBody struct {
	DaemonID  string                           `json:"daemon_id"`
	Decisions []placement.HeldChannelsDecision `json:"decisions"`
}

// handleHeldChannelsAck records the server's cold-start held-channel
// decisions. Rejected entries trigger unload so the daemon cannot keep
// mutating a channel after ownership was revoked.
func (d *Daemon) handleHeldChannelsAck(ctx context.Context, frame daemonbus.Frame) error {
	var body heldChannelsAckBody
	if len(frame.Payload) > 0 {
		if err := json.Unmarshal(frame.Payload, &body); err != nil {
			return fmt.Errorf("runtime: decode held_channels_ack: %w", err)
		}
	}
	for _, dec := range body.Decisions {
		if dec.Accepted {
			d.reconciler.AcceptHeldChannel(dec.ChannelID)
			continue
		}
		d.reconciler.RejectHeldChannel(dec.ChannelID, dec.Reason)
		// Trigger per-channel unload — zombie writes are forbidden once
		// the server has revoked our ownership claim.
		if d.HasChannel(dec.ChannelID) {
			if err := d.unloader.Unload(ctx, dec.ChannelID, lifecycle.UnloadStale); err != nil {
				d.log.Warn().Err(err).
					Str("event", "runtime.held_channels_reject_unload_failed").
					Str("channel_id", string(dec.ChannelID)).
					Msg("failed to unload channel after held-channel rejection")
			}
		}
	}
	return nil
}

// openChannelDB reuses the cache populated by lifecycle.LockOpener.
// Subsequent calls return the same *sql.DB so the channel sqlite
// stays single-handle (sqlite WAL works best that way).
func (d *Daemon) openChannelDB(ctx context.Context, sqlitePath string) (*sql.DB, error) {
	d.channelDBsMu.Lock()
	defer d.channelDBsMu.Unlock()
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
func (d *Daemon) routeWrite(_ context.Context, ch channel.ID) (transit.HarnessChain, actorreg.Registry, transit.CallerStamper, bool) {
	cr, ok := d.getChannel(ch)
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
// fan-out. Ack frames don't carry channel_id at the kernel
// level — for the M1.6 baseline (1 channel ↔ 1 device), we fan them
// out to EVERY channel's DeviceTransit; each channel's correlation
// tracker decides whether the frame matches an outstanding request.
//
// Unknown channels / disabled hooks are dropped silently — at-least-
// once delivery means callers will retry if the daemon is not ready.
func (d *Daemon) handleDeviceTransitFrame(ctx context.Context, frame daemonbus.Frame) error {
	switch frame.FrameKind {
	case daemonbus.FrameTypeDeviceTransitSend:
		// Inbound `device_transit.send` (impl-layer2 §5.3.1, device →
		// server → daemon adapter). Decode the SendFrame payload to
		// extract the routing key (channel_id), then hand off to the
		// per-channel DeviceTransit for adapter-side fan-out.
		var payload devicetransit.SendFrame
		if err := transit.DecodePayload(frame, &payload); err != nil {
			return fmt.Errorf("runtime: decode device_transit %s: %w", frame.FrameKind, err)
		}
		if d.channelFrozen(payload.ChannelID) {
			return nil
		}
		cr, ok := d.getChannel(payload.ChannelID)
		if !ok || cr.deviceTransit == nil {
			// Unknown channel or no device transit wired — drop.
			return nil
		}
		return cr.deviceTransit.DispatchIncoming(ctx, frame)
	case daemonbus.FrameTypeDeviceTransitAck:
		// No channel_id on ack — fan out to every channel; each
		// channel's DeviceTransit checks frame_id correlation locally.
		// Returns the first non-nil error so observability still surfaces
		// the failure; downstream channels are best-effort.
		var firstErr error
		for _, cr := range d.snapshotChannelRuntimes() {
			if cr.deviceTransit == nil || (cr.wrappedChain != nil && cr.wrappedChain.IsFrozen()) {
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
//     trigger.Gateway.Dispatch + messages.MarkDelivered. Dispatch
//     failures record messages.MarkDeliveryError and leave delivered_at
//     NULL. This makes `emit not_before=future` honour its delayed-delivery
//     contract across daemon restarts (the persisted row remains scannable
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
//     can never be answered because nobody is listening). This path
//     is scanned independently from expires_at so NULL/future-deadline
//     rows close immediately per L1 §6.4 / L2 §3.7.1 Step 3.
//
// The synthesised response uses the system actor as sender (the canonical
// "harness wrote this on the channel's behalf" identity per L1 §3.2) and
// a deterministic id derived from the request id + reason so re-firing
// the scan idempotently dedupes via harness step 0.5.
func (d *Daemon) scanLongPending(ctx context.Context, nowMs int64) error {
	for _, cr := range d.snapshotChannelRuntimes() {
		chID := cr.channelID
		if cr.gateway == nil || cr.messages == nil {
			continue
		}

		// Pass 1 — §5.3 future-message drain.
		due, err := cr.messages.PendingDue(ctx, nowMs, 64)
		if err != nil {
			d.log.Warn().Err(err).
				Str("event", "runtime.scheduler_scan_failed").
				Str("channel_id", string(chID)).
				Msg("scheduler failed to scan due messages")
		}
		for i := range due {
			env := due[i]
			// scheduler is the dispatch-path upstream — bypass §5.1
			// step 3 self-trigger ban (L1 §5.3 explicit semantics).
			if _, derr := cr.gateway.Dispatch(ctx, &env, trigger.Options{BypassSelfTriggerBan: true}); derr != nil {
				d.log.Warn().Err(derr).
					Str("event", "runtime.scheduler_dispatch_failed").
					Str("channel_id", string(chID)).
					Str("message_id", string(env.ID)).
					Msg("scheduler failed to dispatch due message")
				if err := cr.messages.MarkDeliveryError(ctx, env.ID, nowMs, derr.Error()); err != nil {
					d.log.Warn().Err(err).
						Str("event", "runtime.scheduler_mark_delivery_error_failed").
						Str("channel_id", string(chID)).
						Str("message_id", string(env.ID)).
						Msg("scheduler failed to mark delivery error")
				}
				continue
			}
			if err := cr.messages.MarkDelivered(ctx, env.ID, nowMs); err != nil {
				d.log.Warn().Err(err).
					Str("event", "runtime.scheduler_mark_delivered_failed").
					Str("channel_id", string(chID)).
					Str("message_id", string(env.ID)).
					Msg("scheduler failed to mark message delivered")
			}
		}

		// Pass 2 — §6.4 receiver_unavailable immediate fallback emit.
		unavailable, err := cr.messages.ReceiverUnavailableRequests(ctx, 64)
		if err != nil {
			d.log.Warn().Err(err).
				Str("event", "runtime.scheduler_receiver_unavailable_scan_failed").
				Str("channel_id", string(chID)).
				Msg("scheduler failed to scan receiver-unavailable requests")
		}
		for i := range unavailable {
			req := unavailable[i]
			if err := d.emitLongPendingFallback(ctx, cr, &req, nowMs); err != nil {
				d.log.Warn().Err(err).
					Str("event", "runtime.scheduler_receiver_unavailable_emit_failed").
					Str("channel_id", string(chID)).
					Str("message_id", string(req.ID)).
					Msg("scheduler failed to emit receiver-unavailable fallback")
				continue
			}
		}

		// Pass 3 — §6.4 expires_at-gated long-pending fallback emit.
		overdue, err := cr.messages.LongPendingRequests(ctx, nowMs, 64)
		if err != nil {
			d.log.Warn().Err(err).
				Str("event", "runtime.scheduler_long_pending_scan_failed").
				Str("channel_id", string(chID)).
				Msg("scheduler failed to scan long-pending requests")
			continue
		}
		for i := range overdue {
			req := overdue[i]
			if err := d.emitLongPendingFallback(ctx, cr, &req, nowMs); err != nil {
				d.log.Warn().Err(err).
					Str("event", "runtime.scheduler_long_pending_emit_failed").
					Str("channel_id", string(chID)).
					Str("message_id", string(req.ID)).
					Msg("scheduler failed to emit long-pending fallback")
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
	Status         string `json:"status"`
	Reason         string `json:"reason"`
	MissingActorID string `json:"missing_actor_id,omitempty"`
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
	receiverID := req.Audience[0]

	reason, emit, err := d.classifyLongPendingReason(ctx, cr, receiverID)
	if err != nil {
		return fmt.Errorf("classify %s: %w", req.ID, err)
	}
	if !emit {
		return nil
	}

	body := longPendingPayload{Status: "failed", Reason: string(reason)}
	if reason == message.TerminalReceiverUnavailable {
		body.MissingActorID = receiverID.String()
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	// Deterministic id keeps the scan idempotent across ticks: every
	// re-fire builds the same id, hits harness step 0.5 dedupe, and
	// short-circuits (canonical-hash-equal → Deduped=true).
	envID := message.ID("response:" + string(req.ID) + ":sys-" + string(reason))

	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = req.ID
	}

	env := &message.Envelope{
		ID:            envID,
		TS:            nowMs,
		ChannelID:     req.ChannelID,
		Sender:        message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:          message.KindResponse,
		Type:          req.Type,
		Payload:       payload,
		ParentID:      req.ID,
		CorrelationID: correlationID,
		Visibility:    req.Visibility,
		Audience:      message.Audience{req.Sender.ID},
	}

	// The scheduler is a system caller; stamp the harness context with
	// the system actor + permit kind pre-fill (we set Kind=actor.KindSystem
	// above so the registry-truth overwrite path remains exact-match).
	chainCtx := harness.CtxWithCaller(ctx, harness.CallerContext{
		ActorID:                 actor.SystemActorID,
		ChannelID:               req.ChannelID,
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
	case actor.KindTool:
		// Adapter framework F3 timer owns this case — MUTUAL EXCLUSION.
		return "", false, nil
	case actor.KindHuman:
		// Baseline: humans do not have an SLA in M1.6.
		return "", false, nil
	case actor.KindAgent, actor.KindSystem:
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
//   - FIX-T6: the wrapped harness chain carries the channel's explicit
//     (fencing_token, daemon_epoch) tuple so the messages-insert tx
//     validates the fencing pair (a silent placement reclaim under us
//     surfaces as HarnessWorkerFencingStale instead of corrupting outbox).
//   - FIX-T3/T155-B5: after a successful, non-deduped, non-rejected write
//     call trigger.Gateway.Dispatch (L1 §5.1 fan-out) + messages.MarkDelivered.
//     Dispatch failures record messages.MarkDeliveryError and leave the row
//     retryable. Dedupe / deferred / reject paths skip dispatch.
//
// Implements kernel/harness.Chain so the framework Manager can consume
// the same wrapper without taking a dependency on the daemon package.
type postHarnessChain struct {
	chain    *harness.Chain
	gateway  *trigger.Gateway
	messages *store.Messages
	log      *zerolog.Logger
	nowFn    func() int64

	frozen       atomic.Bool
	frozenReason atomic.Value // string
}

func (a *postHarnessChain) Freeze(reason string) {
	a.frozen.Store(true)
	if reason == "" {
		reason = "channel_frozen"
	}
	a.frozenReason.Store(reason)
}

func (a *postHarnessChain) IsFrozen() bool {
	return a != nil && a.frozen.Load()
}

// Write implements kernel/harness.Chain.
func (a *postHarnessChain) Write(ctx context.Context, env *message.Envelope) (khar.WriteResult, error) {
	if a.IsFrozen() {
		reason, _ := a.frozenReason.Load().(string)
		if reason == "" {
			reason = "channel_frozen"
		}
		var messageID message.ID
		if env != nil {
			messageID = env.ID
		}
		return khar.WriteResult{
			MessageID:    messageID,
			RejectReason: message.HarnessWorkerFencingStale,
			RejectDetail: "channel frozen: " + reason,
		}, nil
	}
	res, err := a.chain.Write(ctx, env)
	if err != nil {
		return khar.WriteResult{}, err
	}
	if res.RejectReason == "" && !res.Deduped && a.gateway != nil {
		dr, derr := a.gateway.Dispatch(ctx, env, trigger.Options{})
		if derr != nil {
			if a.log != nil {
				a.log.Warn().Err(derr).
					Str("event", "runtime.gateway_dispatch_failed").
					Str("message_id", string(env.ID)).
					Msg("post-harness gateway dispatch failed")
			}
			if a.messages != nil {
				if err := a.messages.MarkDeliveryError(ctx, env.ID, a.nowFn(), derr.Error()); err != nil {
					if a.log != nil {
						a.log.Warn().Err(err).
							Str("event", "runtime.mark_delivery_error_failed").
							Str("message_id", string(env.ID)).
							Msg("failed to mark post-harness delivery error")
					}
				}
			}
		} else if !dr.Deferred && a.messages != nil {
			if err := a.messages.MarkDelivered(ctx, env.ID, a.nowFn()); err != nil {
				if a.log != nil {
					a.log.Warn().Err(err).
						Str("event", "runtime.mark_delivered_failed").
						Str("message_id", string(env.ID)).
						Msg("failed to mark post-harness message delivered")
				}
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

// Saga exposes the channel bootstrap saga for tests.
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
	_, ok := d.getChannel(id)
	return ok
}

// OpenChannelLock returns the channel_lock store for a channel sqlite
// path (cached). Useful for FencingChecker wiring in tests.
func (d *Daemon) OpenChannelLock(ctx context.Context, sqlitePath string) (*store.ChannelLock, error) {
	d.channelDBsMu.Lock()
	defer d.channelDBsMu.Unlock()
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
	d.channelDBsMu.Lock()
	dbs := make([]*sql.DB, 0, len(d.channelDBs))
	for _, db := range d.channelDBs {
		dbs = append(dbs, db)
	}
	d.channelDBsMu.Unlock()
	for _, db := range dbs {
		_ = db.Close()
	}
	if d.daemonDB != nil {
		_ = d.daemonDB.Close()
	}
	return nil
}
