package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/bootstrap"
	"github.com/wanpengxie/ActOS/runtime/lifecycle"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

// DaemonConfig is the cmd/daemon assembly knobs.
type DaemonConfig struct {
	DataDir     string
	ChannelsDir string
	DaemonID    string
	DaemonEpoch int64
	UseMockBus  bool

	// NowFn / FrameIDGen optional — production injects time.Now and uuid.
	NowFn      func() int64
	FrameIDGen func() string

	// PostBoot is invoked once phase 4 starts. May be nil. Used by
	// tests to inspect state without racing with shutdown.
	PostBoot func(ctx context.Context, d *Daemon) error
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
	booter     *lifecycle.Bootstrapper
	reconciler *bootstrap.Reconciler
	saga       *bootstrap.Saga
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
		// EmitReclaim left nil — T6 wires the WS client; until then the
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

	// Phase 3: would normally start outbox pump + scheduler + supervisor
	// loop. M1.5-T3 wiring point — keep this method synchronous so
	// tests can inspect post-boot state.
	d.booter.MarkRecovering()

	// Phase 4: accept new control.create_channel frames.
	d.booter.MarkAcceptingNew()
	return nil
}

// Phase returns the current boot phase (for tests / observability).
func (d *Daemon) Phase() lifecycle.Phase { return d.booter.Phase() }

// BootResult returns the phase-2 reclaim outcome.
func (d *Daemon) BootResult() lifecycle.BootResult { return d.bootRes }

// Saga exposes the channel bootstrap saga (used by lifecycle.Creator).
func (d *Daemon) Saga() *bootstrap.Saga { return d.saga }

// DaemonDB exposes the daemon-level sqlite (tests / future server.go).
func (d *Daemon) DaemonDB() *sql.DB { return d.daemonDB }

// Transit returns the daemonbus client (nil when --mock-bus=false and
// T6 wiring is incomplete).
func (d *Daemon) Transit() *transit.Client { return d.transit }

// Bus returns the underlying MockBus (nil unless UseMockBus).
func (d *Daemon) Bus() *transit.MockBus { return d.bus }

// FrameIDGen returns the configured frame id generator.
func (d *Daemon) FrameIDGen() func() string { return d.cfg.FrameIDGen }

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

// Close releases all open sqlite handles + the mock bus.
func (d *Daemon) Close() error {
	if d.bus != nil {
		_ = d.bus.Close()
	}
	for _, db := range d.channelDBs {
		_ = db.Close()
	}
	if d.daemonDB != nil {
		_ = d.daemonDB.Close()
	}
	return nil
}
