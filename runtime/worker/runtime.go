package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/coagent-ai/coagent/kernel/ledger"
	"github.com/coagent-ai/coagent/kernel/message"
)

// Config wires a Runtime.
type Config struct {
	// LeaseID identifies the worker lease (assigned by daemon).
	LeaseID string
	// In / Out are the stdio streams for IPC. Set by cmd/worker/main.go
	// to os.Stdin / os.Stdout.
	In  io.Reader
	Out io.Writer
	// NowFn returns unix-ms; default time.Now().UnixMilli.
	NowFn func() int64
	// HeartbeatEvery is the worker → daemon heartbeat cadence (per
	// spec: 30s; covered by codex review #10's lease vs heartbeat split).
	HeartbeatEvery time.Duration
	// Bridge converts go-kimi wire events into v4 envelopes. Optional —
	// when nil, Run() is a pure protocol harness (used by tests).
	Bridge Bridge
}

// Runtime is the worker subprocess main loop. It performs:
//
//  1. Handshake with the daemon (learn fencing_token + daemon_epoch).
//  2. Start a heartbeat goroutine (30s cadence).
//  3. Hand control to the Bridge (or test harness) which makes calls
//     into IPCClient.WriteMessage / ReserveLedger / CommitLedger.
//  4. On Bridge return: Shutdown IPC.
//  5. On *FenceInvalidError anywhere: exit immediately.
type Runtime struct {
	cfg    Config
	client *IPCClient
}

// New builds a Runtime.
func New(cfg Config) (*Runtime, error) {
	if cfg.LeaseID == "" {
		return nil, errors.New("worker: Config.LeaseID empty")
	}
	if cfg.In == nil || cfg.Out == nil {
		return nil, errors.New("worker: Config.In/Out nil")
	}
	if cfg.NowFn == nil {
		cfg.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.HeartbeatEvery <= 0 {
		cfg.HeartbeatEvery = 30 * time.Second
	}
	return &Runtime{
		cfg:    cfg,
		client: NewIPCClient(cfg.In, cfg.Out),
	}, nil
}

// Client returns the underlying IPC client (used by Bridge + tests).
func (r *Runtime) Client() *IPCClient { return r.client }

// Run executes the worker main loop. Returns when:
//   - the Bridge function returns (normal completion → graceful Shutdown)
//   - the IPC pipe closes (daemon went away)
//   - a *FenceInvalidError is received (worker MUST exit)
func (r *Runtime) Run(ctx context.Context) error {
	r.client.Start(ctx)
	defer r.client.Stop()

	if _, err := r.client.Handshake(ctx, r.cfg.LeaseID); err != nil {
		return fmt.Errorf("worker: handshake: %w", err)
	}

	// Heartbeat goroutine.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go r.heartbeatLoop(hbCtx)

	var runErr error
	if r.cfg.Bridge != nil {
		runErr = r.cfg.Bridge.Run(ctx, r.client)
	}

	// Graceful shutdown — even on error we politely close the IPC.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = r.client.Shutdown(shutdownCtx)

	return runErr
}

func (r *Runtime) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.HeartbeatEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.client.Heartbeat(ctx, r.cfg.NowFn()); err != nil {
				var fence *FenceInvalidError
				if errors.As(err, &fence) {
					// Fence invalid — worker MUST exit. Cancel ctx so Run
					// unwinds and main does os.Exit.
					return
				}
				// transient — let the next tick retry.
			}
		}
	}
}

// Compile-only re-exports so callers (tests) can build envelopes /
// ledger entries without re-importing kernel themselves.
var (
	_ message.Envelope = message.Envelope{}
	_ ledger.Entry     = ledger.Entry{}
)
