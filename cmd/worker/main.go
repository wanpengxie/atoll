// Package main wires the coagent worker subprocess binary.
//
// Authoritative spec: launch-ticket notes §T3 (worker IPC-only
// constraint — codex review #9).
//
// The worker binary is a thin shell: it constructs runtime/worker.Runtime
// with os.Stdin / os.Stdout as the IPC stream and calls Run. The agent
// loop itself (go-kimi or replacement) is wired by composing a
// worker.Bridge implementation here so that runtime/worker stays
// vendor-light (no go-kimi import inside runtime/worker).
//
// Two bridges ship:
//
//	--provider=mock   M1.6-T1 MockBridge — deterministic, no network.
//	                  Default for dev / CI so tests run without external
//	                  side effects.
//	--provider=kimi   M1.6-T7 phase-4 LLM bridge — wraps go-kimi via
//	                  DeepSeek anthropic-compat endpoint (KIMI_API_KEY
//	                  / KIMI_BASE_URL / KIMI_MODEL env). Reads
//	                  COAGENT_DOMAIN_PROMPT to assemble the prompt-
//	                  cache friendly system prompt.
//
// Logging note: stdout is the IPC frame stream (read by the parent
// daemon as length-prefixed binary frames). Logs MUST go to stderr
// otherwise we corrupt the protocol. cmd/worker installs the
// structured logger with Writer=os.Stderr accordingly.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/wanpengxie/ActOS/adapters/llm/kimi"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/pkg/logger"
	"github.com/wanpengxie/ActOS/runtime/ipc"
	"github.com/wanpengxie/ActOS/runtime/worker"
)

// version is set via -ldflags at build time.
var version = "dev"

const (
	providerMock = "mock"
	providerKimi = "kimi"
)

func main() {
	leaseID := flag.String("lease-id", os.Getenv("COAGENT_WORKER_LEASE_ID"),
		"lease id assigned by daemon (also via COAGENT_WORKER_LEASE_ID)")
	// max-turns defaults to 0 = unlimited. The LLM itself decides when a
	// reaction is finished (stop_reason=end_turn / stop). External
	// cancellation paths (lease expire, SIGINT, IPC EOF) are the only
	// hard-stop signals. A positive value still caps reactions — useful
	// for unit tests that want deterministic exit, but never set from
	// the daemon spawn path.
	maxTurns := flag.Int("max-turns", 0,
		"cap on trigger reactions before next_action=done exit (0 = unlimited; LLM-driven stop)")
	provider := flag.String("provider", envOr("COAGENT_WORKER_PROVIDER", providerMock),
		"agent provider — mock (deterministic) or kimi (go-kimi via DeepSeek anthropic-compat). "+
			"Also via COAGENT_WORKER_PROVIDER env.")
	pretty := flag.Bool("pretty-logs", false,
		"emit human-readable console logs on stderr (dev only; production stays on JSON)")
	flag.Parse()

	if *leaseID == "" {
		// Pre-logger fail-fast — print to stderr directly so the daemon
		// supervisor sees the reason even if logger init is skipped.
		fmt.Fprintln(os.Stderr, "worker: --lease-id required (or COAGENT_WORKER_LEASE_ID env)")
		os.Exit(2)
	}

	// M1.6-T7 phase-2 — structured logger. CRITICAL: writer MUST be
	// os.Stderr because os.Stdout is the daemon ↔ worker IPC frame
	// stream (binary length-prefixed frames).
	lg := logger.New(logger.Config{
		Component: "worker",
		Version:   version,
		Writer:    os.Stderr,
		Pretty:    *pretty,
		Level:     os.Getenv("COAGENT_LOG_LEVEL"),
	})
	restore := lg.RedirectStdlib()
	defer restore()

	lg.Z().Info().
		Str("event", "worker.starting").
		Str("lease_id", *leaseID).
		Int("max_turns", *maxTurns).
		Str("provider", *provider).
		Msg("coagent-worker starting")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	bridge, err := buildBridge(*provider, *maxTurns)
	if err != nil {
		lg.Z().Error().Err(err).Str("event", "worker.bridge_init_failed").
			Str("provider", *provider).Msg("bridge init failed")
		os.Exit(1)
	}

	rt, err := worker.New(worker.Config{
		LeaseID: *leaseID,
		In:      os.Stdin,
		Out:     os.Stdout,
		Bridge:  bridge,
	})
	if err != nil {
		lg.Z().Error().Err(err).Str("event", "worker.assemble_failed").Msg("worker assemble failed")
		os.Exit(1)
	}
	if err := rt.Run(ctx); err != nil {
		lg.Z().Error().Err(err).Str("event", "worker.exit_error").Msg("worker exited with error")
		os.Exit(1)
	}
	lg.Z().Info().Str("event", "worker.stopped").Msg("worker stopped cleanly")
}

// buildBridge picks the Bridge implementation. The kimi path reads its
// env contract at construction time so fail-fast is loud + immediate.
func buildBridge(provider string, maxTurns int) (worker.Bridge, error) {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case providerMock, "":
		mock := worker.NewMockBridge()
		mock.MaxTurns = maxTurns
		return mock, nil

	case providerKimi:
		// Build the prompt-cache friendly base prompt from the spawn
		// env. The daemon set COAGENT_DOMAIN_PROMPT during worker
		// fork (M1.6-T5 phase-3) so the L4 template segment is
		// available without an IPC round-trip.
		//
		// COAGENT_CHANNEL_DB points at channel.sqlite — the
		// authoritative actor_registry / type_registry source. The
		// worker opens it read-only and snapshots channel state at
		// boot to seed the system prompt and the LLM `AdditionalTools`
		// list. Mid-session registry mutations are picked up via the
		// same store (kimi bridge re-queries on each turn boundary
		// where dynamic re-registration is feasible). This replaces
		// the prior `worker-context.json` static snapshot file the
		// daemon used to write at spawn time — that file froze tool /
		// actor state at spawn time and could not reflect subsequent
		// type install / actor register events.
		channelStore, storeErr := kimi.OpenChannelStore(os.Getenv(kimi.EnvKeyChannelDB))
		if storeErr != nil {
			fmt.Fprintf(os.Stderr, "worker: channel store open failed (continuing without context): %v\n", storeErr)
		}
		channelID := os.Getenv(kimi.EnvKeyChannelID)
		channelType := os.Getenv(kimi.EnvKeyChannelType)
		channelCtx, ctxErr := channelStore.Snapshot(context.Background(), channelID, channelType)
		if ctxErr != nil {
			fmt.Fprintf(os.Stderr, "worker: channel snapshot failed (continuing without appendix): %v\n", ctxErr)
		}
		basePrompt := kimi.BuildBasePrompt(
			channelType,
			os.Getenv(kimi.EnvKeyDomainPrompt),
			channelCtx,
		)
		cfg, err := kimi.NewConfigFromEnv(basePrompt)
		if err != nil {
			return nil, err
		}
		cfg.ChannelContext = channelCtx
		cfg.ChannelStore = channelStore
		kb, err := kimi.NewBridge(cfg)
		if err != nil {
			return nil, fmt.Errorf("kimi bridge: %w", err)
		}
		return worker.BridgeFunc(func(ctx context.Context, client *worker.IPCClient) error {
			return kb.Run(ctx, &kimiIPCAdapter{client: client})
		}), nil

	default:
		return nil, fmt.Errorf("worker: unknown --provider %q (want mock|kimi)", provider)
	}
}

// kimiIPCAdapter bridges runtime/worker.IPCClient → adapters/llm/kimi.
// IPCFacade. The arch-lint boundary forbids adapters/** from importing
// runtime/worker directly, so this thin adapter lives here in
// cmd/worker (the composition root, allowed to touch both).
type kimiIPCAdapter struct {
	client *worker.IPCClient
}

func (a *kimiIPCAdapter) ChannelID() channel.ID { return a.client.ChannelID() }
func (a *kimiIPCAdapter) WorkerID() string      { return a.client.WorkerID() }
func (a *kimiIPCAdapter) WorkerActorID() actor.ActorID {
	return a.client.WorkerActorID()
}

// Triggers wraps the worker.IPCClient trigger channel by spinning a
// goroutine that translates each ipc.TriggerPayload into the
// kimi.TriggerPayload shape (only field names + package origin differ).
//
// Cheap: triggers fire at human-message cadence (single-digit/min).
func (a *kimiIPCAdapter) Triggers() <-chan kimi.TriggerPayload {
	out := make(chan kimi.TriggerPayload, 4)
	go func() {
		defer close(out)
		for p := range a.client.Triggers() {
			out <- kimi.TriggerPayload{
				Envelope:      p.Envelope,
				CorrelationID: p.CorrelationID,
				Cursor:        p.Cursor,
			}
		}
	}()
	return out
}

// WriteEnvelope forwards to IPCClient.WriteMessage and discards the
// returned WriteMessageResult. The kimi bridge does not consult
// dedup / sequence info today (parity with MockBridge).
func (a *kimiIPCAdapter) WriteEnvelope(ctx context.Context, env message.Envelope) error {
	_, err := a.client.WriteMessage(ctx, env)
	return err
}

// envOr returns the env value or fallback. Inlined so the cmd binary
// stays free of helper imports.
func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// Compile-time anchor so the runtime/ipc package stays in the dep
// graph — keeps `go vet` happy when only the type-erased branch above
// references it.
var _ = ipc.KindTrigger
