package coagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Config wires the CLI library to its environment. The binary builds
// one of these from os.* (Args, Getenv, Stdout, Stderr); tests build
// a fully-stubbed Config with deterministic Clock + NewID + injected
// Binding. Every field is optional — see the per-field doc for the
// default applied when zero.
type Config struct {
	// Args is the argv slice the CLI parses, NOT including the
	// program name (i.e. os.Args[1:] when called from the binary).
	// When empty, Run prints usage and exits with code 2.
	Args []string

	// Env reads environment variables. Defaults to os.Getenv when
	// nil. Tests supply EnvFromMap(map[string]string{...}).
	Env EnvSource

	// HomeDir overrides the directory containing ~/.coagent/turn-ctx.json.
	// Defaults to Env("HOME") when zero — production should leave
	// this empty.
	HomeDir string

	// Stdout / Stderr receive the CLI's normal / error output. nil
	// values fall back to os.Stdout / os.Stderr.
	Stdout io.Writer
	Stderr io.Writer

	// Clock returns "now" — used for envelope.ts and for time-relative
	// flag parsing (--not-before +30m). Defaults to time.Now.
	Clock func() time.Time

	// NewID returns a fresh UUID string — used for envelope.id and
	// `--correlation-id new`. Defaults to uuid.NewString. Tests inject
	// a deterministic counter so envelope ids are predictable.
	NewID func() string

	// Binding is the pre-constructed harness write surface. When
	// non-nil it is used as-is regardless of env. When nil, Run
	// constructs a daemon_rpc HTTP binding from the loaded TurnCtx
	// (DaemonURL + AuthToken). The CLI never constructs an
	// in_worker_bus binding on its own — that binding requires
	// harness Deps the binary does not own; library callers (the
	// worker process) must inject it.
	Binding Binding
}

// Run is the unified entrypoint. Returns the process exit code (0 on
// success, non-zero on usage / harness reject / infra failure).
//
// On reject the CLI writes a one-line stderr message of the form:
//
//	coagent: <subcommand> rejected: <reason>: <detail>
//
// On success the CLI writes a one-line stdout message of the form:
//
//	{"id":"<id>","correlation_id":"<cid>","kind":"<kind>","dedupe":<bool>}
//
// — i.e. the L2 §3.6.1 success body shape, JSON-encoded for easy
// parsing by shell callers.
func Run(ctx context.Context, cfg Config) int {
	cfg = applyDefaults(cfg)

	if len(cfg.Args) == 0 {
		printUsage(cfg.Stderr)
		return exitUsage
	}
	sub := cfg.Args[0]
	rest := cfg.Args[1:]

	switch sub {
	case "emit":
		return runEmit(ctx, cfg, rest)
	case "ask":
		return runAsk(ctx, cfg, rest)
	case "answer":
		return runAnswer(ctx, cfg, rest)
	case "-h", "--help", "help":
		printUsage(cfg.Stdout)
		return 0
	default:
		fmt.Fprintf(cfg.Stderr, "coagent: unknown subcommand %q\n", sub)
		printUsage(cfg.Stderr)
		return exitUsage
	}
}

// Exit codes — kept small + stable so shell scripts can branch on
// them. The reject-vs-infra split mirrors HTTP 4xx vs 5xx.
const (
	exitUsage      = 2 // bad args / usage error
	exitReject     = 3 // harness or client-side reject (RejectError)
	exitInfra      = 4 // infrastructure error (transport, sql, ctx)
	exitNoBinding  = 5 // no binding wired (env missing, library caller forgot to inject)
	exitFlagFormat = 6 // flag value failed to parse (json, duration, etc.)
)

func applyDefaults(cfg Config) Config {
	if cfg.Env == nil {
		cfg.Env = os.Getenv
	}
	if cfg.Stdout == nil {
		cfg.Stdout = os.Stdout
	}
	if cfg.Stderr == nil {
		cfg.Stderr = os.Stderr
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.NewID == nil {
		cfg.NewID = uuid.NewString
	}
	return cfg
}

// printUsage writes the top-level CLI usage line. Subcommand-specific
// flag detail is owned by the per-subcommand FlagSet (printed when
// the subcommand parses `--help`).
func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: coagent <subcommand> [flags]")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Subcommands:")
	fmt.Fprintln(w, "  emit             declare a fact (kind=event)")
	fmt.Fprintln(w, "  ask              ask / call a tool (kind=request)")
	fmt.Fprintln(w, "  answer <reqID>   respond to a prior request (kind=response)")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "See L2 §3 for the full flag table.")
}

// resolveBinding picks the Binding for this Run call. Library callers
// already supplied one via cfg.Binding; the binary path constructs a
// daemon_rpc binding from the loaded TurnCtx. Returns (nil, exitCode,
// error-message) on failure so the caller can write a one-line stderr
// and return the matching exit code.
func resolveBinding(cfg Config, tc TurnCtx) (Binding, int, string) {
	if cfg.Binding != nil {
		return cfg.Binding, 0, ""
	}
	// COAGENT_IN_WORKER without an injected binding is a programming
	// mistake — workers must pass cfg.Binding directly (the in_worker_bus
	// binding owns the channel sqlite handle, env vars are not enough).
	if tc.InWorker {
		return nil, exitNoBinding, "COAGENT_IN_WORKER set but cfg.Binding not injected (in_worker_bus requires library wiring)"
	}
	if strings.TrimSpace(tc.DaemonURL) == "" {
		return nil, exitNoBinding, "no binding wired: set DAEMON_URL or inject cfg.Binding"
	}
	return NewDaemonRPCBinding(DaemonRPCOptions{
		BaseURL:   tc.DaemonURL,
		AuthToken: tc.AuthToken,
	}), 0, ""
}

// writeReject is the common stderr writer for *RejectError. The
// shape mirrors L2 §3.6.1's reason/detail pair so shell callers can
// grep on the reason.
func writeReject(w io.Writer, sub string, re *RejectError) {
	fmt.Fprintf(w, "coagent: %s rejected: %s", sub, re.Reason)
	if re.Detail != "" {
		fmt.Fprintf(w, ": %s", re.Detail)
	}
	if re.DedupeResponseID != "" {
		fmt.Fprintf(w, " (dedupe_response_id=%s)", re.DedupeResponseID)
	}
	if re.MessageIDIfPartial != "" {
		fmt.Fprintf(w, " (message_id_if_partial=%s)", re.MessageIDIfPartial)
	}
	fmt.Fprintln(w)
}

// writeSuccess prints the success body JSON on stdout. Format matches
// L2 §3.6.1 ("成功 response: HTTP 200 + {id, correlation_id, kind}")
// plus the `dedupe` flag for observability symmetry with binding HTTP.
func writeSuccess(w io.Writer, r *SendResult) {
	fmt.Fprintf(w, "{\"id\":%q,\"correlation_id\":%q,\"kind\":%q,\"dedupe\":%t}\n",
		r.ID, r.CorrelationID, string(r.Kind), r.Dedupe)
}

// classifyErr separates RejectError from infra error and returns the
// matching exit code + a stderr-friendly message.
func classifyErr(sub string, err error) (exitCode int, msg string) {
	if err == nil {
		return 0, ""
	}
	var re *RejectError
	if errors.As(err, &re) {
		return exitReject, "" // caller writes the structured reject message via writeReject
	}
	return exitInfra, fmt.Sprintf("coagent: %s failed: %v", sub, err)
}
