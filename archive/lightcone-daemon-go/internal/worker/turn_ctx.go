package worker

// turn_ctx.go owns the worker-side spawn argument plumbing per T10
// ticket spec (L2 §3.9.3 + §3.4.2):
//
//   - parse the CLI flag / env input the supervisor (T6) ships into the
//     worker process;
//   - persist a JSON snapshot at `~/.coagent/turn-ctx.json` so the
//     coagent CLI fallback path (pkg/coagent.loadTurnCtxFile) can
//     read the same context across a fork chain
//     (agent → bash → coagent emit ...).
//
// The on-disk schema MUST stay byte-compatible with the read side at
// pkg/coagent/turn_ctx.go (turnCtxFile struct). Adding fields here is
// safe (json.Unmarshal tolerates extras over there) — renaming or
// dropping a key is NOT (worker version skew breaks fallback).

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// TurnCtx is the per-spawn context every worker boots with. It is
// populated from a mix of CLI flags (supervisor ExecSpawner sets these)
// and env vars (turn-ctx propagation via L2 §3.4.2). CLI flags win when
// both are present — explicit > implicit.
//
// Field semantics mirror L2 §3.9.3 + the SpawnContext struct in
// internal/supervisor.
type TurnCtx struct {
	// DaemonURL is the daemon HTTP RPC endpoint. Optional in worker
	// scope (the in_worker_bus binding skips HTTP entirely) but written
	// to the file so spawned coagent CLI subprocesses can reach back.
	DaemonURL string

	// ChannelID is the channel the worker serves. Required.
	ChannelID string

	// AgentID is the actor_registry id this worker represents. The
	// harness Step 3 (sender identity) compares it against caller_ctx.
	// Required.
	AgentID string

	// WorkerID is the worker_locks PK assigned by the supervisor for
	// this spawn. Required for heartbeat CAS.
	WorkerID string

	// FencingToken is the worker_locks fencing_token. The harness Step 3
	// rejects writes with `worker_fencing_stale` when this token no
	// longer matches the active lease. Must be > 0 for a real spawn.
	FencingToken int64

	// TriggerMsgID is the optional id of the message that triggered the
	// turn the supervisor handed off. Informational — parent_id is
	// caller policy per L2 §3.4.2.
	TriggerMsgID string

	// TriggerCorrelationID is the optional correlation_id from the
	// trigger envelope. Per L1 §2.2.1 the harness uses it as the first
	// correlation_id fallback when callers omit the field.
	TriggerCorrelationID string

	// AuthToken is the bearer token the daemon_rpc binding attaches as
	// HTTP Authorization. Optional in pure in_worker_bus paths.
	AuthToken string

	// SenderKind is the declared sender.kind for this actor (typically
	// "agent"). Optional — empty skips the Step 3 sender_kind_mismatch
	// check.
	SenderKind string

	// ChannelWorkdir is the absolute path of the channel's workdir
	// (where messages.sqlite lives). Required: the worker opens the
	// sqlite file inside this directory.
	ChannelWorkdir string

	// LeaseTTL is the worker_locks lease lifetime in seconds. The
	// heartbeat goroutine ticks at LeaseTTL/2. Supervisor default is
	// 60s (DefaultLeaseTTL).
	LeaseTTL int64
}

// Validate enforces the minimum field set the worker needs to boot. It
// returns an error wrapping ErrTurnCtxInvalid so callers can detect
// "bad spawn input" vs other infrastructure failures.
func (c TurnCtx) Validate() error {
	switch {
	case strings.TrimSpace(c.ChannelID) == "":
		return fmt.Errorf("%w: channel_id required", ErrTurnCtxInvalid)
	case strings.TrimSpace(c.AgentID) == "":
		return fmt.Errorf("%w: agent_id required", ErrTurnCtxInvalid)
	case strings.TrimSpace(c.WorkerID) == "":
		return fmt.Errorf("%w: worker_id required", ErrTurnCtxInvalid)
	case c.FencingToken <= 0:
		return fmt.Errorf("%w: fencing_token must be positive, got %d", ErrTurnCtxInvalid, c.FencingToken)
	case strings.TrimSpace(c.ChannelWorkdir) == "":
		return fmt.Errorf("%w: channel_workdir required", ErrTurnCtxInvalid)
	case c.LeaseTTL <= 0:
		return fmt.Errorf("%w: lease_ttl must be positive, got %d", ErrTurnCtxInvalid, c.LeaseTTL)
	}
	return nil
}

// ErrTurnCtxInvalid is the sentinel callers check with errors.Is when
// they want to distinguish "the spawn args themselves were malformed"
// from "the spawn process ran but failed downstream".
var ErrTurnCtxInvalid = errors.New("turn_ctx_invalid")

// ParseFlags wires a TurnCtx into a *flag.FlagSet. ExecSpawner builds
// the equivalent argv when launching the worker binary; main.go uses
// the FlagSet for CLI parsing. The two paths share this single source
// of truth so flag rename / default tweaks land in one place.
//
// Defaults:
//   - lease_ttl defaults to the supervisor DefaultLeaseTTL value (60s)
//     so a missing flag still produces a usable heartbeat cadence.
//   - All other fields default to zero/empty; Validate enforces the
//     required ones.
func (c *TurnCtx) ParseFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.DaemonURL, "daemon-url", "", "daemon HTTP RPC endpoint (optional)")
	fs.StringVar(&c.ChannelID, "channel-id", "", "channel id this worker serves (required)")
	fs.StringVar(&c.AgentID, "agent-id", "", "agent actor id (required)")
	fs.StringVar(&c.WorkerID, "worker-id", "", "worker_locks worker_id (required)")
	fs.Int64Var(&c.FencingToken, "fencing-token", 0, "worker_locks fencing_token (required, >0)")
	fs.StringVar(&c.TriggerMsgID, "trigger-msg-id", "", "id of the message that triggered this turn (optional)")
	fs.StringVar(&c.TriggerCorrelationID, "trigger-correlation-id", "", "correlation_id of the trigger envelope (optional)")
	fs.StringVar(&c.AuthToken, "auth-token", "", "bearer token for daemon_rpc (optional)")
	fs.StringVar(&c.SenderKind, "sender-kind", "agent", "declared sender.kind (default agent)")
	fs.StringVar(&c.ChannelWorkdir, "channel-workdir", "", "absolute path of the channel workdir (required)")
	fs.Int64Var(&c.LeaseTTL, "lease-ttl", DefaultLeaseTTL, "worker_locks lease ttl in seconds")
}

// FromEnv overlays env-variable values onto c. CLI flag values take
// priority — FromEnv only fills fields that are still zero / empty.
// This matches the L2 §3.4.2 contract: explicit args > env propagation.
//
// getenv is injected for tests (use os.Getenv in production).
func (c *TurnCtx) FromEnv(getenv func(string) string) {
	if getenv == nil {
		getenv = os.Getenv
	}
	if c.DaemonURL == "" {
		c.DaemonURL = getenv("DAEMON_URL")
	}
	if c.ChannelID == "" {
		c.ChannelID = getenv("COAGENT_CHANNEL_ID")
	}
	if c.AgentID == "" {
		c.AgentID = getenv("COAGENT_SELF_ID")
	}
	if c.WorkerID == "" {
		c.WorkerID = getenv("COAGENT_WORKER_ID")
	}
	if c.FencingToken == 0 {
		if raw := getenv("COAGENT_FENCING_TOKEN"); raw != "" {
			var v int64
			_, _ = fmt.Sscanf(raw, "%d", &v)
			c.FencingToken = v
		}
	}
	if c.TriggerMsgID == "" {
		c.TriggerMsgID = getenv("COAGENT_TRIGGER_MSG_ID")
	}
	if c.TriggerCorrelationID == "" {
		c.TriggerCorrelationID = getenv("COAGENT_TRIGGER_CORRELATION_ID")
	}
	if c.AuthToken == "" {
		c.AuthToken = getenv("COAGENT_AUTH_TOKEN")
	}
	if c.SenderKind == "" {
		c.SenderKind = getenv("COAGENT_SENDER_KIND")
	}
	if c.ChannelWorkdir == "" {
		c.ChannelWorkdir = getenv("COAGENT_CHANNEL_WORKDIR")
	}
	if c.LeaseTTL == 0 {
		if raw := getenv("COAGENT_LEASE_TTL"); raw != "" {
			var v int64
			_, _ = fmt.Sscanf(raw, "%d", &v)
			c.LeaseTTL = v
		}
	}
}

// DefaultLeaseTTL mirrors supervisor.DefaultLeaseTTL (60s). Duplicated
// here so the worker package does not import internal/supervisor purely
// for the constant — keeps the dependency direction one-way
// (cmd/worker → internal/worker → internal/supervisor, not the
// reverse).
const DefaultLeaseTTL int64 = 60

// turnCtxFile mirrors the on-disk schema. Field names MUST match
// pkg/coagent.turnCtxFile exactly — the worker writes this file and
// the coagent CLI reads it.
//
// Adding new optional fields is allowed (json.Unmarshal on the reader
// side tolerates extras); rename / drop is a breaking change.
type turnCtxFile struct {
	DaemonURL            string `json:"daemon_url,omitempty"`
	ChannelID            string `json:"channel_id,omitempty"`
	SelfID               string `json:"actor_id,omitempty"`
	TriggerMsgID         string `json:"trigger_msg_id,omitempty"`
	TriggerCorrelationID string `json:"trigger_correlation_id,omitempty"`
	FencingToken         int64  `json:"fencing_token,omitempty"`
	AuthToken            string `json:"auth_token,omitempty"`
	SenderKind           string `json:"sender_kind,omitempty"`
}

// toFileShape projects the worker TurnCtx onto the on-disk schema. The
// projection is lossy on purpose: worker_id / channel_workdir / lease
// are worker-internal concerns the CLI fallback never needs.
func (c TurnCtx) toFileShape() turnCtxFile {
	return turnCtxFile{
		DaemonURL:            c.DaemonURL,
		ChannelID:            c.ChannelID,
		SelfID:               c.AgentID,
		TriggerMsgID:         c.TriggerMsgID,
		TriggerCorrelationID: c.TriggerCorrelationID,
		FencingToken:         c.FencingToken,
		AuthToken:            c.AuthToken,
		SenderKind:           c.SenderKind,
	}
}

// WriteTurnCtxFile writes c to `<homeDir>/.coagent/turn-ctx.json`,
// creating the directory if needed. Passing homeDir="" falls back to
// $HOME (the env var, not os.UserHomeDir — same convention as
// pkg/coagent.loadTurnCtxFile so writer + reader agree on which dir
// they target inside tests).
//
// File mode is 0o600: the JSON carries the bearer token; readable only
// by the agent uid.
func WriteTurnCtxFile(c TurnCtx, homeDir string) (string, error) {
	if homeDir == "" {
		homeDir = os.Getenv("HOME")
	}
	if strings.TrimSpace(homeDir) == "" {
		return "", fmt.Errorf("write_turn_ctx: HOME not set and homeDir argument empty")
	}
	dir := filepath.Join(homeDir, ".coagent")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("write_turn_ctx: mkdir %s: %w", dir, err)
	}
	path := filepath.Join(dir, "turn-ctx.json")
	raw, err := json.MarshalIndent(c.toFileShape(), "", "  ")
	if err != nil {
		return path, fmt.Errorf("write_turn_ctx: marshal: %w", err)
	}
	// Append a trailing newline — easier to diff / cat -E.
	raw = append(raw, '\n')
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return path, fmt.Errorf("write_turn_ctx: write %s: %w", path, err)
	}
	return path, nil
}

// readTurnCtxFile is the symmetric reader. The worker doesn't normally
// read its own write (the worker has the in-process struct), but the
// test suite uses it to verify round-trip key alignment with
// pkg/coagent.
func readTurnCtxFile(homeDir string) (turnCtxFile, error) {
	if homeDir == "" {
		homeDir = os.Getenv("HOME")
	}
	path := filepath.Join(homeDir, ".coagent", "turn-ctx.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return turnCtxFile{}, fmt.Errorf("read_turn_ctx: read %s: %w", path, err)
	}
	var f turnCtxFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return turnCtxFile{}, fmt.Errorf("read_turn_ctx: parse %s: %w", path, err)
	}
	return f, nil
}
