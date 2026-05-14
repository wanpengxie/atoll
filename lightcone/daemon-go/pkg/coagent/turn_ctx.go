package coagent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// TurnCtx is the per-turn context the CLI needs to fill envelope
// defaults — sender id, channel id, the trigger's correlation_id (for
// L1 §2.2.1 propagation), worker fencing token. Two sources feed it:
//
//   - process environment variables (L2 §3.4.2 — what the worker
//     daemon-side spawn puts in the worker's env)
//   - ~/.coagent/turn-ctx.json (the spawn-time snapshot used as
//     fallback when env vars are lost across a fork chain, e.g.
//     agent → bash → child → coagent — see L2 §3.4.2 "sub-process env
//     断链兜底")
//
// LoadTurnCtx implements the env-first, file-fallback rule. Tests
// build a TurnCtx directly via the literal struct.
type TurnCtx struct {
	// DaemonURL is the daemon HTTP RPC endpoint (env DAEMON_URL). Used
	// by the daemon_rpc binding to address message.send.
	DaemonURL string

	// ChannelID is the current channel (env COAGENT_CHANNEL_ID). The
	// CLI fills envelope.channel_id when caller omits --channel-id.
	ChannelID string

	// SelfID is the caller actor's id (env COAGENT_SELF_ID). The CLI
	// fills envelope.sender.id when caller omits --sender-id.
	SelfID string

	// TriggerMsgID is the message id that triggered this turn (env
	// COAGENT_TRIGGER_MSG_ID). Informational — L2 §3.4.2 spec says
	// the CLI does NOT auto-use it as parent_id default (parent is
	// caller policy). Exposed so tests / callers can opt-in via
	// `--parent $COAGENT_TRIGGER_MSG_ID`.
	TriggerMsgID string

	// TriggerCorrelationID is the trigger envelope's correlation_id
	// (env COAGENT_TRIGGER_CORRELATION_ID). Per L1 §2.2.1 the harness
	// uses it as the first-tier correlation_id fallback.
	TriggerCorrelationID string

	// FencingToken is the worker's active fencing token (env
	// COAGENT_FENCING_TOKEN, decimal string). Zero when absent or
	// unparsable — the binding then skips the fencing check.
	FencingToken int64

	// AuthToken is the bearer token the daemon_rpc binding attaches
	// to the HTTP Authorization header (env COAGENT_AUTH_TOKEN).
	// Empty token → daemon AuthFunc rejects with auth_failed.
	AuthToken string

	// SenderKind is the caller's declared sender.kind (env
	// COAGENT_SENDER_KIND). Optional — empty skips the
	// sender_kind_mismatch check on harness Step 3.
	SenderKind string

	// InWorker reports whether COAGENT_IN_WORKER was set (any non-empty
	// value). The binary's auto-binding rule consults this; library
	// callers can ignore it.
	InWorker bool
}

// EnvSource is the lookup function the CLI uses to read environment
// variables. Tests inject a stub map; production passes os.Getenv.
type EnvSource func(string) string

// EnvFromMap returns an EnvSource backed by m (returns "" for missing
// keys). Useful in tests where we want a fully-stubbed environment
// regardless of the real process state.
func EnvFromMap(m map[string]string) EnvSource {
	return func(k string) string {
		if m == nil {
			return ""
		}
		return m[k]
	}
}

// LoadTurnCtx assembles TurnCtx by reading env via getenv first, then
// filling any still-empty field from ~/.coagent/turn-ctx.json
// (homeDir overrideable for tests via the second arg — pass "" for
// production wall-clock $HOME lookup).
//
// File parse failure is treated as "no fallback available" and
// returned as a wrapped error so callers can choose to ignore it
// (the file is best-effort, env is authoritative).
func LoadTurnCtx(getenv EnvSource, homeDir string) (TurnCtx, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	tc := TurnCtx{
		DaemonURL:            getenv("DAEMON_URL"),
		ChannelID:            getenv("COAGENT_CHANNEL_ID"),
		SelfID:               getenv("COAGENT_SELF_ID"),
		TriggerMsgID:         getenv("COAGENT_TRIGGER_MSG_ID"),
		TriggerCorrelationID: getenv("COAGENT_TRIGGER_CORRELATION_ID"),
		AuthToken:            getenv("COAGENT_AUTH_TOKEN"),
		SenderKind:           getenv("COAGENT_SENDER_KIND"),
		InWorker:             getenv("COAGENT_IN_WORKER") != "",
	}
	if raw := getenv("COAGENT_FENCING_TOKEN"); raw != "" {
		// Best-effort decimal parse — silently skip on malformed input.
		// A malformed value means the binding will see FencingToken=0
		// and skip the lease check; harness will still reject if the
		// caller had a fencing-protected sender.
		fmt.Sscanf(raw, "%d", &tc.FencingToken)
	}

	// All env values present → no file lookup needed.
	if tc.complete() {
		return tc, nil
	}

	fileCtx, ferr := loadTurnCtxFile(homeDir, getenv)
	if ferr != nil {
		// Wrapped error returned but partial TurnCtx still usable.
		return tc, ferr
	}
	mergeTurnCtx(&tc, fileCtx)
	return tc, nil
}

// complete reports whether tc has every "required" field populated.
// daemon_rpc usage needs DaemonURL + ChannelID + SelfID at minimum;
// in_worker_bus needs ChannelID + SelfID. We use the union as the
// `is everything filled` heuristic — file fallback fires whenever
// any field is missing, since the file write is cheap (~150 bytes).
func (tc TurnCtx) complete() bool {
	switch {
	case tc.DaemonURL == "",
		tc.ChannelID == "",
		tc.SelfID == "",
		tc.TriggerCorrelationID == "",
		tc.AuthToken == "":
		return false
	}
	return true
}

// turnCtxFile mirrors ~/.coagent/turn-ctx.json fields. The worker
// process owns the write — these names match L2 §3.4.2 + L4 §1.4
// turn-ctx schema.
//
// New fields can be added (json.Unmarshal tolerates extras) but old
// fields MUST keep their key names to preserve backward compat with
// older worker versions still landing the file.
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

// loadTurnCtxFile reads ~/.coagent/turn-ctx.json. Returns a zero
// turnCtxFile with no error when the file is missing (the fallback
// is optional). Returns an error only when the file exists but is
// unreadable / un-parseable so callers can surface the problem.
func loadTurnCtxFile(homeDir string, getenv EnvSource) (turnCtxFile, error) {
	if homeDir == "" {
		// Use HOME env, NOT os.UserHomeDir — tests stub HOME and we
		// want consistency between the daemon's spawn and the CLI's
		// fallback reader.
		homeDir = getenv("HOME")
	}
	if strings.TrimSpace(homeDir) == "" {
		// Without a home dir we cannot locate the fallback file; treat
		// as "no fallback" instead of erroring.
		return turnCtxFile{}, nil
	}
	path := filepath.Join(homeDir, ".coagent", "turn-ctx.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return turnCtxFile{}, nil
		}
		return turnCtxFile{}, fmt.Errorf("read %s: %w", path, err)
	}
	var f turnCtxFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return turnCtxFile{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return f, nil
}

// mergeTurnCtx fills empty fields in tc from f. Env values always win
// — we never overwrite a populated env value with a file value (file
// is fallback only per L2 §3.4.2).
func mergeTurnCtx(tc *TurnCtx, f turnCtxFile) {
	if tc.DaemonURL == "" {
		tc.DaemonURL = f.DaemonURL
	}
	if tc.ChannelID == "" {
		tc.ChannelID = f.ChannelID
	}
	if tc.SelfID == "" {
		tc.SelfID = f.SelfID
	}
	if tc.TriggerMsgID == "" {
		tc.TriggerMsgID = f.TriggerMsgID
	}
	if tc.TriggerCorrelationID == "" {
		tc.TriggerCorrelationID = f.TriggerCorrelationID
	}
	if tc.FencingToken == 0 {
		tc.FencingToken = f.FencingToken
	}
	if tc.AuthToken == "" {
		tc.AuthToken = f.AuthToken
	}
	if tc.SenderKind == "" {
		tc.SenderKind = f.SenderKind
	}
}
