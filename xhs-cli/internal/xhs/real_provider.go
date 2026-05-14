package xhs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Env names — aligned with coagent CLI (lightcone/daemon-go/cmd/coagent
// + L4 §2.3.2).
//
// CLI no longer talks HTTP directly; it spawns `coagent ask`, which
// internally reads these envs to build the daemon_rpc binding.
const (
	// EnvDaemonHTTP is the daemon HTTP base URL. coagent's CLI looks
	// for DAEMON_URL; the legacy xhs-cli name COAGENT_DAEMON_HTTP is
	// preserved as a fallback so existing deployments keep working
	// while the env is renamed.
	EnvDaemonHTTP    = "DAEMON_URL"
	EnvDaemonHTTPAlt = "COAGENT_DAEMON_HTTP"

	// EnvDaemonToken is the bearer token. Same fallback story.
	EnvDaemonToken    = "COAGENT_AUTH_TOKEN"
	EnvDaemonTokenAlt = "COAGENT_DAEMON_TOKEN"

	// EnvChannelID is the channel scope (unchanged).
	EnvChannelID = "COAGENT_CHANNEL_ID"

	// EnvCoagentBin overrides the `coagent` binary lookup. Empty →
	// resolves via $PATH ("coagent").
	EnvCoagentBin = "COAGENT_BIN"
)

// AdapterActor is the canonical adapter actor id the audience flag
// points at on every xhs ask invocation. Keep in sync with
// daemon-go/internal/adapters/xhs.AdapterActorID.
const AdapterActor = "tool:xhs-adapter"

// Business types — v4 (L4 §2.1). Note the renames from the legacy
// device.command.send convention.
const (
	cmdTypePublish     = "xhs.publish"
	cmdTypeSearch      = "xhs.search"
	cmdTypeRecentFetch = "xhs.recent.fetch"
	cmdTypeNoteFetch   = "xhs.note.fetch"
	cmdTypeCookieSync  = "xhs.cookie.sync"
)

// RealConfig describes the env xhs-cli needs to spawn `coagent ask`.
// The CLI does NOT need to know the daemon URL itself — only that the
// required envs are present so the child process can resolve them.
type RealConfig struct {
	DaemonHTTP string        // forwarded as DAEMON_URL to the child
	Token      string        // forwarded as COAGENT_AUTH_TOKEN
	ChannelID  string        // forwarded as COAGENT_CHANNEL_ID
	Timeout    time.Duration // bounds per-invocation; 0 → 30s
	CoagentBin string        // override binary path; "" → "coagent"
	// Env returns the env list passed to the spawned coagent process.
	// Defaults to a deep-copy of os.Environ() with the three required
	// vars overridden from this Config; tests inject deterministic
	// values.
	Env []string
}

// LoadRealConfigFromEnv 从环境变量加载 RealConfig。优先读 v4 标准命名
// (DAEMON_URL / COAGENT_AUTH_TOKEN / COAGENT_CHANNEL_ID)；缺失时回退
// 到 legacy 命名 (COAGENT_DAEMON_HTTP / COAGENT_DAEMON_TOKEN)。三个
// 字段任一缺失返回 CodeError{Code:"config_missing"}.
func LoadRealConfigFromEnv() (RealConfig, error) {
	cfg := RealConfig{
		DaemonHTTP: firstNonEmpty(os.Getenv(EnvDaemonHTTP), os.Getenv(EnvDaemonHTTPAlt)),
		Token:      firstNonEmpty(os.Getenv(EnvDaemonToken), os.Getenv(EnvDaemonTokenAlt)),
		ChannelID:  strings.TrimSpace(os.Getenv(EnvChannelID)),
		CoagentBin: strings.TrimSpace(os.Getenv(EnvCoagentBin)),
	}
	if cfg.DaemonHTTP == "" {
		return cfg, &CodeError{Code: "config_missing", Msg: fmt.Sprintf("%s (or %s) is required", EnvDaemonHTTP, EnvDaemonHTTPAlt)}
	}
	if cfg.Token == "" {
		return cfg, &CodeError{Code: "config_missing", Msg: fmt.Sprintf("%s (or %s) is required", EnvDaemonToken, EnvDaemonTokenAlt)}
	}
	if cfg.ChannelID == "" {
		return cfg, &CodeError{Code: "config_missing", Msg: fmt.Sprintf("%s is required", EnvChannelID)}
	}
	if !strings.HasPrefix(cfg.DaemonHTTP, "http://") && !strings.HasPrefix(cfg.DaemonHTTP, "https://") {
		return cfg, &CodeError{Code: "config_invalid", Msg: fmt.Sprintf("invalid %s: must be absolute http(s) URL (got %q)", EnvDaemonHTTP, cfg.DaemonHTTP)}
	}
	return cfg, nil
}

// firstNonEmpty returns the first trimmed-non-empty string from xs.
func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if s := strings.TrimSpace(x); s != "" {
			return s
		}
	}
	return ""
}

// CoagentRunner is the seam that spawns coagent. Production uses
// execCoagentRunner; tests inject a deterministic stub.
type CoagentRunner interface {
	Run(ctx context.Context, cfg RealConfig, args []string) (CoagentResult, error)
}

// CoagentResult mirrors the relevant subset of coagent's stdout
// success JSON (`{id, correlation_id, kind, dedupe?}`).
type CoagentResult struct {
	ID            string `json:"id"`
	CorrelationID string `json:"correlation_id"`
	Kind          string `json:"kind"`
	Dedupe        bool   `json:"dedupe,omitempty"`
}

// RealProvider 把 5 命令统一翻译成 spawn `coagent ask --type xhs.X
// --audience tool:xhs-adapter --payload <json>`。
//
// 行为契约：
//  1. 命令仅 dispatch，不阻塞等结果（与 legacy 一致）。
//  2. coagent 成功（exit 0 + stdout JSON）→ DispatchAck{
//     correlation_id, status:"dispatched", id, dedupe}。
//  3. coagent reject（exit 3 + stderr JSON）→ CodeError{Code: harness
//     reason, Msg: detail}。
//  4. coagent 启动失败 / spawn error → CodeError{Code:"coagent_unavailable"}.
//  5. coagent 任何其它非零 exit → CodeError{Code:"coagent_failed",
//     Msg: stderr first 200 chars}。
type RealProvider struct {
	cfg    RealConfig
	runner CoagentRunner
}

// NewRealProvider 构造 RealProvider，runner 默认走真实 coagent 子进程。
// Timeout 默认 30s。
func NewRealProvider(cfg RealConfig) *RealProvider {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &RealProvider{cfg: cfg, runner: execCoagentRunner{}}
}

// WithRunner returns a copy of p whose CoagentRunner is replaced.
// Used by tests to inject a stub without spawning subprocesses.
func (p *RealProvider) WithRunner(r CoagentRunner) *RealProvider {
	cp := *p
	cp.runner = r
	return &cp
}

// Name 实现 Provider.Name。
func (p *RealProvider) Name() string { return "real" }

// Publish dispatches xhs.publish.
//
// real-mode contract (carried forward from legacy: fix-spec.md
// §Fix-T1.1 + L4 §2.6): NEVER forward inline `content`. The daemon
// (and adapter) reads `content_path` from disk on push, so we strip
// `content` when `content_path` is present.
func (p *RealProvider) Publish(ctx context.Context, args PublishArgs) (any, error) {
	params := map[string]any{
		"title": args.Title,
	}
	if args.ContentPath != "" {
		params["content_path"] = args.ContentPath
	} else if args.Content != "" {
		// content_path missing AND inline content given — pass it
		// through (legacy fallback / pure mock-compat).
		params["content"] = args.Content
	}
	if len(args.Tags) > 0 {
		params["tags"] = args.Tags
	}
	if len(args.ImageData) > 0 {
		params["images"] = args.ImageData
	}
	return p.dispatch(ctx, cmdTypePublish, params)
}

// Search dispatches xhs.search.
func (p *RealProvider) Search(ctx context.Context, args SearchArgs) (any, error) {
	params := map[string]any{"query": args.Keyword}
	if args.Limit > 0 {
		params["limit"] = args.Limit
	}
	return p.dispatch(ctx, cmdTypeSearch, params)
}

// GetMyRecent dispatches xhs.recent.fetch.
func (p *RealProvider) GetMyRecent(ctx context.Context, args GetMyRecentArgs) (any, error) {
	params := map[string]any{}
	if args.Limit > 0 {
		params["limit"] = args.Limit
	}
	return p.dispatch(ctx, cmdTypeRecentFetch, params)
}

// GetNote dispatches xhs.note.fetch.
func (p *RealProvider) GetNote(ctx context.Context, args GetNoteArgs) (any, error) {
	params := map[string]any{}
	if args.NoteID != "" {
		params["note_id"] = args.NoteID
	}
	if args.URL != "" {
		params["url"] = args.URL
	}
	if args.XsecToken != "" {
		params["xsec_token"] = args.XsecToken
	}
	return p.dispatch(ctx, cmdTypeNoteFetch, params)
}

// SyncCookie dispatches xhs.cookie.sync.
func (p *RealProvider) SyncCookie(ctx context.Context, _ SyncCookieArgs) (any, error) {
	return p.dispatch(ctx, cmdTypeCookieSync, map[string]any{})
}

// dispatch is the shared launcher: it serializes params → --payload,
// invokes coagent, and packages the result as DispatchAck.
func (p *RealProvider) dispatch(ctx context.Context, typeName string, params map[string]any) (DispatchAck, error) {
	payloadBytes, err := json.Marshal(params)
	if err != nil {
		return DispatchAck{}, fmt.Errorf("marshal payload: %w", err)
	}
	argv := []string{
		"ask",
		"--type", typeName,
		"--audience", AdapterActor,
		"--payload", string(payloadBytes),
	}

	ctxTimeout := ctx
	if p.cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctxTimeout, cancel = context.WithTimeout(ctx, p.cfg.Timeout)
		defer cancel()
	}

	res, runErr := p.runner.Run(ctxTimeout, p.cfg, argv)
	if runErr != nil {
		return DispatchAck{}, runErr
	}
	if strings.TrimSpace(res.CorrelationID) == "" {
		return DispatchAck{}, &CodeError{
			Code: "invalid_daemon_response",
			Msg:  "coagent ask succeeded but stdout missing correlation_id",
		}
	}
	return DispatchAck{
		CorrelationID: res.CorrelationID,
		ID:            res.ID,
		Status:        "dispatched",
		Dedupe:        res.Dedupe,
	}, nil
}

// execCoagentRunner is the production CoagentRunner: it locates the
// `coagent` binary and exec's it with the constructed argv.
type execCoagentRunner struct{}

// Run implements CoagentRunner by spawning `coagent ask ...`.
//
// Exit code mapping mirrors lightcone/daemon-go/cmd/coagent/main.go:
//
//	0 success                 → unmarshal stdout
//	2 usage / bad args        → CodeError{coagent_usage_error}
//	3 harness reject          → CodeError{<reason from stderr JSON>}
//	4 infra error             → CodeError{coagent_infra}
//	5 no binding              → CodeError{coagent_no_binding}
//	6 flag format             → CodeError{coagent_flag_format}
//	other                     → CodeError{coagent_failed}
func (execCoagentRunner) Run(ctx context.Context, cfg RealConfig, args []string) (CoagentResult, error) {
	bin := cfg.CoagentBin
	if bin == "" {
		bin = "coagent"
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = buildEnv(cfg)
	stdout, stderr := &captureBuf{}, &captureBuf{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code := exitErr.ExitCode()
			return CoagentResult{}, classifyExit(code, stdout.String(), stderr.String())
		}
		// Not an ExitError → spawn failure (binary not found, perm error, etc.).
		return CoagentResult{}, &CodeError{
			Code: "coagent_unavailable",
			Msg:  fmt.Sprintf("spawn %s: %s", bin, err),
		}
	}
	var out CoagentResult
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		return CoagentResult{}, &CodeError{
			Code: "invalid_daemon_response",
			Msg:  fmt.Sprintf("decode coagent stdout: %s (body=%s)", err, truncate(stdout.String(), 200)),
		}
	}
	return out, nil
}

// classifyExit maps coagent exit codes to CodeError. stderr is parsed
// for a reject JSON `{"error":{"reason":...,"detail":...}}` shape when
// exit=3 so the upstream reason name flows through verbatim.
func classifyExit(code int, _, stderr string) error {
	switch code {
	case 2:
		return &CodeError{Code: "coagent_usage_error", Msg: truncate(stderr, 200)}
	case 3:
		return rejectFromStderr(stderr)
	case 4:
		return &CodeError{Code: "coagent_infra", Msg: truncate(stderr, 200)}
	case 5:
		return &CodeError{Code: "coagent_no_binding", Msg: truncate(stderr, 200)}
	case 6:
		return &CodeError{Code: "coagent_flag_format", Msg: truncate(stderr, 200)}
	default:
		return &CodeError{Code: "coagent_failed", Msg: fmt.Sprintf("exit=%d stderr=%s", code, truncate(stderr, 200))}
	}
}

// rejectFromStderr parses the reject envelope coagent writes to
// stderr on exit 3 and turns it into a CodeError whose Code is the
// harness reason. Best-effort: when the stderr is not parseable we
// fall back to a generic code.
func rejectFromStderr(stderr string) error {
	// coagent writes a header line like "coagent: ask: reject reason=..."
	// followed by a JSON body. Pull out the JSON portion if present.
	trimmed := strings.TrimSpace(stderr)
	if i := strings.Index(trimmed, "{"); i >= 0 {
		trimmed = trimmed[i:]
	}
	var body struct {
		Error struct {
			Reason string `json:"reason"`
			Detail string `json:"detail"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(trimmed), &body); err == nil && body.Error.Reason != "" {
		return &CodeError{Code: body.Error.Reason, Msg: body.Error.Detail}
	}
	return &CodeError{Code: "coagent_reject", Msg: truncate(stderr, 200)}
}

// buildEnv produces the env slice for the child process. Starts from
// cfg.Env (or os.Environ() when nil) and overrides the three required
// vars from cfg so the child sees the same values regardless of how
// the parent process was launched.
func buildEnv(cfg RealConfig) []string {
	base := cfg.Env
	if base == nil {
		base = os.Environ()
	}
	overrides := map[string]string{
		EnvDaemonHTTP:  cfg.DaemonHTTP,
		EnvDaemonToken: cfg.Token,
		EnvChannelID:   cfg.ChannelID,
	}
	out := make([]string, 0, len(base)+len(overrides))
	seen := map[string]bool{}
	for _, kv := range base {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			out = append(out, kv)
			continue
		}
		key := kv[:eq]
		if v, ok := overrides[key]; ok {
			out = append(out, key+"="+v)
			seen[key] = true
			continue
		}
		out = append(out, kv)
	}
	for k, v := range overrides {
		if !seen[k] {
			out = append(out, k+"="+v)
		}
	}
	return out
}

// captureBuf is a tiny io.Writer that captures bytes for both Bytes()
// and String() readers. Avoids the bytes package dependency wobble in
// vendored modules; the implementation is intentionally trivial.
type captureBuf struct{ b []byte }

func (c *captureBuf) Write(p []byte) (int, error) {
	c.b = append(c.b, p...)
	return len(p), nil
}
func (c *captureBuf) String() string { return string(c.b) }
func (c *captureBuf) Bytes() []byte  { return c.b }

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
