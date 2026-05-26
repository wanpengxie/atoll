package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"time"

	adapterframework "github.com/wanpengxie/ActOS/adapters/framework"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Config tunes a Module instance.
type Config struct {
	// AdapterActorID overrides the actor_registry id. Empty → DefaultAdapterActorID.
	AdapterActorID actor.ActorID

	// MaxPendingMs overrides per-request budget. Zero → DefaultMaxPendingMs.
	MaxPendingMs int64

	// AllowedBinaries is the install-time allowlist. Empty slice ≠ nil:
	//   - nil      → DefaultAllowedBinaries (safe POSIX utilities)
	//   - []string{} → fail-closed (every cmd.exec rejected with binary_not_allowed)
	//   - non-nil  → exactly these binaries are allowed (basename match)
	AllowedBinaries []string

	// LookPath overrides exec.LookPath. Tests inject stubs; production
	// leaves nil → real LookPath.
	LookPath func(string) (string, error)

	// Now overrides time.Now. Tests inject stubs.
	Now func() time.Time
}

// Module implements kernel/adapter.Module for the cmd adapter.
type Module struct {
	cfg        Config
	mctx       *adapter.ModuleContext
	allowed    map[string]struct{}
	lookPath   func(string) (string, error)
	now        func() time.Time
}

// New constructs a Module from cfg. Defaults applied here so callers
// can use the zero value safely.
func New(cfg Config) *Module {
	if cfg.AdapterActorID == "" {
		cfg.AdapterActorID = DefaultAdapterActorID
	}
	if cfg.MaxPendingMs <= 0 {
		cfg.MaxPendingMs = DefaultMaxPendingMs
	}
	allowedList := cfg.AllowedBinaries
	if allowedList == nil {
		allowedList = DefaultAllowedBinaries
	}
	allowed := make(map[string]struct{}, len(allowedList))
	for _, b := range allowedList {
		allowed[b] = struct{}{}
	}
	lookPath := cfg.LookPath
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Module{
		cfg:      cfg,
		allowed:  allowed,
		lookPath: lookPath,
		now:      now,
	}
}

// Declares returns the static adapter metadata. Read once at Install.
func (m *Module) Declares() adapter.Declaration {
	return adapter.Declaration{
		Description:      actorDescription,
		SkillDoc:         actorSkillDoc,
		Name:             AdapterName,
		ActorID:          m.cfg.AdapterActorID,
		Types:            append([]string{}, AllTypes...),
		TypeDeclarations: DeclarationTypeDeclarations(),
		Binding:          Binding,
		MaxPendingMs:     m.cfg.MaxPendingMs,
	}
}

// Init captures the ModuleContext. Embedded binding — no DeviceTransit
// or upstream credentials required.
func (m *Module) Init(_ context.Context, mctx *adapter.ModuleContext) error {
	if mctx == nil {
		return errors.New("cmd.Init: ModuleContext is nil")
	}
	if mctx.Respond == nil {
		return errors.New("cmd.Init: ModuleContext.Respond is nil")
	}
	m.mctx = mctx
	return nil
}

// Shutdown is a no-op — cmd adapter holds no long-lived resources.
func (m *Module) Shutdown(_ context.Context) error { return nil }

// Handle dispatches by env.Type. Unknown types fail-now with
// payload_decode_failed (the harness already rejects unknown types in
// Step 5, but defensive Handle keeps tests honest).
func (m *Module) Handle(ctx context.Context, env *message.Envelope) error {
	if m.mctx == nil {
		return errors.New("cmd.Handle: Init was not called")
	}
	if env == nil {
		return errors.New("cmd.Handle: nil envelope")
	}
	if env.Kind != message.KindRequest {
		return fmt.Errorf("cmd.Handle: kind=%s (must be request)", env.Kind)
	}
	switch env.Type {
	case TypeExec:
		return m.handleExec(ctx, env)
	case TypeWhich:
		return m.handleWhich(ctx, env)
	default:
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID: env.ID,
			ErrorCode: "type_unknown",
			Detail:    fmt.Sprintf("cmd adapter does not handle type %q", env.Type),
		})
	}
}

// OnExternalCallback is unused — cmd adapter is embedded and synchronous.
func (m *Module) OnExternalCallback(_ context.Context, _ []byte) error { return nil }

func (m *Module) handleExec(ctx context.Context, env *message.Envelope) error {
	var req ExecRequest
	if err := json.Unmarshal(env.Payload, &req); err != nil {
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID: env.ID,
			ErrorCode: "payload_decode_failed",
			Detail:    err.Error(),
		})
	}
	if req.Binary == "" {
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID: env.ID,
			ErrorCode: "payload_decode_failed",
			Detail:    "binary is required",
		})
	}
	if !m.isAllowed(req.Binary) {
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID:      env.ID,
			TerminalReason: message.TerminalReceiverUnavailable,
			ErrorCode:      "binary_not_allowed",
			Detail:         fmt.Sprintf("binary %q is not in the install allowlist", req.Binary),
		})
	}
	if req.Cwd != "" && !filepath.IsAbs(req.Cwd) {
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID: env.ID,
			ErrorCode: "payload_decode_failed",
			Detail:    fmt.Sprintf("cwd must be absolute path (got %q)", req.Cwd),
		})
	}
	resolved, err := m.lookPath(req.Binary)
	if err != nil {
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID:      env.ID,
			TerminalReason: message.TerminalReceiverInternalError,
			ErrorCode:      "binary_not_found",
			Detail:         err.Error(),
		})
	}

	timeoutMs := req.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = DefaultBinaryTimeoutMs
	}
	if timeoutMs > m.cfg.MaxPendingMs {
		timeoutMs = m.cfg.MaxPendingMs
	}
	execCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	cmd := exec.CommandContext(execCtx, resolved, req.Args...)
	if req.Cwd != "" {
		cmd.Dir = req.Cwd
	}
	if len(req.Env) > 0 {
		envSlice := make([]string, 0, len(req.Env))
		for k, v := range req.Env {
			envSlice = append(envSlice, k+"="+v)
		}
		cmd.Env = envSlice
	}
	if req.Stdin != "" {
		cmd.Stdin = bytes.NewReader([]byte(req.Stdin))
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := m.now()
	runErr := cmd.Run()
	durationMs := m.now().Sub(start).Milliseconds()

	if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID:      env.ID,
			TerminalReason: message.TerminalReceiverInternalError,
			ErrorCode:      "exec_timeout",
			Detail:         fmt.Sprintf("binary %s exceeded timeout_ms=%d", req.Binary, timeoutMs),
		})
	}

	exitCode := 0
	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
				RequestID:      env.ID,
				TerminalReason: message.TerminalReceiverInternalError,
				ErrorCode:      "exec_failed",
				Detail:         runErr.Error(),
			})
		}
	}

	respPayload, err := json.Marshal(ExecResponse{
		Stdout:     stdout.String(),
		Stderr:     stderr.String(),
		ExitCode:   exitCode,
		DurationMs: durationMs,
	})
	if err != nil {
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID:      env.ID,
			TerminalReason: message.TerminalReceiverInternalError,
			ErrorCode:      "marshal_failed",
			Detail:         err.Error(),
		})
	}
	_, err = m.mctx.Respond(ctx, adapter.CorrelationKey(env.ID), respPayload, adapter.RespondOptions{
		Status: "completed",
	})
	return err
}

func (m *Module) handleWhich(ctx context.Context, env *message.Envelope) error {
	var req WhichRequest
	if err := json.Unmarshal(env.Payload, &req); err != nil {
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID: env.ID,
			ErrorCode: "payload_decode_failed",
			Detail:    err.Error(),
		})
	}
	if req.Binary == "" {
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID: env.ID,
			ErrorCode: "payload_decode_failed",
			Detail:    "binary is required",
		})
	}
	path, _ := m.lookPath(req.Binary)
	resp := WhichResponse{
		Binary:  req.Binary,
		Path:    path,
		Allowed: m.isAllowed(req.Binary),
	}
	payload, err := json.Marshal(resp)
	if err != nil {
		return adapterframework.FailNow(ctx, m.mctx, adapterframework.FailNowParams{
			RequestID: env.ID,
			ErrorCode: "marshal_failed",
			Detail:    err.Error(),
		})
	}
	_, err = m.mctx.Respond(ctx, adapter.CorrelationKey(env.ID), payload, adapter.RespondOptions{
		Status: "completed",
	})
	return err
}

func (m *Module) isAllowed(binary string) bool {
	if _, ok := m.allowed[binary]; ok {
		return true
	}
	// Also accept basename match for absolute paths so callers can pass
	// `/usr/bin/echo` when `echo` is allowed. Conservative: do NOT
	// dereference symlinks or resolve `..`.
	if filepath.IsAbs(binary) {
		base := filepath.Base(binary)
		if _, ok := m.allowed[base]; ok {
			return true
		}
	}
	return false
}
