package kimibridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/adapters/framework"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Config tunes a Module instance. Everything carries a defensible default.
type Config struct {
	// AdapterActorID overrides the actor_registry row this Module owns.
	// Empty defaults to DefaultAdapterActorID.
	AdapterActorID actor.ActorID

	// BaseURL is the kimi-webbridge daemon endpoint. Defaults to
	// DefaultBaseURL (http://127.0.0.1:10086).
	BaseURL string

	// MaxPendingMs overrides the per-request timeout. Zero defaults to
	// DefaultMaxPendingMs (30s).
	MaxPendingMs int64

	// DefaultSession is the `session` field stamped onto every
	// CommandRequest when the envelope payload doesn't supply one.
	// Empty leaves session unset (daemon uses its own default).
	DefaultSession string

	// Now is a clock injection point for tests. Defaults to time.Now.
	Now func() time.Time
}

// Option mutates a Module during construction. Mirrors the feishu
// adapter pattern (adapters/feishu/module.go) — daemon composition root
// passes Deps + actor id without exploding the constructor signature.
type Option func(*Module)

// WithDeps copies the framework Deps bundle onto the module. The
// composition root calls this from the factory closure so the same
// HTTPClient pool is shared across adapters in the daemon process.
// Required before Init.
func WithDeps(deps framework.Deps) Option {
	return func(m *Module) {
		m.httpClient = deps.HTTPClient
		m.logger = deps.Logger
		m.metrics = deps.Metrics
	}
}

// WithBaseURL overrides the daemon endpoint (used by tests + custom
// deployments that put kimi-webbridge on a non-default port).
func WithBaseURL(u string) Option {
	return func(m *Module) { m.cfg.BaseURL = u }
}

// WithHTTPClient overrides the HTTPClient injected by Deps. Test-only
// hook for stubbing out the network.
func WithHTTPClient(c *framework.HTTPClient) Option {
	return func(m *Module) { m.httpClient = c }
}

// Module implements kernel/adapter.Module for the kimi-webbridge
// adapter. Binding = runtime_outbound: the adapter dials the local
// daemon at BaseURL on every Handle.
//
// Cross-binding helpers in use:
//   - adapters/framework.FailNow: synchronous failure path (decode
//     errors, transport errors, daemon-reported failure)
type Module struct {
	cfg Config

	mctx       *adapter.ModuleContext
	httpClient *framework.HTTPClient
	client     *Client
	logger     framework.Logger
	metrics    framework.Metrics
	now        func() time.Time

	statusMu     sync.Mutex
	daemonKnown  bool
	daemonOnline bool
}

// New constructs a Module from Config + optional overrides. Returns an
// error when required dependencies (HTTPClient via WithDeps) are
// missing — fail-fast at composition root rather than at first Handle
// dispatch.
func New(cfg Config, opts ...Option) (*Module, error) {
	if cfg.AdapterActorID == "" {
		cfg.AdapterActorID = DefaultAdapterActorID
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.MaxPendingMs <= 0 {
		cfg.MaxPendingMs = DefaultMaxPendingMs
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	m := &Module{cfg: cfg, now: cfg.Now}
	for _, opt := range opts {
		opt(m)
	}
	if m.logger == nil {
		m.logger = framework.NoopLogger{}
	}
	if m.metrics == nil {
		m.metrics = framework.NoopMetrics{}
	}
	return m, nil
}

// Declares returns the static adapter metadata. Called exactly once
// per Install per channel by the framework (L2 §8.1).
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

// Init builds the HTTPClient (if not already injected) + the
// kimibridge Client wrapper. Per framework convention, ModuleContext
// supplies Correlation / ErrorPolicy / Respond / HarnessChain; this
// adapter doesn't need DeviceTransit (outbound binding) so the
// ModuleContext field stays nil.
func (m *Module) Init(_ context.Context, mctx *adapter.ModuleContext) error {
	if mctx == nil {
		return errors.New("kimibridge.Init: ModuleContext is nil")
	}
	if mctx.Correlation == nil {
		return errors.New("kimibridge.Init: ModuleContext.Correlation is nil")
	}
	if mctx.ErrorPolicy == nil {
		return errors.New("kimibridge.Init: ModuleContext.ErrorPolicy is nil")
	}
	if mctx.Respond == nil {
		return errors.New("kimibridge.Init: ModuleContext.Respond is nil")
	}
	if mctx.AdapterActorID == "" {
		mctx.AdapterActorID = m.cfg.AdapterActorID
	}

	if m.httpClient == nil {
		m.httpClient = framework.NewHTTPClient(framework.HTTPClientConfig{
			BaseURL: m.cfg.BaseURL,
			Timeout: time.Duration(m.cfg.MaxPendingMs) * time.Millisecond,
			Logger:  m.logger,
			Metrics: m.metrics,
		})
	}
	m.client = NewClient(m.httpClient)
	m.mctx = mctx
	return nil
}

// Shutdown is a no-op. The HTTPClient itself is process-scoped (shared
// across adapters); the per-Module wrapper has no resources to release.
// Pending requests resolve via the F3 timer on the next daemon boot.
func (m *Module) Shutdown(_ context.Context) error { return nil }

// Heartbeat probes the local kimi-webbridge daemon and reports the
// combined daemon+extension readiness into actor_registry.
func (m *Module) Heartbeat(ctx context.Context) (adapter.HeartbeatReport, error) {
	report := m.probeStatus(ctx)
	m.maybeEmitDaemonEvent(ctx, report)
	return report, nil
}

// Status enriches actor.status with the latest daemon / extension
// probe detail without emitting lifecycle events.
func (m *Module) Status(ctx context.Context) (adapter.StatusReport, error) {
	report := m.probeStatus(ctx)
	return adapter.StatusReport{
		Available: report.Available,
		Reason:    report.Reason,
		Detail:    report.Detail,
		CheckedAt: report.CheckedAt,
	}, nil
}

func (m *Module) probeStatus(ctx context.Context) adapter.HeartbeatReport {
	checkedAt := m.now()
	if m.client == nil {
		return adapter.HeartbeatReport{
			Available: false,
			Reason:    "initializing",
			CheckedAt: checkedAt,
			Detail: map[string]any{
				"daemon_url": m.cfg.BaseURL,
			},
		}
	}
	status, httpStatus, err := m.client.Status(ctx)
	if err != nil {
		return adapter.HeartbeatReport{
			Available: false,
			Reason:    "daemon_unreachable",
			CheckedAt: checkedAt,
			Detail: map[string]any{
				"daemon_url":  m.cfg.BaseURL,
				"http_status": httpStatus,
				"error":       err.Error(),
			},
		}
	}
	detail := map[string]any{
		"daemon_url":          m.cfg.BaseURL,
		"running":             status.Running,
		"version":             status.Version,
		"port":                status.Port,
		"uptime_seconds":      status.UptimeSeconds,
		"extension_connected": status.ExtensionConnected,
		"extension_id":        status.ExtensionID,
		"extension_version":   status.ExtensionVersion,
	}
	switch {
	case !status.Running:
		return adapter.HeartbeatReport{Available: false, Reason: "daemon_unreachable", Detail: detail, CheckedAt: checkedAt}
	case !status.ExtensionConnected:
		return adapter.HeartbeatReport{Available: false, Reason: "extension_disconnected", Detail: detail, CheckedAt: checkedAt}
	default:
		return adapter.HeartbeatReport{Available: true, Reason: "ok", Detail: detail, CheckedAt: checkedAt}
	}
}

func (m *Module) maybeEmitDaemonEvent(ctx context.Context, report adapter.HeartbeatReport) {
	online := report.Reason != "daemon_unreachable" && report.Reason != "initializing"
	m.statusMu.Lock()
	known := m.daemonKnown
	changed := known && m.daemonOnline != online
	m.daemonKnown = true
	m.daemonOnline = online
	m.statusMu.Unlock()
	if !changed || m.mctx == nil || m.mctx.HarnessChain == nil {
		return
	}
	eventType := TypeDaemonOffline
	if online {
		eventType = TypeDaemonOnline
	}
	body, err := json.Marshal(map[string]any{
		"available":  online,
		"reason":     report.Reason,
		"detail":     report.Detail,
		"checked_at": report.CheckedAt.UnixMilli(),
	})
	if err != nil {
		m.logger.Warn("kimibridge.daemon_event.marshal", "err", err.Error())
		return
	}
	now := report.CheckedAt.UnixMilli()
	env := &message.Envelope{
		ID:         message.ID(fmt.Sprintf("event:%s:%s:%d", m.mctx.AdapterActorID, eventType, now)),
		TS:         now,
		TSReceived: now,
		ChannelID:  m.mctx.ChannelID,
		Sender:     message.Sender{Kind: actor.KindTool, ID: m.mctx.AdapterActorID},
		Kind:       message.KindEvent,
		Type:       eventType,
		Payload:    body,
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{actor.SystemActorID},
	}
	if _, err := m.mctx.HarnessChain.Write(ctx, env); err != nil {
		m.logger.Warn("kimibridge.daemon_event.write", "event_type", eventType, "err", err.Error())
	}
}

// Handle dispatches one inbound kind=request envelope to the
// kimi-webbridge daemon. Translates envelope.type → wire action,
// envelope.payload → wire args, optional `session` → wire session,
// then POSTs to /command and emits a terminal response via
// ModuleContext.Respond.
func (m *Module) Handle(ctx context.Context, env *message.Envelope) error {
	if m.mctx == nil || m.client == nil {
		return errors.New("kimibridge.Handle: Init was not called")
	}
	if env == nil {
		return errors.New("kimibridge.Handle: envelope is nil")
	}
	if env.Kind != message.KindRequest {
		return fmt.Errorf("kimibridge.Handle: envelope kind must be %q, got %q", message.KindRequest, env.Kind)
	}

	action, ok := ActionForType(env.Type)
	if !ok {
		return framework.FailNow(ctx, m.mctx, framework.FailNowParams{
			RequestID: env.ID,
			ErrorCode: "unknown_type",
			Detail:    fmt.Sprintf("kimibridge does not handle type %q", env.Type),
		})
	}

	args, session, err := decodeRequest(env.Payload)
	if err != nil {
		return framework.FailNow(ctx, m.mctx, framework.FailNowParams{
			RequestID: env.ID,
			ErrorCode: "payload_decode_failed",
			Detail:    err.Error(),
		})
	}
	if session == "" {
		session = m.cfg.DefaultSession
	}

	cmdResp, status, callErr := m.client.Call(ctx, CommandRequest{
		Action:  action,
		Args:    args,
		Session: session,
	})
	if callErr != nil {
		// Transport error / HTTP 4xx-5xx — daemon either unreachable
		// or rejected the call. Map to receiver_unavailable so the
		// LLM gets a clean closed-set reason; preserve adapter detail
		// in error_code for diagnostics.
		errCode := cmdResp.ErrorCode()
		if errCode == "" {
			errCode = "daemon_call_failed"
		}
		return framework.FailNow(ctx, m.mctx, framework.FailNowParams{
			RequestID:      env.ID,
			TerminalReason: message.TerminalReceiverUnavailable,
			ErrorCode:      errCode,
			Detail:         fmt.Sprintf("HTTP %d: %s", status, callErr.Error()),
		})
	}

	// Daemon returned 2xx + JSON envelope. Two business outcomes:
	if !cmdResp.Succeeded() {
		// Tool-level failure (e.g. "no tab found", "selector did not
		// match", "no extension connected"). Surface daemon's error
		// message + code; map to receiver_internal_error since the
		// daemon was reachable and chose to reject.
		errCode := cmdResp.ErrorCode()
		if errCode == "" {
			errCode = "tool_failed"
		}
		return framework.FailNow(ctx, m.mctx, framework.FailNowParams{
			RequestID:      env.ID,
			TerminalReason: message.TerminalReceiverInternalError,
			ErrorCode:      errCode,
			Detail:         cmdResp.ErrorMessage(),
		})
	}

	// Happy path. Daemon returned `{success:true, data:<payload>}` —
	// surface data verbatim as the response payload. Level A: the
	// adapter doesn't reshape per-tool fields; agents agree with the
	// daemon on SKILL.md §Tools schemas.
	body := cmdResp.Data
	if len(body) == 0 {
		body = []byte(`{}`)
	}
	_, err = m.mctx.Respond(ctx, adapter.CorrelationKey(env.ID), body, adapter.RespondOptions{
		Status: "completed",
	})
	return err
}

// OnExternalCallback is unused on the outbound path — the daemon's
// response is the HTTP body returned synchronously from Call. No
// out-of-band callback channel exists. Defined to satisfy the Module
// interface; framework will only invoke it for adapters that arm
// async callbacks themselves.
func (m *Module) OnExternalCallback(_ context.Context, _ []byte) error {
	return errors.New("kimibridge.OnExternalCallback: not used (outbound binding has no async callback channel)")
}

// requestEnvelope is the optional outer wrapper a caller MAY use to
// pin a `session` next to the tool args. The simpler form — passing
// the SKILL.md tool args directly as payload — also works; decodeRequest
// peels both shapes uniformly.
type requestEnvelope struct {
	Session string          `json:"session,omitempty"`
	Args    json.RawMessage `json:"args,omitempty"`
}

// decodeRequest unwraps the inbound envelope payload into (args,
// session). Two recognised shapes:
//
//  1. SKILL.md tool args directly:
//     {"url":"https://example.com","newTab":true}
//     → args=<entire payload>, session=Config.DefaultSession
//
//  2. Wrapped with explicit session selector:
//     {"session":"my-task","args":{"url":"https://example.com"}}
//     → args=payload.args, session=payload.session
//
// Shape (2) is detected when the payload object contains both
// `session` and `args` keys at the top level. Anything else flows
// through as (1).
func decodeRequest(raw json.RawMessage) (json.RawMessage, string, error) {
	if len(raw) == 0 {
		return nil, "", nil
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, "", fmt.Errorf("payload is not a JSON object: %w", err)
	}
	_, hasSession := probe["session"]
	_, hasArgs := probe["args"]
	if hasSession && hasArgs {
		var env requestEnvelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return nil, "", fmt.Errorf("decode wrapped request: %w", err)
		}
		return env.Args, env.Session, nil
	}
	return raw, "", nil
}
