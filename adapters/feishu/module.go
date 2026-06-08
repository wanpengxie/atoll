package feishu

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
)

// DefaultActorID is the actor_registry row this adapter binds to. The
// daemon bootstrap is expected to seed this actor with binding=runtime_outbound.
const DefaultActorID actor.ActorID = "tool:feishu-adapter"

// DefaultMaxPendingMs is the per-request timeout (30s) used when the
// caller does not override.
const DefaultMaxPendingMs int64 = 30_000

// Option mutates a Module during construction. Designed so the daemon
// can pass overrides (custom BaseURL for testing, custom Logger, etc.)
// without polluting the Factory signature.
type Option func(*Module)

// WithDeps copies the framework deps bundle onto the module. Required
// before Init runs.
func WithDeps(deps behavior.Deps) Option {
	return func(m *Module) {
		m.httpClient = deps.HTTPClient
		m.credStore = deps.CredentialStore
		m.logger = deps.Logger
		m.metrics = deps.Metrics
		m.clock = deps.Clock
	}
}

// SetCredentialStore is called by behavior.Manager during Install with a
// scoped credential view. Direct tests may still supply a store through
// WithDeps before calling Init manually.
func (m *Module) SetCredentialStore(store behavior.CredentialStore) {
	m.credStore = store
}

// WithActorID overrides the default actor id.
func WithActorID(id actor.ActorID) Option {
	return func(m *Module) { m.actorID = id }
}

// WithBaseURL overrides the default Feishu OpenAPI host (used by
// httptest-driven feishu_test.go).
func WithBaseURL(u string) Option {
	return func(m *Module) { m.baseURL = u }
}

// WithMaxPendingMs overrides the per-request timeout.
func WithMaxPendingMs(ms int64) Option {
	return func(m *Module) { m.maxPendingMs = ms }
}

// WithHTTPClient overrides the HTTPClient injected by Deps. Test-only
// hook for stubbing out the network without redefining Deps.
func WithHTTPClient(c *behavior.HTTPClient) Option {
	return func(m *Module) { m.httpClient = c }
}

// Module is the kernel/behavior.Module implementation for feishu. One
// instance per channel — the daemon creates a fresh Module for each
// Manager.
type Module struct {
	actorID      actor.ActorID
	baseURL      string
	maxPendingMs int64

	httpClient *behavior.HTTPClient
	credStore  behavior.CredentialStore
	logger     behavior.Logger
	metrics    behavior.Metrics
	clock      func() time.Time

	mctx   *behavior.ModuleContext
	creds  credentialBundle
	tokens *tokenCache
	client *client
}

// New constructs a Module with the supplied options applied. Use
// WithDeps to inject the framework Deps bundle before Install.
func New(opts ...Option) *Module {
	m := &Module{
		actorID:      DefaultActorID,
		baseURL:      DefaultBaseURL,
		maxPendingMs: DefaultMaxPendingMs,
		clock:        time.Now,
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.logger == nil {
		m.logger = behavior.NoopLogger{}
	}
	if m.metrics == nil {
		m.metrics = behavior.NoopMetrics{}
	}
	if m.clock == nil {
		m.clock = time.Now
	}
	return m
}

// Declares returns the static metadata that satisfies §T4 install
// rules: runtime_outbound binding, AllTypes, per-type timeout.
func (m *Module) Declares() behavior.Declaration {
	return behavior.Declaration{
		Name:         "feishu",
		ActorID:      m.actorID,
		Types:        append([]string(nil), AllTypes...),
		Binding:      actor.BindingRuntimeOutbound,
		MaxPendingMs: m.maxPendingMs,
		Needs:        []string{"http_helper", "credentials"},
	}
}

// Init loads credentials and assembles the HTTP client. Returns an
// error wrapping behavior.ErrCredentialMissing when app_id / app_secret
// are absent — the daemon treats this as an install failure.
func (m *Module) Init(ctx context.Context, mctx *behavior.ModuleContext) error {
	if mctx == nil {
		return errors.New("feishu: Init mctx nil")
	}
	if m.credStore == nil {
		return errors.New("feishu: Init CredentialStore not injected")
	}
	creds, err := loadCredentials(ctx, m.credStore)
	if err != nil {
		return err
	}
	if m.httpClient == nil {
		m.httpClient = behavior.NewHTTPClient(behavior.HTTPClientConfig{
			BaseURL: m.baseURL,
			Logger:  m.logger,
			Metrics: m.metrics,
			Clock:   m.clock,
		})
	}
	m.creds = creds
	m.tokens = newTokenCache(m.clock)
	m.client = newClient(m.httpClient, creds, m.tokens, m.logger, m.metrics)
	m.mctx = mctx
	m.logger.Info("feishu.init.ok",
		"channel_id", string(mctx.ChannelID),
		"actor_id", string(mctx.AdapterActorID),
		"app_id", creds.AppID,
		"app_secret", behavior.Redact(creds.AppSecret),
	)
	return nil
}

// Shutdown clears in-memory token state. The HTTPClient itself is
// shared with other adapters via Deps — we do NOT close it here.
func (m *Module) Shutdown(_ context.Context) error {
	if m.tokens != nil {
		m.tokens.invalidate()
	}
	return nil
}

// Heartbeat reports the installed outbound adapter baseline. Feishu has
// no persistent connection lifecycle; Init already validates the
// credential bundle, and each Handle performs its own HTTP call.
func (m *Module) Heartbeat(_ context.Context) (behavior.HeartbeatReport, error) {
	checkedAt := m.clock()
	if m.client == nil {
		return behavior.HeartbeatReport{
			Available: false,
			Reason:    "initializing",
			CheckedAt: checkedAt,
		}, nil
	}
	return behavior.HeartbeatReport{
		Available: true,
		Reason:    "ok",
		CheckedAt: checkedAt,
		Detail: map[string]any{
			"app_id":             m.creds.AppID,
			"credentials_loaded": true,
		},
	}, nil
}

// Status enriches actor.status with the same credential baseline.
func (m *Module) Status(ctx context.Context) (behavior.StatusReport, error) {
	hb, err := m.Heartbeat(ctx)
	return behavior.StatusReport(hb), err
}

// Handle dispatches by env.Type. Unknown types are rejected with a
// failed terminal so the caller observes a definite outcome instead of
// waiting for the framework timer. The domain code is carried in
// payload.error_code; payload.reason stays in the terminal_failure_reason
// closed set.
func (m *Module) Handle(ctx context.Context, env *message.Envelope) error {
	switch env.Type {
	case TypeChatSend:
		return m.handleChatSend(ctx, env)
	case TypeChatCreate:
		return m.handleChatCreate(ctx, env)
	}
	return m.fail(ctx, env, "type_unsupported", fmt.Sprintf("feishu adapter does not handle %s", env.Type))
}

// OnExternalCallback is intentionally a no-op: the current feishu adapter
// is outbound-only. Future inbound work will fill this in.
func (m *Module) OnExternalCallback(_ context.Context, _ []byte) error {
	m.logger.Warn("feishu.callback.dropped",
		"note", "feishu adapter is outbound-only in launch")
	return nil
}
