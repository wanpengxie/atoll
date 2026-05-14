package xhs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/coagent-ai/daemon-go/pkg/adapter"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// AdapterName is the framework module name; matches the registry key
// the daemon uses to look up this adapter via OnExternalCallback.
const AdapterName = "xhs"

// AdapterActorID is the canonical actor_id this adapter owns. v4-message
// -definition §1.2.5 + L4 §2.1 mandate sender.id = tool:xhs-adapter on
// every adapter-emitted response.
const AdapterActorID = "tool:xhs-adapter"

// Binding name expected by the framework. xhs is daemon-resident
// (Chrome extension WS push happens out-of-process), so daemon_rpc.
const adapterBinding = "daemon_rpc"

// Default per-type pending budget. M1.3 baseline keeps every xhs op at
// 5 minutes — large enough to cover Chrome extension throttling, small
// enough that a hanging request surfaces a F3 timeout failed terminal
// within one human attention span. Override via Config.MaxPendingMs.
const defaultMaxPendingMs int64 = 5 * 60 * 1000

// Type names — closed set (L4 §2.1). The 5 R/R types travel both
// directions; xhs.note.archived is agent-emitted and the adapter only
// declares it so the type_registry HandlerActorID lookup resolves to
// tool:xhs-adapter (keeps the type registry consistent with the
// "adapter owns the actor" invariant).
const (
	TypePublish      = "xhs.publish"
	TypeSearch       = "xhs.search"
	TypeNoteFetch    = "xhs.note.fetch"
	TypeRecentFetch  = "xhs.recent.fetch"
	TypeCookieSync   = "xhs.cookie.sync"
	TypeNoteArchived = "xhs.note.archived"
)

// requestResponseTypes is the subset that travels request → response.
// Used by Declares() to populate MaxPendingMs only for the types the
// framework actually arms a timer for. xhs.note.archived (event only)
// still appears in Declares.Types because the type_registry binds it
// to AdapterActorID; the framework's Validate tolerates a
// MaxPendingMs entry for it (and the adapter sets one defensively).
var requestResponseTypes = []string{
	TypePublish,
	TypeSearch,
	TypeNoteFetch,
	TypeRecentFetch,
	TypeCookieSync,
}

// allTypes is the full closed set Declares() returns.
var allTypes = append(append([]string{}, requestResponseTypes...), TypeNoteArchived)

// Config tunes a Module instance. Required: DeviceClient (production:
// the daemon WS server wrapper; tests: MockDeviceClient).
type Config struct {
	// DeviceClient is the WS push seam. Required.
	DeviceClient DeviceClient

	// DefaultDeviceID is the fallback device_id when the request
	// payload omits one. Optional — if empty AND the payload also
	// omits device_id the adapter returns a failed terminal with
	// reason "device_id_missing" rather than guessing.
	DefaultDeviceID string

	// MaxPendingMs overrides the per-type pending budget. Keys MUST
	// be entries from allTypes; missing entries default to
	// defaultMaxPendingMs.
	MaxPendingMs map[string]int64
}

// Module is the xhs adapter implementing adapter.Module. One instance
// per daemon process per channel.
type Module struct {
	cfg  Config
	mctx *adapter.ModuleContext
}

// New constructs a Module from cfg. Panics on nil DeviceClient — that
// is a programming error (production must wire the WS server; tests
// must wire a mock). Failing fast keeps the framework Install path
// from observing a half-initialised adapter.
func New(cfg Config) *Module {
	if cfg.DeviceClient == nil {
		panic("xhs.New: Config.DeviceClient is required")
	}
	if cfg.MaxPendingMs == nil {
		cfg.MaxPendingMs = map[string]int64{}
	}
	return &Module{cfg: cfg}
}

// Declares returns the static adapter metadata. Called once per
// Install per channel (L2 §8.1).
func (m *Module) Declares() adapter.Declaration {
	pending := make(map[string]int64, len(allTypes))
	for _, t := range allTypes {
		if v, ok := m.cfg.MaxPendingMs[t]; ok && v > 0 {
			pending[t] = v
			continue
		}
		pending[t] = defaultMaxPendingMs
	}
	return adapter.Declaration{
		Name:         AdapterName,
		ActorID:      AdapterActorID,
		Types:        append([]string{}, allTypes...),
		Binding:      adapterBinding,
		MaxPendingMs: pending,
	}
}

// Init captures the framework-provided ModuleContext so Handle /
// OnExternalCallback can call Correlation / Respond / ErrorPolicy.
func (m *Module) Init(_ context.Context, mctx *adapter.ModuleContext) error {
	if mctx == nil {
		return errors.New("xhs: ModuleContext is nil")
	}
	m.mctx = mctx
	return nil
}

// Shutdown is a no-op for the xhs adapter. Pending requests get
// resolved by the F3 timer (default_timeout) on the next daemon boot
// if the process exits mid-flight (BootRecoverTimers covers them).
func (m *Module) Shutdown(_ context.Context) error { return nil }

// Handle translates one inbound kind=request envelope into a Chrome
// extension WS frame + a tracked correlation entry. On push failure
// the adapter calls FailTerminal so the agent observes a clean
// `failed` response without waiting on the F3 default timeout.
func (m *Module) Handle(ctx context.Context, env *v4types.Envelope) error {
	if m.mctx == nil {
		return errors.New("xhs: Init was not called")
	}
	if env == nil {
		return errors.New("xhs: Handle envelope is nil")
	}

	// Decode payload as JSON object so we can extract device_id +
	// build the WS params (which omits framework-only fields).
	var payload map[string]any
	if len(env.Payload) > 0 {
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			return m.failNow(ctx, env.ID, "payload_decode_failed",
				map[string]any{"error": err.Error()})
		}
	} else {
		payload = map[string]any{}
	}

	deviceID, _ := payload["device_id"].(string)
	if deviceID == "" {
		deviceID = m.cfg.DefaultDeviceID
	}
	if deviceID == "" {
		return m.failNow(ctx, env.ID, "device_id_missing", nil)
	}

	// Strip "xhs." prefix so the WS frame `cmd` matches the M1.2
	// extension expectation. The legacy daemon stripped it the same
	// way (lightcone/daemon/src/channel-manager.js → pushCommand).
	cmd := env.Type
	cmd = strings.TrimPrefix(cmd, "xhs.")

	// Params on the wire MUST NOT carry `device_id` (it lives in the
	// payload metadata, not in the extension command). Build a fresh
	// map so we don't mutate the unmarshalled view.
	params := make(map[string]any, len(payload))
	for k, v := range payload {
		if k == "device_id" {
			continue
		}
		params[k] = v
	}

	frame := Command{
		Type:          "command",
		CorrelationID: env.ID,
		Cmd:           cmd,
		Params:        params,
	}

	// Track the correlation before push: a callback arriving before
	// PushCommand returns (rare but possible under load / retransmit)
	// must already see a Recover hit.
	deadline := framePendingDeadline(env, m.mctx)
	if err := m.mctx.Correlation.Track(ctx, env.ID, env.ID, deadline); err != nil {
		return fmt.Errorf("xhs: track correlation: %w", err)
	}

	if err := m.cfg.DeviceClient.PushCommand(ctx, deviceID, frame); err != nil {
		// Push failure → emit failed terminal immediately. The
		// framework's F3 timer would also catch this, but a prompt
		// reason makes agent retry policy easier.
		reason := "device_push_failed"
		if errors.Is(err, ErrDeviceOffline) {
			reason = "device_offline"
		}
		return m.failNow(ctx, env.ID, reason, map[string]any{
			"device_id": deviceID,
			"error":     err.Error(),
		})
	}
	return nil
}

// OnExternalCallback parses an HTTP-callback body and translates the
// extension's success / error reply into ctx.Respond. The body shape
// matches the M1.2 protocol:
//
//	{ "correlation_id": "<envelope.id>",
//	  "device_id":      "<deviceId>",
//	  "status":         "ok" | "error",
//	  "result":         <object?>,   // when status=ok
//	  "error":          <object?> }  // when status=error
func (m *Module) OnExternalCallback(ctx context.Context, raw []byte) error {
	if m.mctx == nil {
		return errors.New("xhs: Init was not called")
	}
	var cb callbackBody
	if err := json.Unmarshal(raw, &cb); err != nil {
		return fmt.Errorf("xhs: decode callback: %w", err)
	}
	correlation := strings.TrimSpace(cb.CorrelationID)
	if correlation == "" {
		return errors.New("xhs: callback missing correlation_id")
	}

	requestID, ok, err := m.mctx.Correlation.Recover(ctx, correlation)
	if err != nil {
		return fmt.Errorf("xhs: recover correlation: %w", err)
	}
	if !ok {
		// Orphan callback — the request is either GC'd, terminalized,
		// or was never tracked. M1.3 baseline: drop silently per L1
		// §6.5 (observability event is emitted by the framework GC).
		return nil
	}

	// Build the Respond payload. device_id always flows back via the
	// payload (v4-message-definition §1.2.5 — sender.id stays
	// tool:xhs-adapter; device identity lives in payload.device_id).
	payload := map[string]any{}
	if cb.DeviceID != "" {
		payload["device_id"] = cb.DeviceID
	}
	status := adapter.StatusCompleted
	reason := ""
	switch strings.ToLower(cb.Status) {
	case "ok", "completed", "success":
		// Merge the extension's `result` object into the payload so
		// the agent sees domain fields (note_id / url / notes / ...)
		// at the top level — matches L4 §2.1.3.
		for k, v := range cb.Result {
			payload[k] = v
		}
	case "error", "failed", "failure":
		status = adapter.StatusFailed
		reason = errorReason(cb.Error)
		for k, v := range cb.Error {
			if k == "reason" {
				continue
			}
			payload[k] = v
		}
	default:
		status = adapter.StatusFailed
		reason = "callback_status_unknown"
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("xhs: marshal respond payload: %w", err)
	}
	_, err = m.mctx.Respond(ctx, requestID, body, adapter.RespondOptions{
		Status: status,
		Reason: reason,
	})
	return err
}

// callbackBody mirrors the HTTP callback payload shape.
type callbackBody struct {
	CorrelationID string         `json:"correlation_id"`
	DeviceID      string         `json:"device_id,omitempty"`
	Status        string         `json:"status"`
	Result        map[string]any `json:"result,omitempty"`
	Error         map[string]any `json:"error,omitempty"`
}

// errorReason picks a human-readable reason string from the
// callback `error` object. Falls back to "callback_failed" when
// neither `reason` nor `code` are set.
func errorReason(e map[string]any) string {
	if e == nil {
		return "callback_failed"
	}
	if v, ok := e["reason"].(string); ok && v != "" {
		return v
	}
	if v, ok := e["code"].(string); ok && v != "" {
		return v
	}
	return "callback_failed"
}

// framePendingDeadline picks the deadline (wall-ms) to register with
// the framework correlation tracker. Uses envelope.expires_at when
// the writer stamped one, otherwise falls back to "now + default
// pending budget" so Recover keeps working even on legacy envelopes.
func framePendingDeadline(env *v4types.Envelope, _ *adapter.ModuleContext) int64 {
	if env.ExpiresAt != nil && *env.ExpiresAt > 0 {
		return *env.ExpiresAt
	}
	// Fallback: now + budget. We can't read the framework's clock
	// directly (it's a closure), so use the envelope.ts as a stable
	// anchor — the GC grace covers a wider window.
	return env.TS + defaultMaxPendingMs
}

// failNow emits a failed terminal via the framework's FailTerminal
// helper. Used for synchronous Handle failures (bad payload, missing
// device_id, WS push error). Errors from the underlying respond are
// returned so the dispatcher can log them.
func (m *Module) failNow(ctx context.Context, requestID, reason string, detail map[string]any) error {
	_, err := m.mctx.ErrorPolicy.FailTerminal(ctx, requestID, reason, detail)
	return err
}

// register publishes the xhs Factory into the framework registry so
// daemon main can pick it up by import side-effect.
//
// NOTE: this Factory uses a placeholder DeviceClient that always
// returns ErrDeviceOffline. Production wiring MUST replace it via
// adapter.NewManager(ManagerConfig{Modules: ...}) directly. The
// Register hook exists so test code (and future daemon main) can
// discover the adapter name without importing this package by name.
func init() {
	adapter.Register(AdapterName, func() adapter.Module {
		return New(Config{DeviceClient: offlineDeviceClient{}})
	})
}

// offlineDeviceClient is the placeholder DeviceClient used by the
// Register init-time Factory. It always returns ErrDeviceOffline so a
// daemon process that forgets to swap it sees prompt `device_offline`
// failed terminals rather than silently hanging.
type offlineDeviceClient struct{}

func (offlineDeviceClient) PushCommand(_ context.Context, _ string, _ Command) error {
	return ErrDeviceOffline
}
