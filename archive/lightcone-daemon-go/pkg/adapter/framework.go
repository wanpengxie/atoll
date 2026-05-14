package adapter

// framework.go defines the public F1 surface (L2 §8.1) plus the
// shared types used by the F2/F3/F5/F6 implementations:
//
//   - Module interface – the contract every adapter implements.
//   - Declaration      – what an adapter announces at install time.
//   - ModuleContext    – the injected helper bundle (CorrelationTracker,
//                        ErrorPolicy, Respond, identity / channel fields).
//   - HarnessWriter    – the 1-method seam over pkg/harness.InWorkerBus
//                        so tests can hand in a recording stub.
//   - RespondOptions / RespondResult / Status enum.
//
// The Manager (lifecycle.go) glues them together at runtime. The
// package-level Register pattern (compile-time, per ticket §T13 "关键
// 技术决策") lets adapters self-publish into a registry the daemon
// reads at install time.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// -----------------------------------------------------------------------------
// F1 — Module interface (L2 §8.1)
// -----------------------------------------------------------------------------

// Module is the contract every adapter implements. The framework calls
// Declares first (no ctx — the result must be static + side-effect-free),
// then Init (with the populated ModuleContext), then Handle /
// OnExternalCallback on demand, and finally Shutdown.
//
// All methods MUST be safe for concurrent invocation: the framework may
// dispatch overlapping Handle calls (one per inbound request) and an
// OnExternalCallback at the same time.
//
// Method semantics (mirrors L2 §8.1):
//
//   - Declares           Static metadata; called exactly once per Install.
//     See Declaration for required fields.
//   - Init               Receives the framework-built ModuleContext. The
//     adapter MUST persist the pointer if it intends
//     to call helpers (Respond / ErrorPolicy /
//     Correlation) from Handle / OnExternalCallback.
//   - Handle             Translates one inbound kind=request envelope
//     into outbound protocol traffic. Returning an
//     error leaves the request pending — F3 timer
//     eventually emits adapter_default_timeout via
//     the framework. Adapters typically return nil
//     once the external call is launched + the
//     correlation is tracked.
//   - OnExternalCallback Translates one inbound external callback (e.g.
//     webhook body, WS message) into an Respond call.
//     The framework already pre-filters duplicate
//     callbacks (terminal already exists) before
//     invoking this method.
//   - Shutdown           Cancels in-flight work + releases external
//     connections. M1.3 baseline: best-effort,
//     invoked from Manager.Shutdown only.
type Module interface {
	Declares() Declaration
	Init(ctx context.Context, mctx *ModuleContext) error
	Shutdown(ctx context.Context) error
	Handle(ctx context.Context, env *v4types.Envelope) error
	OnExternalCallback(ctx context.Context, payload []byte) error
}

// Declaration is the static metadata the framework reads at install
// time. The fields mirror L2 §8.1 declares() (Go-flavoured).
//
// Field semantics:
//
//   - Name          Adapter identifier (e.g. "xhs"). Used for logging,
//     orphan callback event payload, and routing OnExternal
//     Callback by adapter name. MUST be non-empty +
//     unique within a Manager.
//   - ActorID       The actor_registry row this adapter owns. Every
//     request envelope whose audience[0] equals ActorID is
//     dispatched to this Module's Handle. MUST be a
//     pre-registered tool actor (actor_kind='tool').
//   - Types         The envelope.type set the adapter accepts. Each entry
//     MUST already exist in type_registry with
//     handler_actor_id == ActorID; the framework verifies
//     this in Install.
//   - Binding       "daemon_rpc" or "in_worker_bus" — must match the
//     adapter actor's actor_binding. M1.3 daemon-side
//     adapters are typically daemon_rpc.
//   - MaxPendingMs  Per-type request timeout (milliseconds). Used by F3
//     timer when boot recovery / Manager.RegisterTimer
//     needs to derive a deadline. Each Types entry MUST
//     have a positive value.
//   - Needs         Optional helper opt-in declared per L2 §8.1
//     declares.needs. M1.3 framework ignores this list
//     (no F4/F8 helpers wired); it stays in the struct so
//     adapters can stay forward-compatible.
type Declaration struct {
	Name         string
	ActorID      string
	Types        []string
	Binding      string
	MaxPendingMs map[string]int64
	Needs        []string
}

// Validate checks the structural invariants the framework relies on.
// Returns an error mentioning Name when set so callers get a useful
// diagnostic at Install time.
func (d Declaration) Validate() error {
	if strings.TrimSpace(d.Name) == "" {
		return errors.New("adapter: Declaration.Name is required")
	}
	if strings.TrimSpace(d.ActorID) == "" {
		return fmt.Errorf("adapter[%s]: Declaration.ActorID is required", d.Name)
	}
	if len(d.Types) == 0 {
		return fmt.Errorf("adapter[%s]: Declaration.Types must be non-empty", d.Name)
	}
	switch d.Binding {
	case "daemon_rpc", "in_worker_bus":
		// ok
	default:
		return fmt.Errorf("adapter[%s]: Declaration.Binding %q is invalid (must be daemon_rpc|in_worker_bus)", d.Name, d.Binding)
	}
	if len(d.MaxPendingMs) == 0 {
		return fmt.Errorf("adapter[%s]: Declaration.MaxPendingMs is required", d.Name)
	}
	// Deterministic ordering for error messages.
	keys := make([]string, 0, len(d.Types))
	keys = append(keys, d.Types...)
	sort.Strings(keys)
	for _, t := range keys {
		ms, ok := d.MaxPendingMs[t]
		if !ok {
			return fmt.Errorf("adapter[%s]: MaxPendingMs missing for type %q", d.Name, t)
		}
		if ms <= 0 {
			return fmt.Errorf("adapter[%s]: MaxPendingMs[%q]=%d must be > 0", d.Name, t, ms)
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// ModuleContext — injected helper bundle (L2 §8.1 AdapterCtx)
// -----------------------------------------------------------------------------

// ModuleContext is the helper bundle the framework hands to each Module
// during Init. Methods (Respond / errorPolicy.*) on this struct are
// safe to call concurrently — the underlying helpers serialize access
// to their own state.
//
// M1.3 baseline: only F2 / F3 (subset) / F5 are populated. F4 State /
// F8 helpers stay nil and adapters MUST NOT use them.
type ModuleContext struct {
	// Name mirrors Declaration.Name. Adapters use it for log context.
	Name string

	// ActorID mirrors Declaration.ActorID. The framework uses it to
	// build response envelopes; adapters may read it for diagnostics.
	ActorID string

	// ChannelID is the channel sqlite this Manager is bound to.
	ChannelID string

	// Correlation is the F2 tracker. Adapters call it from Handle to
	// register external ids and from OnExternalCallback (via Recover)
	// to map back to the originating request_id.
	Correlation CorrelationTracker

	// ErrorPolicy is the F3 helper subset (Timeout + FailTerminal).
	// retry / logSystemEvent are deferred (M1.x).
	ErrorPolicy ErrorPolicy

	// Respond is the F5 helper. The signature matches L2 §8.5
	// ctx.respond — passing a request_id, a domain payload, and a
	// status/reason envelope.
	Respond RespondFn
}

// RespondFn is the type of ModuleContext.Respond. It is exposed as a
// named type so adapters can store the closure if they want to thread
// it across goroutines without keeping the full ModuleContext.
type RespondFn func(ctx context.Context, requestID string, payload json.RawMessage, opts RespondOptions) (RespondResult, error)

// -----------------------------------------------------------------------------
// Respond options + status enum
// -----------------------------------------------------------------------------

// Status enumerates the L2 §8.5 ctx.respond status values. The string
// values match the protocol baseline payload schema (`status: 'completed'
// | 'failed'`).
type Status string

// Status enum values.
const (
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

// IsValid reports whether s is one of the closed-set Status values.
func (s Status) IsValid() bool {
	return s == StatusCompleted || s == StatusFailed
}

// RespondOptions carries the `status` + optional `reason` block L2
// §8.5 mandates on every adapter response. Detail is an optional bag of
// extra payload top-level keys (e.g. `error_class`, `external_id`); the
// framework merges them with the user-supplied payload before computing
// the deterministic id.
type RespondOptions struct {
	// Status is required ("completed" | "failed").
	Status Status

	// Reason is optional; when set it lands as payload.reason (the
	// adapter response schema usually exposes a free-string reason
	// column for failed terminals).
	Reason string

	// Detail is an optional bag of extra top-level keys for the response
	// payload. They are folded into the final payload AFTER the user
	// payload, so Detail overrides on key collision. Status / Reason
	// are written last (they always win).
	Detail map[string]any
}

// RespondResult is the structured outcome of a Respond call.
//
//   - ID            The envelope id that ended up in the store (which is
//     always the deterministic id derived from the payload).
//   - CorrelationID The persisted correlation_id (sourced from the
//     original request when the caller leaves payload's
//     correlation_id empty).
//   - Dedupe        True when the success came from harness idempotent
//     dedupe (Step 0.5 same-id retry OR terminal_duplicate
//     on the same id — both mean "we already wrote this
//     exact response"; both are observable success).
type RespondResult struct {
	ID            string
	CorrelationID string
	Dedupe        bool
}

// -----------------------------------------------------------------------------
// HarnessWriter seam
// -----------------------------------------------------------------------------

// HarnessWriter is the 1-method surface the framework calls when it
// needs to commit a write through the harness. Production code wires
// this with a closure over pkg/harness.InWorkerBus; tests inject a
// recording stub.
//
// Returning WriteResult lets the framework distinguish a harness reject
// (WriteResult.OK==false) from an infrastructure error (non-nil error).
// The L2 §8.5 "terminal_duplicate => idempotent success" handling lives
// inside Respond (see respond.go).
type HarnessWriter interface {
	Write(ctx context.Context, env *v4types.Envelope, callerCtx pkgharness.CallerCtx) (pkgharness.WriteResult, error)
}

// HarnessWriterFunc adapts a function value to HarnessWriter so tests +
// production wiring can pass a closure without declaring a struct.
type HarnessWriterFunc func(ctx context.Context, env *v4types.Envelope, callerCtx pkgharness.CallerCtx) (pkgharness.WriteResult, error)

// Write satisfies HarnessWriter.
func (f HarnessWriterFunc) Write(ctx context.Context, env *v4types.Envelope, cc pkgharness.CallerCtx) (pkgharness.WriteResult, error) {
	return f(ctx, env, cc)
}

// DefaultHarnessWriter returns a HarnessWriter that calls
// pkg/harness.InWorkerBus with the provided deps. The default Manager
// uses this so production callers only need to wire one Deps bundle.
func DefaultHarnessWriter(deps pkgharness.Deps) HarnessWriter {
	return HarnessWriterFunc(func(ctx context.Context, env *v4types.Envelope, cc pkgharness.CallerCtx) (pkgharness.WriteResult, error) {
		return pkgharness.InWorkerBus(ctx, deps, env, cc)
	})
}

// -----------------------------------------------------------------------------
// Logger seam
// -----------------------------------------------------------------------------

// Logger mirrors the minimal slog-ish interface other packages in this
// module use (wire_bridge / v4tool / tools). Callers can hand in their
// existing *slog.Logger directly — the three methods are satisfied by
// slog's Info / Warn / Error.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// noopLogger is the default Logger when ManagerConfig.Logger is unset.
type noopLogger struct{}

func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

// -----------------------------------------------------------------------------
// Register pattern (compile-time)
// -----------------------------------------------------------------------------

// Factory is the constructor type adapters use to publish themselves
// into the package-level registry. The daemon (or a test) collects the
// factories at init time and hands the resulting Module instances to
// NewManager.
type Factory func() Module

var (
	registryMu      sync.RWMutex
	registryFactory = map[string]Factory{}
)

// Register publishes a Factory under name. Calling Register twice with
// the same name panics — adapters are statically known + duplicates are
// a programmer error.
//
// Adapters typically call Register from an init() function in their
// package; the daemon imports the package (with a blank import if
// needed) so the init() side-effect lands before NewManager runs.
func Register(name string, f Factory) {
	if strings.TrimSpace(name) == "" {
		panic("adapter.Register: name is required")
	}
	if f == nil {
		panic("adapter.Register: factory is nil")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registryFactory[name]; dup {
		panic(fmt.Sprintf("adapter.Register: duplicate name %q", name))
	}
	registryFactory[name] = f
}

// RegisteredModules returns a snapshot of every adapter Factory
// currently registered. The map is a copy — callers may mutate it
// freely without affecting the registry.
//
// Order is not specified; callers that need deterministic ordering
// should sort the keys themselves.
func RegisteredModules() map[string]Factory {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make(map[string]Factory, len(registryFactory))
	for k, v := range registryFactory {
		out[k] = v
	}
	return out
}

// ResetRegistry clears the package-level registry. Tests use it to
// avoid cross-test contamination when they call Register in a fixture.
// Not part of the public production surface.
func ResetRegistry() {
	registryMu.Lock()
	defer registryMu.Unlock()
	registryFactory = map[string]Factory{}
}
