package harness

import (
	"context"
	"fmt"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// CallerCtx is the unforgeable identity + context the binding layer
// (daemon_rpc HTTP / in_worker_bus in-process) hands the shared Write
// body. The harness MUST trust callerCtx as authoritative — callers
// cannot fabricate or override these values without first passing the
// binding's auth layer.
//
// Field semantics (mirrors L1 §10.2.1 pseudocode `caller_ctx`):
//
//   - Authenticated:       Step 1 gate. Bindings set this true after their
//     transport-specific auth check. Step 1 rejects
//     `auth_failed` when false.
//   - ActorID:             The authenticated identity from the token /
//     binding (e.g. bearer token claims). Step 3
//     enforces `envelope.sender.id == ActorID`.
//   - DeclaredSenderKind:  The sender.kind value the caller embedded in the
//     wire envelope. The harness compares it to the
//     actor_registry truth and rejects on mismatch
//     (L1 §10.2.1 sender_kind_mismatch). Empty string
//     means caller omitted the field — no mismatch
//     check, registry value is force-written.
//   - FencingToken:        Optional fencing token for the worker write
//     path. Present iff caller is a worker; harness
//     verifies worker_locks.is_active.
//   - Trigger:             Optional trigger context. When non-nil, Step 0
//     normalize uses Trigger.CorrelationID as the
//     correlation_id fallback per L1 §2.2.1.
//   - ExplicitCorrelationID: Caller-side `--correlation-id new` override.
//     Used by Step 0 normalize as the 2nd fallback.
type CallerCtx struct {
	Authenticated         bool
	ActorID               string
	DeclaredSenderKind    v4types.SenderKind
	FencingToken          int64
	Trigger               *TriggerCtx
	ExplicitCorrelationID string
}

// TriggerCtx is the subset of trigger metadata the harness needs for
// correlation_id propagation (L1 §2.2.1). Bindings populate it from
// trigger gateway state when the caller is responding to a trigger
// dispatch.
type TriggerCtx struct {
	// CorrelationID is the trigger envelope's correlation_id — Step 0
	// normalize uses it as the first fallback when the new envelope
	// omits correlation_id.
	CorrelationID string
}

// Result is the success value returned by Write when the 9-step chain
// passes. Mirrors L1 §10.2.1 return shape `{id, correlation_id, kind}`
// with `Dedupe` set when Step 0.5 / Step 8 catch path returned an existing
// row instead of inserting fresh.
type Result struct {
	// ID is the message id that ended up in the store. For the dedupe
	// path this is the existing row's id (which equals envelope.ID for
	// step 0.5 same-id retry, or the winner's id for terminal_duplicate
	// races — but terminal_duplicate is a RejectError, not a Result).
	ID string

	// CorrelationID is the final correlation_id of the persisted row.
	// May differ from the incoming envelope when Step 0 normalize fills
	// the value, or when dedupe returns an earlier row with its own
	// correlation_id.
	CorrelationID string

	// Kind is the persisted envelope's kind.
	Kind v4types.Kind

	// Dedupe reports whether the success came from the idempotent
	// dedupe path (Step 0.5 or Step 8 catch). Useful for observability
	// — callers do not branch behaviourally on this flag.
	Dedupe bool
}

// RejectError is the structured failure value returned by Write when
// any of Step 1–8 rejects. The triplet (Reason, Detail, MessageIDIfPartial)
// matches the L2 §3.6.1 wire body `{error: {reason, detail,
// message_id_if_partial?}}`.
//
// `DedupeResponseID` is populated only for the `terminal_duplicate`
// reason (L1 §10.2.1 Step 8 "Err(terminal_duplicate,
// dedupe_response_id=...)"). All other reasons leave the field empty.
//
// Bindings unwrap the error and convert it to their transport-specific
// reject form: HTTP status + JSON body for daemon_rpc, Result.Err for
// in_worker_bus.
type RejectError struct {
	Reason              v4types.HarnessRejectReason
	Detail              string
	MessageIDIfPartial  string
	DedupeResponseID    string
	UnderlyingErrorText string // for sql / driver errors; never sent over the wire by daemon_rpc binding
}

// Error implements the standard `error` interface. The format mirrors
// the registry InstallError convention so log + grep workflows stay
// uniform across reason classes.
func (e *RejectError) Error() string {
	base := "harness reject " + string(e.Reason)
	if e.Detail != "" {
		base += ": " + e.Detail
	}
	return base
}

// Is supports `errors.Is(err, &harness.RejectError{Reason: ...})` style
// checks: two RejectError values match when their Reason is equal.
func (e *RejectError) Is(target error) bool {
	if other, ok := target.(*RejectError); ok {
		return other.Reason == e.Reason
	}
	return false
}

// rejectf builds a RejectError with a formatted detail string. Internal
// helper so reject sites stay one-liner readable.
func rejectf(reason v4types.HarnessRejectReason, format string, args ...any) *RejectError {
	return &RejectError{
		Reason: reason,
		Detail: sprintf(format, args...),
	}
}

// ----------------------------------------------------------------------------
// Dependency interfaces — bindings + tests supply real / mock implementations.
// ----------------------------------------------------------------------------

// Store is the message + transaction surface the harness needs from the
// channel-local sqlite layer. Real implementations wrap *sql.DB +
// internal/store helpers; test mocks return canned values.
//
// All methods take context.Context so deadlines / cancellation propagate
// from the binding layer.
type Store interface {
	// FindByID looks up a message row by envelope id. Returns (nil, nil)
	// when no row exists, the row + nil error otherwise.
	FindByID(ctx context.Context, id string) (*v4types.Envelope, error)

	// FindParent looks up a message row by id for Step 8 parent-exists
	// check. Returns (nil, nil) when no row exists.
	FindParent(ctx context.Context, id string) (*v4types.Envelope, error)

	// FindTerminalResponse returns the existing terminal response row
	// pointing at the given parent_id, or (nil, nil) when none exists.
	// Used by Step 8 inside the IMMEDIATE tx.
	FindTerminalResponse(ctx context.Context, parentID string) (*v4types.Envelope, error)

	// InsertMessage writes the envelope row. The implementation MUST
	// surface UNIQUE constraint violations as ErrUniqueViolation so
	// callers can fall back to the dedupe / terminal_duplicate paths
	// described in L1 §10.2.1.
	InsertMessage(ctx context.Context, env *v4types.Envelope, tsReceived int64) error

	// WithTerminalTx runs `body` inside a single IMMEDIATE transaction
	// suitable for Step 8 (read-then-insert atomicity). The body
	// receives a Store view bound to the tx so FindTerminalResponse /
	// InsertMessage run on the same connection.
	WithTerminalTx(ctx context.Context, body func(txStore Store) error) error
}

// ActorLookup is the subset of internal/registry/actor.go the harness
// needs (Step 3 + Step 5 audience check). Real implementations call
// registry.Get / registry.ListActive; test mocks return canned data.
type ActorLookup interface {
	// Get returns the actor_registry row for actorID regardless of
	// deregistration state. Returns (nil, nil) when the actor does
	// not exist.
	Get(ctx context.Context, actorID string) (*ActorMeta, error)
}

// ActorMeta is the in-memory representation of one actor_registry row,
// mirroring registry.ActorMeta with stable field names so the harness
// does not import internal/registry directly (avoids cycles when
// registry itself starts using harness types in the future).
type ActorMeta struct {
	ActorID        string
	Kind           v4types.SenderKind
	Binding        string // "" / "daemon_rpc" / "in_worker_bus"
	DeregisteredAt *int64
}

// TypeLookup is the subset of internal/registry/type.go the harness
// needs. Real implementations cache compiled schemas (the registry
// package already compiles each schema during Install). Tests can hand
// in a fixture map.
type TypeLookup interface {
	// Get returns the TypeInfo for the named business type, or
	// (nil, false) when no such row exists in the channel-local
	// type_registry.
	Get(t string) (*TypeInfo, bool)
}

// TypeInfo is the harness-facing view of one type_registry row. Field
// names are aligned with L1 §1.3 / L2 §1.4.2.
type TypeInfo struct {
	Type               string
	AllowedKinds       []v4types.Kind
	HandlerBinding     string
	HandlerActorID     string
	TerminalConvention string // "" / "payload_status" / "single-response"
	// Schemas holds the compiled JSON Schema per kind. nil entry → no
	// schema constraint declared for that kind (caller treats as
	// "everything passes").
	Schemas map[v4types.Kind]*jsonschema.Schema
}

// WorkerLockLookup is the subset of internal/supervisor/worker_locks.go
// the harness needs for Step 3 fencing. Real implementations call
// supervisor.Get + compare FencingToken; test mocks return canned data.
type WorkerLockLookup interface {
	// IsActive reports whether (agentID, fencingToken) currently
	// matches the active worker_locks row. Returns false when the lock
	// is missing, the fencing_token differs, or the lease has expired.
	IsActive(ctx context.Context, agentID string, fencingToken int64) (bool, error)
}

// Dispatcher is the post-write trigger gateway + view sync fan-out.
// M1.3 supplies a noop implementation; T8 (trigger gateway) and T15
// (view sync) replace it later. Dispatch errors are logged inside the
// implementation — the harness never rolls back a successful insert
// because dispatch failed (L1 §10.2.1 "dispatch ... 在事务外").
type Dispatcher interface {
	Dispatch(ctx context.Context, env *v4types.Envelope) error
}

// NoopDispatcher is the M1.3 baseline implementation — `Dispatch` is a
// no-op returning nil. Bindings inject it until T8 / T15 ship their
// real fan-out.
type NoopDispatcher struct{}

// Dispatch satisfies the Dispatcher interface as a no-op.
func (NoopDispatcher) Dispatch(_ context.Context, _ *v4types.Envelope) error { return nil }

// Clock returns "now" in milliseconds (matches the L0 §2.1 unit). The
// harness uses it to stamp ts_received on inserts so tests inject a
// deterministic clock.
type Clock func() int64

// ErrUniqueViolation is the sentinel Store.InsertMessage MUST wrap when
// the underlying driver surfaces a UNIQUE constraint violation (either
// on messages.id or on the partial UNIQUE INDEX
// ux_terminal_response_per_request). Callers use `errors.Is` to detect
// it.
var ErrUniqueViolation = sentinelError("harness: store unique constraint violation")

// sentinelError is a tiny stand-in for github.com/pkg/errors-style
// sentinels — using a typed string keeps the package dependency-free.
type sentinelError string

func (e sentinelError) Error() string { return string(e) }

// sprintf wraps fmt.Sprintf so rejectf and other detail-builders share a
// single formatting call site (easier to swap formatting later — e.g.
// adding structured fields).
func sprintf(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}
