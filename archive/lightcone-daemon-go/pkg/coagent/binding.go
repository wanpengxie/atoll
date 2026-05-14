package coagent

import (
	"context"
	"errors"
	"fmt"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// Binding is the harness write surface the CLI dispatches against.
// Two implementations live in this package:
//
//   - daemon_rpc: HTTP client posting to $DAEMON_URL/api/rpc/message.send
//     (L2 §3.6.1 wire shape). The default for binary-mode calls.
//   - in_worker_bus: direct invocation of pkg/harness.InWorkerBus
//     (L2 §3.6.2 in-process binding). Used when the worker process
//     drives the CLI as a library so it can share the harness Deps
//     bound to the same sqlite handle / WAL.
//
// Subcommand code is binding-agnostic — it builds an envelope, hands
// it to Binding.Send, and lets the binding map success / reject onto
// the transport-specific wire shape. The CLI's three audience reject
// branches (L2 §3.2.2) come back through:
//
//   - request_audience_invalid       → client-side reject (CLI builds
//     a *RejectError before Send)
//   - audience_actor_not_registered  → harness reject via Send
//   - audience_handler_mismatch      → harness reject via Send
//
// Optional capabilities (LookupRequest / ResolveHandlerActorID) let
// the binding answer questions the CLI would otherwise need to make
// out-of-band. Bindings that cannot answer return `ok=false`; the CLI
// then falls back to per-subcommand error reporting (e.g. answer
// requires --type when LookupRequest is unsupported).
type Binding interface {
	// Send drives the harness 9-step chain on env and returns either
	// a *SendResult (success) or an error. On harness reject the
	// error is a *RejectError. On infrastructure failure (HTTP
	// transport / sql / ctx cancel) the error is something else.
	Send(ctx context.Context, env *v4types.Envelope, opts SendOptions) (*SendResult, error)

	// LookupRequest fetches the prior request message identified by
	// requestID so `coagent answer` can sync parent_id / correlation_id /
	// audience / type from the request. Returns (nil, false, nil) when
	// the binding does not have query capability (e.g. a stateless
	// daemon_rpc binding without channel sqlite access). The CLI then
	// requires the caller to pass --type / --audience explicitly.
	LookupRequest(ctx context.Context, requestID string) (*v4types.Envelope, bool, error)

	// ResolveHandlerActorID returns type_registry.handler_actor_id for
	// typeName so `coagent ask` can fill audience when caller omitted
	// --audience. Returns ("", false, nil) when the binding cannot
	// answer or the type is unknown — the CLI then client-side rejects
	// `request_audience_invalid` with a hint.
	ResolveHandlerActorID(ctx context.Context, typeName string) (string, bool, error)
}

// SendOptions carries the per-call binding-side metadata the harness
// CallerCtx needs. The CLI populates this from the parsed flags +
// turn context; the binding forwards it as appropriate (daemon_rpc
// puts it in the request body fields, in_worker_bus passes it
// directly into pkgharness.CallerCtx).
type SendOptions struct {
	// DeclaredSenderKind is the sender.kind the CLI declared on the
	// wire (L2 §3.6.1 declared_sender_kind). Harness Step 3 compares
	// it to actor_registry truth and rejects sender_kind_mismatch
	// when they disagree. Empty string skips the check.
	DeclaredSenderKind v4types.SenderKind

	// FencingToken is the worker fencing token (L1 §1.4.9). Present
	// when the CLI is invoked from within a worker turn — harness
	// Step 3 verifies the lease is still active.
	FencingToken int64

	// TriggerCorrelationID is the trigger envelope's correlation_id
	// (L1 §2.2.1). Harness normalize uses it as the first fallback
	// when the new envelope omits correlation_id.
	TriggerCorrelationID string

	// ExplicitCorrelationID is the caller-side `--correlation-id new`
	// override — the CLI generates a UUID and parks it here so the
	// harness picks it up as the second-tier fallback. When the
	// caller passes `--correlation-id <id>` the value is written
	// directly into env.CorrelationID and this field stays empty.
	ExplicitCorrelationID string
}

// SendResult mirrors the L1 §10.2.1 Result triplet plus the dedupe
// flag so callers / tests can branch on the idempotent path.
type SendResult struct {
	ID            string
	CorrelationID string
	Kind          v4types.Kind
	Dedupe        bool
}

// RejectError surfaces L1 §10.3.1 harness rejects up through Binding.Send.
// Bindings translate their transport-specific reject body into this
// shape so subcommand code stays binding-agnostic:
//
//   - daemon_rpc: HTTP 4xx body { error: { reason, detail,
//     message_id_if_partial?, dedupe_response_id? } } → RejectError
//   - in_worker_bus: pkgharness.RejectError (same fields, no HTTP) →
//     RejectError
//
// The CLI's three audience reject branches (L2 §3.2.2) — including
// the client-side request_audience_invalid — all surface as
// *RejectError so callers can switch on Reason uniformly.
type RejectError struct {
	Reason             v4types.HarnessRejectReason
	Detail             string
	MessageIDIfPartial string
	DedupeResponseID   string
	// HTTPStatus is the status code daemon_rpc returned (0 for
	// in_worker_bus / client-side rejects).
	HTTPStatus int
}

// Error implements the standard `error` interface so RejectError
// composes with errors.As / errors.Is at call sites.
func (e *RejectError) Error() string {
	base := fmt.Sprintf("coagent reject %s", e.Reason)
	if e.Detail != "" {
		base += ": " + e.Detail
	}
	return base
}

// Is supports `errors.Is(err, &RejectError{Reason: ...})` matching:
// two RejectError values are equal when their Reason is equal.
func (e *RejectError) Is(target error) bool {
	if other, ok := target.(*RejectError); ok {
		return other.Reason == e.Reason
	}
	return false
}

// AsReject is a small helper around errors.As keeping reject-classification
// sites one-liner readable.
func AsReject(err error) (*RejectError, bool) {
	if err == nil {
		return nil, false
	}
	var re *RejectError
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}
