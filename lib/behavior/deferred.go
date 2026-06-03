package behavior

import "errors"

// ErrHandleDeferred — Handle returns it to signal "no terminal yet". The
// adapter answers later (e.g. once its own external/async work completes and it
// self-delivers the result onto its cell, then calls Respond/Fail). Until then
// the pending request stays open, bounded by the caller-scoped closure timeout
// (substrate author #2) or the receiver-death signal (author #3) — never an
// auto-finalize on Handle return.
//
// Sentinel hardening: the adapter identifies it via errors.Is(err,
// ErrHandleDeferred). Adapters MUST NOT wrap it with fmt.Errorf("%w", ...) and
// then return business semantics — that pollutes the discriminator. A
// synchronous hard failure returns a plain error (collapsed to a
// receiver_internal_error terminal); the deferred path returns Deferred() and
// MUST later Respond/Fail.
var ErrHandleDeferred = errors.New("adapter: handle deferred")

// Deferred is the semantic constructor for ErrHandleDeferred, for use as
// `return behavior.Deferred()` from Handle.
func Deferred() error { return ErrHandleDeferred }
