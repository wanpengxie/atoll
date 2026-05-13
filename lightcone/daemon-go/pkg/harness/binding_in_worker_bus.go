package harness

import (
	"context"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// WriteResult is the discriminated-union result type for the in_worker_bus
// binding per L2 §3.6.2:
//
//	{ ok: true,  value: { id, correlation_id, kind } }
//	{ ok: false, error: { reason, detail, message_id_if_partial? } }
//
// Bindings hand callers a WriteResult instead of (*Result, error) so the
// happy path / reject path live in symmetric branches — easier for go-kimi
// workers to drive the harness without try/recover boilerplate. Real
// infrastructure errors (sql / ctx cancel / driver bugs) still surface
// via the second return value (an `error` that is NOT a RejectError),
// matching how the daemon_rpc HTTP binding distinguishes 5xx from 4xx.
type WriteResult struct {
	OK    bool
	Value *Result
	Error *RejectError
}

// IsReject reports whether the call rejected via the L1 §10.3.1 closed
// reject reason set. Callers that need the reject reason should switch
// on `WriteResult.Error.Reason`.
func (r WriteResult) IsReject() bool { return !r.OK && r.Error != nil }

// InWorkerBus is the in-process binding (L2 §3.6.2). It delegates to the
// shared `Write` body and re-wraps the result so callers get a
// `WriteResult` instead of a (*Result, error) pair — matching the spec
// `{ok, value | error}` shape exactly.
//
// Non-reject errors (sql infrastructure, ctx done, etc.) are returned
// via the second return value so the binding stays distinguishable
// from a harness reject. WriteResult is meaningful only on a nil error.
func InWorkerBus(
	ctx context.Context,
	deps Deps,
	env *v4types.Envelope,
	callerCtx CallerCtx,
) (WriteResult, error) {
	result, err := Write(ctx, deps, env, callerCtx)
	if err != nil {
		var rerr *RejectError
		if asReject(err, &rerr) {
			return WriteResult{OK: false, Error: rerr}, nil
		}
		return WriteResult{}, err
	}
	return WriteResult{OK: true, Value: result}, nil
}

// asReject is a small wrapper around errors.As that keeps the call site
// readable. Returns true and populates *out when err is a *RejectError.
func asReject(err error, out **RejectError) bool {
	if err == nil {
		return false
	}
	if r, ok := err.(*RejectError); ok {
		*out = r
		return true
	}
	return false
}
