package framework

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// FailNowParams describes a synchronous failed-terminal that an
// adapter's Module.Handle decides to emit before any wire round-trip
// (bad payload, state gate, push error). See FailNow.
type FailNowParams struct {
	// RequestID is the original request envelope id (the correlation
	// key the framework's CorrelationTracker uses). Required.
	RequestID message.ID

	// TerminalReason is the closed-set proto-layer0 §2.6 failure
	// reason stamped onto payload.reason and propagated through the
	// F3 ErrorPolicy hook. Leave empty when the failure is pre-transit
	// (payload decode etc.) and no F3 hook firing is needed; FailNow
	// then defaults the response.reason to `receiver_internal_error`
	// and skips the policy hook.
	TerminalReason message.TerminalFailureReason

	// ErrorCode is an adapter / business-defined diagnostic code that
	// rides on payload.error_code (free-text per impl-vocabulary §3.1).
	// Recommended convention: `<adapter>_<reason>` — e.g. `device_offline`
	// / `device_token_expired` / `payload_decode_failed`. Required.
	ErrorCode string

	// Detail is a human-readable diagnostic string (informative, not
	// closed-set). Optional. Stored in payload.detail.
	Detail string
}

// FailNow is the canonical synchronous-failure path every adapter
// Module shares. It emits a failed terminal Respond + walks back the
// framework bookkeeping (correlation tracker + ErrorPolicy timer) so no
// zombie pending entry remains.
//
// Use this from Module.Handle whenever the adapter can decide the
// terminal before any wire round-trip. The post-callback / timeout
// path keeps using ModuleContext.Respond directly because it has the
// real domain payload.
//
// Spec ref: proto-layer1 §3.3 O3 Terminal Closure (voluntary closure
// path) + proto-layer0 §2.6 reason closed set.
func FailNow(ctx context.Context, mctx *adapter.ModuleContext, p FailNowParams) error {
	if mctx == nil {
		return fmt.Errorf("framework.FailNow: ModuleContext is nil (error_code=%s)", p.ErrorCode)
	}
	if p.RequestID == "" {
		return fmt.Errorf("framework.FailNow: RequestID empty (error_code=%s)", p.ErrorCode)
	}
	if p.ErrorCode == "" {
		return fmt.Errorf("framework.FailNow: ErrorCode empty for request %s", p.RequestID)
	}

	fields := map[string]any{"error_code": p.ErrorCode}
	if p.Detail != "" {
		fields["detail"] = p.Detail
	}
	payload, err := json.Marshal(fields)
	if err != nil {
		// Marshal of map[string]string can't realistically fail, but
		// fail-closed anyway — a synthesized empty object keeps the
		// terminal valid for the closure-policy guard.
		payload = []byte(`{}`)
	}

	respondReason := message.TerminalReceiverInternalError
	if p.TerminalReason != "" {
		respondReason = p.TerminalReason
	}

	corKey := adapter.CorrelationKey(p.RequestID)

	// Walk back framework bookkeeping FIRST so the F3 default-timeout
	// timer does not also fire later AND orphan correlation entries
	// don't linger. Both ops are idempotent.
	//
	// Do NOT call ErrorPolicy.OnExternalError here: that helper itself
	// emits a terminal response via the framework fallback path with the
	// reason payload only (no error_code), which deterministic-id dedup
	// would then prefer over the adapter-specific Respond below — the
	// caller would lose the error_code + detail FailNow was meant to
	// surface. FailNow owns the terminal write end-to-end; F3 observability
	// for pre-transit / state-gate failures is captured by the adapter's
	// own logging / metrics path, not by emitting a duplicate response.
	if mctx.ErrorPolicy != nil {
		_ = mctx.ErrorPolicy.CancelTimer(ctx, corKey)
	}
	if mctx.Correlation != nil {
		_ = mctx.Correlation.MarkExpired(ctx, corKey)
	}

	_, err = mctx.Respond(ctx, corKey, payload, adapter.RespondOptions{
		Status: "failed",
		Reason: string(respondReason),
	})
	return err
}
