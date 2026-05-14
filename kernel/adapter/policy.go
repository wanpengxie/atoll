package adapter

import (
	"context"
	"time"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// ErrorPolicy is the F3 interface (L2 §8.3) — the Ad-2 timeout +
// retry + terminal-emit policy bundle that every adapter relies on.
//
// adapters/framework provides a default timer-based implementation
// (`timerPolicy`); domain adapters can hand a custom policy in via
// ModuleContext when their external system needs different semantics.
type ErrorPolicy interface {
	// RegisterTimer arms the Ad-2 default timeout timer for one
	// in-flight request. When the timer fires, the implementation MUST
	// emit a terminal response with `payload={status:'failed', reason:
	// 'adapter_default_timeout'}` via the framework Respond closure.
	//
	// Returning an error indicates the implementation could not arm the
	// timer — caller treats it as a hard failure (request abandoned).
	RegisterTimer(ctx context.Context, requestID CorrelationKey, deadline time.Time) error

	// CancelTimer is the adapter-driven escape hatch — call it when the
	// adapter has emitted the real Respond and the default-timeout
	// fallback should NOT fire. Idempotent.
	CancelTimer(ctx context.Context, requestID CorrelationKey) error

	// OnExternalError is invoked by the adapter when the external
	// system returns a non-recoverable error before the request
	// completes. The implementation MUST emit a terminal_failure
	// response (reason chosen from message.TerminalFailureReason or a
	// domain-specific payload.reason — adapter decides).
	OnExternalError(
		ctx context.Context,
		requestID CorrelationKey,
		reason message.TerminalFailureReason,
		detail string,
	) error
}
