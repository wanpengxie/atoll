package framework

import (
	"context"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/adapter/futurereg"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// responseRouter is the Manager-level single center for the framework
// sync/async mechanism (§3.1). It separates two concerns that an earlier
// per-module correlation tracker conflated:
//
//   - caller side (futures): actor A waits on a response via Await / Watch.
//     Manager-level, shared across modules.
//   - receiver side (receiverOwner): reqID → the module B that accepted the
//     request (its correlation + F3 timer live there). Used ONLY at final
//     time to locate the right correlation to complete + the timer to cancel.
//
// The two indexes are deliberately disjoint (M1): A's waiting lives only in
// futures, B's closure lives only in receiverOwner. The same reqID may appear
// in both (A waits + B accepts), but the semantics are independent.
type responseRouter struct {
	futures *futurereg.FutureRegistry
	logger  Logger

	mu            sync.Mutex
	receiverOwner map[message.ID]*boundModule
}

func newResponseRouter(logger Logger) *responseRouter {
	if logger == nil {
		logger = NoopLogger{}
	}
	return &responseRouter{
		futures:       futurereg.New(),
		logger:        logger,
		receiverOwner: map[message.ID]*boundModule{},
	}
}

// trackReceiver records that module b accepted the request reqID, so a later
// final can locate b's correlation + timer. Written at reservePendingRequest
// (same point/lock as correlation reserve) and rebuilt by BootRecoverTimers.
func (r *responseRouter) trackReceiver(reqID message.ID, b *boundModule) {
	r.mu.Lock()
	r.receiverOwner[reqID] = b
	r.mu.Unlock()
}

func (r *responseRouter) receiverOwnerOf(reqID message.ID) *boundModule {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.receiverOwner[reqID]
}

func (r *responseRouter) forgetReceiver(reqID message.ID) {
	r.mu.Lock()
	delete(r.receiverOwner, reqID)
	r.mu.Unlock()
}

// ObserveResponse is the single feed-in point (§3.3) for every response
// envelope that has been accepted by the harness. It is invoked from the
// shared terminal/response write helper (runRespondWithSender) AND from the
// external-response path AFTER Chain.Write accepts — this covers Respond /
// Fail / Provisional / Resolve and the F3 fallback (the fifth exit), so no
// response slips past.
//
// Algorithm (§3.3):
//  1. Feed the response into the caller-side futures registry (Watch sees
//     all, Await only final). Deliver returns the atomic Disposition (M2).
//  2. On a FINAL, locate the receiver-owner module B via receiverOwner and
//     run lifecycle exactly once — MarkDone(correlation) + CancelTimer(F3) —
//     then drop the receiverOwner entry. This is the single lifecycle center;
//     respond.go no longer does this itself.
func (r *responseRouter) ObserveResponse(ctx context.Context, env *message.Envelope) {
	if env == nil || env.Kind != message.KindResponse || env.ParentID == "" {
		return
	}
	status := parseResponseStatus(env.Payload)
	final := message.IsFinalStatus(status)

	// Caller side: deliver to any waiter. Disposition is consumed by the
	// transport (in-daemon embedded callers do not re-surface a final as a
	// trigger; cross-process transports — kimi worker — do, via their own
	// futurereg instance, not this one).
	_ = r.futures.Deliver(env)

	if !final {
		return
	}

	pid := env.ParentID
	b := r.receiverOwnerOf(pid)
	if b == nil {
		return
	}
	requestID := adapter.CorrelationKey(pid)
	if err := b.correlation.MarkDone(ctx, requestID); err != nil {
		r.logger.Warn("framework.router.mark_done.error",
			"adapter", b.declaration.Name,
			"request_id", requestID, "err", err.Error())
	}
	if b.policy != nil {
		_ = b.policy.CancelTimer(ctx, requestID)
	}
	r.forgetReceiver(pid)
}

// parseResponseStatus extracts payload.status from a response envelope,
// returning "" on any parse trouble. Mirrors extractPayloadStatus but never
// errors (router observation must not block the write path).
func parseResponseStatus(raw []byte) string {
	s, err := extractPayloadStatus(raw)
	if err != nil {
		return ""
	}
	return s
}
