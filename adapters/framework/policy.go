package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// terminalPayload is the response payload the framework emits when a
// terminal failure must be synthesised (timer expiry, OnExternalError).
type terminalPayload struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
	Detail string `json:"detail,omitempty"`
}

// ScheduleToCloseCeiling is the zombie-guard hard cap (Temporal
// ScheduleToClose analogue, temporal-termination-consistency.md §6.3).
// A provisional heartbeat re-arms the F3 timer to keep a live receiver
// alive past the static max_pending_ms, but heartbeats MUST NOT push the
// deadline past `request-creation-time + ScheduleToCloseCeiling`. A
// receiver that keeps heart-beating forever (or a stuck one whose host
// keeps emitting provisionals) still gets force-failed at the ceiling so
// pending entries cannot leak indefinitely.
//
// It is a package-level var (not a const) so it stays a tunable knob; v1
// keeps a deliberately generous 30m default since coagent is pre-launch
// with one slow side-effect device (xhs long publish) and no SLA. A
// future config seam can override it per channel / per type.
var ScheduleToCloseCeiling = 30 * time.Minute

// timerPolicy implements kernel/adapter.ErrorPolicy on top of a
// time.AfterFunc per pending request. The Manager holds one policy per
// bound module and tears it down on Shutdown.
//
// On RegisterTimer the policy stores the deadline + arms a timer. When
// the timer fires the policy emits a terminal_failure response through the
// framework synthesized fallback path with reason=unanswered_timeout.
type timerPolicy struct {
	adapterName string
	channelID   channel.ID
	fallback    terminalFallbackFunc
	chain       harness.Chain
	correlation *memoryCorrelationTracker
	logger      Logger
	metrics     Metrics
	clock       func() time.Time

	mu     sync.Mutex
	timers map[adapter.CorrelationKey]*time.Timer
	// createdAt records the first-arm wall clock per pending request. It
	// anchors the ScheduleToClose hard ceiling so provisional heartbeats
	// (ReArm) can extend the F3 deadline but never past
	// createdAt + ScheduleToCloseCeiling (zombie guard, §6.3).
	createdAt map[adapter.CorrelationKey]time.Time
}

func newTimerPolicy(
	adapterName string,
	correlation *memoryCorrelationTracker,
	logger Logger,
	metrics Metrics,
	clock func() time.Time,
	channelID channel.ID,
	chain harness.Chain,
) *timerPolicy {
	if logger == nil {
		logger = NoopLogger{}
	}
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	if clock == nil {
		clock = time.Now
	}
	return &timerPolicy{
		adapterName: adapterName,
		channelID:   channelID,
		correlation: correlation,
		chain:       chain,
		logger:      logger,
		metrics:     metrics,
		clock:       clock,
		timers:      map[adapter.CorrelationKey]*time.Timer{},
		createdAt:   map[adapter.CorrelationKey]time.Time{},
	}
}

// bindFallback wires the framework/runtime-owned terminal closure emitter.
func (p *timerPolicy) bindFallback(fallback terminalFallbackFunc) {
	p.fallback = fallback
}

// CancelTimer stops the timer for requestID. Idempotent.
func (p *timerPolicy) CancelTimer(_ context.Context, requestID adapter.CorrelationKey) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.timers[requestID]; ok {
		t.Stop()
		delete(p.timers, requestID)
		delete(p.createdAt, requestID)
		p.metrics.IncCounter("adapter.timer.cancelled", "adapter", p.adapterName)
	}
	return nil
}

// OnExternalError emits a terminal_failure response with the provided
// reason + detail. detail is redacted only if the caller already
// redacted it — the framework does not auto-redact (it does not know
// the secret set).
func (p *timerPolicy) OnExternalError(
	ctx context.Context,
	requestID adapter.CorrelationKey,
	reason message.TerminalFailureReason,
	detail string,
) error {
	if requestID == "" {
		return errors.New("framework: OnExternalError requestID required")
	}
	if p.fallback == nil {
		return errors.New("framework: policy.fallback not bound")
	}
	payload, err := marshalTerminalPayload(string(reason), detail)
	if err != nil {
		return err
	}
	res, err := p.fallback(ctx, requestID, payload, adapter.RespondOptions{
		Status: "failed",
		Reason: string(reason),
	})
	if err != nil {
		return err
	}
	p.logger.Warn("framework.policy.external_error",
		"adapter", p.adapterName,
		"request_id", requestID.String(),
		"reason", string(reason),
		"message_id", res.MessageID.String(),
	)
	p.metrics.IncCounter("adapter.policy.external_error",
		"adapter", p.adapterName, "reason", string(reason))
	// Cancel the local timer to prevent double-firing.
	_ = p.CancelTimer(ctx, requestID)
	return nil
}

// shutdown stops every armed timer. Called from Manager.Shutdown.
func (p *timerPolicy) shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, t := range p.timers {
		t.Stop()
		delete(p.timers, k)
		delete(p.createdAt, k)
	}
}

func marshalTerminalPayload(reason, detail string) (json.RawMessage, error) {
	pl := terminalPayload{Status: "failed", Reason: reason, Detail: detail}
	b, err := json.Marshal(pl)
	if err != nil {
		return nil, fmt.Errorf("framework: marshal terminal payload: %w", err)
	}
	return b, nil
}
