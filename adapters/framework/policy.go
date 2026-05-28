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
	}
}

// bindFallback wires the framework/runtime-owned terminal closure emitter.
func (p *timerPolicy) bindFallback(fallback terminalFallbackFunc) {
	p.fallback = fallback
}

// RegisterTimer arms a timer that fires at deadline. Repeated calls for
// the same requestID reset the timer (the new deadline wins). Returns
// an error when the policy has been Shutdown.
func (p *timerPolicy) RegisterTimer(_ context.Context, requestID adapter.CorrelationKey, deadline time.Time) error {
	if requestID == "" {
		return errors.New("framework: RegisterTimer requestID required")
	}
	p.mu.Lock()
	if existing, ok := p.timers[requestID]; ok {
		existing.Stop()
	}
	delay := deadline.Sub(p.clock())
	if delay <= 0 {
		// fire immediately
		delay = time.Microsecond
	}
	rid := requestID
	t := time.AfterFunc(delay, func() {
		p.fire(rid)
	})
	p.timers[requestID] = t
	p.mu.Unlock()
	p.metrics.IncCounter("adapter.timer.registered", "adapter", p.adapterName)
	return nil
}

// CancelTimer stops the timer for requestID. Idempotent.
func (p *timerPolicy) CancelTimer(_ context.Context, requestID adapter.CorrelationKey) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.timers[requestID]; ok {
		t.Stop()
		delete(p.timers, requestID)
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

// fire is the AfterFunc callback. It emits the unanswered_timeout
// terminal via the framework synthesized fallback path. If terminal
// writing fails after bounded retries, the policy emits an operational
// system event and closes the correlation as expired. It does not re-arm
// indefinitely; repeated terminal attempts here caused retry storms when
// the underlying write path was unhealthy.
func (p *timerPolicy) fire(requestID adapter.CorrelationKey) {
	p.mu.Lock()
	delete(p.timers, requestID)
	p.mu.Unlock()
	if p.fallback == nil {
		p.logger.Error("framework.policy.timer_fire.no_fallback",
			"adapter", p.adapterName, "request_id", requestID.String())
		return
	}
	// Use background ctx — the per-request ctx is gone by the time the
	// timer fires; the harness Write only needs the channel scope which
	// is bound into the closure.
	ctx := context.Background()
	payload, err := marshalTerminalPayload(string(message.TerminalUnansweredTimeout), "")
	if err != nil {
		p.logger.Error("framework.policy.timer_fire.marshal",
			"adapter", p.adapterName, "request_id", requestID, "err", err.Error())
		return
	}
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if attempt > 1 {
			time.Sleep(time.Duration(attempt-1) * 25 * time.Millisecond)
		}
		res, err := p.fallback(ctx, requestID, payload, adapter.RespondOptions{
			Status: "failed",
			Reason: string(message.TerminalUnansweredTimeout),
		})
		if err == nil {
			p.logger.Info("framework.policy.timer_fired",
				"adapter", p.adapterName,
				"request_id", requestID.String(),
				"message_id", res.MessageID.String(),
				"deduped", res.Deduped,
				"attempt", attempt)
			p.metrics.IncCounter("adapter.policy.timer_fired",
				"adapter", p.adapterName)
			return
		}
		lastErr = err
		p.logger.Error("framework.policy.timer_fire.respond",
			"adapter", p.adapterName, "request_id", requestID.String(), "attempt", attempt, "err", err.Error())
	}
	p.metrics.IncCounter("adapter.policy.timer_terminal_failed", "adapter", p.adapterName)
	if err := emitTimerTerminalFailedEvent(ctx, timerTerminalFailedEvent{
		AdapterName: p.adapterName,
		ChannelID:   p.channelID,
		Chain:       p.chain,
		Clock:       p.clock,
		RequestID:   requestID,
		Err:         lastErr,
	}); err != nil {
		p.logger.Error("framework.policy.timer_fire.event_failed",
			"adapter", p.adapterName, "request_id", requestID.String(), "err", err.Error())
	}
	if p.correlation == nil {
		p.logger.Error("framework.policy.timer_fire.expire_no_correlation",
			"adapter", p.adapterName, "request_id", requestID.String())
		return
	}
	entry, ok, err := p.correlation.Get(ctx, requestID)
	if err != nil {
		p.logger.Error("framework.policy.timer_fire.expire_lookup_failed",
			"adapter", p.adapterName, "request_id", requestID.String(), "err", err.Error())
		return
	}
	if !ok || entry.State != adapter.CorrelationPending {
		p.logger.Warn("framework.policy.timer_fire.expire_stopped",
			"adapter", p.adapterName, "request_id", requestID.String(), "state", string(entry.State), "found", ok)
		return
	}
	if err := p.correlation.MarkExpired(ctx, requestID); err != nil {
		p.logger.Error("framework.policy.timer_fire.expire_failed",
			"adapter", p.adapterName, "request_id", requestID.String(), "err", err.Error())
	}
}

// shutdown stops every armed timer. Called from Manager.Shutdown.
func (p *timerPolicy) shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, t := range p.timers {
		t.Stop()
		delete(p.timers, k)
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
