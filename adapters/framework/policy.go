package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coagent-ai/coagent/kernel/adapter"
	"github.com/coagent-ai/coagent/kernel/message"
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
// the timer fires the policy emits a terminal_failure response via the
// adapter's RespondFunc with reason=adapter_default_timeout.
type timerPolicy struct {
	adapterName string
	respond     adapter.RespondFunc
	correlation *memoryCorrelationTracker
	logger      Logger
	metrics     Metrics
	clock       func() time.Time

	mu     sync.Mutex
	timers map[string]*time.Timer
}

func newTimerPolicy(
	adapterName string,
	correlation *memoryCorrelationTracker,
	logger Logger,
	metrics Metrics,
	clock func() time.Time,
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
		correlation: correlation,
		logger:      logger,
		metrics:     metrics,
		clock:       clock,
		timers:      map[string]*time.Timer{},
	}
}

// bindRespond wires the adapter-specific RespondFunc after the framework
// has built it. Separated from constructor so newTimerPolicy can be
// called before the Respond closure is composed.
func (p *timerPolicy) bindRespond(respond adapter.RespondFunc) {
	p.respond = respond
}

// RegisterTimer arms a timer that fires at deadline. Repeated calls for
// the same requestID reset the timer (the new deadline wins). Returns
// an error when the policy has been Shutdown.
func (p *timerPolicy) RegisterTimer(_ context.Context, requestID string, deadline time.Time) error {
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
func (p *timerPolicy) CancelTimer(_ context.Context, requestID string) error {
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
	if p.respond == nil {
		return errors.New("framework: policy.respond not bound")
	}
	payload, err := marshalTerminalPayload(string(reason), detail)
	if err != nil {
		return err
	}
	res, err := p.respond(ctx, requestID, payload, adapter.RespondOptions{
		Status: "failed",
		Reason: string(reason),
	})
	if err != nil {
		return err
	}
	p.logger.Warn("framework.policy.external_error",
		"adapter", p.adapterName,
		"request_id", requestID,
		"reason", string(reason),
		"message_id", res.MessageID,
	)
	p.metrics.IncCounter("adapter.policy.external_error",
		"adapter", p.adapterName, "reason", string(reason))
	// Cancel the local timer to prevent double-firing.
	_ = p.CancelTimer(ctx, requestID)
	return nil
}

// fire is the AfterFunc callback. It emits the adapter_default_timeout
// terminal via Respond. We swallow most errors so a timer panic cannot
// poison the rest of the daemon — anything non-trivial is logged.
func (p *timerPolicy) fire(requestID string) {
	p.mu.Lock()
	delete(p.timers, requestID)
	p.mu.Unlock()
	if p.respond == nil {
		p.logger.Error("framework.policy.timer_fire.no_respond",
			"adapter", p.adapterName, "request_id", requestID)
		return
	}
	// Use background ctx — the per-request ctx is gone by the time the
	// timer fires; the harness Write only needs the channel scope which
	// is bound into the closure.
	ctx := context.Background()
	payload, err := marshalTerminalPayload(string(message.TerminalAdapterDefaultTimeout), "")
	if err != nil {
		p.logger.Error("framework.policy.timer_fire.marshal",
			"adapter", p.adapterName, "request_id", requestID, "err", err.Error())
		return
	}
	res, err := p.respond(ctx, requestID, payload, adapter.RespondOptions{
		Status: "failed",
		Reason: string(message.TerminalAdapterDefaultTimeout),
	})
	if err != nil {
		p.logger.Error("framework.policy.timer_fire.respond",
			"adapter", p.adapterName, "request_id", requestID, "err", err.Error())
		return
	}
	p.logger.Info("framework.policy.timer_fired",
		"adapter", p.adapterName,
		"request_id", requestID,
		"message_id", res.MessageID,
		"deduped", res.Deduped)
	p.metrics.IncCounter("adapter.policy.timer_fired",
		"adapter", p.adapterName)
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
