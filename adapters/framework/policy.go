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

	// onExpire is called when a request is force-expired because the F3
	// fallback could not write a final after bounded retries (the MarkExpired
	// path). It lets the Manager drop the router's receiverOwner entry so the
	// reqID does not leak (F5): the normal close path goes through the router's
	// ObserveResponse on a successful fallback write, but a write that never
	// succeeds never reaches the router, so the owner index must be cleaned
	// here. nil = no hook (e.g. unit tests with no router).
	onExpire func(adapter.CorrelationKey)

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

// bindOnExpire wires the F5 hook: when a request is force-expired (the F3
// fallback exhausted its write retries), this is called so the Manager can
// drop the router's receiverOwner entry and avoid a leak.
func (p *timerPolicy) bindOnExpire(onExpire func(adapter.CorrelationKey)) {
	p.onExpire = onExpire
}

// RegisterTimer arms a timer that fires at deadline. Repeated calls for
// the same requestID reset the timer (the new deadline wins). The first
// arm for a requestID also stamps createdAt, the anchor for the
// ScheduleToClose hard ceiling. Returns an error when the policy has been
// Shutdown.
func (p *timerPolicy) RegisterTimer(_ context.Context, requestID adapter.CorrelationKey, deadline time.Time) error {
	if requestID == "" {
		return errors.New("framework: RegisterTimer requestID required")
	}
	p.mu.Lock()
	if _, ok := p.createdAt[requestID]; !ok {
		p.createdAt[requestID] = p.clock()
	}
	p.armLocked(requestID, deadline)
	p.mu.Unlock()
	p.metrics.IncCounter("adapter.timer.registered", "adapter", p.adapterName)
	return nil
}

// RecoverTimer re-arms the F3 timer for a pending request rehydrated from the
// persistent correlation store after a daemon restart (temporal R1).
//
// It differs from RegisterTimer in two ways:
//
//  1. The ScheduleToClose anchor (createdAt) is seeded from createdAtMs — the
//     persisted EnqueuedAt (original creation time) — NOT p.clock(). On recovery
//     "now" is the restart instant, not request creation; seeding from now would
//     hand every restart a fresh ceiling window so a request could outlive its
//     original creation + ceiling across restarts (zombie escape). Seeding from
//     the original creation keeps the hard ceiling anchored no matter how many
//     restarts happen.
//
//  2. The recovered deadline is the persisted LIVE deadline — RearmedExpiresAt
//     when the request was heart-beating, else the original ExpiresAt — chosen by
//     the caller (recoverTimersForBoundModule). It is re-clamped here defensively
//     to createdAt + ceiling so a corrupt / pre-fix persisted value can never arm
//     past the ceiling.
//
// armLocked still fires (near-)immediately for a genuinely past deadline — that
// is correct: a request whose live deadline truly elapsed while the daemon was
// down (or which never heart-beat past its original ExpiresAt) SHOULD F3-fail.
// The bug this fixes is the opposite case: a still-live, heart-beating request
// whose extended deadline is in the future must NOT be force-failed.
func (p *timerPolicy) RecoverTimer(_ context.Context, requestID adapter.CorrelationKey, deadline time.Time, createdAtMs int64) error {
	if requestID == "" {
		return errors.New("framework: RecoverTimer requestID required")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if createdAtMs > 0 {
		p.createdAt[requestID] = time.UnixMilli(createdAtMs)
	} else if _, ok := p.createdAt[requestID]; !ok {
		// No persisted anchor (legacy entry) — fall back to recovery time.
		p.createdAt[requestID] = p.clock()
	}
	if created, ok := p.createdAt[requestID]; ok {
		if ceiling := created.Add(ScheduleToCloseCeiling); deadline.After(ceiling) {
			deadline = ceiling
		}
	}
	p.armLocked(requestID, deadline)
	p.metrics.IncCounter("adapter.timer.recovered", "adapter", p.adapterName)
	return nil
}

// ReArm extends (re-arms) the F3 timer for an already-armed request in
// response to a provisional liveness heartbeat
// (temporal-termination-consistency.md §6.2). It re-arms ONLY — it does
// NOT touch correlation state / resolve closure (§6.1: provisional MUST
// NOT resolve closure, but DOES re-arm the F3 timer).
//
// Rules:
//   - No-op when no timer is currently armed for requestID. A heartbeat
//     after the request already closed (final emitted / F3 fired) must not
//     resurrect a timer. createdAt only exists while a timer is armed.
//   - The new deadline is clamped to createdAt + ScheduleToCloseCeiling
//     (zombie guard, §6.3): a heartbeat can never push the deadline past
//     the hard ceiling. When the requested deadline already exceeds the
//     ceiling the timer is armed exactly at the ceiling, so the request
//     still F3-fails on schedule and no further heartbeat extends it.
//
// RegisterTimer's "Stop existing before reset" semantics are reused via
// armLocked, so re-arm never leaks a timer.
//
// Persistence (temporal R1): after clamping + arming the in-memory timer, the
// (ceiling-clamped) deadline is mirrored onto the persistent CorrelationEntry's
// RearmedExpiresAt via the correlation tracker. The in-memory timer alone is
// lost on daemon restart and recovery would otherwise re-arm against the stale
// original ExpiresAt and force-fail a still-live receiver at 1µs. The mirror
// targets RearmedExpiresAt ONLY — the immutable ExpiresAt (tamper anchor /
// envelope.expires_at) is never rewritten (append-only log, INVARIANT-12), and
// the persisted value is the same ceiling-clamped deadline so the
// ScheduleToClose ceiling is preserved across restarts.
func (p *timerPolicy) ReArm(ctx context.Context, requestID adapter.CorrelationKey, newDeadline time.Time) error {
	if requestID == "" {
		return errors.New("framework: ReArm requestID required")
	}
	p.mu.Lock()
	if _, armed := p.timers[requestID]; !armed {
		// Closed or never armed — a provisional heartbeat must not
		// resurrect a dead request's timer.
		p.mu.Unlock()
		return nil
	}
	created, ok := p.createdAt[requestID]
	if ok {
		if ceiling := created.Add(ScheduleToCloseCeiling); newDeadline.After(ceiling) {
			newDeadline = ceiling
			p.metrics.IncCounter("adapter.timer.heartbeat_capped", "adapter", p.adapterName)
		}
	}
	p.armLocked(requestID, newDeadline)
	p.metrics.IncCounter("adapter.timer.heartbeat_rearm", "adapter", p.adapterName)
	p.mu.Unlock()

	// Mirror the clamped deadline so a restart recovers the live (extended)
	// deadline, not the stale original. Done outside p.mu: the correlation
	// tracker has its own lock and may hit the StateStore; holding the timer
	// lock across a store write would widen the critical section and risk
	// lock-order coupling with fire().
	if p.correlation != nil {
		if err := p.correlation.ExtendDeadline(ctx, requestID, newDeadline.UnixMilli()); err != nil {
			p.logger.Warn("framework.policy.rearm_persist_failed",
				"adapter", p.adapterName,
				"request_id", requestID.String(),
				"err", err.Error())
		}
	}
	return nil
}

// armLocked installs (or resets) the AfterFunc timer for requestID. The
// caller MUST hold p.mu. Repeated calls Stop the existing timer first so
// only one timer per request is ever live.
func (p *timerPolicy) armLocked(requestID adapter.CorrelationKey, deadline time.Time) {
	if existing, ok := p.timers[requestID]; ok {
		existing.Stop()
	}
	delay := deadline.Sub(p.clock())
	if delay <= 0 {
		// fire immediately
		delay = time.Microsecond
	}
	rid := requestID
	p.timers[requestID] = time.AfterFunc(delay, func() {
		p.fire(rid)
	})
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

// fire is the AfterFunc callback. It emits the unanswered_timeout
// terminal via the framework synthesized fallback path. If terminal
// writing fails after bounded retries, the policy emits an operational
// system event and closes the correlation as expired. It does not re-arm
// indefinitely; repeated terminal attempts here caused retry storms when
// the underlying write path was unhealthy.
func (p *timerPolicy) fire(requestID adapter.CorrelationKey) {
	p.mu.Lock()
	delete(p.timers, requestID)
	delete(p.createdAt, requestID)
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
	// F5: the fallback final was never written, so the router's ObserveResponse
	// will not run and would never drop this reqID from receiverOwner. Force the
	// cleanup here regardless of how the expire bookkeeping below resolves, so
	// the owner index does not leak.
	if p.onExpire != nil {
		p.onExpire(requestID)
	}
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
