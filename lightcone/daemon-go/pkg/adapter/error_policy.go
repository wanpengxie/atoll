package adapter

// error_policy.go implements the M1.3 subset of L2 §8.3 — Timeout +
// FailTerminal. The Should-tier helpers (retry / logSystemEvent)
// remain on the interface but are not exposed yet; M1.x picks them up
// when adapter code starts asking.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ErrorPolicy is the F3 surface adapters call from Handle to install a
// per-request timeout (Ad-2 enforce) and from anywhere to emit a
// failed terminal short of the normal Respond path.
//
// Method semantics (mirrors L2 §8.3):
//
//   - Timeout      Registers a one-shot timer for requestID; on fire
//     the framework emits a failed terminal carrying
//     reason. Repeated Timeout calls for the same
//     requestID stop the previous timer + register a
//     fresh one (lets adapters extend deadlines on
//     external retry).
//   - FailTerminal Emit a failed terminal NOW (no timer). Detail
//     optionally seeds payload top-level keys (e.g.
//     upstream HTTP status). Returns the RespondResult
//     from the underlying harness write so callers can
//     observe the dedupe flag.
type ErrorPolicy interface {
	Timeout(requestID string, afterMs int64, reason string) error
	FailTerminal(ctx context.Context, requestID, reason string, detail map[string]any) (RespondResult, error)
}

// timerPolicy implements ErrorPolicy using time.AfterFunc + an
// internal map. The map is guarded by a mutex; CancelTimer is invoked
// by Respond after a successful write so the registered timer does
// not later re-emit a duplicate (which would still dedupe at the
// harness layer thanks to deterministic ids, but is noisy in logs).
type timerPolicy struct {
	adapterName string
	respond     RespondFn
	logger      Logger
	clock       func() int64

	mu     sync.Mutex
	timers map[string]*time.Timer
	// closed reports whether Shutdown was already called; further
	// Timeout calls become a no-op so we don't reattach timers after
	// the Manager is winding down.
	closed bool
}

// newTimerPolicy constructs an ErrorPolicy bound to adapterName +
// respond. logger defaults to noopLogger.
func newTimerPolicy(adapterName string, respond RespondFn, clock func() int64, logger Logger) *timerPolicy {
	if logger == nil {
		logger = noopLogger{}
	}
	return &timerPolicy{
		adapterName: adapterName,
		respond:     respond,
		logger:      logger,
		clock:       clock,
		timers:      map[string]*time.Timer{},
	}
}

// Timeout (re)registers a timer for requestID. After afterMs the
// framework calls FailTerminal(requestID, reason, nil) on a
// background context.
//
// Returns nil immediately after registering the timer. Subsequent
// Timeout for the same requestID stops the previous timer and
// re-arms — adapters that learn the external API is slower than
// expected can extend the deadline without coordinating cancellation.
func (p *timerPolicy) Timeout(requestID string, afterMs int64, reason string) error {
	if strings.TrimSpace(requestID) == "" {
		return errors.New("adapter: Timeout requestID is required")
	}
	if afterMs <= 0 {
		return fmt.Errorf("adapter: Timeout afterMs must be > 0, got %d", afterMs)
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("adapter: ErrorPolicy is shut down")
	}
	if existing, ok := p.timers[requestID]; ok {
		existing.Stop()
	}
	timer := time.AfterFunc(time.Duration(afterMs)*time.Millisecond, func() {
		p.handleTimerFire(requestID, reason)
	})
	p.timers[requestID] = timer
	p.mu.Unlock()
	return nil
}

// FailTerminal emits a failed terminal via the Respond seam. Detail
// flows into RespondOptions.Detail so adapters can add provenance
// fields without re-marshalling JSON.
func (p *timerPolicy) FailTerminal(
	ctx context.Context,
	requestID, reason string,
	detail map[string]any,
) (RespondResult, error) {
	if strings.TrimSpace(requestID) == "" {
		return RespondResult{}, errors.New("adapter: FailTerminal requestID is required")
	}
	return p.respond(ctx, requestID, json.RawMessage(`{}`), RespondOptions{
		Status: StatusFailed,
		Reason: reason,
		Detail: detail,
	})
}

// cancelTimer is the framework-internal hook Respond calls after a
// successful write so the registered timer does not fire later. Safe
// to call when no timer is registered.
func (p *timerPolicy) cancelTimer(requestID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.timers[requestID]; ok {
		t.Stop()
		delete(p.timers, requestID)
	}
}

// pendingTimerCount returns the number of currently registered
// timers. Exposed for tests + RunGC observability.
func (p *timerPolicy) pendingTimerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.timers)
}

// shutdown stops every timer + flips the closed flag so subsequent
// Timeout calls reject. Idempotent.
func (p *timerPolicy) shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	for id, t := range p.timers {
		t.Stop()
		delete(p.timers, id)
	}
}

// handleTimerFire is the goroutine body time.AfterFunc invokes when a
// timer matures. It clears the map entry, then emits the failed
// terminal. Errors are logged but not propagated — the harness write
// already shapes them and the timer goroutine has nowhere to return.
func (p *timerPolicy) handleTimerFire(requestID, reason string) {
	p.mu.Lock()
	delete(p.timers, requestID)
	closed := p.closed
	p.mu.Unlock()
	if closed {
		// Shutdown raced the firing timer; leave the rest as a no-op.
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := p.respond(ctx, requestID, json.RawMessage(`{}`), RespondOptions{
		Status: StatusFailed,
		Reason: reason,
	})
	if err != nil {
		p.logger.Warn("adapter.timeout.fire.error",
			"adapter", p.adapterName,
			"request_id", requestID,
			"reason", reason,
			"err", err.Error(),
		)
		return
	}
	p.logger.Info("adapter.timeout.fired",
		"adapter", p.adapterName,
		"request_id", requestID,
		"reason", reason,
		"response_id", res.ID,
		"dedupe", res.Dedupe,
	)
}
