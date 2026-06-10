package callkit

import (
	"context"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// Disposition reports how the correlator handled a delivered envelope.
type Disposition int

const (
	// DeliveredToWaiter means the envelope was consumed by an active waiter.
	DeliveredToWaiter Disposition = iota
	// NoActiveWaiter means no waiter was active for this envelope.
	NoActiveWaiter
)

type awaitState int

const (
	awaitNotStarted awaitState = iota
	awaitActive
	awaitDone
)

// RequestCorrelator tracks in-flight request futures and correlates
// inbound response envelopes to their waiters.
type RequestCorrelator struct {
	mu      sync.Mutex
	pending map[message.ID]*pendingReq
}

type pendingReq struct {
	ch           chan *message.Envelope
	expectsAwait bool
	state        awaitState
}

// NewRequestCorrelator returns a ready-to-use correlator.
func NewRequestCorrelator() *RequestCorrelator {
	return &RequestCorrelator{pending: make(map[message.ID]*pendingReq)}
}

// Register records an in-flight request. If expectsAwait is true, a
// final that arrives before Await parks will be buffered for it.
func (rc *RequestCorrelator) Register(id message.ID, expectsAwait bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if _, ok := rc.pending[id]; ok {
		return
	}
	rc.pending[id] = &pendingReq{
		ch:           make(chan *message.Envelope, 1),
		expectsAwait: expectsAwait,
		state:        awaitNotStarted,
	}
}

// Registered reports whether a future exists for id.
func (rc *RequestCorrelator) Registered(id message.ID) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	_, ok := rc.pending[id]
	return ok
}

// Deliver routes an inbound response envelope to its registered waiter.
func (rc *RequestCorrelator) Deliver(env *message.Envelope) Disposition {
	if env == nil {
		return NoActiveWaiter
	}
	final := behavior.IsEnvFinal(env)

	rc.mu.Lock()
	p, ok := rc.pending[env.ParentID]
	if !ok {
		rc.mu.Unlock()
		return NoActiveWaiter
	}

	switch {
	case p.state == awaitActive:
		if !final {
			// Provisional — swallow; don't wake the Await goroutine.
			rc.mu.Unlock()
			return DeliveredToWaiter
		}
		rc.mu.Unlock()
		select {
		case p.ch <- env:
		default:
		}
		return DeliveredToWaiter

	case p.state == awaitNotStarted && p.expectsAwait:
		if !final {
			rc.mu.Unlock()
			return DeliveredToWaiter
		}
		// Buffer the final for a future Await.
		rc.mu.Unlock()
		select {
		case p.ch <- env:
		default:
		}
		return DeliveredToWaiter

	default:
		// awaitDone (timed out/cancelled) OR !expectsAwait — no active waiter.
		if final {
			delete(rc.pending, env.ParentID)
		}
		rc.mu.Unlock()
		return NoActiveWaiter
	}
}

// Await blocks until the final for id arrives, the window elapses, or ctx is
// done. window <= 0 means "do not wait at all".
func (rc *RequestCorrelator) Await(ctx context.Context, id message.ID, window time.Duration) (*message.Envelope, bool, error) {
	if window <= 0 {
		return nil, false, nil
	}
	rc.mu.Lock()
	p, ok := rc.pending[id]
	if !ok {
		rc.mu.Unlock()
		return nil, false, nil
	}
	p.state = awaitActive
	rc.mu.Unlock()

	timer := time.NewTimer(window)
	defer timer.Stop()
	select {
	case env := <-p.ch:
		rc.Cancel(id)
		return env, true, nil
	case <-timer.C:
		rc.mu.Lock()
		if pp, ok := rc.pending[id]; ok {
			pp.state = awaitDone
		}
		rc.mu.Unlock()
		return nil, false, nil
	case <-ctx.Done():
		rc.mu.Lock()
		if pp, ok := rc.pending[id]; ok {
			pp.state = awaitDone
		}
		rc.mu.Unlock()
		return nil, false, ctx.Err()
	}
}

// Cancel removes the future for id.
func (rc *RequestCorrelator) Cancel(id message.ID) {
	rc.mu.Lock()
	delete(rc.pending, id)
	rc.mu.Unlock()
}

// Pending returns the in-flight request ids.
func (rc *RequestCorrelator) Pending() []message.ID {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	ids := make([]message.ID, 0, len(rc.pending))
	for id := range rc.pending {
		ids = append(ids, id)
	}
	return ids
}
