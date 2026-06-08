package agent

import (
	"context"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/protocol/message"
)

// disposition describes how Deliver routed a response.
type disposition int

const (
	deliveredToWaiter disposition = iota
	noActiveWaiter
)

// requestCorrelator is the worker-side request-response correlation.
// It replaces the deleted futurereg package with a local, minimal implementation.
// The worker subprocess needs blocking semantics because go-kimi tool calls are synchronous.
type requestCorrelator struct {
	mu      sync.Mutex
	pending map[message.ID]*pendingReq
}

type pendingReq struct {
	ch           chan *message.Envelope
	expectsAwait bool
	awaiting     bool
}

func newRequestCorrelator() *requestCorrelator {
	return &requestCorrelator{pending: make(map[message.ID]*pendingReq)}
}

// Register creates or returns the existing pending entry for id.
func (rc *requestCorrelator) Register(id message.ID, expectsAwait bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if _, ok := rc.pending[id]; ok {
		return // idempotent
	}
	rc.pending[id] = &pendingReq{
		ch:           make(chan *message.Envelope, 1),
		expectsAwait: expectsAwait,
	}
}

// Registered reports whether id has a pending entry.
func (rc *requestCorrelator) Registered(id message.ID) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	_, ok := rc.pending[id]
	return ok
}

// Deliver routes a response envelope to its pending request.
func (rc *requestCorrelator) Deliver(env *message.Envelope) disposition {
	if env == nil {
		return noActiveWaiter
	}
	rc.mu.Lock()
	p, ok := rc.pending[env.ParentID]
	if !ok {
		rc.mu.Unlock()
		return noActiveWaiter
	}
	// If there's an active waiter, deliver to it
	if p.awaiting {
		rc.mu.Unlock()
		select {
		case p.ch <- env:
			return deliveredToWaiter
		default:
			return deliveredToWaiter // channel already has a value
		}
	}
	// No active waiter yet — buffer for a future Await.
	rc.mu.Unlock()
	select {
	case p.ch <- env:
		// Buffered; a later Await will pick it up.
	default:
		// Channel already has a buffered value.
	}
	return noActiveWaiter
}

// Await blocks until a response for id arrives or the timeout/ctx expires.
func (rc *requestCorrelator) Await(ctx context.Context, id message.ID, window time.Duration) (*message.Envelope, bool, error) {
	if window <= 0 {
		return nil, false, nil
	}
	rc.mu.Lock()
	p, ok := rc.pending[id]
	if !ok {
		rc.mu.Unlock()
		return nil, false, nil
	}
	p.awaiting = true
	rc.mu.Unlock()

	timer := time.NewTimer(window)
	defer timer.Stop()
	select {
	case env := <-p.ch:
		rc.Cancel(id)
		return env, true, nil
	case <-timer.C:
		return nil, false, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

// Cancel removes the pending entry for id.
func (rc *requestCorrelator) Cancel(id message.ID) {
	rc.mu.Lock()
	delete(rc.pending, id)
	rc.mu.Unlock()
}

// Pending returns the list of in-flight request IDs.
func (rc *requestCorrelator) Pending() []message.ID {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	ids := make([]message.ID, 0, len(rc.pending))
	for id := range rc.pending {
		ids = append(ids, id)
	}
	return ids
}
