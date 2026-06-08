package agent

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/protocol/message"
)

type disposition int

const (
	deliveredToWaiter disposition = iota
	noActiveWaiter
)

type awaitState int

const (
	awaitNotStarted awaitState = iota
	awaitActive
	awaitDone
)

type requestCorrelator struct {
	mu      sync.Mutex
	pending map[message.ID]*pendingReq
}

type pendingReq struct {
	ch           chan *message.Envelope
	expectsAwait bool
	state        awaitState
}

func newRequestCorrelator() *requestCorrelator {
	return &requestCorrelator{pending: make(map[message.ID]*pendingReq)}
}

func (rc *requestCorrelator) Register(id message.ID, expectsAwait bool) {
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

func (rc *requestCorrelator) Registered(id message.ID) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	_, ok := rc.pending[id]
	return ok
}

func (rc *requestCorrelator) Deliver(env *message.Envelope) disposition {
	if env == nil {
		return noActiveWaiter
	}
	final := isEnvFinal(env)

	rc.mu.Lock()
	p, ok := rc.pending[env.ParentID]
	if !ok {
		rc.mu.Unlock()
		return noActiveWaiter
	}

	switch {
	case p.state == awaitActive:
		if !final {
			// Provisional — swallow; don't wake the Await goroutine.
			rc.mu.Unlock()
			return deliveredToWaiter
		}
		rc.mu.Unlock()
		select {
		case p.ch <- env:
		default:
		}
		return deliveredToWaiter

	case p.state == awaitNotStarted && p.expectsAwait:
		if !final {
			rc.mu.Unlock()
			return deliveredToWaiter
		}
		// Buffer the final for a future Await.
		rc.mu.Unlock()
		select {
		case p.ch <- env:
		default:
		}
		return deliveredToWaiter

	default:
		// awaitDone (timed out/cancelled) OR !expectsAwait — no active waiter.
		if final {
			delete(rc.pending, env.ParentID)
		}
		rc.mu.Unlock()
		return noActiveWaiter
	}
}

func isEnvFinal(env *message.Envelope) bool {
	if env.Kind != message.KindResponse {
		return false
	}
	var p struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return false
	}
	return message.IsFinalStatus(p.Status)
}

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

func (rc *requestCorrelator) Cancel(id message.ID) {
	rc.mu.Lock()
	delete(rc.pending, id)
	rc.mu.Unlock()
}

func (rc *requestCorrelator) Pending() []message.ID {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	ids := make([]message.ID, 0, len(rc.pending))
	for id := range rc.pending {
		ids = append(ids, id)
	}
	return ids
}
