package actorbase

import (
	"sync"

	"github.com/wanpengxie/atoll/protocol/message"
)

// workDeque is the engine's inbound work buffer — a bounded, LOCK-guarded deque
// replacing the bare chan the pump and worker used to share (G0-4). The bare chan
// made the overflow policy unsafe: enqueueWork drained-then-refilled to evict the
// oldest, but the worker's Recv could concurrently grab the drained slot, breaking
// FIFO and letting an event flood evict an ALREADY-ADMITTED request (its ledger
// entry then lingers until the caller's deadline — a white-wait). A lock makes
// "evict the oldest NON-request, keep FIFO" one atomic step (P0-4=A + review #6).
//
// The pump (Receive, on the cell goroutine) push()es; the worker (Recv) pop()s.
// A cap-1 coalesced signal channel wakes a blocked Recv — the worker drains the
// deque directly and only blocks on sig when it is empty.
type workDeque struct {
	mu    sync.Mutex
	items []*message.Envelope
	cap   int
	sig   chan struct{}
}

func newWorkDeque(capacity int) *workDeque {
	// Fail loud at assembly time — called only from New (a compile-time constant)
	// and whitebox tests, so a panic surfaces the mistake at wiring, never at
	// request time. A silent <=0 → 256 remap would hide a mis-wired capacity the
	// same way it did for serveLedger (see F8).
	if capacity <= 0 {
		panic("actorbase: work deque capacity must be positive")
	}
	return &workDeque{cap: capacity, sig: make(chan struct{}, 1)}
}

// wake posts a coalesced signal (cap-1, non-blocking) so a Recv parked on sig
// re-checks the deque. Coalesced: N pushes need not wake N times — the worker's
// pop loop drains everything a single wake reveals.
func (q *workDeque) wake() {
	select {
	case q.sig <- struct{}{}:
	default:
	}
}

// push appends env with the cross-kind-FIFO overflow policy. It returns the
// dropped envelope (for the caller's obs record) or nil:
//   - room → append at the tail, wake a waiter.
//   - full → evict the OLDEST NON-request (skip requests — an event flood must
//     never evict an admitted request whose account is open), append env, wake.
//   - full AND every queued item is a request → drop the NEW env (never evict a
//     request to seat another: its account would be orphaned mid-FIFO). Honest
//     degrade — dropping the newcomer beats destroying an open account.
func (q *workDeque) push(env *message.Envelope) (dropped *message.Envelope) {
	q.mu.Lock()
	if len(q.items) < q.cap {
		q.items = append(q.items, env)
		q.mu.Unlock()
		q.wake()
		return nil
	}
	for i, it := range q.items {
		if it.Kind != message.KindRequest {
			dropped = it
			q.items[i] = nil
			q.items = append(q.items[:i], q.items[i+1:]...)
			q.items = append(q.items, env)
			q.mu.Unlock()
			q.wake()
			return dropped
		}
	}
	// All queued items are requests: refuse the newcomer (no wake — nothing was
	// enqueued).
	q.mu.Unlock()
	return env
}

// pop removes and returns the oldest item (FIFO front), or ok=false if empty.
func (q *workDeque) pop() (*message.Envelope, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil, false
	}
	env := q.items[0]
	q.items[0] = nil // release the reference so a drained slot retains nothing
	q.items = q.items[1:]
	return env, true
}
