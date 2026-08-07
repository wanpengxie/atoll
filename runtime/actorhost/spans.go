package actorhost

import (
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
)

type hostSpan struct {
	mu   sync.Mutex
	refs int
}

// spanRegistry provides a semantic per-ActorID serializer without leaking an
// unbounded keyed-lock map under churn.
type spanRegistry struct {
	mu    sync.Mutex
	spans map[actor.ActorID]*hostSpan
}

func (r *spanRegistry) lock(id actor.ActorID) func() {
	r.mu.Lock()
	if r.spans == nil {
		r.spans = make(map[actor.ActorID]*hostSpan)
	}
	span := r.spans[id]
	if span == nil {
		span = &hostSpan{}
		r.spans[id] = span
	}
	span.refs++
	r.mu.Unlock()

	span.mu.Lock()
	return func() {
		span.mu.Unlock()
		r.mu.Lock()
		span.refs--
		if span.refs == 0 {
			delete(r.spans, id)
		}
		r.mu.Unlock()
	}
}
