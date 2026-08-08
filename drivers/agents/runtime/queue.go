package runtime

import "sync"

// unboundedQueue gives producer-side O(1) ownership transfer without allowing
// a blocked consumer to backpressure a provider pump.
type unboundedQueue[T any] struct {
	mu     sync.Mutex
	items  []T
	wake   chan struct{}
	sealed bool
}

func newQueue[T any]() *unboundedQueue[T] { return &unboundedQueue[T]{wake: make(chan struct{}, 1)} }
func (q *unboundedQueue[T]) push(v T) bool {
	q.mu.Lock()
	if q.sealed {
		q.mu.Unlock()
		return false
	}
	q.items = append(q.items, v)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return true
}
func (q *unboundedQueue[T]) pop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		var z T
		return z, false
	}
	v := q.items[0]
	var z T
	q.items[0] = z
	q.items = q.items[1:]
	return v, true
}
func (q *unboundedQueue[T]) seal() {
	q.mu.Lock()
	q.sealed = true
	q.items = nil
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
