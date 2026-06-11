package tap

import "sync"

// Signal is the lossy-by-design fan-out wake: every committed envelope wakes all
// subscribers, which then read forward from their own seq cursor. Correctness is
// the seq read, not the signal — a coalesced wake still triggers a read that
// sees every new seq. Both client-push streams (SDK WS tails) and Pumps
// subscribe through it.
type Signal struct {
	mu   sync.Mutex
	subs map[int]chan struct{}
	next int
}

// NewSignal builds an empty signal.
func NewSignal() *Signal { return &Signal{subs: map[int]chan struct{}{}} }

// Notify wakes every subscriber (non-blocking: a subscriber already pending a
// wake keeps its single buffered slot — it will read all new seqs on wake).
func (s *Signal) Notify() {
	s.mu.Lock()
	for _, ch := range s.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	s.mu.Unlock()
}

// Subscribe registers a subscriber; the returned channel signals "new commits
// available", and the cancel func unregisters it.
func (s *Signal) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	s.mu.Lock()
	id := s.next
	s.next++
	s.subs[id] = ch
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		delete(s.subs, id)
		s.mu.Unlock()
	}
}
