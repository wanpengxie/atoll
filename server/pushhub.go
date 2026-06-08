package server

import "sync"

// pushHub is the client-push fan-out signal: every committed envelope wakes all
// subscribed client streams (SDK WS tails), which then read forward from their
// own seq cursor (so the signal is lossy-by-design -- correctness is the seq read,
// not the signal). This is the external-client half of the fanout (cells get the
// envelope directly; remote clients get a "go read more" nudge).
//
// Moved from channelhost to the assembly root (server package) because the hub
// is wired into postCommitWriter which the assembly root owns.
type pushHub struct {
	mu   sync.Mutex
	subs map[int]chan struct{}
	next int
}

func newPushHub() *pushHub { return &pushHub{subs: map[int]chan struct{}{}} }

// notify wakes every subscriber (non-blocking: a subscriber already pending a
// wake keeps its single buffered slot -- it will read all new seqs on wake).
func (h *pushHub) notify() {
	h.mu.Lock()
	for _, ch := range h.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	h.mu.Unlock()
}

// subscribe registers a client stream; the returned channel signals "new commits
// available", and the cancel func unregisters it.
func (h *pushHub) subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	id := h.next
	h.next++
	h.subs[id] = ch
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subs, id)
		h.mu.Unlock()
	}
}
