package futurereg

import (
	"context"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// Await blocks until the request's final arrives, ctx is done, or timeout
// (when > 0) elapses. A final that arrived before this call (the final-
// before-await buffer set up at Register) is returned immediately.
//
// On ctx-cancel / timeout, Await detaches its parked channel but leaves the
// waiterSet registered so a subsequent final still routes through Deliver
// (becoming NoActiveWaiter → the caller's follow-up decision). On a final
// already consumed by another Await, it returns ErrClosed.
func (h *Handle) Await(ctx context.Context, timeout time.Duration) (*message.Envelope, error) {
	r := h.reg
	r.mu.Lock()
	ws, ok := r.waiters[h.id]
	if !ok {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	if ws.closed {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	// final-before-await buffer: a final already arrived.
	if ws.finalBuf != nil {
		env := ws.finalBuf
		ws.finalBuf = nil
		ws.finalConsumed = true
		r.closeWatchersLocked(ws)
		delete(r.waiters, h.id)
		r.mu.Unlock()
		return env, nil
	}
	if ws.finalConsumed {
		r.mu.Unlock()
		return nil, ErrClosed
	}
	if ws.awaitCh != nil {
		// Another Await is already parked on this id; only one supported.
		r.mu.Unlock()
		return nil, ErrClosed
	}
	ch := make(chan awaitResult, 1)
	ws.awaitCh = ch
	r.mu.Unlock()

	var timer *time.Timer
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timeoutCh = timer.C
		defer timer.Stop()
	}

	select {
	case res := <-ch:
		return res.env, res.err
	case <-ctx.Done():
		h.detach(ch)
		return nil, ctx.Err()
	case <-timeoutCh:
		h.detach(ch)
		return nil, context.DeadlineExceeded
	}
}

// detach removes the parked await channel if it is still the active one,
// leaving the waiterSet registered (so a later final routes through Deliver
// as NoActiveWaiter). If a final was already pushed onto ch (raced with
// timeout), it is preserved as the final-before-await buffer so it is not
// lost.
func (h *Handle) detach(ch chan awaitResult) {
	r := h.reg
	r.mu.Lock()
	defer r.mu.Unlock()
	ws, ok := r.waiters[h.id]
	if ok && ws.awaitCh == ch {
		// Still the active parked channel and no final raced in: just detach.
		ws.awaitCh = nil
		return
	}
	// Either Deliver detached/deleted the set after pushing a final onto ch
	// (final won the race) — drain ch so the final is not lost.
	select {
	case res := <-ch:
		if res.env == nil {
			return
		}
		if ok {
			ws.finalBuf = res.env
			ws.finalConsumed = false
			return
		}
		// Set was deleted by Deliver; re-create a minimal buffered set so a
		// fresh Await still recovers the final.
		r.waiters[h.id] = &waiterSet{
			id:       h.id,
			finalBuf: res.env,
			watchers: map[*watcher]struct{}{},
		}
	default:
	}
}

// Watch returns a stream of every response (provisional + final) for the
// request. The stream closes after the final or on Close().
func (h *Handle) Watch() (Watcher, error) {
	r := h.reg
	r.mu.Lock()
	defer r.mu.Unlock()
	ws, ok := r.waiters[h.id]
	if !ok || ws.closed {
		return nil, ErrClosed
	}
	w := &watcher{
		events: make(chan WatchEvent, 16),
		set:    ws,
		reg:    r,
		id:     h.id,
	}
	ws.watchers[w] = struct{}{}
	// If a final was already buffered, emit it immediately and close.
	if ws.finalBuf != nil {
		w.push(WatchEvent{Envelope: ws.finalBuf, IsFinal: true})
		w.closeOnce()
		delete(ws.watchers, w)
	}
	return w, nil
}

// Close releases the handle: it cancels the underlying waiterSet (wakes any
// Await, closes watch streams) without touching the substrate.
func (h *Handle) Close() error {
	h.reg.Cancel(h.id)
	return nil
}

// WatchEvent mirrors adapter.WatchEvent — kept local so the package does not
// import adapter (which would create a cycle). The router/SDK convert.
type WatchEvent struct {
	Envelope *message.Envelope
	IsFinal  bool
	Err      error
}

// Watcher is the stream handle returned by Handle.Watch.
type Watcher interface {
	Events() <-chan WatchEvent
	Close() error
}

type watcher struct {
	events    chan WatchEvent
	set       *waiterSet
	reg       *FutureRegistry
	id        message.ID
	closeMu   sync.Once
	closeFlag bool
}

func (w *watcher) Events() <-chan WatchEvent { return w.events }

// push delivers an event to the watcher non-blockingly (drop on full buffer
// to keep Deliver's single lock from blocking on a slow consumer).
func (w *watcher) push(ev WatchEvent) {
	if w.closeFlag {
		return
	}
	select {
	case w.events <- ev:
	default:
	}
}

func (w *watcher) closeOnce() {
	w.closeMu.Do(func() {
		w.closeFlag = true
		close(w.events)
	})
}

// Close detaches the watcher from its waiterSet and closes the stream.
func (w *watcher) Close() error {
	w.reg.mu.Lock()
	if ws, ok := w.reg.waiters[w.id]; ok {
		delete(ws.watchers, w)
	}
	w.reg.mu.Unlock()
	w.closeOnce()
	return nil
}
