package futurereg

import (
	"context"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// Await blocks until the request's final arrives, ctx is done, or the local
// timeout (when > 0) elapses. A final that arrived before this call (the final-
// before-await buffer set up at Register) is returned immediately.
//
// The wake error distinguishes the two non-delivery exits: the local timeout
// returns ErrLocalTimeout (the fast-path window closed — soft degradation), a
// ctx cancel / ctx deadline returns ctx.Err() (the call was aborted). Either
// way Await detaches its parked channel but leaves the waiterSet registered, so
// a subsequent final still routes through Deliver (becoming NoActiveWaiter →
// the caller's follow-up). On a final already consumed by another Await, it
// returns ErrClosed.
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
	ws.expectsAwait = true
	ws.abandoned = false
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
		return h.resolveOnWake(ch, ctx.Err())
	case <-timeoutCh:
		return h.resolveOnWake(ch, ErrLocalTimeout)
	}
}

// resolveOnWake is the M2 timeout/cancel side of the single-lock disposition
// state machine (§3.0). It is the EXACT mutual-exclusion counterpart of
// Deliver(final): both take r.mu, so for any given final there is one and only
// one outcome —
//
//   - If Deliver(final) already won the lock first, it pushed the final onto
//     ch and returned DeliveredToAwait. resolveOnWake then takes the lock,
//     drains ch, and returns the FINAL (not timeout). The disposition
//     DeliveredToAwait is honoured: the awaiter consumed it.
//   - If resolveOnWake wins the lock first (timer fired, Deliver not yet),
//     it detaches awaitCh under the lock and returns the wakeErr (timeout /
//     ctx cancel). A subsequent Deliver(final) then sees awaitCh==nil →
//     buffers the final → returns NoActiveWaiter.
//
// There is NO window where Await times out AND Deliver also reports
// DeliveredToAwait: the lock serialises the two, and the side that runs
// second observes the first's effect (final-in-ch ⇒ consume; awaitCh-nil ⇒
// buffer). A final is never lost: it is either consumed here or buffered by
// Deliver.
func (h *Handle) resolveOnWake(ch chan awaitResult, wakeErr error) (*message.Envelope, error) {
	r := h.reg
	r.mu.Lock()
	defer r.mu.Unlock()

	// A final (or close) may have been pushed onto ch by Deliver/closeLocked
	// before we took the lock. Draining ch under the lock makes the decision
	// deterministic: if a result is present, that result wins over the wake.
	select {
	case res := <-ch:
		// Deliver/Cancel already resolved this awaiter. Honour its result;
		// the disposition it returned (DeliveredToAwait) is now consistent.
		return res.env, res.err
	default:
	}

	// No result raced in: we won the lock first. Detach our awaitCh (if it is
	// still the active one), mark the waiter abandoned, and return the wake
	// error. The waiterSet stays registered so a later final routes through
	// Deliver as NoActiveWaiter instead of being mistaken for
	// fast-final-before-await.
	if ws, ok := r.waiters[h.id]; ok && ws.awaitCh == ch {
		ws.awaitCh = nil
		ws.abandoned = true
	}
	return nil, wakeErr
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
	// If a final was already buffered, emit it and SETTLE the whole set — the
	// final is the closure terminal, so consuming it here must mirror Await's
	// buffered-final path (await.go) and Deliver's watched-final path: clear the
	// buffer, mark consumed, close every watcher, drop the waiter. Without this
	// the buffered final lingers — a later Watch/Await re-consumes it and the
	// waiter leaks in Pending().
	if ws.finalBuf != nil {
		w.push(WatchEvent{Envelope: ws.finalBuf, IsFinal: true})
		ws.finalBuf = nil
		ws.finalConsumed = true
		r.closeWatchersLocked(ws)
		delete(r.waiters, h.id)
	}
	return w, nil
}

// Close releases the handle: it cancels the underlying waiterSet (wakes any
// Await, closes watch streams) without touching the substrate.
func (h *Handle) Close() error {
	h.reg.Cancel(h.id)
	return nil
}

// WatchEvent is one item on a Watch stream: a response envelope and whether it
// is the final one. The stream's end / cancellation is conveyed by closing the
// Events() channel (closeOnce), not by an in-band error field — so there is no
// Err here. Defined locally to keep the package free of any transport import.
type WatchEvent struct {
	Envelope *message.Envelope
	IsFinal  bool
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

// push delivers an event to the watcher (called under r.mu).
//
// F3 invariant: a FINAL event is NEVER dropped. Under a provisional storm the
// 16-slot buffer can fill; if we blindly dropped on full (the old behaviour)
// the final could be lost, and after the stream closes the caller's subsequent
// Await would observe a spurious timeout instead of the final. So:
//
//   - provisional: best-effort — push if there is room, else drop the OLDEST
//     buffered provisional to make room (keep the buffer fresh; never block
//     Deliver's single lock on a slow consumer).
//   - final: MUST be delivered. Push if there is room; if full, evict buffered
//     provisionals until the final fits. The final is the last event the
//     stream carries before closeOnce(), so a guaranteed slot for it always
//     exists once provisionals are evicted (buffer cap ≥ 1).
//
// Eviction stays non-blocking (only ever drains the channel we own), so this
// cannot deadlock against the consumer while holding r.mu.
func (w *watcher) push(ev WatchEvent) {
	if w.closeFlag {
		return
	}
	if !ev.IsFinal {
		// provisional: try once, else drop oldest provisional and retry once.
		select {
		case w.events <- ev:
			return
		default:
		}
		select {
		case <-w.events: // evict oldest buffered event to keep the stream fresh
		default:
		}
		select {
		case w.events <- ev:
		default:
		}
		return
	}
	// final: guarantee delivery — evict buffered provisionals until it fits.
	for {
		select {
		case w.events <- ev:
			return
		default:
		}
		select {
		case <-w.events: // make room by dropping an older (provisional) event
		default:
			// Channel reports full but we could not drain it (a concurrent
			// consumer just took one) — loop and retry the send.
		}
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
