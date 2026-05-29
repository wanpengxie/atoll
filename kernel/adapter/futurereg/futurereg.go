// Package futurereg is the transport-agnostic caller-side core for the
// framework sync/async mechanism: register a request_id, match inbound
// response envelopes against registered waiters, buffer a final that arrives
// before anyone awaits, and atomically (single-lock) compute the delivery
// disposition of each response.
//
// Three transports bind to it — the in-daemon response router, the kimi
// worker-side caller helper, and the SDK — so the matching / buffering /
// disposition semantics never drift between them.
//
// INVARIANT-1 hard constraint (§3.0): this package imports ONLY the standard
// library + kernel/message. No clock / logger / backend / transport — kernel
// must stay pure protocol.
package futurereg

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// Disposition is how Deliver, in one lock, decides where a response went
// (§3.0, M2). The caller transport acts on the returned value.
type Disposition int

const (
	// DeliveredToAwait — a final was handed to an active Await; the caller
	// MUST NOT also surface it as a new trigger.
	DeliveredToAwait Disposition = iota
	// DeliveredToWatch — a provisional or final was pushed onto a Watch
	// stream (and, if final, also resolved any Await).
	DeliveredToWatch
	// NoActiveWaiter — no active waiter (includes an Await that already
	// timed out and exited, or an abandoned request). The caller decides
	// the follow-up (kimi worker: surface as a new trigger; in-daemon:
	// quarantine if receiverOwner is also gone).
	NoActiveWaiter
)

// ErrClosed is returned by Await when the registry entry was cancelled
// (abandoned) or closed before a final arrived.
var ErrClosed = errors.New("futurereg: waiter closed")

// FutureRegistry maps request_id → waiterSet. Pure in-memory, transport
// agnostic, safe for concurrent use.
type FutureRegistry struct {
	mu      sync.Mutex
	waiters map[message.ID]*waiterSet
}

// New constructs an empty FutureRegistry.
func New() *FutureRegistry {
	return &FutureRegistry{waiters: map[message.ID]*waiterSet{}}
}

// waiterSet holds the per-request waiter state. All fields are guarded by the
// registry's single mutex (FutureRegistry.mu) so Deliver is atomic.
type waiterSet struct {
	id message.ID

	// finalBuf holds a final that arrived before any Await consumed it
	// (final-before-await buffer). nil = no buffered final.
	finalBuf *message.Envelope
	// finalConsumed is true once the buffered final has been handed to an
	// Await, so a late second Await reports closed rather than re-deliver.
	finalConsumed bool

	// awaitCh is the 1-buffered channel an active Await parks on. nil when
	// no Await is parked. A final delivered here counts as DeliveredToAwait.
	awaitCh chan awaitResult

	// watchers are the active Watch streams. Each receives every response
	// (provisional + final).
	watchers map[*watcher]struct{}

	// closed marks the entry as cancelled/closed; no further delivery.
	closed bool
}

type awaitResult struct {
	env *message.Envelope
	err error
}

// Handle is the per-request caller handle returned by Register. It exposes
// Await / Watch / Close. final-before-await is covered by the buffered slot
// created at Register time.
type Handle struct {
	reg *FutureRegistry
	id  message.ID
}

// ID returns the request id this handle waits on.
func (h *Handle) ID() message.ID { return h.id }

// Register creates a waiterSet (with the final-before-await buffer slot ready)
// for id and returns its Handle. Calling Register twice for the same id
// returns a fresh Handle bound to the same underlying set.
func (r *FutureRegistry) Register(id message.ID) *Handle {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.waiters[id]; !ok {
		r.waiters[id] = &waiterSet{
			id:       id,
			watchers: map[*watcher]struct{}{},
		}
	}
	return &Handle{reg: r, id: id}
}

// Registered reports whether id currently has a waiterSet.
func (r *FutureRegistry) Registered(id message.ID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.waiters[id]
	return ok
}

// Pending returns the ids of every currently registered (not yet
// closed/removed) waiterSet. Used by list_pending derived views.
func (r *FutureRegistry) Pending() []message.ID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]message.ID, 0, len(r.waiters))
	for id := range r.waiters {
		out = append(out, id)
	}
	return out
}

// Deliver matches env against the registered waiterSet for env.ParentID and
// atomically (under the single registry lock) computes the Disposition.
//
//   - Watch streams always receive env (provisional + final).
//   - A final additionally resolves an active Await, or is buffered if no
//     Await is parked yet (final-before-await buffer).
//   - Disposition reflects where a FINAL went; for provisionals with at least
//     one watcher it is DeliveredToWatch, otherwise NoActiveWaiter.
//
// This single-lock decision is the M2 guarantee: it closes the race between
// "Await just timed out and exited" and "final just arrived" — there is no
// window where a final is both delivered to an Await and surfaced as a
// trigger.
func (r *FutureRegistry) Deliver(env *message.Envelope) Disposition {
	if env == nil {
		return NoActiveWaiter
	}
	status := parseStatus(env.Payload)
	final := message.IsFinalStatus(status)

	r.mu.Lock()
	defer r.mu.Unlock()

	ws, ok := r.waiters[env.ParentID]
	if !ok || ws.closed {
		return NoActiveWaiter
	}

	// Watch streams see every response.
	watched := false
	for w := range ws.watchers {
		watched = true
		w.push(WatchEvent{Envelope: env, IsFinal: final})
	}

	if !final {
		if watched {
			return DeliveredToWatch
		}
		return NoActiveWaiter
	}

	// Final: resolve an active Await, else buffer it.
	if ws.awaitCh != nil {
		ws.awaitCh <- awaitResult{env: env}
		ws.awaitCh = nil
		ws.finalConsumed = true
		// Final also closes watch streams.
		r.closeWatchersLocked(ws)
		delete(r.waiters, env.ParentID)
		return DeliveredToAwait
	}

	// No parked Await: buffer the final so a later Await does not miss it.
	if !ws.finalConsumed {
		ws.finalBuf = env
	}
	if watched {
		// Watchers got the final; close their streams and drop the set.
		r.closeWatchersLocked(ws)
		// Keep the buffered final around for a possible Await only if no
		// watcher consumed it; but since a Watch consumer is the active
		// waiter here, treat as DeliveredToWatch and remove the set.
		delete(r.waiters, env.ParentID)
		return DeliveredToWatch
	}
	// Final buffered, no active waiter at all → caller decides follow-up.
	return NoActiveWaiter
}

// Cancel abandons local waiting for id: it wakes any parked Await with
// ErrClosed, closes watch streams, and removes the set. It does NOT touch the
// substrate (the daemon-side pending + F3 stay intact).
func (r *FutureRegistry) Cancel(id message.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ws, ok := r.waiters[id]
	if !ok {
		return
	}
	r.closeLocked(ws)
	delete(r.waiters, id)
}

// closeLocked wakes any Await and closes watch streams. Caller holds mu.
func (r *FutureRegistry) closeLocked(ws *waiterSet) {
	ws.closed = true
	if ws.awaitCh != nil {
		ws.awaitCh <- awaitResult{err: ErrClosed}
		ws.awaitCh = nil
	}
	r.closeWatchersLocked(ws)
}

func (r *FutureRegistry) closeWatchersLocked(ws *waiterSet) {
	for w := range ws.watchers {
		w.closeOnce()
		delete(ws.watchers, w)
	}
}

func parseStatusErr(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		return "", fmt.Errorf("futurereg: response payload must be a JSON object: %w", err)
	}
	v, ok := fields["status"]
	if !ok {
		return "", nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("futurereg: response payload.status must be a string, got %T", v)
	}
	return s, nil
}

// parseStatus extracts payload.status, returning "" on any parse trouble.
// The single source of truth for status extraction across the three
// transports (§3.0). Final-status classification stays in
// message.IsFinalStatus (kernel single source).
func parseStatus(raw json.RawMessage) string {
	s, _ := parseStatusErr(raw)
	return s
}
