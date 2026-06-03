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
	// BufferedPendingAwait — a final arrived before an expected Await parked.
	// The final is buffered for that Await and MUST NOT be surfaced as a new
	// trigger.
	BufferedPendingAwait
	// NoActiveWaiter — a well-formed response that correctly routed to nobody:
	// no active waiter (an Await that already timed out and exited, or an
	// abandoned request). This is a genuine routing OUTCOME — the caller
	// decides the follow-up for an unclaimed-but-valid response.
	NoActiveWaiter
	// InvalidDisposition — the envelope handed to Deliver is not a routable
	// response: nil, a non-response Kind, or an unparseable status. This is NOT
	// a routing outcome but a precondition failure surfaced as a total-function
	// return, so the Disposition vocabulary stays honest — "input that does not
	// classify" is a distinct fact from NoActiveWaiter ("a valid response that
	// routed to nobody"). The caller MUST NOT treat it as a no-waiter final.
	InvalidDisposition
)

// ErrClosed is returned by Await when the registry entry was cancelled
// (abandoned) or closed before a final arrived.
var ErrClosed = errors.New("futurereg: waiter closed")

// ErrLocalTimeout is returned by Await when its local await window (the timeout
// argument) elapsed before a final arrived. It is NORMAL fast-path degradation,
// NOT a call abort: the waiterSet stays registered, so a later final still
// routes through Deliver (as NoActiveWaiter for the caller's follow-up). It is
// deliberately distinct from ctx cancel / ctx-deadline (which surface as
// ctx.Err()), so a caller can tell "the fast-path await window closed" from
// "the call itself was aborted / the process is shutting down".
var ErrLocalTimeout = errors.New("futurereg: local await window elapsed")

// RegisterOpts controls the initial waiter mode for a request id.
type RegisterOpts struct {
	// ExpectsAwait means a final that arrives before an Await parks is a
	// fast-final-before-await and should be buffered for that future Await.
	// false means pure async / fan-out: an un-awaited final is a genuine
	// no-waiter final and should surface through the caller transport.
	ExpectsAwait bool
}

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
	// expectsAwait distinguishes fast-final-before-await from pure async
	// fan-out. It is read only under FutureRegistry.mu.
	expectsAwait bool
	// abandoned means the current expected Await gave up (timeout / ctx cancel /
	// explicit abandon path that keeps the set), so a later final should surface
	// instead of being silently buffered forever.
	abandoned bool
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
// for id and returns its Handle. With no opts, Register defaults to
// ExpectsAwait=true: the normal Submit/Call fast-path expects a future Await
// and must preserve fast-final-before-await.
//
// Registration is idempotent: calling Register again for a still-live id
// returns a fresh Handle bound to the EXISTING set WITHOUT mutating its
// waiting-mode flags. An id already abandoned (its Await timed out) must not be
// silently revived into buffering a later final — re-Register cannot resurrect
// an abandoned waiter.
func (r *FutureRegistry) Register(id message.ID, opts ...RegisterOpts) *Handle {
	ro := RegisterOpts{ExpectsAwait: true}
	if len(opts) > 0 {
		ro = opts[0]
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.waiters[id]; !ok {
		r.waiters[id] = &waiterSet{
			id:           id,
			watchers:     map[*watcher]struct{}{},
			expectsAwait: ro.ExpectsAwait,
		}
	}
	return &Handle{reg: r, id: id}
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
	// Structural precondition: Deliver routes a well-formed RESPONSE envelope.
	// A nil envelope, a non-response Kind, or an unparseable status does not
	// classify as a routing outcome — it is invalid input, distinct from a
	// valid response that finds no waiter. Keep Deliver total: return
	// InvalidDisposition rather than silently collapsing garbage into
	// NoActiveWaiter (which would assert "valid response, nobody waiting").
	if env == nil || env.Kind != message.KindResponse {
		return InvalidDisposition
	}
	status, err := parseStatusErr(env.Payload)
	if err != nil {
		return InvalidDisposition
	}
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

	// Final: resolve an active Await, else decide whether this is a
	// fast-final-before-await (buffer) or a genuine no-waiter final (surface).
	if ws.awaitCh != nil {
		ws.awaitCh <- awaitResult{env: env}
		ws.awaitCh = nil
		ws.finalConsumed = true
		// Final also closes watch streams.
		r.closeWatchersLocked(ws)
		delete(r.waiters, env.ParentID)
		return DeliveredToAwait
	}

	if watched {
		// Watchers got the final; close their streams and drop the set.
		r.closeWatchersLocked(ws)
		delete(r.waiters, env.ParentID)
		return DeliveredToWatch
	}

	if ws.expectsAwait && !ws.abandoned {
		if !ws.finalConsumed {
			ws.finalBuf = env
		}
		return BufferedPendingAwait
	}

	// Pure async / abandoned: this final is going to be surfaced by the caller
	// transport. Clear the set while still under the Deliver lock so a racing
	// await_result cannot also consume the same final.
	ws.finalConsumed = true
	r.closeWatchersLocked(ws)
	delete(r.waiters, env.ParentID)
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

// parseStatusErr extracts payload.status — the single source of truth for
// status extraction across the three transports (§3.0). It surfaces a parse
// error rather than swallowing it so Deliver can classify a malformed payload
// as InvalidDisposition. Final-status classification stays in
// message.IsFinalStatus (kernel single source).
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
