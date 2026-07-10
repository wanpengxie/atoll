package actorbase

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// ErrCallClosed is Pending.Wait/JobTable.Await's verdict for an id whose
// out-station entry is already gone (matched, timed out, or cancelled) —
// the out-station mirror of ErrRequestClosed. Tested with errors.Is.
var ErrCallClosed = errors.New("actorbase: call already closed")

// callEntry is one out-station account row (spec §1.5's call ledger): a
// request THIS actor is owed a terminal for. req is the (pen-welded, so
// carrying this actor's own identity after Write) outbound envelope — the
// "request held in hand" fireTimeout/cancel need to author a self-closed
// terminal from. ch is a buffered(1) channel: at most one final response
// ever lands, so a non-blocking send into it can never fail on a live
// entry, and a final arriving BEFORE anyone calls Wait/Await just sits
// there — no separate "buffered final" bookkeeping needed.
type callEntry struct {
	target actor.ActorID
	req    *message.Envelope
	ch     chan *message.Envelope
	timer  *time.Timer
	// final marks the entry buffered-final (F4): set under the ledger lock the
	// instant match hands a FINAL response to ch. Once set, the entry's ONLY
	// exit is wait consuming it — fireTimeout and cancel become no-ops (they
	// must never delete the entry nor write a self-close over a real terminal
	// already in hand) and list hides it (it is no longer in-flight). This is
	// the "缓冲 final 永远赢" invariant made structural rather than a scatter of
	// if-guards.
	final bool
}

// callLedger is the out-station account (spec §1.5): "InFlight →(回信对号|
// author#2 超时终态写成|pending.Cancel)→Closed". The former behavior.Caller's
// timer (拆删于期12 S6) and
// metatool Shell.pending's correlator are the two historical fragments this
// ledger is — the SAME machine, moved house (spec: "同一台机器换了家和所有权
// ,不是消失"). Touched from the pump (match, on Receive) and from whichever
// goroutine drives Submit/Wait/Cancel (the worker via sys.Call, OR a later,
// separate turn's mind-binding tool call through JobTable) — hence its own
// lock.
type callLedger struct {
	life  func() context.Context
	pen   harness.Pen
	clock func() time.Time
	hooks Hooks
	// onFault is the fault sink (F5): a failed/rejected obligation write
	// (fireTimeout/cancel) — a liveness-guarantee hole — is reported here. Wired
	// by engine.New to engine.closureFault; nil = faults ignored (the ledger
	// holds no obs face of its own).
	onFault func(id message.ID, err error)

	mu      sync.Mutex
	entries map[message.ID]*callEntry
}

func newCallLedger(life func() context.Context, pen harness.Pen, clock func() time.Time, hooks Hooks, onFault func(message.ID, error)) *callLedger {
	return &callLedger{life: life, pen: pen, clock: clock, hooks: hooks, onFault: onFault, entries: make(map[message.ID]*callEntry)}
}

// fault reports a closure-write fault if a sink is wired.
func (l *callLedger) fault(id message.ID, err error) {
	if l.onFault != nil {
		l.onFault(id, err)
	}
}

// register reserves env.ID as InFlight BEFORE the write runs (subscribe-
// before-send — a response racing back before this returns can never find
// no entry to match against).
func (l *callLedger) register(env *message.Envelope, target actor.ActorID) *callEntry {
	e := &callEntry{target: target, req: env, ch: make(chan *message.Envelope, 1)}
	l.mu.Lock()
	l.entries[env.ID] = e
	l.mu.Unlock()
	return e
}

// forget unwinds a register() whose write never landed (a Go error or a
// harness reject) — the request was never really sent, so there is nothing
// to arm or wait for.
func (l *callLedger) forget(id message.ID) {
	l.mu.Lock()
	delete(l.entries, id)
	l.mu.Unlock()
}

// arm starts entry id's author#2 durable deadline timer (only once the
// write has landed — a request that never sent has no deadline to keep).
func (l *callLedger) arm(id message.ID, d time.Duration) {
	if d <= 0 {
		return
	}
	l.mu.Lock()
	e, ok := l.entries[id]
	if ok {
		e.timer = time.AfterFunc(d, func() { l.fireTimeout(id) })
	}
	l.mu.Unlock()
}

// stopTimers halts every armed author#2 deadline timer — engine.Stop calls it
// on teardown (D 族小账) so no AfterFunc outlives the dead incarnation. A timer
// that survived would only fire l.fireTimeout into a fail-closed pen (the live
// membrane rejects the write once lifeCtx is cancelled), so reclaiming it drops
// a dangling timer without losing any closure: the entries themselves stay, and
// their terminal obligation is owned by the ORIGINAL callers' durable backstop,
// exactly as the reject lane leaves its residue (spec §1.5 teardown 残留同兜底).
// Lock-held, so it is safe against a concurrent match/arm.
func (l *callLedger) stopTimers() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		if e.timer != nil {
			e.timer.Stop()
			e.timer = nil
		}
	}
}

// match is the pump's (Receive's) O(1) response-side dispatch: env is a
// response addressed to one of this ledger's InFlight requests iff its
// ParentID resolves. A PROVISIONAL response is swallowed (the entry stays
// InFlight — nothing here decides when a caller stops caring about
// provisionals, that is Wait/Await's window, not the ledger's); only a
// FINAL response disarms the timer and hands the envelope to whoever is (or
// later will be) waiting on ch. The entry itself is NOT deleted here — a
// final that lands before anyone calls Wait/Await must sit there for a LATER
// wait to consume (spec's buffered-final guarantee); wait() is what deletes
// the entry, at the moment it actually hands the envelope back.
func (l *callLedger) match(env *message.Envelope) bool {
	if env == nil || env.Kind != message.KindResponse {
		return false
	}
	l.mu.Lock()
	e, ok := l.entries[env.ParentID]
	if !ok {
		l.mu.Unlock()
		return false
	}
	_, final := behavior.ParseFinalStatus(env.Payload)
	if !final {
		l.mu.Unlock()
		return true
	}
	if e.timer != nil {
		e.timer.Stop()
		e.timer = nil
	}
	// Mark buffered-final AND hand the envelope to ch in ONE lock scope (F4;
	// tightened in the round-2 review): ch is buffered(1) and at most one
	// final ever lands, so the send can never block while the lock is held.
	// Setting the flag and filling the buffer atomically w.r.t. lock holders
	// removes the "final=true but ch still empty" intermediate — any lock
	// holder that observes the flag can also claim the envelope (see
	// claimBuffered), so a wait conceding its deadline can never strand a
	// final that had already been marked buffered.
	e.final = true
	select {
	case e.ch <- env:
	default: // unreachable for a well-formed ledger (at most one final ever lands)
	}
	l.mu.Unlock()
	return true
}

// claimBuffered atomically claims a final that match has already buffered:
// whenever a lock holder observes final=true the envelope is already in ch
// (match sets both in one lock scope), so the receive here never blocks. A
// concurrent waiter may have consumed it first — then the entry is theirs
// and this returns false. wait's deadline/ctx branches call this instead of
// a bare non-blocking channel peek, so "the final landed just as I gave up"
// resolves to claiming it, and a genuine timeout leaves the entry a normal
// InFlight row (visible to list, cancellable, awaitable later) — the
// stranded "marked buffered yet unreachable" state cannot exist.
func (l *callLedger) claimBuffered(id message.ID) (*message.Envelope, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.entries[id]
	if !ok || !e.final {
		return nil, false
	}
	select {
	case env := <-e.ch:
		delete(l.entries, id)
		return env, true
	default:
		return nil, false
	}
}

// fireTimeout is author#2's fast-path deadline executor (spec §1.5; the
// former behavior.Caller precedent collapsed into this ledger and the helper
// itself was拆删 in 期12 S6 — the durable guarantee is the substrate expiry
// reaper's, this timer merely races it for a live caller): it commits an
// unanswered_timeout terminal to truth AS the caller, closing the entry
// itself. A buffered-final entry (final=true) is a no-op — a real terminal
// already landed and is owed to wait, so this must neither delete it nor write
// over it (F4). An id already gone (a real terminal claimed and consumed it) is
// likewise a silent no-op — the happy-race outcome, not a fault.
//
// CLOSED GAP (期12 义务归位): if this caller横死 before its out-station
// entry closes, this timer dies with it — and the SUBSTRATE EXPIRY REAPER
// (platform sweepExpired over ix_messages_expires, exactly the "generalise
// ReconcileReceiverUnavailable's geometry to the caller-absent cell" fix
// this comment used to defer) closes the truth row at its declared deadline,
// system-authored. This timer is now the fast-path observer of that same
// fact, not the obligation's owner. A sibling local-account note
// (round-2 review, M1): a buffered-final entry whose caller never comes back
// for it (async Submit → its own wait gave up → final landed late → no
// further Await/Cancel) lingers in the map until teardown — truth is already
// closed by the real final, only this local row idles; same defer.
func (l *callLedger) fireTimeout(id message.ID) {
	l.mu.Lock()
	e, ok := l.entries[id]
	if !ok || e.final {
		l.mu.Unlock()
		return
	}
	delete(l.entries, id)
	// Close ch to wake any parked waiter NOW (round-3/fable review): the
	// account is closed, so a goroutine blocked in wait() must learn it here
	// rather than sit out its whole window (or, on an unbounded wait, hang
	// forever past a terminal that is already in truth). Safe under the lock:
	// match sends only within its own lock scope after finding the entry, and
	// the entry is deleted above — no send can follow this close.
	close(e.ch)
	l.mu.Unlock()
	payload, _ := json.Marshal(map[string]string{
		"error_code": string(message.TerminalUnansweredTimeout),
		"detail":     "no response before deadline",
	})
	// The WHEN authority for this obligation write is the pen's live membrane,
	// not the ctx (F5): WithoutCancel keeps the provenance value chain but strips
	// the teardown cancel, so a legitimate Draining-window timeout still lands.
	// The membrane fail-closes a genuinely-dead write; ctx is only the transport
	// pipe. A failed/rejected write (Respond maps a benign terminal-duplicate to
	// a nil error, so that stays silent) is a liveness hole and is reported.
	_, werr := behavior.Respond(context.WithoutCancel(l.life()), l.pen, l.clock, e.req, behavior.ResponseSpec{
		Status:  message.StatusFailed,
		Reason:  string(message.TerminalUnansweredTimeout),
		Payload: payload,
	})
	if werr != nil {
		l.fault(id, werr)
	}
}

// cancel is pending.Cancel's disposition (spec §1.5): close id's entry NOW
// (a legal self-close, never a forged verdict) — write the caller's OWN
// unanswered_timeout terminal, marked cancelled so it reads distinctly from
// a natural deadline fire, then (if Hooks.Canceller is wired) let the
// signal reach the receiver's in-station account. Idempotent: cancelling an
// already-closed id is a no-op.
func (l *callLedger) cancel(id message.ID) error {
	l.mu.Lock()
	e, ok := l.entries[id]
	if !ok || e.final {
		// Not in-flight, or a real final already landed and is buffered for wait
		// (F4): cancel is a no-op — never discard the real final nor write a
		// self-close over it.
		l.mu.Unlock()
		return nil
	}
	delete(l.entries, id)
	// Wake parked waiters, same as fireTimeout: account closed, no send can
	// follow (match's lookup+send share one lock scope; entry is gone).
	close(e.ch)
	l.mu.Unlock()
	if e.timer != nil {
		e.timer.Stop()
	}
	payload, _ := json.Marshal(map[string]any{
		"error_code": string(message.TerminalUnansweredTimeout),
		"detail":     "cancelled by caller",
		"cancelled":  true,
	})
	// WithoutCancel (F5): same rationale as fireTimeout — the obligation write's
	// WHEN authority is the pen membrane, not the ctx; a failed/rejected write
	// (duplicate excepted) is reported as a liveness fault.
	_, werr := behavior.Respond(context.WithoutCancel(l.life()), l.pen, l.clock, e.req, behavior.ResponseSpec{
		Status:  message.StatusFailed,
		Reason:  string(message.TerminalUnansweredTimeout),
		Payload: payload,
	})
	if werr != nil {
		l.fault(id, werr)
	}
	if l.hooks.Canceller != nil {
		l.hooks.Canceller(e.target, id)
	}
	return nil
}

// list returns the InFlight request ids (list_pending's engine-side source).
func (l *callLedger) list() []message.ID {
	l.mu.Lock()
	defer l.mu.Unlock()
	ids := make([]message.ID, 0, len(l.entries))
	for id, e := range l.entries {
		if e.final {
			continue // buffered-final entries are no longer in-flight (F4)
		}
		ids = append(ids, id)
	}
	return ids
}

// wait is the shared selective-receive core BOTH Pending.Wait and
// JobTable.Await drive (spec: "同一本账同一接口的两类caller"): block for
// id's final response until it arrives, d elapses (d<=0 = no time bound,
// wait on ctx alone), or ctx is done — whichever first. On a timer/ctx race
// against an already-buffered final, the buffered final ALWAYS wins (a
// final that landed a moment before must never be stranded as a phantom
// still-in-flight entry — the historical Shell.reconcileAfterWait
// guarantee, preserved here structurally rather than as a second pass).
func (l *callLedger) wait(ctx context.Context, id message.ID, d time.Duration) (*message.Envelope, bool, error) {
	l.mu.Lock()
	e, ok := l.entries[id]
	l.mu.Unlock()
	if !ok {
		return nil, false, ErrCallClosed
	}
	// consume distinguishes the two things a ch receive can yield: a real
	// final (chOk=true) is claimed and the entry deleted; a zero value from a
	// CLOSED ch (chOk=false) is the close-side waking us — the account was
	// closed by fireTimeout/cancel (round-3 review: a parked waiter must not
	// sit out its window, let alone hang an unbounded wait, past a terminal
	// already committed to truth) — surface ErrCallClosed.
	consume := func(env *message.Envelope, chOk bool) (*message.Envelope, bool, error) {
		if !chOk {
			return nil, false, ErrCallClosed
		}
		l.mu.Lock()
		delete(l.entries, id)
		l.mu.Unlock()
		return env, true, nil
	}
	if d <= 0 {
		select {
		case env, chOk := <-e.ch:
			return consume(env, chOk)
		case <-ctx.Done():
			if env, ok := l.claimBuffered(id); ok {
				return env, true, nil
			}
			return nil, false, ctx.Err()
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case env, chOk := <-e.ch:
		return consume(env, chOk)
	case <-timer.C:
		if env, ok := l.claimBuffered(id); ok {
			return env, true, nil
		}
		return nil, false, nil
	case <-ctx.Done():
		if env, ok := l.claimBuffered(id); ok {
			return env, true, nil
		}
		return nil, false, ctx.Err()
	}
}

// JobTable is the engine's exported, cross-goroutine narrow face onto the
// SAME out-station account sys.Call()/Pending touch (spec §1.5). Unlike a
// Proc's own sys.Call().Wait — one call frame, one wait — a mind-binding
// tool call (await_result/list_pending/cancel) resumes from OUTSIDE the
// turn that submitted the request, in a LATER, separate dispatch, so it
// needs a durable handle reachable across turns instead of a one-shot
// Pending ticket. Both caller classes touch the identical ledger row.
type JobTable interface {
	// Submit builds and writes a kind=request envelope from spec and
	// registers its out-station entry. spec.ExpiresAt left nil resolves
	// through Hooks.TimeoutResolver (else DefaultTimeout), exactly as
	// sys.Call does.
	Submit(spec behavior.RequestSpec) (message.ID, error)
	// Await blocks for id's final response up to window (<=0 = do not wait
	// at all — the immediate-ack shape; the DELIBERATE inverse of
	// Pending.Wait's d<=0="unbounded", see Pending's godoc), ctx, or
	// arrival, whichever first. An await parked when the account closes
	// under it returns ErrCallClosed immediately.
	Await(ctx context.Context, id message.ID, window time.Duration) (*message.Envelope, bool, error)
	// List returns the in-flight request ids (list_pending).
	List() []message.ID
	// Cancel is pending.Cancel by id (the cancel tool) — idempotent to an id
	// already closed.
	Cancel(id message.ID) error
}
