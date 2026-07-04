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
}

// callLedger is the out-station account (spec §1.5): "InFlight →(回信对号|
// author#2 超时终态写成|pending.Cancel)→Closed". behavior.Caller's timer and
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

	mu      sync.Mutex
	entries map[message.ID]*callEntry
}

func newCallLedger(life func() context.Context, pen harness.Pen, clock func() time.Time, hooks Hooks) *callLedger {
	return &callLedger{life: life, pen: pen, clock: clock, hooks: hooks, entries: make(map[message.ID]*callEntry)}
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
	l.mu.Unlock()
	select {
	case e.ch <- env:
	default: // unreachable for a well-formed ledger (at most one final ever lands)
	}
	return true
}

// fireTimeout is author#2's durable-deadline executor (spec §1.5, the
// behavior.Caller precedent collapsed into this ledger): it commits an
// unanswered_timeout terminal to truth AS the caller, closing the entry
// itself. A benign race against a real terminal that landed microseconds
// earlier (Match already deleted the entry) is a silent no-op — the
// happy-race outcome, not a fault.
func (l *callLedger) fireTimeout(id message.ID) {
	l.mu.Lock()
	e, ok := l.entries[id]
	if ok {
		delete(l.entries, id)
	}
	l.mu.Unlock()
	if !ok {
		return
	}
	payload, _ := json.Marshal(map[string]string{
		"error_code": string(message.TerminalUnansweredTimeout),
		"detail":     "no response before deadline",
	})
	_, _ = behavior.Respond(l.life(), l.pen, l.clock, e.req, behavior.ResponseSpec{
		Status:  message.StatusFailed,
		Reason:  string(message.TerminalUnansweredTimeout),
		Payload: payload,
	})
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
	if ok {
		delete(l.entries, id)
	}
	l.mu.Unlock()
	if !ok {
		return nil
	}
	if e.timer != nil {
		e.timer.Stop()
	}
	payload, _ := json.Marshal(map[string]any{
		"error_code": string(message.TerminalUnansweredTimeout),
		"detail":     "cancelled by caller",
		"cancelled":  true,
	})
	_, _ = behavior.Respond(l.life(), l.pen, l.clock, e.req, behavior.ResponseSpec{
		Status:  message.StatusFailed,
		Reason:  string(message.TerminalUnansweredTimeout),
		Payload: payload,
	})
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
	for id := range l.entries {
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
	consume := func(env *message.Envelope) (*message.Envelope, bool, error) {
		l.mu.Lock()
		delete(l.entries, id)
		l.mu.Unlock()
		return env, true, nil
	}
	if d <= 0 {
		select {
		case env := <-e.ch:
			return consume(env)
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case env := <-e.ch:
		return consume(env)
	case <-timer.C:
		select {
		case env := <-e.ch:
			return consume(env)
		default:
			return nil, false, nil
		}
	case <-ctx.Done():
		select {
		case env := <-e.ch:
			return consume(env)
		default:
			return nil, false, ctx.Err()
		}
	}
}

// JobTable is the engine's exported, cross-goroutine narrow face onto the
// SAME out-station account sys.Call()/Pending touch (spec §1.5). Unlike a
// Proc's own sys.Call().Wait — one call frame, one wait — a mind-binding
// tool call (await_result/list_pending/abandon) resumes from OUTSIDE the
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
	// at all — the immediate-ack shape), ctx, or arrival, whichever first.
	Await(ctx context.Context, id message.ID, window time.Duration) (*message.Envelope, bool, error)
	// List returns the in-flight request ids (list_pending).
	List() []message.ID
	// Cancel is pending.Cancel by id (abandon) — idempotent to an id
	// already closed.
	Cancel(id message.ID) error
}
