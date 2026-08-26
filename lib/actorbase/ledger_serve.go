package actorbase

import (
	"context"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

// serveEntry is one in-station account row (spec §1.5's serve ledger): a
// request this actor owes a terminal for. ctx/cancel are the request's OWN
// derived scope — a request delivery's Msg.Ctx() IS this entry's ctx, so a
// deadline firing or a delivered cancel closing the entry cascades directly
// into whatever the Proc's handler is doing with it.
type serveEntry struct {
	ctx    context.Context
	cancel context.CancelFunc
	// timer is the entry's own deadline auto-close (nil when the request
	// carries no ExpiresAt — an in-flight-forever entry, closed only by a
	// terminal write or a delivered cancel).
	timer *time.Timer
	// span is the request's sliding deadline window (expires_at − ts); touch
	// restarts timer from each provisional response this actor writes.
	span time.Duration
}

// serveLedger is the in-station account (spec §1.5): "只登记 kind=request…
// Admitted →(终态写成|ExpiresAt 到|cancel 投递到)→Closed". It is touched from
// two engine goroutines — the pump (admit, on Receive) and the worker (a
// Reply/Fail/CancelRequest closing an entry) — so the ACCOUNT holds its own
// lock (spec's "账内必须持锁": callerMu-the-precedent promoted into the
// ledger itself, not left to its callers).
type serveLedger struct {
	// life returns the process-life ctx every entry's own ctx derives from —
	// a func, not a stored ctx, because the ledger is constructed before
	// Start() hands the engine its lifeCtx.
	life func() context.Context

	mu      sync.Mutex
	entries map[message.ID]*serveEntry
	cap     int
}

func newServeLedger(life func() context.Context, capacity int) *serveLedger {
	// A non-positive cap is a construction-time wiring bug, never an author's
	// intent (F8): the old silent <=0 → 256 remap is exactly what let a test
	// pass serveCap=0 and unknowingly exercise the work queue instead of the
	// reject lane. Fail loud at assembly time — this is called only from New
	// (always 256) and whitebox tests, so a panic surfaces the mistake at wiring,
	// never at request time.
	if capacity <= 0 {
		panic("actorbase: serve ledger capacity must be positive")
	}
	return &serveLedger{life: life, entries: make(map[message.ID]*serveEntry), cap: capacity}
}

// admit registers env.ID as Admitted, deriving its ctx from the ledger's own
// process-life ctx (a deadline when ExpiresAt is set). false means the
// account is full — the pump's non-blocking reject-lane branch, never a
// blocking wait. A redelivery of an id already Admitted is a no-op success
// (idempotent — the entry keeps its original scope).
func (l *serveLedger) admit(env *message.Envelope) bool {
	admitted, _ := l.admitOnce(env)
	return admitted
}

// admitOnce also reports whether the id was already admitted. The delivery
// pump uses this distinction to suppress duplicate work while preserving the
// original request scope; tests and other account-only callers use admit.
func (l *serveLedger) admitOnce(env *message.Envelope) (admitted, existed bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.entries[env.ID]; exists {
		return true, true
	}
	if len(l.entries) >= l.cap {
		return false, false
	}
	ctx, cancel := context.WithCancel(l.life())
	e := &serveEntry{ctx: ctx, cancel: cancel}
	if env.ExpiresAt != nil {
		id := env.ID
		e.span = deadlineSpan(env)
		e.timer = time.AfterFunc(time.Until(time.UnixMilli(*env.ExpiresAt)), func() { l.close(id) })
	}
	l.entries[env.ID] = e
	return true, false
}

// touch restarts id's sliding deadline after this actor wrote a provisional
// response for it: the caller's window and the reaper's view both restart
// from that row, and the in-station ctx must not collapse ahead of them.
func (l *serveLedger) touch(id message.ID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.entries[id]
	if !ok || e.span <= 0 || e.timer == nil {
		return
	}
	if !e.timer.Stop() {
		return // close is already firing
	}
	e.timer = time.AfterFunc(e.span, func() { l.close(id) })
}

// ctxFor resolves the ctx an Admitted id's delivery should carry — the
// worker's Recv path consults this at POP time (not at admit time), so a
// delivery that survived in the work queue past its entry's close sees no
// entry here and the caller skips it (spec's "Recv 弹出已 Closed 条目的 msg
// 时跳过不交付"). false means id is not currently Admitted.
func (l *serveLedger) ctxFor(id message.ID) (context.Context, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.entries[id]
	if !ok {
		return nil, false
	}
	return e.ctx, true
}

// isClosed reports whether id has no Admitted entry — the late-write
// judgement's core predicate (Reply/Fail/Progress consult this BEFORE
// touching the pen; spec's "本地判定").
func (l *serveLedger) isClosed(id message.ID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.entries[id]
	return !ok
}

// close is the ONE disposition every closer (a landed terminal write,
// ExpiresAt firing, a delivered cancel) funnels through: fire the entry's
// own cancel (cascading into every downstream holder of its ctx) and delete
// it. It NEVER writes a terminal itself (spec: "本地关账从不写终态" — a
// deadline/cancel close has no terminal of its own to write; that
// obligation belongs to the ORIGINATING caller's own author#2, a wholly
// different actor's out-station ledger). Idempotent: closing an
// already-gone id is a no-op.
func (l *serveLedger) close(id message.ID) {
	l.mu.Lock()
	e, ok := l.entries[id]
	if ok {
		delete(l.entries, id)
	}
	l.mu.Unlock()
	if !ok {
		return
	}
	if e.timer != nil {
		e.timer.Stop()
	}
	e.cancel()
}

// stopTimers halts every armed serve-entry deadline timer on teardown —
// callLedger.stopTimers's serve-side twin (期12 glue 顺手项): an AfterFunc
// surviving the incarnation would only fire close() on a dead ledger
// (harmless but dangling). Entries stay; a deadline close is bookkeeping,
// never a terminal write, so nothing is lost by reclaiming the timers.
func (l *serveLedger) stopTimers() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		if e.timer != nil {
			e.timer.Stop()
			e.timer = nil
		}
	}
}
