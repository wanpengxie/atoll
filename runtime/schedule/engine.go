package schedule

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

const (
	// backoffDuration is the sleep-until-retry the run loop arms after a
	// tick makes zero progress across BOTH families (every due row/entry
	// left in place — a pure transient-failure tick). Real, non-zero pacing
	// so the loop never busy-spins re-querying an unchanged due set.
	backoffDuration       = 1 * time.Second
	perFireTimeout        = 10 * time.Second
	maxMemTimersPerAuthor = 1024
)

// memTimer is one incarnation-bind entry in the engine's in-memory due-set —
// NEVER a store row, NEVER serialised. inc is the
// attach reference captured at Schedule time; the drop check at fire time
// compares it by POINTER identity via LivenessProbe.IsLive (ABA-safe: a
// same-id successor being live does not rescue a predecessor's timer).
type memTimer struct {
	id            TimerID
	author        actor.ActorID
	inc           actorrt.Incarnation
	fireAt        int64
	typ           string
	payload       []byte
	correlationID string
}

// Engine is the time axis's single poll/wake loop + fire path, driving BOTH
// lifecycle families (durable identity rows via Deps.Store, in-memory
// incarnation entries via mem) through one run goroutine (tap.Pump
// structural twin). Schedule/Cancel run on the CALLER's goroutine (a
// ScheduleHandle may be held by any actor cell); mem is the only state they
// share with run, guarded by mu in short critical sections.
type Engine struct {
	deps Deps

	mu  sync.Mutex
	mem map[TimerID]memTimer

	wake      chan struct{} // capacity 1, coalesced — a lost wake is harmless (the next tick recomputes due from scratch)
	stop      chan struct{}
	done      chan struct{}
	ctx       context.Context
	cancelRun context.CancelFunc
	closeOnce sync.Once

	// started guards the Start/Close lifecycle pair: double Start panics
	// loud, Close before Start returns without joining (done is only ever
	// closed by a run loop that ran). Lifecycle misuse is an assembly bug —
	// it must fail at the misuse site, not deadlock or double-fire later.
	started atomic.Bool
}

// New assembles the engine from deps and returns its two outward faces — a
// Minter (the caps surface) and the bare *Engine (for the assembly root to
// Start/Close) — never a way to reach mem or the raw TimerStore. Fail-fasts
// at assembly, not at first Schedule: every Dep is required, Revive
// included — reviving on a wake with no live actor is not an increment, it
// is the reason a timer exists at all.
func New(deps Deps) (Minter, *Engine, error) {
	switch {
	case deps.Store == nil:
		return nil, nil, errors.New("schedule: Deps.Store required")
	case deps.Fire == nil:
		return nil, nil, errors.New("schedule: Deps.Fire required")
	case deps.Host == nil:
		return nil, nil, errors.New("schedule: Deps.Host required")
	case deps.Revive == nil:
		return nil, nil, errors.New("schedule: Deps.Revive required")
	case deps.Clock == nil:
		return nil, nil, errors.New("schedule: Deps.Clock required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	e := &Engine{
		deps: deps,
		mem:  make(map[TimerID]memTimer),
		wake: make(chan struct{}, 1),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	e.ctx, e.cancelRun = context.WithCancel(context.Background())
	return &minter{engine: e}, e, nil
}

// Start launches the run-loop goroutine. It does not block; Close joins it.
func (e *Engine) Start() {
	if !e.started.CompareAndSwap(false, true) {
		// Loud, not silent: a second run loop would race fires against the
		// first and re-close done on exit. Misassembly fails at the call
		// site, not as a heisen-double-fire later.
		panic("schedule: Engine.Start called twice")
	}
	go e.run()
}

// Close stops the run loop and joins its goroutine, then never touches the
// store again (mirrors tap.Pump.Close). Safe to call once; a second call
// would panic on the closed stop channel, same discipline as Pump.
//
// Close BEFORE Start (an assembly error-path `defer Close()` that fires when
// a later wiring step failed) is legal and returns immediately: there is no
// run loop to join, and waiting on done would deadlock the teardown forever.
// The stop channel is still closed, so a Start that races/follows sees an
// already-stopped engine and its run loop exits at once.
func (e *Engine) Close() {
	e.closeOnce.Do(func() { e.cancelRun(); close(e.stop) })
	if !e.started.Load() {
		return
	}
	<-e.done
}

// mintTimerID mints a fresh, never-reused TimerID: reusing a TimerID would
// let an old fire's messages.id UNIQUE swallow a legitimate new fire. The
// engine is the ONLY minter — ScheduleReq has no ID field, so caller-supply
// is unrepresentable at compile time.
func mintTimerID() TimerID { return TimerID(uuid.NewString()) }

// schedule validates req, mints a TimerID, and routes the intent to its
// bind's home. Runs on the caller's goroutine — a short critical section
// for the incarnation path, no I/O under the lock.
//
// Unexported: author is a free parameter here, so this is the UN-WELDED face
// — the schedule-package twin of harness's bare chain, and it stays inside
// the package for the same reason the chain does. Every consumption path
// (caps-injected cell handle, host-side per-call mint at the port arm, the
// platform's own system timers) closes over Minter.Mint(author), and Mint is
// the one seam future per-author enforcement (liveSchedule membrane, storm
// quotas, principal checks) attaches to — an exported free-author method
// would be a standing structural bypass of that seam.
func (e *Engine) schedule(ctx context.Context, author actor.ActorID, req ScheduleReq) (TimerID, error) {
	if err := validateScheduleReq(req); err != nil {
		return "", err
	}

	id := mintTimerID()
	now := e.deps.Clock.Now().UnixMilli()

	switch req.Bind {
	case BindIdentity:
		row := timerspec.TimerRow{
			ID:            id,
			AuthorID:      author,
			FireAt:        req.FireAt,
			Type:          req.Type,
			Payload:       req.Payload,
			CorrelationID: req.CorrelationID,
			CreatedAt:     now,
		}
		if err := e.deps.Store.Insert(ctx, row); err != nil {
			if errors.Is(err, timerspec.ErrAuthorInactive) {
				return "", ErrAuthorInactive
			}
			if errors.Is(err, timerspec.ErrScheduleQuota) {
				return "", ErrScheduleQuota
			}
			return "", err
		}
	case BindIncarnation:
		// Attach: self-read whichever embodiment is live for
		// author RIGHT NOW. No live embodiment → nothing to weld to, and an
		// incarnation-bind timer with no incarnation is a contradiction —
		// ErrBadSchedule (this only guards "no embodiment at all"; a racing
		// caller's stale mental model of WHICH embodiment is live is fenced
		// downstream at the platform link layer, actorrt.CurrentIncarnation
		// doc).
		inc, ok := e.deps.Host.CurrentIncarnation(author)
		if !ok {
			return "", ErrBadSchedule
		}
		e.mu.Lock()
		count := 0
		for _, existing := range e.mem {
			if existing.author == author {
				count++
			}
		}
		if count >= maxMemTimersPerAuthor {
			e.mu.Unlock()
			return "", ErrScheduleQuota
		}
		e.mem[id] = memTimer{
			id:            id,
			author:        author,
			inc:           inc,
			fireAt:        req.FireAt,
			typ:           req.Type,
			payload:       req.Payload,
			correlationID: req.CorrelationID,
		}
		e.mu.Unlock()
	}

	e.wakeUp()
	return id, nil
}

// cancel deletes a pending timer/entry IFF author owns it — a single logical
// entry point over both homes. It checks mem first
// (the fast, lock-only path), then always also asks the durable store
// (CancelOwned's WHERE clause is the non-ambient author check) — a given id
// lives in exactly one home, so the other check is a harmless existed=false
// no-op, and this ordering never has to know in advance which home holds id.
// Already-fired / never-existed / not-owned are all the same silent no-op
// (fired truth is not retractable; a foreign id never leaks existence).
//
// Deadline race, DECLARED: a Cancel landing
// after fireDue has already snapshotted this id returns just as silently while
// the in-flight fire lands as truth — "cancelled, but it still rang". This is
// inherent to any timer at the deadline boundary (time.Timer.Stop's false
// return names the same window) and sits in the accepted in-flight-window
// class. No claim machinery: the handle contract is
// deliberately ack-less (error-only; existed is never surfaced), so no caller
// promise is broken — and closing the window would need cancelled-while-
// claimed tracking (mem) or a persisted claim (durable, the claim-lease
// explicitly deferred), a state machine for a promise the API never made.
//
// Unexported for the same reason as schedule: the welded face is
// ScheduleHandle.Cancel, minted per author.
func (e *Engine) cancel(ctx context.Context, author actor.ActorID, id TimerID) error {
	e.mu.Lock()
	if t, ok := e.mem[id]; ok && t.author == author {
		delete(e.mem, id)
	}
	e.mu.Unlock()

	_, err := e.deps.Store.CancelOwned(ctx, id, author)
	return err
}

// wakeUp posts a coalesced wake — non-blocking send into the capacity-1
// channel (tap.Signal.Notify structural twin). A pending wake already
// buffered absorbs this one; the run loop always recomputes the full due set
// from scratch on wake, so coalescing never loses a fire.
func (e *Engine) wakeUp() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// validateScheduleReq is the Go-error ingress gate (protocol layer, not a
// verdict — a timer is not a plane-2 concept): Bind outside the closed set,
// FireAt<=0, or an empty/reserved-prefixed Type all reject before anything
// is minted or stored. A PAST FireAt is legal — it fires
// immediately; refusing it would make "a millisecond before vs after the
// deadline" two different behaviours.
func validateScheduleReq(req ScheduleReq) error {
	switch req.Bind {
	case BindIdentity, BindIncarnation:
	default:
		return ErrBadSchedule
	}
	if req.FireAt <= 0 {
		return ErrBadSchedule
	}
	if req.Type == "" || strings.HasPrefix(req.Type, reservedTypePrefix) {
		return ErrBadSchedule
	}
	return nil
}

// run is the single engine goroutine driving both families through one
// poll/wake/fire cycle (tap.Pump structural twin). It blocks on whichever
// comes first: stop, a coalesced wake, or
// the alarm for the nearest known due instant — never a busy for{sleep}.
func (e *Engine) run() {
	defer close(e.done)

	var alarm Timer
	stopAlarm := func() {
		if alarm == nil {
			return
		}
		if !alarm.Stop() {
			// Already fired: drain the pending value so a subsequent arm
			// does not race a stale fire (Go time.Timer discipline —
			// guards against the compute-then-sleep window).
			select {
			case <-alarm.C():
			default:
			}
		}
		alarm = nil
	}
	defer stopAlarm()

	ctx := e.ctx
	for {
		nowTime := e.deps.Clock.Now()
		now := nowTime.UnixMilli()
		next, ok := e.nextFireAt(ctx)

		switch {
		case !ok:
			stopAlarm()
			select {
			case <-e.stop:
				return
			case <-e.wake:
			}
		case next > now:
			stopAlarm()
			// Absolute deadline (clock.go doc): `next` was already read as
			// part of THIS iteration's snapshot, so converting it straight
			// to time.UnixMilli — never to a relative duration — cannot go
			// stale between this line and the alarm actually being armed.
			alarm = e.deps.Clock.NewAlarm(time.UnixMilli(next))
			select {
			case <-e.stop:
				return
			case <-e.wake:
			case <-alarm.C():
			}
		default:
			stopAlarm()
			progress := e.fireDue(ctx, now, nowTime)
			if !progress {
				// Every due row/entry left in place (transient failures
				// only) — a real retry pace, never a hot loop re-querying
				// the same unchanged due set.
				alarm = e.deps.Clock.NewAlarm(nowTime.Add(backoffDuration))
				select {
				case <-e.stop:
					return
				case <-e.wake:
				case <-alarm.C():
				}
			}
		}
	}
}

// nextFireAt returns the earliest pending FireAt across BOTH families
// (ok=false when both are empty) — the run loop's sleep-until target. A
// store query fault is logged and treated as "durable family has nothing
// due" for this tick (the mem family and the next tick's retry still make
// progress; the loop must never wedge on a transient store fault).
func (e *Engine) nextFireAt(ctx context.Context) (int64, bool) {
	e.mu.Lock()
	memNext, memOK := int64(0), false
	for _, t := range e.mem {
		if !memOK || t.fireAt < memNext {
			memNext, memOK = t.fireAt, true
		}
	}
	e.mu.Unlock()

	storeNext, storeOK, err := e.deps.Store.NextFireAt(ctx)
	if err != nil {
		e.deps.Logger.Error("schedule.next_fire_at_query_failed", "err", err)
		// Degrade to a backoff-paced RETRY, never a bare wait: folding this
		// fault into "durable family has nothing due" would — on a tick where
		// the mem family is also empty — park the run loop on wake alone, so a
		// quiet channel (nobody schedules again) would never fire its durable
		// rows. Same posture the Due-fault path already has via
		// progress=false; this is its NextFireAt twin.
		storeNext, storeOK = e.deps.Clock.Now().Add(backoffDuration).UnixMilli(), true
	}

	switch {
	case memOK && storeOK:
		if memNext < storeNext {
			return memNext, true
		}
		return storeNext, true
	case memOK:
		return memNext, true
	case storeOK:
		return storeNext, true
	default:
		return 0, false
	}
}

// fireDue runs one tick's fire path over both families and reports whether
// EITHER family made progress (deleted at least one row/entry) — the run
// loop's real-retry-vs-busy-loop gate. mem access is snapshotted
// under mu and released BEFORE any I/O (fire.Append), per the concurrency
// model's snapshot-then-release discipline: Schedule/Cancel must never block
// on a fire in flight.
func (e *Engine) fireDue(ctx context.Context, now int64, nowTime time.Time) bool {
	progress := false

	e.mu.Lock()
	due := make([]memTimer, 0, len(e.mem))
	for _, t := range e.mem {
		if t.fireAt <= now {
			due = append(due, t)
		}
	}
	e.mu.Unlock()

	for _, t := range due {
		if !e.deps.Host.IsLive(t.inc) {
			// Dead (die'd or replaced) — drop, never fire. A same-id
			// successor being live does not rescue this entry (pointer-level
			// ABA guard).
			e.mu.Lock()
			delete(e.mem, t.id)
			e.mu.Unlock()
			progress = true
			continue
		}
		env := buildFireEnvelope(t.id, t.author, t.typ, t.payload, message.ID(t.correlationID), nowTime)
		callCtx, cancel := context.WithTimeout(ctx, perFireTimeout)
		err := e.deps.Fire.Append(callCtx, t.author, env)
		cancel()
		switch {
		case err == nil || errors.Is(err, ErrDuplicateFire):
			e.mu.Lock()
			delete(e.mem, t.id)
			e.mu.Unlock()
			progress = true
		case isFireRejected(err):
			e.mu.Lock()
			delete(e.mem, t.id)
			e.mu.Unlock()
			progress = true
			e.loudLog(t.id, t.author, err)
		default:
			// transient — leave the entry, retry next tick.
		}
	}

	rows, err := e.deps.Store.Due(ctx, now, 0)
	if err != nil {
		e.deps.Logger.Error("schedule.due_query_failed", "err", err)
		return progress
	}
	blockedAuthor := make(map[actor.ActorID]bool)
	for _, row := range rows {
		if blockedAuthor[row.AuthorID] {
			continue
		}
		// Wake-first ordering, welded: the identity family's
		// "no live actor" case is the NORMAL restart path, not an edge case
		// — reviving before appending is what keeps the wake from being
		// lost into a mailbox nobody is hosting yet.
		callCtx, cancel := context.WithTimeout(ctx, perFireTimeout)
		reviveErr := e.deps.Revive.EnsureLive(callCtx, row.AuthorID)
		cancel()
		if reviveErr != nil {
			err := reviveErr
			if isReviveRejected(err) {
				// Deterministic — this row can NEVER fire (its author is
				// permanently unrevivable). Dispose it, same arm
				// as a FireRejected poison row: left in place it would
				// retry hot forever and, at ≥dueBatchLimit oldest rows,
				// starve every later-due legitimate row behind it.
				reason, detail := rejectionDetails(err)
				if _, evicted, derr := e.deps.Store.MoveToDead(ctx, row.ID, timerspec.DeathReviveRejected, reason, detail, now); derr != nil {
					e.deps.Logger.Error("schedule.poison_row_delete_failed", "timer_id", string(row.ID), "err", derr)
					continue
				} else if evicted > 0 {
					e.deps.Logger.Warn("schedule.timer_dead_evicted", "count", evicted)
				}
				progress = true
				e.loudLog(row.ID, row.AuthorID, err)
				continue
			}
			blockedAuthor[row.AuthorID] = true
			continue // transient — leave the row, retry next tick.
		}
		env := buildFireEnvelope(row.ID, row.AuthorID, row.Type, row.Payload, message.ID(row.CorrelationID), nowTime)
		callCtx, cancel = context.WithTimeout(ctx, perFireTimeout)
		err := e.deps.Fire.Append(callCtx, row.AuthorID, env)
		cancel()
		switch {
		case err == nil || errors.Is(err, ErrDuplicateFire):
			if _, derr := e.deps.Store.Delete(ctx, row.ID); derr != nil {
				e.deps.Logger.Error("schedule.completed_row_delete_failed", "timer_id", string(row.ID), "err", derr)
				continue
			}
			progress = true
		case isFireRejected(err):
			reason, detail := rejectionDetails(err)
			if _, evicted, derr := e.deps.Store.MoveToDead(ctx, row.ID, timerspec.DeathFireRejected, reason, detail, now); derr != nil {
				e.deps.Logger.Error("schedule.poison_row_delete_failed", "timer_id", string(row.ID), "err", derr)
				continue
			} else if evicted > 0 {
				e.deps.Logger.Warn("schedule.timer_dead_evicted", "count", evicted)
			}
			progress = true
			e.loudLog(row.ID, row.AuthorID, err)
		default:
			// transient — leave the row, at-least-once.
		}
	}
	return progress
}

func rejectionDetails(err error) (string, string) {
	var fire FireRejected
	if errors.As(err, &fire) {
		return fire.Reason, fire.Detail
	}
	var revive ReviveRejected
	if errors.As(err, &revive) {
		return revive.Reason, revive.Detail
	}
	return "rejected", err.Error()
}

// isFireRejected reports whether err is (or wraps) a FireRejected — the
// deterministic-reject class disposed as a poison row/entry.
func isFireRejected(err error) bool {
	var rejected FireRejected
	return errors.As(err, &rejected)
}

// isReviveRejected reports whether err is (or wraps) a ReviveRejected — the
// permanently-unrevivable class, disposed via the same poison-row arm.
func isReviveRejected(err error) bool {
	var rejected ReviveRejected
	return errors.As(err, &rejected)
}

// loudLog is the disposal signal for a poison row/entry: the fire
// never became truth (it never passed the harness), so writing a system
// message to announce the disposal would be substrate ghost-writing
// protocol truth on the author's behalf — layer error. The information
// survives on the obs/log plane instead (id/author/reason visible, the
// author's own domain can re-schedule if it wants to).
func (e *Engine) loudLog(id TimerID, author actor.ActorID, err error) {
	var rejected FireRejected
	reason, detail := "", ""
	if errors.As(err, &rejected) {
		reason, detail = rejected.Reason, rejected.Detail
	}
	e.deps.Logger.Warn("schedule.fire_rejected_dropped",
		"timer_id", string(id),
		"author", string(author),
		"reason", reason,
		"detail", detail,
	)
}

// buildFireEnvelope constructs the fire envelope per the field table:
// TS is the engine's injected clock (never the pen — the pen leaves TS to
// its caller, and here the engine IS the caller); Kind is welded event
// (fire is a notification, never a request); Audience is
// self-targeted — [author], the only legal audience for a timer's own fire;
// Sender/ChannelID stay zero (the FireSink's pen welds them —
// pen.Write fail-fasts on a non-empty value, so the engine must never fill
// them); Visibility stays zero (StepNormalize defaults to public);
// ParentID/ExpiresAt are never set (fire is not a reply, and event kind
// carries no request-expiry semantics). fireMessageID's `timer:`
// namespace + the never-reused TimerID make this id permanently unique
// — crash-window replay is caught by messages.id UNIQUE, not by any
// state this engine keeps.
func buildFireEnvelope(id TimerID, author actor.ActorID, typ string, payload []byte, correlationID message.ID, now time.Time) *message.Envelope {
	if len(payload) == 0 {
		payload = []byte("{}") // proto baseline: payload={} legal, payload=null is not
	}
	return &message.Envelope{
		ID:            fireMessageID(id),
		TS:            now.UnixMilli(),
		Kind:          message.KindEvent,
		Type:          typ,
		Payload:       payload,
		Audience:      message.Audience{author},
		CorrelationID: correlationID,
	}
}

// fireMessageID derives the deterministic, permanently-unique fire message
// id from TimerID — the `timer:` namespace keeps it apart from the uuid
// space ordinary writers mint into. Without the
// namespace, or with a reused TimerID, a stale fire's UNIQUE row could
// swallow a legitimate new one.
func fireMessageID(id TimerID) message.ID { return message.ID("timer:" + string(id)) }
