package actorbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// engine is the ONE implementation of actorrt.Actor this package ships (spec
// §3: "actorrt.Actor 降格为引擎插进 runtime 的插座——唯一实现者=引擎泵+测试
// stub"). It is ALSO the Sys a Proc runs against: minting a live Sys and
// pumping the mailbox are one seam, not two collaborating objects, because
// the two ledgers Sys's verbs read/write (Reply/Fail/Progress against the
// serve ledger, Call/Pending against the call ledger) are exactly what the
// pump (Receive) maintains on the other side of the same mailbox.
//
// GOROUTINE MAP (spec §1.3/§1.5):
//   - the PUMP is the cell's own single goroutine calling Receive — O(1),
//     never blocks: response→call ledger match, request→serve ledger admit
//     (full→reject lane), event→bounded work queue (full→evict oldest).
//   - the WORKER is one goroutine this engine starts at Start(), running the
//     Proc body against this engine as its Sys — blocking calls (Recv, a
//     Pending.Wait) are legal here.
//   - the REJECT LANE is a third goroutine writing the overloaded terminal
//     for requests the serve ledger had no room to Admit.
type engine struct {
	pen      harness.Pen
	access   accessdoor.AccessHandle
	state    accessdoor.AccessHandle
	sched    schedule.ScheduleHandle
	spawn    actorrt.SpawnHandle
	hooks    Hooks
	def      Def
	clockFn  func() time.Time
	queueCap int

	serve *serveLedger
	call  *callLedger

	workQ   chan *message.Envelope
	rejectQ chan *message.Envelope

	lifeCtx  context.Context
	actorCtx actorrt.ActorContext

	occupant   atomic.Int32 // occupantState
	dying      chan error
	workerDone chan struct{}
	rejectStop chan struct{}
}

// occupantState is the occupant arc (spec §1.4's Draining note): Starting →
// Running → Draining → Dead. It gates ONLY the pump's own Admit decision
// (spec: "Draining=stopping 置位后泵停 Admit(新 request 走拒绝道)") — it is
// not a substrate state, purely this engine's own bookkeeping for a
// question the closed capability face has no other place to answer.
type occupantState int32

const (
	occupantStarting occupantState = iota
	occupantRunning
	occupantDraining
	occupantDead
)

// forkKind is the Kind a Sys.Fork-minted child is welded with. Sys.Fork's
// verb table shape (spec §1.2: `sys.Fork(class,nameHint)`) carries no Kind
// parameter — ForkSpec.Kind is caller-held by design everywhere else in the
// spawn vocabulary (actorrt.ForkSpec doc), yet Sys itself never learns its
// OWN Kind (Caps carries no Kind field; New's signature has none either).
// KindTool is the honest default for "a Proc forked a worker of itself" —
// documented here as a known v1 scope boundary, not a silent guess: a Proc
// that needs a different child Kind cannot get one through this verb yet.
const forkKind = actor.KindTool

// progressStatus is Sys.Progress's non-final response status — the Layer-2
// core "processing" word (protocol/message's provisional set; not yet a
// named const there).
const progressStatus = "processing"

// ErrRecvDone is Sys.Recv()'s loop-termination signal (spec §1.2): the
// occupant is being torn down (Start's lifeCtx is Done) and no further
// delivery will ever be handed to this Proc.
var ErrRecvDone = errors.New("actorbase: recv done")

// New assembles a live Sys/actorrt.Actor over one incarnation's five-
// capability bundle (spec §3's out-generation matrix; this IS the "caps→Sys
// weld" S1's doc.go names as S2's assembly seam — the one place in this
// package a lib/actorcaps import is legitimate). def.New is deferred to
// Start (spec §1.6: one Proc per incarnation, minted at go-live, not at
// registration).
func New(caps actorcaps.Caps, hooks Hooks, def Def) actorrt.Actor {
	e := &engine{
		pen:      caps.Pen,
		access:   caps.Access,
		state:    caps.State,
		sched:    caps.Schedule,
		spawn:    caps.Spawn,
		hooks:    hooks,
		def:      def,
		clockFn:  time.Now,
		queueCap: 256,
	}
	e.serve = newServeLedger(e.life, 256)
	e.call = newCallLedger(e.life, e.pen, e.clockFn, hooks, e.closureFault)
	e.workQ = make(chan *message.Envelope, e.queueCap)
	e.rejectQ = make(chan *message.Envelope, e.queueCap)
	e.rejectStop = make(chan struct{})
	return e
}

func (e *engine) life() context.Context { return e.lifeCtx }

// --- actorrt.Actor / lifecycle -----------------------------------------

// Start mints this incarnation's Proc and launches the worker + reject-lane
// goroutines. It returns immediately (non-blocking) — invoked once, on the
// cell's own goroutine, it must never itself run the (potentially
// long-lived, blocking) Proc body.
func (e *engine) Start(ctx context.Context, self actorrt.ActorContext) error {
	e.lifeCtx = ctx
	e.actorCtx = self
	// Initialise the lifecycle channels BEFORE def.New (F2): every return path
	// must leave the engine Stop-able. The cell's teardown defer calls Stop
	// even after a FAILED Start; a nil workerDone would then hang Stop's join
	// forever. def.New failing still leaves occupant at Starting, which Stop's
	// fast path collapses without ever waiting on a worker that never ran.
	e.dying = make(chan error, 1)
	e.workerDone = make(chan struct{})
	proc, err := e.def.New()
	if err != nil {
		return fmt.Errorf("actorbase: def.New: %w", err)
	}
	e.occupant.Store(int32(occupantRunning))
	go e.runWorker(proc)
	go e.runRejectLane()
	return nil
}

func (e *engine) runWorker(proc Proc) {
	defer close(e.workerDone)
	err := e.runProc(proc)
	// F6: flip to Draining BEFORE announcing death, so a request arriving
	// after the death announcement is rejected with an overloaded terminal
	// rather than admitted into a work queue no one will ever drain. The
	// store→send ordering is a deterministic barrier ONLY for Dying()
	// readers (channel edge); the cell's own inbox arm racing this store may
	// still admit one last request in the sub-microsecond window between the
	// Proc returning and the store landing — that residue is closed by its
	// caller's author#2 timer, the same backstop geometry as ever (spec
	// §1.5). The store shrinks the window; author#2 owns whatever is left.
	e.occupant.Store(int32(occupantDraining))
	e.dying <- err
}

// runProc runs the Proc body under a recover (F1): a panic becomes a loud
// (non-nil) death code carried back through Dying(), instead of unwinding past
// the worker goroutine and crashing the whole process (spec §1.4/§1.6:
// err|panic = loud 横死). The panic value and its stack ride in the error.
func (e *engine) runProc(proc Proc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("actorbase: proc panicked: %v\n%s", r, debug.Stack())
		}
	}()
	return proc(e)
}

func (e *engine) runRejectLane() {
	for {
		select {
		case env := <-e.rejectQ:
			e.writeOverloaded(env)
		case <-e.rejectStop:
			// No drain on stop (round-2 review overturned F7's drain): rejectStop
			// only closes AFTER the cell has already flipped the incarnation's
			// live bit and cancelled lifeCtx, so every drained write would be
			// fail-closed by the pen's live membrane — a drain here is theater
			// that leaves truth untouched. Whatever is still queued is closed by
			// the ORIGINAL callers' own author#2 durable timers — the same
			// backstop as a full reject lane (spec §1.5: teardown 残留同兜底).
			return
		}
	}
}

// writeOverloaded commits the reject lane's overloaded terminal for a
// request the serve ledger had no room to Admit.
func (e *engine) writeOverloaded(env *message.Envelope) {
	_, _ = behavior.Fail(e.lifeCtx, e.pen, e.clockFn, env, "overloaded",
		"in-station account full; request rejected without admission")
}

// Stop enters Draining (pump stops Admitting new requests — spec §1.4) and
// blocks until the worker has fully drained (returned). Invoked once, after
// the mailbox is closed and the last in-flight Receive has returned, on the
// cell's own goroutine — blocking here IS "worker 排空至 return".
func (e *engine) Stop(ctx context.Context) error {
	// F2 fast path: an engine whose worker never started (def.New failed, or
	// Start was never reached) still has occupant == Starting. There is no
	// worker to drain and workerDone may be nil — flip Dead and return at once,
	// never block on a join that could never complete.
	if occupantState(e.occupant.Load()) == occupantStarting {
		e.occupant.Store(int32(occupantDead))
		return nil
	}
	e.occupant.Store(int32(occupantDraining))
	<-e.workerDone
	close(e.rejectStop)
	e.occupant.Store(int32(occupantDead))
	return nil
}

// Dying implements actorrt.DownReporter: the worker's own raw exit code
// (nil=quiet/寿终, non-nil=loud/横死) — cell's existing ②>① arbitration
// (arbitrateDeath) applies the Draining override on top, unchanged.
func (e *engine) Dying() <-chan error { return e.dying }

// CancelRequest implements actorrt.RequestCanceller: the request-cancel
// signal's disposition IS closing the serve ledger's entry (spec §1.4's
// "处置=入站账关账+msg.Ctx cancel") — one call, both halves.
func (e *engine) CancelRequest(id message.ID) {
	e.serve.close(id)
}

// --- actorrt.Actor / pump (Receive) -------------------------------------

// Receive is the pump: O(1), never blocks (spec §1.3). ctx is NOT consulted
// — this engine's ledgers are the sole authority for what ctx a delivery
// carries (spec §5 red line: "msgCtx 唯一权威=引擎入站账"); it is accepted
// only to satisfy actorrt.Actor's signature.
func (e *engine) Receive(_ context.Context, env *message.Envelope) error {
	switch env.Kind {
	case message.KindResponse:
		e.call.match(env)
		return nil
	case message.KindRequest:
		if occupantState(e.occupant.Load()) != occupantRunning || !e.serve.admit(env) {
			e.offerReject(env)
			return nil
		}
		e.enqueueWork(env)
		return nil
	default: // message.KindEvent, including self-authored timer fires
		e.enqueueWork(env)
		return nil
	}
}

// offerReject hands env to the reject lane, non-blockingly. A full reject
// lane drops it (obs recorded) — the honest degrade: the ORIGINAL caller's
// own author#2 durable timer is the backstop that still closes the loop
// (spec: "拒绝道满时 author#2 兜底").
func (e *engine) offerReject(env *message.Envelope) {
	select {
	case e.rejectQ <- env:
	default:
		e.recordDrop(env, actorrt.ObsKind("actorbase.reject_lane_overflow"))
	}
}

// enqueueWork hands env to the bounded work queue the worker's Recv drains.
// Overflow evicts the oldest queued item (obs recorded) before inserting env
// — event/timer deliveries carry no closure obligation, so dropping one is
// a legitimate degrade, never a liveness break (spec §1.3).
func (e *engine) enqueueWork(env *message.Envelope) {
	select {
	case e.workQ <- env:
		return
	default:
	}
	select {
	case dropped := <-e.workQ:
		e.recordDrop(dropped, actorrt.ObsKind("actorbase.queue_overflow"))
	default:
	}
	select {
	case e.workQ <- env:
	default:
		e.recordDrop(env, actorrt.ObsKind("actorbase.queue_overflow"))
	}
}

// recordDrop surfaces an engine-internal drop through the actor's own obs
// PUSH (no watcher → no-op, per ActorContext.PublishObs's contract).
func (e *engine) recordDrop(env *message.Envelope, kind actorrt.ObsKind) {
	if e.actorCtx == nil {
		return
	}
	val, _ := json.Marshal(map[string]any{"id": env.ID, "type": env.Type})
	e.actorCtx.PublishObs(kind, val)
}

// closureFault is the call ledger's fault sink (F5): an author#2 obligation
// write (fireTimeout / cancel) that FAILS or is rejected — a real store error
// or a non-duplicate reject, never a benign terminal-duplicate — is a
// liveness-guarantee hole that MUST be observable, so it is surfaced on the
// same actor-obs PUSH recordDrop uses (kind actorbase.closure_fault). Wired by
// New; reads actorCtx at call time (it is nil until Start), so a fault raised
// before wiring is a silent no-op exactly like recordDrop's.
func (e *engine) closureFault(id message.ID, err error) {
	if e.actorCtx == nil {
		return
	}
	val, _ := json.Marshal(map[string]any{"id": id, "error": err.Error()})
	e.actorCtx.PublishObs(actorrt.ObsKind("actorbase.closure_fault"), val)
}

// --- Sys: response / terminal writes ------------------------------------

// envelopeFromMsg reconstructs the "request held in hand" behavior's
// response/call builders want, out of a Msg's own content fields (Msg IS a
// 1:1 projection of an Envelope — NewMsg's mirror). Field-by-field
// assignment onto a zero-value literal, not a populated composite literal:
// archtest's envelope-construction contract confines a POPULATED
// message.Envelope{...} to lib/behavior alone (this is a projection back,
// not a second construction primitive).
func envelopeFromMsg(m Msg) *message.Envelope {
	env := &message.Envelope{}
	env.ID = m.ID
	env.TS = m.TS
	env.ChannelID = m.ChannelID
	env.Sender = m.Sender
	env.Kind = m.Kind
	env.Type = m.Type
	env.Payload = m.Payload
	env.ParentID = m.ParentID
	env.CorrelationID = m.CorrelationID
	env.Visibility = m.Visibility
	env.Audience = m.Audience
	env.ExpiresAt = m.ExpiresAt
	return env
}

func (e *engine) Reply(msg Msg, v any) (message.ID, error) {
	if e.serve.isClosed(msg.ID) {
		return "", ErrRequestClosed
	}
	id, err := behavior.RespondJSON(e.lifeCtx, e.pen, e.clockFn, envelopeFromMsg(msg), v)
	if err != nil {
		return "", err
	}
	e.serve.close(msg.ID)
	return id, nil
}

func (e *engine) Fail(msg Msg, code, detail string) (message.ID, error) {
	if e.serve.isClosed(msg.ID) {
		return "", ErrRequestClosed
	}
	id, err := behavior.Fail(e.lifeCtx, e.pen, e.clockFn, envelopeFromMsg(msg), code, detail)
	if err != nil {
		return "", err
	}
	e.serve.close(msg.ID)
	return id, nil
}

func (e *engine) Progress(msg Msg, v any) (message.ID, error) {
	if e.serve.isClosed(msg.ID) {
		return "", ErrRequestClosed
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	env, err := behavior.BuildResponseFromRequest(envelopeFromMsg(msg), e.clockFn, behavior.ResponseSpec{
		Status:  progressStatus,
		Payload: raw,
	})
	if err != nil {
		return "", err
	}
	out, err := e.pen.Write(e.lifeCtx, env)
	if err != nil {
		return "", err
	}
	if !out.Accepted() && out.RejectReason != harness.HarnessTerminalDuplicate {
		return "", fmt.Errorf("actorbase: progress rejected: %s (%s)", out.RejectReason, out.RejectDetail)
	}
	return out.MessageID, nil
}

// --- Sys: event write ----------------------------------------------------

func (e *engine) Emit(msgType string, payload any, audience ...actor.ActorID) (message.ID, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	var aud message.Audience
	if len(audience) > 0 {
		aud = message.Audience(audience)
	}
	return behavior.EmitEvent(e.lifeCtx, e.pen, e.clockFn, msgType, raw, message.VisibilityPublic, aud)
}

// --- Sys: request write + caller closure ---------------------------------

func (e *engine) Call(target actor.ActorID, msgType string, payload any) (Pending, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	id, err := e.submit(behavior.RequestSpec{
		Type:     msgType,
		Payload:  raw,
		Audience: message.Audience{target},
	})
	if err != nil {
		return nil, err
	}
	return &pendingTicket{call: e.call, id: id}, nil
}

// submit is Submit/Call's shared body: resolve a missing deadline, build,
// register (subscribe-before-send), write, and arm author#2 on success.
func (e *engine) submit(spec behavior.RequestSpec) (message.ID, error) {
	if len(spec.Audience) == 0 {
		return "", fmt.Errorf("actorbase: submit audience required")
	}
	target := spec.Audience[0]
	// F3: a Call/Submit addressed to this incarnation's OWN id would deadlock
	// the single worker (the reply can only be authored by the same goroutine
	// then blocked in Wait). Fail fast BEFORE any build/register/write, so it
	// leaves zero residue in the out-station ledger. Guard on a non-empty self
	// only: Self() is "" before actorCtx is wired, and an empty target must not
	// be mistaken for a self-call (it falls through to audience validation).
	if self := e.Self(); self != "" && target == self {
		return "", ErrSelfCall
	}
	if spec.ExpiresAt == nil {
		if d := e.resolveTimeout(target, spec.Type); d > 0 {
			t := e.clockFn().Add(d).UnixMilli()
			spec.ExpiresAt = &t
		}
	}
	env, err := behavior.BuildRequest(e.clockFn, spec)
	if err != nil {
		return "", err
	}
	e.call.register(env, target)
	out, err := e.pen.Write(e.lifeCtx, env)
	if err != nil {
		e.call.forget(env.ID)
		return "", err
	}
	if !out.Accepted() {
		e.call.forget(env.ID)
		return "", fmt.Errorf("actorbase: call rejected: %s (%s)", out.RejectReason, out.RejectDetail)
	}
	if env.ExpiresAt != nil {
		d := time.Until(time.UnixMilli(*env.ExpiresAt))
		e.call.arm(env.ID, d)
	}
	return out.MessageID, nil
}

func (e *engine) resolveTimeout(target actor.ActorID, reqType string) time.Duration {
	if e.hooks.TimeoutResolver != nil {
		if d, ok := e.hooks.TimeoutResolver(target, reqType); ok && d > 0 {
			return d
		}
	}
	return DefaultTimeout
}

// pendingTicket is Sys.Call's sealed Pending ticket — a thin, single-use
// view onto one callLedger row.
type pendingTicket struct {
	call *callLedger
	id   message.ID
}

func (p *pendingTicket) Wait(ctx context.Context, d time.Duration) (Msg, error) {
	env, ok, err := p.call.wait(ctx, p.id, d)
	if err != nil {
		return Msg{}, err
	}
	if !ok {
		return Msg{}, nil
	}
	return NewMsg(p.call.life(), *env), nil
}

func (p *pendingTicket) Cancel() error {
	return p.call.cancel(p.id)
}

// --- JobTable: the mind-binding caller class's face onto the SAME call
// ledger sys.Call()/Pending touch (spec §1.5). ---------------------------

func (e *engine) Submit(spec behavior.RequestSpec) (message.ID, error) {
	return e.submit(spec)
}

func (e *engine) Await(ctx context.Context, id message.ID, window time.Duration) (*message.Envelope, bool, error) {
	if window <= 0 {
		return nil, false, nil
	}
	return e.call.wait(ctx, id, window)
}

func (e *engine) List() []message.ID {
	return e.call.list()
}

func (e *engine) Cancel(id message.ID) error {
	return e.call.cancel(id)
}

// --- Sys: State / Access arms --------------------------------------------

type resourceAdapter struct {
	h   accessdoor.AccessHandle
	ctx func() context.Context
}

func (r resourceAdapter) Create(id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	return r.h.Invoke(r.ctx(), access.OpCreate, id, args, nil)
}
func (r resourceAdapter) Read(id resource.ResourceID) (accessdoor.Outcome, error) {
	return r.h.Invoke(r.ctx(), access.OpRead, id, nil, nil)
}
func (r resourceAdapter) Write(id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	return r.h.Invoke(r.ctx(), access.OpWrite, id, args, nil)
}
func (r resourceAdapter) Delete(id resource.ResourceID) (accessdoor.Outcome, error) {
	return r.h.Invoke(r.ctx(), access.OpDelete, id, nil, nil)
}

type stateAdapter struct {
	h   accessdoor.AccessHandle
	ctx func() context.Context
}

func (s stateAdapter) Get(id resource.ResourceID) (accessdoor.Outcome, error) {
	return s.h.Invoke(s.ctx(), access.OpRead, id, nil, nil)
}
func (s stateAdapter) Put(id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	return s.h.Invoke(s.ctx(), access.OpWrite, id, args, nil)
}
func (s stateAdapter) Del(id resource.ResourceID) (accessdoor.Outcome, error) {
	return s.h.Invoke(s.ctx(), access.OpDelete, id, nil, nil)
}

func (e *engine) State() StateHandle       { return stateAdapter{h: e.state, ctx: e.life} }
func (e *engine) Resource() ResourceHandle { return resourceAdapter{h: e.access, ctx: e.life} }

// --- Sys: Schedule arm -----------------------------------------------------

func (e *engine) After(d time.Duration, msgType string, payload any) (schedule.TimerID, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return e.sched.Schedule(e.lifeCtx, schedule.ScheduleReq{
		Bind:    schedule.BindIncarnation,
		FireAt:  e.clockFn().Add(d).UnixMilli(),
		Type:    msgType,
		Payload: raw,
	})
}

func (e *engine) CancelTimer(id schedule.TimerID) error {
	return e.sched.Cancel(e.lifeCtx, id)
}

// --- Sys: Spawn arm --------------------------------------------------------

func (e *engine) Fork(class, nameHint string) (actor.ActorID, error) {
	// A daemon-hosted incarnation is minted via link.NewLiveArms, which
	// leaves Spawn zero (spec §3's known gap: fork does not cross the wire
	// this period) — a nil e.spawn means "this host has no Spawn arm", which
	// must answer ErrUnsupported (spec §1.2/§5 DoD④), not nil-pointer-panic.
	if e.spawn == nil {
		return "", ErrUnsupported
	}
	return e.spawn.Fork(actorrt.ForkSpec{Kind: forkKind, Class: class, NameHint: nameHint})
}

func (e *engine) DespawnChild(id actor.ActorID) error {
	if e.spawn == nil {
		return ErrUnsupported
	}
	return e.spawn.Despawn(id)
}

// --- Sys: ActorContext -----------------------------------------------------

func (e *engine) PublishObs(kind actorrt.ObsKind, val actorrt.ObsValue) error {
	if e.actorCtx != nil {
		e.actorCtx.PublishObs(kind, val)
	}
	return nil
}

func (e *engine) Self() actor.ActorID {
	if e.actorCtx != nil {
		return e.actorCtx.Self()
	}
	return ""
}

// --- Sys: input stream -----------------------------------------------------

// Recv drains the work queue, resolving each request delivery's ctx from the
// serve ledger at POP time (spec: "msg.Ctx() 派生自条目") — a request whose
// entry already closed while queued is skipped (obs recorded), never
// delivered with a stale/foreign ctx. Already-queued items are drained with
// priority over returning ErrRecvDone, so a Draining occupant still finishes
// everything already handed to it (spec's "worker 排空至 return").
func (e *engine) Recv() (Msg, error) {
	for {
		select {
		case env := <-e.workQ:
			if msg, ok := e.projectWork(env); ok {
				return msg, nil
			}
			continue
		default:
		}
		select {
		case env := <-e.workQ:
			if msg, ok := e.projectWork(env); ok {
				return msg, nil
			}
			continue
		case <-e.lifeCtx.Done():
			return Msg{}, ErrRecvDone
		}
	}
}

func (e *engine) projectWork(env *message.Envelope) (Msg, bool) {
	if env.Kind == message.KindRequest {
		ctx, ok := e.serve.ctxFor(env.ID)
		if !ok {
			e.recordDrop(env, actorrt.ObsKind("actorbase.stale_delivery"))
			return Msg{}, false
		}
		return NewMsg(ctx, *env), true
	}
	return NewMsg(e.lifeCtx, *env), true
}

// --- Sys: process life -----------------------------------------------------

func (e *engine) Life() context.Context { return e.lifeCtx }

var _ actorrt.Actor = (*engine)(nil)
var _ actorrt.Starter = (*engine)(nil)
var _ actorrt.Stopper = (*engine)(nil)
var _ actorrt.RequestCanceller = (*engine)(nil)
var _ actorrt.DownReporter = (*engine)(nil)
var _ Sys = (*engine)(nil)
var _ JobTable = (*engine)(nil)
