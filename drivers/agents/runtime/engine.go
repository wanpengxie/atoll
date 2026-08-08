package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

const (
	ticketPending uint32 = iota
	ticketAccepted
	ticketAbandoned
)

type admissionTicket struct {
	id    uint64
	state atomic.Uint32
	ack   chan struct{}
}

type commandKind uint8

const (
	commandStart commandKind = iota
	commandControl
	commandTerminate
	commandEnsure
)

type runtimeCommand struct {
	kind    commandKind
	ticket  *admissionTicket
	start   base.StartCommand
	control base.ControlCommand
	op      base.OpID
}

type shellPhase uint8

const (
	shellOpen shellPhase = iota
	shellPoisoned
	shellClosed
)

type emergencyHandle struct {
	worker driverproto.Worker
	sink   *eventSink
	cancel context.CancelFunc
}
type runtimeFuse struct {
	mu     sync.Mutex
	phase  shellPhase
	handle *emergencyHandle
}

type Engine struct {
	adapter    driverproto.Adapter
	spec       driverproto.ProviderSpec
	policy     Policy
	deps       base.RuntimeDeps
	events     base.RuntimeEvents
	root       context.Context
	cancel     context.CancelFunc
	commands   chan runtimeCommand
	internal   chan any
	eventq     *unboundedQueue[eventEnvelope]
	outbox     *unboundedQueue[func(base.RuntimeEvents)]
	fuse       runtimeFuse
	nextTicket atomic.Uint64
	closeOnce  sync.Once
	instance   uint64
}

var nextInstance atomic.Uint64

func New(adapter driverproto.Adapter, spec driverproto.ProviderSpec, policy Policy, deps base.RuntimeDeps, seed []byte, events base.RuntimeEvents) (*Engine, error) {
	if adapter == nil || events == nil {
		return nil, errors.New("agent/runtime: adapter and events required")
	}
	parent := deps.Parent
	if parent == nil {
		parent = context.Background()
	}
	root, cancel := context.WithCancel(parent)
	e := &Engine{
		adapter: adapter, spec: spec, policy: policy.normalized(), deps: deps, events: events,
		root: root, cancel: cancel, commands: make(chan runtimeCommand, 64), internal: make(chan any, 256),
		eventq: newQueue[eventEnvelope](), outbox: newQueue[func(base.RuntimeEvents)](), instance: nextInstance.Add(1),
	}
	go e.outputLoop()
	go e.supervise(append([]byte(nil), seed...))
	return e, nil
}

func (e *Engine) Start(c base.StartCommand) error {
	if c.Op == "" {
		return base.ErrRuntimeState
	}
	return e.admit(runtimeCommand{kind: commandStart, start: cloneStart(c)})
}
func (e *Engine) Control(c base.ControlCommand) error {
	if c.Op == "" || c.Kind > base.RuntimeInterrupt {
		return base.ErrRuntimeState
	}
	return e.admit(runtimeCommand{kind: commandControl, control: cloneControl(c)})
}
func (e *Engine) Terminate() error { return e.admit(runtimeCommand{kind: commandTerminate}) }
func (e *Engine) EnsureReady(op base.OpID) error {
	if op == "" {
		return base.ErrRuntimeState
	}
	return e.admit(runtimeCommand{kind: commandEnsure, op: op})
}

func (e *Engine) admit(c runtimeCommand) error {
	e.fuse.mu.Lock()
	phase := e.fuse.phase
	open := phase == shellOpen
	e.fuse.mu.Unlock()
	if !open {
		if phase == shellClosed {
			return base.ErrRuntimeClosed
		}
		return base.ErrRuntimeUnavailable
	}
	t := &admissionTicket{id: e.nextTicket.Add(1), ack: make(chan struct{}, 1)}
	c.ticket = t
	timer := time.NewTimer(e.policy.CommandAdmission)
	defer timer.Stop()
	select {
	case e.commands <- c:
	case <-timer.C:
		t.state.CompareAndSwap(ticketPending, ticketAbandoned)
		e.poison()
		return base.ErrRuntimeUnavailable
	case <-e.root.Done():
		return base.ErrRuntimeUnavailable
	}
	select {
	case <-t.ack:
		return nil
	case <-timer.C:
		t.state.CompareAndSwap(ticketPending, ticketAbandoned)
		e.poison()
		return base.ErrRuntimeUnavailable
	case <-e.root.Done():
		return base.ErrRuntimeUnavailable
	}
}

func (e *Engine) poison() { e.shutdown(shellPoisoned) }
func (e *Engine) Close()  { e.closeOnce.Do(func() { e.shutdown(shellClosed) }) }
func (e *Engine) shutdown(want shellPhase) {
	e.fuse.mu.Lock()
	if e.fuse.phase == shellClosed || (e.fuse.phase == shellPoisoned && want == shellPoisoned) {
		e.fuse.mu.Unlock()
		return
	}
	e.fuse.phase = want
	h := e.fuse.handle
	if h != nil {
		h.sink.seal()
		h.cancel()
	}
	e.eventq.seal()
	e.outbox.seal()
	e.cancel()
	e.fuse.mu.Unlock()
	if h != nil {
		h.worker.Retire()
	}
}

func (e *Engine) install(h *emergencyHandle) bool {
	e.fuse.mu.Lock()
	if e.fuse.phase != shellOpen {
		e.fuse.mu.Unlock()
		h.sink.seal()
		h.cancel()
		h.worker.Retire()
		return false
	}
	e.fuse.handle = h
	e.fuse.mu.Unlock()
	return true
}
func (e *Engine) clearInstalled(h *emergencyHandle) {
	e.fuse.mu.Lock()
	if e.fuse.handle == h {
		e.fuse.handle = nil
	}
	e.fuse.mu.Unlock()
}

func (e *Engine) outputLoop() {
	for {
		select {
		case <-e.root.Done():
			return
		case <-e.outbox.wake:
			for {
				f, ok := e.outbox.pop()
				if !ok {
					break
				}
				f(e.events)
			}
		}
	}
}

type eventEnvelope struct {
	generation uint64
	event      driverproto.DriverEvent
}
type eventSink struct {
	generation uint64
	q          *unboundedQueue[eventEnvelope]
	mu         sync.Mutex
	sealed     bool
	active     driverproto.WorkerTurnTarget
	liveness   uint64
}

func (s *eventSink) Publish(v driverproto.DriverEvent) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v == nil || s.sealed {
		return false
	}
	if target, ok := eventTarget(v); ok && target == s.active {
		s.liveness++
	}
	return s.q.push(eventEnvelope{generation: s.generation, event: v})
}
func (s *eventSink) seal() { s.mu.Lock(); s.sealed = true; s.mu.Unlock() }
func (s *eventSink) activate(target driverproto.WorkerTurnTarget) uint64 {
	s.mu.Lock()
	s.active = target
	s.liveness++
	rev := s.liveness
	s.mu.Unlock()
	return rev
}
func (s *eventSink) deactivate(target driverproto.WorkerTurnTarget) {
	s.mu.Lock()
	if s.active == target {
		s.active = driverproto.WorkerTurnTarget{}
	}
	s.mu.Unlock()
}
func (s *eventSink) pulse(target driverproto.WorkerTurnTarget) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.liveness, s.active == target
}
func eventTarget(v driverproto.DriverEvent) (driverproto.WorkerTurnTarget, bool) {
	switch x := v.(type) {
	case driverproto.Activity:
		return x.Target, true
	case driverproto.Tool:
		return x.Target, true
	case driverproto.TurnEnded:
		return x.Target, true
	default:
		return driverproto.WorkerTurnTarget{}, false
	}
}

type workerPhase uint8

const (
	workerCreated workerPhase = iota
	workerOpening
	workerReady
	workerDraining
	workerReaping
	workerReapStalled
)

type turnPhase uint8

const (
	turnStarting turnPhase = iota
	turnActive
	turnTerminal
)

type runtimeTurn struct {
	serial          uint64
	canonical       base.TurnID
	target          driverproto.WorkerTurnTarget
	startOp         base.OpID
	phase           turnPhase
	life            context.Context
	cancel          context.CancelFunc
	scope           base.EffectScope
	transition      *scopeTransition
	effects         map[effectKey]*effectRow
	eventsSeen      bool
	startAccepted   bool
	startResultSeen bool
	command         base.StartCommand
	resumeRetried   bool
	watchRev        uint64
	pendingEffects  int
	terminalEvent   *driverproto.TurnEnded
	terminalOps     []*controlAction
	terminalSent    bool
}
type scopeTransition struct {
	op        base.OpID
	old       base.EffectScope
	candidate base.EffectScope
	held      []*effectRequest
	heldTools []base.ToolEvent
}
type effectKey struct {
	target   driverproto.WorkerTurnTarget
	callID   driverproto.ProviderToolCallID
	resource bool
}
type effectRow struct {
	fingerprint [32]byte
	done        bool
	dispatched  bool
	tool        driverproto.ToolResult
	resource    driverproto.ResourceResult
	waiters     []*effectRequest
}
type workerGeneration struct {
	id             uint64
	phase          workerPhase
	worker         driverproto.Worker
	host           *workerHost
	sink           *eventSink
	life           context.Context
	cancel         context.CancelFunc
	emergency      *emergencyHandle
	turn           *runtimeTurn
	controls       []*controlAction
	controlRunning bool
	retired        bool
	retirement     *retireDecision
	stagedSeed     []byte
	resumeRetry    bool
}
type retireDecision struct {
	cause      string
	reportLoss bool
}
type startIntent struct {
	command       base.StartCommand
	resumeRetried bool
}
type controlAction struct {
	command    base.ControlCommand
	generation uint64
	serial     uint64
	target     driverproto.WorkerTurnTarget
}
type runtimeBook struct {
	epoch          uint64
	nextGeneration uint64
	nextAttempt    driverproto.AttemptToken
	nextTurn       uint64
	worker         *workerGeneration
	nextStart      *startIntent
	ensure         []base.OpID
	seed           []byte
}

type openDone struct {
	gen    uint64
	result driverproto.OpenResult
}
type startDone struct {
	gen, serial uint64
	attempt     driverproto.AttemptToken
	result      driverproto.StartResult
}
type controlDone struct {
	gen, serial uint64
	action      *controlAction
	result      driverproto.ControlResult
	safety      bool
}
type reapedDone struct {
	gen       uint64
	emergency *emergencyHandle
}
type timerKind uint8

const (
	timerStarted timerKind = iota
	timerWatchdog
	timerSafety
	timerDrain
	timerReap
)

type timerDone struct {
	kind             timerKind
	gen, serial, rev uint64
}
type effectDone struct {
	request  *effectRequest
	tool     driverproto.ToolResult
	resource driverproto.ResourceResult
}

func (e *Engine) supervise(seed []byte) {
	b := runtimeBook{seed: seed}
	for {
		select {
		case <-e.root.Done():
			return
		case c := <-e.commands:
			if !c.ticket.state.CompareAndSwap(ticketPending, ticketAccepted) {
				continue
			}
			effect := e.acceptCommand(&b, c)
			select {
			case c.ticket.ack <- struct{}{}:
			default:
			}
			if effect != nil {
				effect()
			}
		case <-e.eventq.wake:
			for {
				v, ok := e.eventq.pop()
				if !ok {
					break
				}
				e.applyEvent(&b, v)
			}
		case v := <-e.internal:
			e.applyInternal(&b, v)
		}
	}
}

func (e *Engine) acceptCommand(b *runtimeBook, c runtimeCommand) func() {
	switch c.kind {
	case commandStart:
		if b.worker != nil && b.worker.phase == workerReapStalled {
			return func() { e.turnRejected(c.start.Op, "provider_crash", "previous worker has not reaped") }
		}
		if err := validateInput(c.start.Input, e.policy); err != nil {
			return func() { e.turnRejected(c.start.Op, "input_too_large", err.Error()) }
		}
		if b.nextStart != nil || (b.worker != nil && b.worker.turn != nil) {
			return func() { e.turnRejected(c.start.Op, "provider_failed", "turn already in flight") }
		}
		b.nextStart = &startIntent{command: c.start}
		return func() { e.ensureWorker(b) }
	case commandControl:
		return func() { e.acceptControl(b, c.control) }
	case commandTerminate:
		b.nextStart = nil
		if b.worker != nil {
			g := b.worker
			e.claimRetire(g, false, "manual")
			return func() { e.finishRetire(g) }
		}
	case commandEnsure:
		if b.worker != nil && b.worker.phase == workerReapStalled {
			return func() { e.readyDone(c.op, false, "previous worker has not reaped") }
		}
		if b.worker != nil && b.worker.phase == workerReady && !b.worker.retired {
			return func() { e.readyDone(c.op, true, "") }
		}
		b.ensure = append(b.ensure, c.op)
		return func() { e.ensureWorker(b) }
	}
	return nil
}

func (e *Engine) ensureWorker(b *runtimeBook) {
	if b.worker != nil {
		if b.worker.phase == workerReady {
			e.maybeStart(b)
		} else if b.worker.phase == workerCreated {
			e.openGeneration(b.worker, b.seed)
		}
		return
	}
	b.nextGeneration++
	genID := b.nextGeneration
	life, cancel := context.WithCancel(e.root)
	sink := &eventSink{generation: genID, q: e.eventq}
	host := &workerHost{engine: e, generation: genID, life: life, sink: sink, logger: safeLogger(nil), ready: make(chan struct{})}
	w, err := e.adapter.NewWorker(host)
	if err != nil {
		cancel()
		sink.seal()
		e.openFailed(b, "provider worker: "+err.Error())
		return
	}
	if w == nil || w.Reaped() == nil {
		cancel()
		sink.seal()
		if w != nil {
			w.Retire()
		}
		e.openFailed(b, "provider returned invalid worker")
		return
	}
	h := &emergencyHandle{worker: w, sink: sink, cancel: cancel}
	g := &workerGeneration{id: genID, phase: workerCreated, worker: w, host: host, sink: sink, life: life, cancel: cancel, emergency: h}
	b.worker = g
	if !e.install(h) {
		return
	}
	e.openGeneration(g, b.seed)
	go func() { <-w.Reaped(); e.clearInstalled(h); e.sendInternal(reapedDone{gen: genID, emergency: h}) }()
}

func (e *Engine) openGeneration(g *workerGeneration, seed []byte) {
	if g == nil || g.phase != workerCreated || g.retired {
		return
	}
	g.phase = workerOpening
	g.stagedSeed = nil
	ctx, cc := context.WithTimeout(g.life, e.policy.OpenCall)
	req := driverproto.OpenRequest{ResumeSeed: append([]byte(nil), seed...)}
	go func() { defer cc(); r := g.worker.Open(ctx, req); e.sendInternal(openDone{gen: g.id, result: r}) }()
}

func (e *Engine) applyInternal(b *runtimeBook, v any) {
	switch x := v.(type) {
	case openDone:
		e.onOpenDone(b, x)
	case startDone:
		e.onStartDone(b, x)
	case controlDone:
		e.onControlDone(b, x)
	case reapedDone:
		e.onReaped(b, x)
	case timerDone:
		e.onTimer(b, x)
	case *effectRequest:
		e.onEffectRequest(b, x)
	case effectDone:
		e.onEffectDone(b, x)
	}
}

func (e *Engine) onOpenDone(b *runtimeBook, d openDone) {
	g := b.worker
	if g == nil || g.id != d.gen || g.phase != workerOpening {
		return
	}
	if err := d.result.Validate(); err != nil {
		e.protocolFault(b, g, err.Error())
		return
	}
	switch d.result.Verdict() {
	case driverproto.OpenReady:
		g.phase = workerReady
		if len(g.stagedSeed) > 0 {
			b.seed = append([]byte(nil), g.stagedSeed...)
			e.seedUpdated(b.seed)
		}
		for _, op := range b.ensure {
			e.readyDone(op, true, "")
		}
		b.ensure = nil
		e.maybeStart(b)
	case driverproto.OpenResumeInvalid:
		if !g.resumeRetry && len(b.seed) > 0 {
			g.resumeRetry = true
			b.seed = nil
			e.retire(b, g, false, "resume-invalid")
			return
		}
		e.openFailed(b, d.result.Detail())
		e.retire(b, g, false, "open-failed")
	case driverproto.OpenRejected, driverproto.OpenAmbiguous:
		e.openFailed(b, d.result.Detail())
		if d.result.Disposition() == driverproto.RetireWorker {
			e.retire(b, g, false, "open-failed")
		} else {
			g.phase = workerCreated
		}
	}
}

func (e *Engine) openFailed(b *runtimeBook, detail string) {
	if detail == "" {
		detail = "provider open failed"
	}
	if b.nextStart != nil {
		e.turnRejected(b.nextStart.command.Op, "provider_crash", detail)
		b.nextStart = nil
	}
	for _, op := range b.ensure {
		e.readyDone(op, false, detail)
	}
	b.ensure = nil
}

func (e *Engine) maybeStart(b *runtimeBook) {
	g := b.worker
	in := b.nextStart
	if g == nil || g.phase != workerReady || g.turn != nil || in == nil {
		return
	}
	b.nextStart = nil
	b.nextAttempt++
	b.nextTurn++
	life, cancel := context.WithCancel(g.life)
	t := &runtimeTurn{serial: b.nextTurn, startOp: in.command.Op, phase: turnStarting, life: life, cancel: cancel, scope: in.command.Scope, effects: map[effectKey]*effectRow{}, target: driverproto.WorkerTurnTarget{Attempt: b.nextAttempt}, command: cloneStart(in.command), resumeRetried: in.resumeRetried}
	g.turn = t
	req := driverproto.StartRequest{Attempt: b.nextAttempt, Life: life, Messages: translateMessages(in.command.Input.Messages), Background: translateContext(in.command.Input.Background)}
	ctx, cc := context.WithTimeout(g.life, e.policy.StartCall)
	go func(gen, serial uint64, attempt driverproto.AttemptToken) {
		defer cc()
		r := g.worker.Start(ctx, req)
		e.sendInternal(startDone{gen: gen, serial: serial, attempt: attempt, result: r})
	}(g.id, t.serial, t.target.Attempt)
}

func (e *Engine) onStartDone(b *runtimeBook, d startDone) {
	g, t := currentTurn(b, d.gen, d.serial)
	if t == nil || t.target.Attempt != d.attempt {
		return
	}
	if err := d.result.Validate(); err != nil {
		e.protocolFault(b, g, err.Error())
		return
	}
	t.startResultSeen = true
	if d.result.Verdict() == driverproto.StartResumeInvalid {
		if t.eventsSeen || t.resumeRetried {
			e.protocolFault(b, g, "resume invalid after attempt event")
			return
		}
		command := t.command
		t.cancel()
		g.turn = nil
		if len(b.seed) > 0 {
			b.seed = nil
			b.nextStart = &startIntent{command: command, resumeRetried: true}
			e.retire(b, g, false, "resume-invalid")
			return
		}
		e.turnRejected(command.Op, "provider_failed", d.result.Detail())
		e.retire(b, g, false, "resume-invalid")
		return
	}
	if d.result.Certainty() == driverproto.Ambiguous {
		if t.phase == turnTerminal {
			// Natural terminal already won the user-visible first claim.
		} else if t.canonical == "" && !t.eventsSeen {
			e.turnRejected(t.startOp, failureCode(d.result.Failure()), d.result.Detail())
		} else {
			e.providerLost(t.canonical, base.LostCrash, d.result.Detail())
		}
		e.retire(b, g, false, "ambiguous")
		return
	}
	if d.result.Verdict() == driverproto.StartRejected {
		if t.canonical == "" && !t.eventsSeen {
			e.turnRejected(t.startOp, failureCode(d.result.Failure()), d.result.Detail())
			t.cancel()
			g.turn = nil
		} else {
			e.protocolFault(b, g, "start rejected after attempt event")
			return
		}
		if d.result.Disposition() == driverproto.RetireWorker {
			e.retire(b, g, t.canonical != "", "start-rejected")
		}
		return
	}
	t.startAccepted = true
	if d.result.Disposition() == driverproto.RetireWorker {
		if t.canonical == "" {
			e.turnRejected(t.startOp, "provider_crash", "worker retired after accepting start")
		} else {
			e.providerLost(t.canonical, base.LostCrash, "worker retired after accepting start")
		}
		e.retire(b, g, false, "start-disposition")
		return
	}
	if t.canonical == "" {
		e.arm(timerStarted, g.id, t.serial, 0, e.policy.Started)
	} else if t.phase == turnTerminal {
		e.finalizeTurnEnded(b, g, t)
	}
}

func (e *Engine) acceptControl(b *runtimeBook, c base.ControlCommand) {
	g := b.worker
	if c.Kind == base.RuntimeSteer && (c.Content == nil || strings.TrimSpace(c.Content.Text) == "") {
		e.controlResult(c.Op, c.Target, base.ControlEmptyInput, "steer requires input")
		return
	}
	if c.Kind == base.RuntimeSteer {
		if err := validateControlInput(*c.Content, e.policy); err != nil {
			e.controlResult(c.Op, c.Target, base.ControlInputTooLarge, err.Error())
			return
		}
	}
	if g == nil || g.turn == nil || g.turn.canonical == "" || g.turn.phase != turnActive {
		e.controlResult(c.Op, c.Target, base.ControlNoActiveTurn, "no active turn")
		return
	}
	if c.Target == "" || c.Target != g.turn.canonical {
		e.controlResult(c.Op, c.Target, base.ControlMismatch, "turn target mismatch")
		return
	}
	if c.Kind == base.RuntimeSteer && !e.spec.Capabilities.Steer {
		e.controlResult(c.Op, c.Target, base.ControlNotSteerable, "steer unsupported")
		return
	}
	if c.Kind == base.RuntimeInterrupt && !e.spec.Capabilities.Interrupt {
		e.controlResult(c.Op, c.Target, base.ControlRPCError, "interrupt unsupported")
		return
	}
	a := &controlAction{command: c, generation: g.id, serial: g.turn.serial, target: g.turn.target}
	g.controls = append(g.controls, a)
	e.dispatchControl(g)
}

func (e *Engine) dispatchControl(g *workerGeneration) {
	if g.controlRunning || len(g.controls) == 0 || g.retired {
		return
	}
	a := g.controls[0]
	g.controlRunning = true
	kind := driverproto.ControlInterrupt
	var msg *driverproto.DriverMessage
	if a.command.Kind == base.RuntimeSteer {
		kind = driverproto.ControlSteer
		m := translateMessage(*a.command.Content)
		msg = &m
		g.turn.transition = &scopeTransition{op: a.command.Op, old: g.turn.scope, candidate: a.command.Scope}
	}
	req := driverproto.ControlRequest{Kind: kind, Target: a.target, Message: msg}
	ctx, cancel := context.WithTimeout(g.life, e.policy.ControlCall)
	go func() {
		defer cancel()
		r := g.worker.Control(ctx, req)
		e.sendInternal(controlDone{gen: a.generation, serial: a.serial, action: a, result: r})
	}()
}

func (e *Engine) onControlDone(b *runtimeBook, d controlDone) {
	g, t := currentTurn(b, d.gen, d.serial)
	if g == nil {
		return
	}
	if d.safety {
		e.arm(timerDrain, g.id, d.serial, 0, e.policy.TerminalDrain)
		return
	}
	if len(g.controls) == 0 || g.controls[0] != d.action {
		return
	}
	g.controls = g.controls[1:]
	g.controlRunning = false
	if err := d.result.Validate(); err != nil {
		e.controlResult(d.action.command.Op, d.action.command.Target, base.ControlRPCError, err.Error())
		e.protocolFault(b, g, err.Error())
		return
	}
	if d.action.command.Kind == base.RuntimeInterrupt && d.result.Verdict() == driverproto.ControlNotSteerable {
		e.controlResult(d.action.command.Op, d.action.command.Target, base.ControlRPCError, "invalid interrupt verdict")
		e.protocolFault(b, g, "not-steerable returned for interrupt")
		return
	}
	verdict := base.ControlRPCError
	switch d.result.Verdict() {
	case driverproto.ControlAccepted:
		verdict = base.ControlAccepted
	case driverproto.ControlNotSteerable:
		verdict = base.ControlNotSteerable
	case driverproto.ControlTargetGone:
		if t == nil || t.phase == turnTerminal {
			verdict = base.ControlNoActiveTurn
		}
	}
	if t != nil && t.transition != nil && t.transition.op == d.action.command.Op {
		tr := t.transition
		t.transition = nil
		if verdict == base.ControlAccepted {
			t.scope = tr.candidate
		}
		e.controlResult(d.action.command.Op, d.action.command.Target, verdict, d.result.Detail())
		for _, r := range tr.held {
			if d.result.Certainty() == driverproto.Ambiguous {
				r.replyError("effect ownership ambiguous")
			} else {
				e.admitEffect(b, t, r)
			}
		}
		if d.result.Certainty() != driverproto.Ambiguous {
			for _, tool := range tr.heldTools {
				e.tool(t.canonical, tool)
			}
		}
	} else {
		e.controlResult(d.action.command.Op, d.action.command.Target, verdict, d.result.Detail())
	}
	if d.result.Certainty() == driverproto.Ambiguous {
		e.retire(b, g, t != nil && t.canonical != "", "control-ambiguous")
		return
	}
	if d.result.Verdict() == driverproto.ControlTargetGone && t != nil && t.phase == turnActive {
		e.retire(b, g, true, "target-gone")
		return
	}
	if d.result.Disposition() == driverproto.RetireWorker {
		e.retire(b, g, t != nil && t.canonical != "", "control-disposition")
		return
	}
	if d.action.command.Kind == base.RuntimeInterrupt && verdict == base.ControlAccepted && t != nil {
		e.arm(timerDrain, g.id, t.serial, 0, e.policy.InterruptEnded)
	}
	e.dispatchControl(g)
}

func (e *Engine) applyEvent(b *runtimeBook, env eventEnvelope) {
	g := b.worker
	if g == nil || g.id != env.generation {
		return
	}
	switch v := env.event.(type) {
	case driverproto.TurnStarted:
		t := g.turn
		if t == nil || t.phase != turnStarting || v.Target.Attempt != t.target.Attempt || !v.Target.Valid() {
			return
		}
		t.eventsSeen = true
		t.target = v.Target
		t.phase = turnActive
		g.host.activate(v.Target)
		t.watchRev = g.sink.activate(v.Target)
		t.canonical = base.TurnID(fmt.Sprintf("turn-%d-%d", e.instance, t.serial))
		e.turnStarted(t.startOp, t.canonical)
		e.arm(timerWatchdog, g.id, t.serial, t.watchRev, e.policy.Watchdog)
	case driverproto.Activity:
		if t := matchingTurn(g, v.Target); t != nil && t.phase == turnActive {
			t.eventsSeen = true
			t.watchRev, _ = g.sink.pulse(t.target)
			e.arm(timerWatchdog, g.id, t.serial, t.watchRev, e.policy.Watchdog)
		}
	case driverproto.Tool:
		if t := matchingTurn(g, v.Target); t != nil && t.phase == turnActive {
			if v.CallID == "" || v.Name == "" {
				e.protocolFault(b, g, "invalid tool event")
				return
			}
			t.eventsSeen = true
			t.watchRev, _ = g.sink.pulse(t.target)
			e.arm(timerWatchdog, g.id, t.serial, t.watchRev, e.policy.Watchdog)
			ev := base.ToolEvent{CallID: v.CallID, Name: v.Name, Detail: v.Detail}
			if v.Phase == driverproto.ToolEnded {
				ev.Phase = "ended"
			} else {
				ev.Phase = "started"
			}
			if v.Status == driverproto.ToolStatusCompleted {
				ev.Status = "completed"
			} else if v.Status == driverproto.ToolStatusFailed {
				ev.Status = "failed"
			}
			if t.transition != nil {
				t.transition.heldTools = append(t.transition.heldTools, ev)
			} else {
				e.tool(t.canonical, ev)
			}
		}
	case driverproto.TurnEnded:
		e.onTurnEnded(b, g, v)
	case driverproto.SeedUpdated:
		if g.phase == workerOpening {
			g.stagedSeed = append([]byte(nil), v.Value...)
		} else if !g.retired {
			b.seed = append([]byte(nil), v.Value...)
			e.seedUpdated(b.seed)
		}
	case driverproto.WorkerEnded:
		detail := v.Detail
		if detail == "" {
			detail = "provider worker ended"
		}
		if g.turn != nil && g.turn.phase != turnTerminal {
			if g.turn.canonical != "" {
				e.providerLost(g.turn.canonical, base.LostCrash, detail)
			} else {
				e.turnRejected(g.turn.startOp, "provider_crash", detail)
			}
		} else if g.phase == workerOpening {
			e.openFailed(b, detail)
		}
		e.retire(b, g, false, "worker-ended")
	case driverproto.Diagnostic:
		e.safeLog().Log(e.root, slog.LevelDebug, "provider diagnostic", "code", v.Code, "detail", sanitize(v.Detail))
	}
}

func (e *Engine) onTurnEnded(b *runtimeBook, g *workerGeneration, v driverproto.TurnEnded) {
	t := matchingTurn(g, v.Target)
	if t == nil || t.canonical == "" || t.phase == turnTerminal {
		return
	}
	t.eventsSeen = true
	t.phase = turnTerminal
	g.host.deactivate(t.target)
	g.sink.deactivate(t.target)
	t.cancel()
	t.terminalOps = append([]*controlAction(nil), g.controls...)
	vv := v
	t.terminalEvent = &vv
	if t.transition != nil {
		for _, r := range t.transition.held {
			e.rejectEffect(t, r, "turn ended before effect ownership resolved")
		}
		t.transition = nil
	}
	g.controls = nil
	g.controlRunning = false
	e.finalizeTurnEnded(b, g, t)
}

func (e *Engine) finalizeTurnEnded(b *runtimeBook, g *workerGeneration, t *runtimeTurn) {
	if t == nil || t.terminalEvent == nil || t.pendingEffects != 0 {
		return
	}
	v := *t.terminalEvent
	status := base.TurnStatusOK
	if v.Status == driverproto.TurnFailed {
		status = base.TurnStatusFailed
	} else if v.Status == driverproto.TurnInterrupted {
		status = base.TurnStatusInterrupted
	}
	if !t.terminalSent {
		e.turnEnded(t.canonical, status, v.FinalText, v.ErrorDetail)
		for _, a := range t.terminalOps {
			e.controlResult(a.command.Op, a.command.Target, base.ControlNoActiveTurn, "turn already ended")
		}
		t.terminalSent = true
	}
	if !t.startResultSeen {
		return
	}
	g.turn = nil
	if g.phase == workerDraining {
		e.finishRetire(g)
	} else {
		g.phase = workerReady
		e.maybeStart(b)
	}
}

func (e *Engine) onTimer(b *runtimeBook, d timerDone) {
	g, t := currentTurn(b, d.gen, d.serial)
	switch d.kind {
	case timerStarted:
		if t != nil && t.phase == turnStarting && t.canonical == "" {
			e.turnRejected(t.startOp, "provider_timeout", "turn did not start")
			e.retire(b, g, false, "start-timeout")
		}
	case timerWatchdog:
		pulse, active := uint64(0), false
		if g != nil && t != nil {
			pulse, active = g.sink.pulse(t.target)
		}
		if t != nil && t.phase == turnActive && active && pulse == d.rev {
			g.phase = workerDraining
			if e.spec.Capabilities.Interrupt {
				req := driverproto.ControlRequest{Kind: driverproto.ControlInterrupt, Target: t.target}
				ctx, cancel := context.WithTimeout(g.life, e.policy.SafetyInterrupt)
				go func() {
					defer cancel()
					r := g.worker.Control(ctx, req)
					e.sendInternal(controlDone{gen: g.id, serial: t.serial, result: r, safety: true})
				}()
				e.arm(timerSafety, g.id, t.serial, 0, e.policy.SafetyInterrupt)
			} else {
				e.arm(timerDrain, g.id, t.serial, 0, e.policy.TerminalDrain)
			}
		}
	case timerSafety:
		if t != nil && g.phase == workerDraining {
			e.arm(timerDrain, g.id, t.serial, 0, e.policy.TerminalDrain)
		}
	case timerDrain:
		if t != nil && t.phase != turnTerminal && g.phase == workerDraining {
			e.providerLost(t.canonical, base.LostTimeout, "provider turn inactive")
			e.finishRetire(g)
		}
	case timerReap:
		if g != nil && g.phase == workerReaping {
			g.phase = workerReapStalled
			e.openFailed(b, "provider worker did not reap")
		}
	}
}

func (e *Engine) retire(b *runtimeBook, g *workerGeneration, reportLoss bool, cause string) {
	_ = b
	if !e.claimRetire(g, reportLoss, cause) {
		return
	}
	e.finishRetire(g)
}
func (e *Engine) claimRetire(g *workerGeneration, reportLoss bool, cause string) bool {
	if g == nil || g.retired {
		return false
	}
	g.retired = true
	g.retirement = &retireDecision{cause: cause, reportLoss: reportLoss}
	g.phase = workerDraining
	if g.turn != nil {
		if reportLoss && g.turn.canonical != "" {
			e.providerLost(g.turn.canonical, base.LostCrash, sanitize(cause))
		}
		g.turn.cancel()
		g.host.deactivate(g.turn.target)
		g.sink.deactivate(g.turn.target)
		if g.turn.transition != nil {
			for _, r := range g.turn.transition.held {
				e.rejectEffect(g.turn, r, "worker retired")
			}
		}
	}
	return true
}
func (e *Engine) finishRetire(g *workerGeneration) {
	if g.phase == workerReaping || g.phase == workerReapStalled {
		return
	}
	g.sink.seal()
	g.cancel()
	g.phase = workerReaping
	g.worker.Retire()
	e.arm(timerReap, g.id, 0, 0, e.policy.Reap)
}
func (e *Engine) onReaped(b *runtimeBook, d reapedDone) {
	g := b.worker
	if g == nil || g.id != d.gen {
		return
	}
	b.worker = nil
	if e.root.Err() != nil {
		return
	}
	if b.nextStart != nil || len(b.ensure) > 0 {
		e.ensureWorker(b)
	}
}
func (e *Engine) protocolFault(b *runtimeBook, g *workerGeneration, detail string) {
	if g.turn != nil {
		if g.turn.canonical != "" {
			e.providerLost(g.turn.canonical, base.LostCrash, detail)
		} else {
			e.turnRejected(g.turn.startOp, "provider_crash", detail)
		}
	}
	e.retire(b, g, false, "protocol-fault")
}

func currentTurn(b *runtimeBook, gen, serial uint64) (*workerGeneration, *runtimeTurn) {
	g := b.worker
	if g == nil || g.id != gen {
		return nil, nil
	}
	t := g.turn
	if t == nil || t.serial != serial {
		return g, nil
	}
	return g, t
}
func matchingTurn(g *workerGeneration, target driverproto.WorkerTurnTarget) *runtimeTurn {
	if g == nil || g.turn == nil || g.turn.target != target {
		return nil
	}
	return g.turn
}
func (e *Engine) arm(k timerKind, gen, serial, rev uint64, d time.Duration) {
	time.AfterFunc(d, func() { e.sendInternal(timerDone{kind: k, gen: gen, serial: serial, rev: rev}) })
}
func (e *Engine) sendInternal(v any) {
	select {
	case e.internal <- v:
	case <-e.root.Done():
	}
}

func (e *Engine) turnStarted(op base.OpID, id base.TurnID) {
	e.outbox.push(func(x base.RuntimeEvents) { x.TurnStarted(op, id) })
}
func (e *Engine) turnRejected(op base.OpID, code, detail string) {
	detail = sanitize(detail)
	e.outbox.push(func(x base.RuntimeEvents) { x.TurnRejected(op, code, detail) })
}
func (e *Engine) tool(id base.TurnID, v base.ToolEvent) {
	e.outbox.push(func(x base.RuntimeEvents) { x.Tool(id, v) })
}
func (e *Engine) turnEnded(id base.TurnID, s base.TurnStatus, text, detail string) {
	detail = sanitize(detail)
	e.outbox.push(func(x base.RuntimeEvents) { x.TurnEnded(id, s, text, detail) })
}
func (e *Engine) controlResult(op base.OpID, id base.TurnID, v base.ControlVerdict, detail string) {
	detail = sanitize(detail)
	e.outbox.push(func(x base.RuntimeEvents) { x.ControlDone(op, id, v, detail) })
}
func (e *Engine) readyDone(op base.OpID, ok bool, detail string) {
	detail = sanitize(detail)
	e.outbox.push(func(x base.RuntimeEvents) { x.ReadyDone(op, base.ReadyResult{Ready: ok, Detail: detail}) })
}
func (e *Engine) providerLost(id base.TurnID, c base.LostCause, detail string) {
	detail = sanitize(detail)
	e.outbox.push(func(x base.RuntimeEvents) { x.ProviderLost(id, c, detail) })
}
func (e *Engine) seedUpdated(v []byte) {
	v = append([]byte(nil), v...)
	e.outbox.push(func(x base.RuntimeEvents) { x.ResumeSeedUpdated(v) })
}

func validateInput(in base.TurnInput, p Policy) error {
	n := len(in.Messages) + len(in.Background)
	if n == 0 {
		return errors.New("empty input")
	}
	if n > p.InputMaxItems {
		return errors.New("input item limit exceeded")
	}
	total := 0
	for _, m := range in.Messages {
		if strings.HasPrefix(m.Type, "agent.") {
			return errors.New("reserved agent control in runtime input")
		}
		total += len(m.Payload) + len(m.Text)
	}
	for _, m := range in.Background {
		total += len(m.Payload) + len(m.Text)
	}
	if total > p.InputMaxBytes {
		return errors.New("input byte limit exceeded")
	}
	return nil
}
func validateControlInput(in base.RuntimeInput, p Policy) error {
	if len(in.Payload)+len(in.Text) > p.InputMaxBytes {
		return errors.New("input byte limit exceeded")
	}
	if strings.HasPrefix(in.Type, "agent.") {
		return errors.New("reserved agent control in runtime input")
	}
	return nil
}
func failureCode(c driverproto.FailureClass) string {
	switch c {
	case driverproto.FailureInvalidInput:
		return "input_too_large"
	case driverproto.FailureOverloaded:
		return "overloaded"
	case driverproto.FailureTransport:
		return "provider_crash"
	default:
		return "provider_failed"
	}
}

var secretPattern = regexp.MustCompile(`(?i)(bearer\s+)[^\s,;]+|((?:authorization|api[_-]?key|token|password)\s*[:=]\s*)[^\s,;]+`)

func sanitize(s string) string {
	s = strings.TrimSpace(s)
	s = secretPattern.ReplaceAllString(s, "$1$2[redacted]")
	if len(s) > 4096 {
		s = s[:4096]
	}
	return s
}
func safeLogger(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.New(slog.DiscardHandler)
	}
	return l
}
func (e *Engine) safeLog() *slog.Logger { return safeLogger(nil) }

func cloneStart(c base.StartCommand) base.StartCommand {
	c.Input.Messages = append([]base.RuntimeInput(nil), c.Input.Messages...)
	for i := range c.Input.Messages {
		c.Input.Messages[i].Payload = append(json.RawMessage(nil), c.Input.Messages[i].Payload...)
	}
	c.Input.Background = append([]base.RuntimeContextItem(nil), c.Input.Background...)
	for i := range c.Input.Background {
		c.Input.Background[i].Payload = append(json.RawMessage(nil), c.Input.Background[i].Payload...)
	}
	return c
}
func cloneControl(c base.ControlCommand) base.ControlCommand {
	if c.Content != nil {
		x := *c.Content
		x.Payload = append(json.RawMessage(nil), x.Payload...)
		c.Content = &x
	}
	return c
}
func translateMessage(m base.RuntimeInput) driverproto.DriverMessage {
	return driverproto.DriverMessage{SourceID: m.SourceID, Type: m.Type, Sender: m.Sender, Payload: append(json.RawMessage(nil), m.Payload...), Text: m.Text}
}
func translateMessages(ms []base.RuntimeInput) []driverproto.DriverMessage {
	out := make([]driverproto.DriverMessage, len(ms))
	for i := range ms {
		out[i] = translateMessage(ms[i])
	}
	return out
}
func translateContext(ms []base.RuntimeContextItem) []driverproto.ContextMessage {
	out := make([]driverproto.ContextMessage, len(ms))
	for i, m := range ms {
		out[i] = driverproto.ContextMessage{Seq: m.Seq, Sender: m.Sender, Kind: m.Kind, Type: m.Type, Payload: append(json.RawMessage(nil), m.Payload...), Text: m.Text}
	}
	return out
}

var _ base.Runtime = (*Engine)(nil)
var _ driverproto.EventSink = (*eventSink)(nil)

// fingerprint is shared by tool and resource ledgers.
func fingerprint(parts ...[]byte) [32]byte {
	h := sha256.New()
	for _, p := range parts {
		_, _ = h.Write(p)
		_, _ = h.Write([]byte{0})
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
