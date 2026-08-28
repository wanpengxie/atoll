package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	runtimebook "github.com/wanpengxie/atoll/drivers/agents/runtime/internal/book"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
)

type commandKind uint8

const (
	commandStart commandKind = iota
	commandControl
	commandTerminate
	commandEnsureReady
)

type command struct {
	kind    commandKind
	start   runtimeproto.StartCommand
	control runtimeproto.ControlCommand
	op      runtimeproto.OpID
}

type generationPhase uint8

const (
	generationNil generationPhase = iota
	generationOpening
	generationReady
	generationRunning
	generationRetiring
)

type demandKind uint8

const (
	demandStart demandKind = iota
	demandReady
)

type pendingDemand struct {
	kind          demandKind
	op            runtimeproto.OpID
	start         runtimeproto.StartCommand
	resumeRetried bool
	revision      uint64
}

type generationState struct {
	id           uint64
	phase        generationPhase
	openRevision uint64
	reapRevision uint64
	sink         *generationSink
	stagedSeed   []byte
}

type controlState struct {
	op           runtimeproto.OpID
	action       driverproto.ActionToken
	kind         runtimeproto.ControlKind
	target       driverproto.WorkerTurnTarget
	candidate    runtimeproto.ControlCommand
	revision     uint64
	terminalSeen bool
	outcomeSeen  bool
}

type callbackRow struct {
	request *callbackRequest
	running bool
}

type turnState struct {
	op                runtimeproto.OpID
	attempt           driverproto.AttemptToken
	target            driverproto.WorkerTurnTarget
	id                runtimeproto.TurnID
	acceptedSteer     runtimeproto.ControlCommand
	hasAcceptedSteer  bool
	starting          bool
	terminal          bool
	startRevision     uint64
	watchdogRevision  uint64
	interruptRevision uint64
	life              context.Context
	cancel            context.CancelFunc
	start             runtimeproto.StartCommand
	resumeRetried     bool
	control           *controlState
	callbacks         map[string]*callbackRow
	noteCount         int
	lastNote          string
}

type timerKind uint8

const (
	timerOpen timerKind = iota
	timerStart
	timerControl
	timerInterrupt
	timerWatchdog
	timerReapedDemand
)

type timerFact struct {
	kind                 timerKind
	generation, revision uint64
	attempt              driverproto.AttemptToken
	action               driverproto.ActionToken
}
type reapedFact struct {
	generation uint64
	worker     driverproto.Worker
}

type engine struct {
	provider     driverproto.Provider
	providerSpec driverproto.ProviderSpec
	policy       Policy
	deps         runtimeproto.Deps
	events       runtimeproto.Events
	root         context.Context
	cancel       context.CancelFunc
	inbox        *inbox
	slot         workerSlot
	closeOnce    sync.Once
	timers       map[timerKind]*time.Timer
	ids          runtimebook.Counters
	generation   generationState
	pending      *pendingDemand
	turn         *turnState
	seed         []byte
	options      runtimeproto.TurnOptions
}

func newEngine(provider driverproto.Provider, spec driverproto.ProviderSpec, policy Policy, deps runtimeproto.Deps, seed []byte, options runtimeproto.TurnOptions, events runtimeproto.Events) (*engine, error) {
	if deps.Parent == nil {
		return nil, fmt.Errorf("agent/runtime: parent context required")
	}
	if events == nil {
		return nil, fmt.Errorf("agent/runtime: events required")
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	root, cancel := context.WithCancel(deps.Parent)
	e := &engine{provider: provider, providerSpec: spec, policy: policy, deps: deps, events: events, root: root, cancel: cancel, inbox: newInbox(policy), timers: map[timerKind]*time.Timer{}, seed: append([]byte(nil), seed...), options: options}
	go e.run()
	return e, nil
}

func (e *engine) Start(v runtimeproto.StartCommand) error {
	v.Messages = cloneInputs(v.Messages)
	v.Background = cloneContexts(v.Background)
	if !e.inbox.push(classCommand, command{kind: commandStart, start: v}) {
		if e.inbox.isSealed() {
			return runtimeproto.ErrClosed
		}
		return runtimeproto.ErrUnavailable
	}
	return nil
}
func (e *engine) Control(v runtimeproto.ControlCommand) error {
	if v.Content != nil {
		x := runtimeproto.CloneInput(*v.Content)
		v.Content = &x
	}
	if !e.inbox.push(classCommand, command{kind: commandControl, control: v}) {
		if e.inbox.isSealed() {
			return runtimeproto.ErrClosed
		}
		return runtimeproto.ErrUnavailable
	}
	return nil
}
func (e *engine) Terminate() error {
	if !e.inbox.push(classCommand, command{kind: commandTerminate}) {
		if e.inbox.isSealed() {
			return runtimeproto.ErrClosed
		}
		return runtimeproto.ErrUnavailable
	}
	return nil
}
func (e *engine) EnsureReady(op runtimeproto.OpID) error {
	if !e.inbox.push(classCommand, command{kind: commandEnsureReady, op: op}) {
		if e.inbox.isSealed() {
			return runtimeproto.ErrClosed
		}
		return runtimeproto.ErrUnavailable
	}
	return nil
}

func (e *engine) Close() {
	e.closeOnce.Do(func() {
		e.inbox.seal()
		e.cancel()
		e.slot.close()
	})
}

func cloneInputs(v []runtimeproto.Input) []runtimeproto.Input {
	out := make([]runtimeproto.Input, len(v))
	for i := range v {
		out[i] = runtimeproto.CloneInput(v[i])
	}
	return out
}
func cloneContexts(v []runtimeproto.ContextItem) []runtimeproto.ContextItem {
	out := make([]runtimeproto.ContextItem, len(v))
	for i := range v {
		out[i] = runtimeproto.CloneContext(v[i])
	}
	return out
}

func (e *engine) run() {
	defer func() {
		for _, timer := range e.timers {
			timer.Stop()
		}
	}()
	for {
		select {
		case <-e.root.Done():
			return
		case <-e.inbox.wake:
			for {
				item, ok := e.inbox.pop()
				if !ok {
					break
				}
				e.handle(item.value)
				if e.root.Err() != nil {
					return
				}
			}
		}
	}
}

func (e *engine) handle(v any) {
	switch x := v.(type) {
	case command:
		e.handleCommand(x)
	case driverFact:
		e.handleDriverFact(x)
	case timerFact:
		e.handleTimer(x)
	case reapedFact:
		e.handleReaped(x)
	case protocolFault:
		e.handleProtocolFault(x)
	case *callbackRequest:
		e.handleCallback(x)
	case callbackCompletion:
		e.handleCallbackCompletion(x)
	default:
		e.contractFault("unknown_fact", fmt.Sprintf("unexpected runtime fact %T", v))
	}
}

func (e *engine) handleCommand(c command) {
	switch c.kind {
	case commandStart:
		e.acceptStart(c.start)
	case commandControl:
		e.acceptControl(c.control)
	case commandTerminate:
		e.acceptTerminate()
	case commandEnsureReady:
		e.acceptEnsureReady(c.op)
	default:
		e.contractFault("invalid_command", "unknown command kind")
	}
}

func (e *engine) acceptStart(c runtimeproto.StartCommand) {
	if c.Op == 0 || (c.Kind == runtimeproto.TurnChat && len(c.Messages) == 0) || e.pending != nil {
		e.contractFault("invalid_start", "duplicate or malformed Start command")
		return
	}
	if len(c.Messages)+len(c.Background) > e.policy.InputMaxItems || inputBytes(c.Messages, c.Background) > e.policy.InputMaxBytes {
		e.publish(publishTurnRejected{op: c.Op, code: "input_too_large", detail: "runtime input bound exceeded"})
		return
	}
	e.pending = &pendingDemand{kind: demandStart, op: c.Op, start: c}
	switch e.generation.phase {
	case generationNil:
		e.spawn()
	case generationReady:
		e.dispatchDemand()
	case generationOpening, generationRunning:
	case generationRetiring:
		e.armReapedDemand()
	default:
		e.contractFault("invalid_generation", "invalid generation state")
	}
}

func inputBytes(in []runtimeproto.Input, bg []runtimeproto.ContextItem) int {
	n := 0
	for _, x := range in {
		n += len(x.Payload) + len(x.Text)
	}
	for _, x := range bg {
		n += len(x.Payload) + len(x.Text)
	}
	return n
}

func (e *engine) acceptEnsureReady(op runtimeproto.OpID) {
	if op == 0 || e.pending != nil {
		e.contractFault("invalid_ready", "duplicate or malformed EnsureReady command")
		return
	}
	if e.generation.phase == generationReady {
		e.publish(publishReadyDone{op: op, result: runtimeproto.ReadyResult{Ready: true}})
		return
	}
	e.pending = &pendingDemand{kind: demandReady, op: op}
	if e.generation.phase == generationNil {
		e.spawn()
	} else if e.generation.phase == generationRetiring {
		e.armReapedDemand()
	}
}

func (e *engine) acceptControl(c runtimeproto.ControlCommand) {
	if c.Op == 0 || c.Target == "" || e.turn == nil || e.turn.starting || e.turn.control != nil || e.turn.id != c.Target || e.turn.terminal {
		e.contractFault("invalid_control", "Control does not match the single active turn")
		return
	}
	if c.Kind == runtimeproto.ControlSteer && (!e.providerSpec.Capabilities[driverproto.CapabilitySteer] || c.Content == nil) {
		e.contractFault("invalid_control", "unsupported or empty steer command")
		return
	}
	if c.Kind == runtimeproto.ControlInterrupt && (!e.providerSpec.Capabilities[driverproto.CapabilityInterrupt] || c.Content != nil) {
		e.contractFault("invalid_control", "unsupported or content-bearing interrupt command")
		return
	}
	action, ok := e.ids.Action()
	if !ok {
		e.contractFault("counter_overflow", "control action counter overflow")
		return
	}
	cs := &controlState{op: c.Op, action: driverproto.ActionToken(action), kind: c.Kind, target: e.turn.target, candidate: c, revision: e.revision()}
	e.turn.control = cs
	w := e.slot.get(e.generation.id)
	if w == nil {
		e.contractFault("worker_slot", "active generation has no worker")
		return
	}
	req := driverproto.ControlRequest{Action: cs.action, Target: cs.target}
	if c.Kind == runtimeproto.ControlSteer {
		req.Kind = driverproto.ControlSteer
		req.Message = toDriverInput(c.Content)
	} else {
		req.Kind = driverproto.ControlInterrupt
	}
	e.workerControl(w, req)
	e.arm(timerControl, e.policy.ControlFactDeadline, timerFact{kind: timerControl, generation: e.generation.id, revision: cs.revision, action: cs.action})
}

func (e *engine) acceptTerminate() {
	if e.pending != nil && e.pending.kind == demandStart {
		e.pending = nil
	}
	if e.generation.phase == generationNil {
		return
	}
	if e.turn != nil {
		e.abandonTurn()
	}
	e.beginRetire("manual terminate")
}

func (e *engine) spawn() {
	if e.pending == nil || e.generation.phase != generationNil {
		return
	}
	g, ok := e.ids.Generation()
	if !ok {
		e.contractFault("counter_overflow", "generation counter overflow")
		return
	}
	life, cancel := context.WithCancel(e.root)
	gate := &hostAdmission{}
	sink := &generationSink{generation: g, queue: e.inbox, gate: gate, logger: e.deps.Logger}
	catalog := []driverproto.ToolSpec(nil)
	if e.deps.Tools != nil {
		for _, v := range e.deps.Tools.Catalog() {
			catalog = append(catalog, driverproto.ToolSpec{Name: v.Name, Description: v.Description, Schema: append([]byte(nil), v.Schema...)})
		}
	}
	host := &workerHost{life: life, sink: sink, tools: &toolPort{generation: g, q: e.inbox, catalog: catalog, gate: gate}, resources: &resourcePort{generation: g, q: e.inbox, gate: gate}, logger: e.deps.Logger}
	w, err := e.spawnWorker(host)
	if err != nil || w == nil {
		cancel()
		detail := "provider returned nil worker"
		if err != nil {
			detail = err.Error()
		}
		e.settleDemand(false, "provider_unavailable", detail)
		return
	}
	if !e.slot.install(g, w, cancel) {
		return
	}
	e.generation = generationState{id: g, phase: generationOpening, sink: sink, openRevision: e.revision()}
	go e.watchReaped(g, w)
	seed := append([]byte(nil), e.seed...)
	e.workerOpen(w, driverproto.OpenRequest{ResumeSeed: seed, Options: driverproto.TurnOptions{Model: e.options.Model, Effort: e.options.Effort}})
	e.arm(timerOpen, e.policy.OpenFactDeadline, timerFact{kind: timerOpen, generation: g, revision: e.generation.openRevision})
}

func (e *engine) watchReaped(g uint64, w driverproto.Worker) {
	select {
	case <-w.Reaped():
		e.inbox.push(classCompletion, reapedFact{generation: g, worker: w})
	case <-e.root.Done():
	}
}

func (e *engine) dispatchDemand() {
	d := e.pending
	if d == nil || e.generation.phase != generationReady {
		return
	}
	if d.kind == demandReady {
		e.pending = nil
		e.publish(publishReadyDone{op: d.op, result: runtimeproto.ReadyResult{Ready: true}})
		return
	}
	e.pending = nil
	attempt, ok := e.ids.Attempt()
	if !ok {
		e.contractFault("counter_overflow", "attempt counter overflow")
		return
	}
	life, cancel := context.WithCancel(e.root)
	t := &turnState{op: d.op, attempt: driverproto.AttemptToken(attempt), starting: true, startRevision: e.revision(), life: life, cancel: cancel, start: d.start, resumeRetried: d.resumeRetried, callbacks: map[string]*callbackRow{}}
	e.turn = t
	e.generation.phase = generationRunning
	w := e.slot.get(e.generation.id)
	if w == nil {
		e.contractFault("worker_slot", "ready generation has no worker")
		return
	}
	req := driverproto.StartRequest{Attempt: t.attempt, Life: life, Messages: toDriverInputs(d.start.Messages), Background: toDriverContexts(d.start.Background), Kind: d.start.Kind, Options: driverproto.TurnOptions{Model: d.start.Options.Model, Effort: d.start.Options.Effort}}
	e.workerStart(w, req)
	e.arm(timerStart, e.policy.StartFactDeadline, timerFact{kind: timerStart, generation: e.generation.id, revision: t.startRevision, attempt: t.attempt})
}

func toDriverInputs(in []runtimeproto.Input) []driverproto.DriverMessage {
	out := make([]driverproto.DriverMessage, len(in))
	for i, v := range in {
		out[i] = driverproto.DriverMessage{SourceID: v.SourceID, Type: v.Type, Sender: v.Sender, Caller: v.Caller, Origin: toDriverOrigin(v.Origin), Payload: append([]byte(nil), v.Payload...), Text: v.Text, Attachments: toDriverAttachments(v.Attachments)}
	}
	return out
}
func toDriverInput(in *runtimeproto.Input) *driverproto.DriverMessage {
	if in == nil {
		return nil
	}
	return &driverproto.DriverMessage{SourceID: in.SourceID, Type: in.Type, Sender: in.Sender, Caller: in.Caller, Origin: toDriverOrigin(in.Origin), Payload: append([]byte(nil), in.Payload...), Text: in.Text, Attachments: toDriverAttachments(in.Attachments)}
}

func toDriverAttachments(in []runtimeproto.Attachment) []driverproto.Attachment {
	out := make([]driverproto.Attachment, len(in))
	for i, attachment := range in {
		out[i] = driverproto.Attachment{Address: attachment.Address, Name: attachment.Name}
	}
	return out
}
func toDriverContexts(in []runtimeproto.ContextItem) []driverproto.ContextMessage {
	out := make([]driverproto.ContextMessage, len(in))
	for i, v := range in {
		out[i] = driverproto.ContextMessage{Seq: v.Seq, Sender: v.Sender, Kind: v.Kind, Type: v.Type, Payload: append([]byte(nil), v.Payload...), Text: v.Text}
	}
	return out
}

func (e *engine) handleDriverFact(f driverFact) {
	if f.generation != e.generation.id || e.generation.phase == generationNil {
		e.logContradiction("late generation fact", f.event)
		return
	}
	switch x := f.event.(type) {
	case driverproto.WorkerReady:
		e.workerReady()
	case driverproto.OpenRejected:
		e.openRejected(x)
	case driverproto.SubmissionRejected:
		e.submissionRejected(x)
	case driverproto.TurnStarted:
		e.turnStarted(x)
	case driverproto.Activity:
		e.activity(x)
	case driverproto.ProgressNote:
		e.note(x)
	case driverproto.Tool:
		e.nativeTool(x)
	case driverproto.TurnEnded:
		e.turnEnded(x)
	case driverproto.ControlOutcome:
		e.controlOutcome(x)
	case driverproto.SeedUpdated:
		e.seedUpdated(x)
	case driverproto.WorkerEnded:
		e.workerEnded(x)
	case driverproto.Diagnostic:
		e.deps.Logger.Log(e.root, slog.LevelDebug, "agent provider diagnostic", "code", x.Code, "detail", x.Detail)
	default:
		e.handleProtocolFault(protocolFault{generation: f.generation, code: "unknown_event", detail: fmt.Sprintf("unknown driver event %T", f.event)})
	}
}

func (e *engine) workerReady() {
	if e.generation.phase != generationOpening {
		e.logContradiction("contradictory WorkerReady", nil)
		return
	}
	e.generation.phase = generationReady
	if len(e.generation.stagedSeed) > 0 {
		e.publish(publishSeed{value: e.generation.stagedSeed})
		e.generation.stagedSeed = nil
	}
	e.dispatchDemand()
}

func (e *engine) openRejected(x driverproto.OpenRejected) {
	if e.generation.phase != generationOpening {
		e.logContradiction("contradictory OpenRejected", x)
		return
	}
	if x.Class == driverproto.FailureResumeInvalid && len(e.seed) > 0 && e.pending != nil && !e.pending.resumeRetried {
		e.seed = nil
		e.pending.resumeRetried = true
		e.beginRetire("invalid resume seed")
		return
	}
	e.settleDemand(false, failureCode(x.Class), x.Detail)
	e.beginRetire("open rejected")
}

func (e *engine) turnStarted(x driverproto.TurnStarted) {
	t := e.turn
	if t == nil || !t.starting || x.Target.Attempt != t.attempt || !x.Target.Valid() {
		e.logContradiction("contradictory TurnStarted", x)
		return
	}
	id, err := uuid.NewV7()
	if err != nil {
		e.contractFault("turn_id", err.Error())
		return
	}
	t.starting = false
	t.target = x.Target
	t.id = runtimeproto.TurnID(id.String())
	t.watchdogRevision = e.revision()
	e.publish(publishTurnStarted{op: t.op, turn: t.id})
	e.arm(timerWatchdog, e.policy.Watchdog, timerFact{kind: timerWatchdog, generation: e.generation.id, revision: t.watchdogRevision, attempt: t.attempt})
}

func (e *engine) submissionRejected(x driverproto.SubmissionRejected) {
	t := e.turn
	if t == nil || !t.starting || x.Attempt != t.attempt {
		e.logContradiction("contradictory SubmissionRejected", x)
		return
	}
	if x.Class == driverproto.FailureResumeInvalid && len(e.seed) > 0 && !t.resumeRetried {
		e.seed = nil
		e.pending = &pendingDemand{kind: demandStart, op: t.op, start: t.start, resumeRetried: true}
		t.cancel()
		e.turn = nil
		e.beginRetire("invalid resume seed")
		return
	}
	e.publish(publishTurnRejected{op: t.op, code: failureCode(x.Class), detail: x.Detail})
	t.cancel()
	e.turn = nil
	if x.Disposition == driverproto.RetireWorker {
		e.beginRetire("submission rejected")
	} else {
		e.generation.phase = generationReady
		e.dispatchDemand()
	}
}

func (e *engine) activity(x driverproto.Activity) {
	if !e.currentTarget(x.Target) {
		e.logContradiction("late Activity", x)
		return
	}
	e.resetWatchdog()
}

func (e *engine) note(x driverproto.ProgressNote) {
	if !e.currentTarget(x.Target) {
		e.logContradiction("late ProgressNote", x)
		return
	}
	e.resetWatchdog()
	if e.turn == nil || e.turn.terminal || e.turn.id == "" || x.Kind == "" {
		return
	}
	// 无文本的 note 是合法的（codex 的思考区间就没有文本可给），但连续同样
	// 的读数没有信息量——与上一条完全相同则丢。note 本身逐条上报不合并；
	// 64 条/turn 是防疯转的兜底限额，正常回合远到不了。
	key := x.Kind + "\x00" + x.Text
	if key == e.turn.lastNote || e.turn.noteCount >= 64 {
		return
	}
	e.turn.lastNote = key
	e.turn.noteCount++
	e.publish(publishProgress{turn: e.turn.id, event: runtimeproto.ProgressEvent{Kind: x.Kind, Text: x.Text}})
}

func (e *engine) nativeTool(x driverproto.Tool) {
	if !e.currentTarget(x.Target) {
		e.logContradiction("late Tool", x)
		return
	}
	// A host callback (dynamic tool / resource served by us) is already
	// projected authoritatively by handleCallback/handleCallbackCompletion —
	// the provider's own item stream re-narrates the same call under the same
	// call id, which would double every tool event on the ledger. Skip the
	// narration; keep the execution-side record.
	if e.turn != nil {
		for _, row := range e.turn.callbacks {
			if row.request.callID == x.CallID {
				e.resetWatchdog()
				return
			}
		}
	}
	phase := "started"
	if x.Phase == driverproto.ToolEnded {
		phase = "ended"
	}
	status := ""
	if x.Status == driverproto.ToolStatusCompleted {
		status = "completed"
	} else if x.Status == driverproto.ToolStatusFailed {
		status = "failed"
	}
	e.publish(publishTool{turn: e.turn.id, event: runtimeproto.ToolEvent{CallID: x.CallID, Phase: phase, Name: x.Name, Status: status, Detail: x.Detail, Input: x.Input, Output: x.Output}})
	e.resetWatchdog()
}

func (e *engine) turnEnded(x driverproto.TurnEnded) {
	if !e.currentTarget(x.Target) {
		e.logContradiction("contradictory TurnEnded", x)
		return
	}
	if runningCallbacks(e.turn) != 0 {
		e.closeLost(runtimeproto.LostProtocol, "provider ended turn with running host callbacks")
		e.beginRetire("callback order violation")
		return
	}
	status := runtimeproto.TurnStatusOK
	if x.Status == driverproto.TurnFailed {
		status = runtimeproto.TurnStatusFailed
	} else if x.Status == driverproto.TurnInterrupted {
		status = runtimeproto.TurnStatusInterrupted
	}
	usage := runtimeproto.TurnUsage{ContextTokens: x.Usage.ContextTokens, ContextWindow: x.Usage.ContextWindow, Model: x.Usage.Model, Effort: x.Usage.Effort}
	if e.turn.start.Kind == runtimeproto.TurnSelect && status == runtimeproto.TurnStatusOK {
		e.options = runtimeproto.TurnOptions{Model: usage.Model, Effort: usage.Effort}
	}
	e.publish(publishTurnEnded{turn: e.turn.id, status: status, text: x.FinalText, detail: x.ErrorDetail, usage: usage})
	e.turn.terminal = true
	e.turn.cancel()
	if e.turn.control != nil {
		e.turn.control.terminalSeen = true
	} else {
		e.finishTurnReusable()
	}
}

func (e *engine) controlOutcome(x driverproto.ControlOutcome) {
	t := e.turn
	if t == nil || t.control == nil || x.Action != t.control.action || x.Target != t.control.target {
		e.logContradiction("contradictory ControlOutcome", x)
		return
	}
	c := t.control
	c.outcomeSeen = true
	verdict := runtimeproto.ControlRejected
	switch x.Verdict {
	case driverproto.ControlAccepted:
		verdict = runtimeproto.ControlAccepted
	case driverproto.ControlNotSteerable:
		verdict = runtimeproto.ControlNotSteerable
	case driverproto.ControlTargetGone:
		verdict = runtimeproto.ControlTargetGone
	}
	e.publish(publishControlDone{op: c.op, turn: t.id, verdict: verdict, detail: x.Detail})
	if x.Verdict == driverproto.ControlAccepted && c.kind == runtimeproto.ControlSteer && !t.terminal {
		t.acceptedSteer, t.hasAcceptedSteer = c.candidate, true
	}
	if x.Verdict == driverproto.ControlAccepted && c.kind == runtimeproto.ControlInterrupt && !t.terminal {
		t.interruptRevision = e.revision()
		e.arm(timerInterrupt, e.policy.InterruptEnded, timerFact{kind: timerInterrupt, generation: e.generation.id, revision: t.interruptRevision, attempt: t.attempt})
	}
	t.control = nil
	if x.Disposition == driverproto.RetireWorker {
		if !t.terminal {
			e.closeLost(runtimeproto.LostCrash, "provider retired after control outcome")
		}
		e.beginRetire("control disposition")
		return
	}
	if t.terminal {
		e.finishTurnReusable()
	}
}

func (e *engine) seedUpdated(x driverproto.SeedUpdated) {
	v := append([]byte(nil), x.Value...)
	e.seed = v
	if e.generation.phase == generationOpening {
		e.generation.stagedSeed = v
		return
	}
	e.publish(publishSeed{value: v})
}

func (e *engine) workerEnded(x driverproto.WorkerEnded) {
	if e.turn != nil && e.turn.terminal {
		e.rejectControl(x.Detail)
	}
	switch {
	case e.generation.phase == generationOpening:
		e.settleDemand(false, "provider_crash", x.Detail)
	case e.turn != nil && e.turn.starting:
		e.publish(publishTurnRejected{op: e.turn.op, code: "provider_crash", detail: x.Detail})
		e.turn.cancel()
		e.turn = nil
	case e.turn != nil && !e.turn.terminal:
		e.closeLost(runtimeproto.LostCrash, x.Detail)
	}
	e.beginRetire("worker ended")
}

func (e *engine) currentTarget(target driverproto.WorkerTurnTarget) bool {
	return e.turn != nil && !e.turn.starting && !e.turn.terminal && e.turn.target == target
}
func (e *engine) resetWatchdog() {
	if e.turn == nil {
		return
	}
	e.turn.watchdogRevision = e.revision()
	e.arm(timerWatchdog, e.policy.Watchdog, timerFact{kind: timerWatchdog, generation: e.generation.id, revision: e.turn.watchdogRevision, attempt: e.turn.attempt})
}

func (e *engine) handleTimer(t timerFact) {
	if t.generation != e.generation.id {
		return
	}
	switch t.kind {
	case timerOpen:
		if e.generation.phase == generationOpening && t.revision == e.generation.openRevision {
			e.settleDemand(false, "provider_timeout", "provider open produced no fact")
			e.beginRetire("open timeout")
		}
	case timerStart:
		if e.turn != nil && e.turn.starting && t.attempt == e.turn.attempt && t.revision == e.turn.startRevision {
			e.publish(publishTurnRejected{op: e.turn.op, code: "provider_timeout", detail: "provider start produced no fact"})
			e.turn.cancel()
			e.turn = nil
			e.beginRetire("start timeout")
		}
	case timerControl:
		if e.turn != nil && e.turn.control != nil && t.action == e.turn.control.action && t.revision == e.turn.control.revision {
			e.publish(publishControlDone{op: e.turn.control.op, turn: e.turn.id, verdict: runtimeproto.ControlTimeout, detail: "provider control produced no fact"})
			e.turn.control = nil
			if !e.turn.terminal {
				e.closeLost(runtimeproto.LostTimeout, "provider control timeout")
			}
			e.beginRetire("control timeout")
		}
	case timerInterrupt:
		if e.turn != nil && !e.turn.terminal && t.attempt == e.turn.attempt && t.revision == e.turn.interruptRevision {
			e.closeLost(runtimeproto.LostTimeout, "interrupt did not end turn")
			e.beginRetire("interrupt timeout")
		}
	case timerWatchdog:
		if e.turn != nil && !e.turn.starting && !e.turn.terminal && t.attempt == e.turn.attempt && t.revision == e.turn.watchdogRevision {
			e.closeLost(runtimeproto.LostTimeout, "provider turn inactivity timeout")
			e.beginRetire("watchdog")
		}
	case timerReapedDemand:
		if e.generation.phase == generationRetiring && e.pending != nil && t.revision == e.generation.reapRevision {
			e.settleDemand(false, "provider_timeout", "worker did not reap before demand deadline")
		}
	}
}

func (e *engine) arm(kind timerKind, delay time.Duration, fact timerFact) {
	if old := e.timers[kind]; old != nil {
		old.Stop()
	}
	e.timers[kind] = time.AfterFunc(delay, func() {
		if !e.inbox.push(classTimer, fact) {
			e.inbox.latchFault(protocolFault{generation: fact.generation, code: "timer_overflow", detail: "reserved timer slot unavailable"})
		}
	})
}

func (e *engine) beginRetire(detail string) {
	if e.generation.phase == generationNil || e.generation.phase == generationRetiring {
		return
	}
	e.generation.phase = generationRetiring
	if e.generation.sink != nil {
		e.generation.sink.seal()
	}
	e.retireWorker(e.generation.id)
	if e.pending != nil {
		e.armReapedDemand()
	}
	e.deps.Logger.Debug("agent worker retiring", "generation", e.generation.id, "detail", detail)
}

func (e *engine) armReapedDemand() {
	e.generation.reapRevision = e.revision()
	e.arm(timerReapedDemand, e.policy.ReapedDemand, timerFact{kind: timerReapedDemand, generation: e.generation.id, revision: e.generation.reapRevision})
}

func (e *engine) handleReaped(r reapedFact) {
	if r.generation != e.generation.id || !e.slot.clear(r.generation, r.worker) {
		return
	}
	if !e.settleAndWipeReapedGeneration("worker reaped unexpectedly") {
		return
	}
	if e.pending != nil {
		e.spawn()
	}
}

// settleAndWipeReapedGeneration is the sole destructive generation cleanup
// path. A pending demand is deliberately carried across the wipe and causes a
// fresh spawn; every obligation owned by the dead generation is settled first.
func (e *engine) settleAndWipeReapedGeneration(detail string) bool {
	if e.generation.sink != nil {
		e.generation.sink.seal()
	}
	if e.turn != nil {
		if !e.turn.terminal {
			e.closeLost(runtimeproto.LostCrash, detail)
		} else {
			e.rejectControl(detail)
			e.cancelRunningCallbacks(e.turn, detail)
		}
	}
	return e.wipeSettledGeneration()
}

// wipeSettledGeneration is the unique generation zero-write mouth. Refusing
// to wipe on debt turns a future missed settlement into an explicit Runtime
// contract fault instead of silently orphaning an operation.
func (e *engine) wipeSettledGeneration() bool {
	if debt := e.generationDebt(); debt != "" {
		e.contractFault("generation_wipe_debt", debt)
		return false
	}
	e.generation = generationState{}
	if e.turn != nil {
		if e.turn.cancel != nil {
			e.turn.cancel()
		}
		e.turn = nil
	}
	return true
}

func (e *engine) generationDebt() string {
	if e.turn == nil {
		return ""
	}
	var debt []string
	if !e.turn.terminal {
		debt = append(debt, "turn has no terminal fact")
	}
	if e.turn.control != nil {
		debt = append(debt, "control has no outcome")
	}
	if n := runningCallbacks(e.turn); n != 0 {
		debt = append(debt, fmt.Sprintf("%d host callbacks are still running", n))
	}
	return strings.Join(debt, "; ")
}

func (e *engine) handleProtocolFault(f protocolFault) {
	if f.generation != e.generation.id {
		return
	}
	if e.turn != nil && e.turn.terminal {
		e.rejectControl(f.detail)
	}
	if e.turn != nil && e.turn.starting {
		e.publish(publishTurnRejected{op: e.turn.op, code: "provider_protocol", detail: f.detail})
		e.turn.cancel()
		e.turn = nil
	} else if e.turn != nil && !e.turn.terminal {
		e.closeLost(runtimeproto.LostProtocol, f.detail)
	} else if e.generation.phase == generationOpening {
		e.settleDemand(false, "provider_protocol", f.detail)
	}
	e.beginRetire(f.code)
}

func (e *engine) rejectControl(detail string) {
	if e.turn == nil || e.turn.control == nil {
		return
	}
	e.publish(publishControlDone{op: e.turn.control.op, turn: e.turn.id, verdict: runtimeproto.ControlRejected, detail: detail})
	e.turn.control = nil
}

func (e *engine) closeLost(cause runtimeproto.LostCause, detail string) {
	if e.turn == nil || e.turn.terminal {
		return
	}
	id := e.turn.id
	if e.turn.control != nil {
		e.publish(publishControlDone{op: e.turn.control.op, turn: id, verdict: runtimeproto.ControlRejected, detail: detail})
		e.turn.control = nil
	}
	if e.turn.starting {
		e.publish(publishTurnRejected{op: e.turn.op, code: failureForLost(cause), detail: detail})
	} else {
		e.publish(publishProviderLost{turn: id, cause: cause, detail: detail})
	}
	e.turn.terminal = true
	e.turn.cancel()
	e.cancelRunningCallbacks(e.turn, "turn no longer active")
}

func failureForLost(c runtimeproto.LostCause) string {
	if c == runtimeproto.LostTimeout {
		return "provider_timeout"
	}
	if c == runtimeproto.LostProtocol {
		return "provider_protocol"
	}
	return "provider_crash"
}

func (e *engine) abandonTurn() {
	if e.turn == nil {
		return
	}
	e.turn.terminal = true
	e.turn.cancel()
	e.cancelRunningCallbacks(e.turn, "turn abandoned")
	e.turn = nil
}

func (e *engine) cancelRunningCallbacks(t *turnState, detail string) {
	if t == nil {
		return
	}
	for _, row := range t.callbacks {
		if row.running {
			respondCancelled(row.request, detail)
			row.running = false
		}
	}
}

func (e *engine) finishTurnReusable() {
	if e.turn == nil || !e.turn.terminal || e.turn.control != nil {
		return
	}
	e.turn = nil
	if e.generation.phase != generationRetiring {
		e.generation.phase = generationReady
		e.dispatchDemand()
	}
}

func (e *engine) settleDemand(ready bool, code, detail string) {
	d := e.pending
	if d == nil {
		return
	}
	e.pending = nil
	if d.kind == demandReady {
		e.publish(publishReadyDone{op: d.op, result: runtimeproto.ReadyResult{Ready: ready, Code: code, Detail: detail}})
	} else if !ready {
		e.publish(publishTurnRejected{op: d.op, code: code, detail: detail})
	}
}

func (e *engine) contractFault(code, detail string) {
	e.publish(publishFault{code: code, detail: detail})
}
func (e *engine) revision() uint64 {
	revision, ok := e.ids.Revision()
	if !ok {
		e.contractFault("counter_overflow", "revision counter overflow")
	}
	return revision
}
func (e *engine) logContradiction(msg string, fact any) {
	e.deps.Logger.Error(msg, "generation", e.generation.id, "fact", fmt.Sprintf("%T", fact))
}
func failureCode(c driverproto.FailureClass) string {
	switch c {
	case driverproto.FailureInvalidInput:
		return "input_invalid"
	case driverproto.FailureOverloaded:
		return "provider_overloaded"
	case driverproto.FailureTransport:
		return "provider_crash"
	case driverproto.FailureResumeInvalid:
		return "resume_invalid"
	default:
		return "provider_failed"
	}
}

func (e *engine) handleCallback(r *callbackRequest) {
	if r.generation != e.generation.id || !e.currentTarget(r.target) {
		respondCancelled(r, "target is not active")
		return
	}
	key := callbackKey(r)
	if key == "" || e.turn.callbacks[key] != nil {
		respondCancelled(r, "duplicate or empty provider call id")
		e.handleProtocolFault(protocolFault{generation: r.generation, code: "duplicate_callback", detail: "duplicate provider callback"})
		return
	}
	e.turn.callbacks[key] = &callbackRow{request: r, running: true}
	e.resetWatchdog()
	e.publish(publishTool{turn: e.turn.id, event: runtimeproto.ToolEvent{CallID: r.callID, Phase: "started", Name: callbackName(r), Input: callbackInput(r)}})
	e.invokeBridge(r, key, e.turn.start, e.turn.acceptedSteer, e.turn.hasAcceptedSteer)
}

func (e *engine) handleCallbackCompletion(c callbackCompletion) {
	if c.generation != e.generation.id || e.turn == nil {
		return
	}
	row := e.turn.callbacks[c.key]
	if row == nil || !row.running {
		return
	}
	row.running = false
	e.resetWatchdog()
	status, detail := "completed", ""
	if row.request.kind == callbackTool && c.result.tool.IsError {
		status, detail = "failed", c.result.tool.Text
	}
	if row.request.kind == callbackResource && c.result.resource.Error != "" {
		status, detail = "failed", c.result.resource.Error
	}
	e.publish(publishTool{turn: e.turn.id, event: runtimeproto.ToolEvent{CallID: row.request.callID, Phase: "ended", Name: callbackName(row.request), Status: status, Detail: detail, Output: callbackOutput(row.request, c.result)}})
	select {
	case row.request.response <- c.result:
	default:
	}
}

func callbackKey(r *callbackRequest) string {
	if r.callID == "" {
		return ""
	}
	return fmt.Sprintf("%d/%d/%s/%d/%s", r.generation, r.target.Attempt, r.target.Native, r.kind, r.callID)
}
func callbackName(r *callbackRequest) string {
	if r.kind == callbackTool {
		return r.tool.Name
	}
	return "resource." + r.resource.Operation
}

func callbackInput(r *callbackRequest) json.RawMessage {
	if r.kind == callbackTool {
		return append(json.RawMessage(nil), r.tool.Params...)
	}
	raw, _ := json.Marshal(map[string]any{"operation": r.resource.Operation, "resource_id": r.resource.ResourceID, "payload": r.resource.Payload})
	return raw
}

func callbackOutput(r *callbackRequest, result callbackResult) json.RawMessage {
	if r.kind == callbackResource {
		if len(result.resource.Payload) != 0 {
			return append(json.RawMessage(nil), result.resource.Payload...)
		}
		if result.resource.Error != "" {
			raw, _ := json.Marshal(result.resource.Error)
			return raw
		}
		return nil
	}
	text := strings.TrimSpace(result.tool.Text)
	if text == "" {
		return nil
	}
	if json.Valid([]byte(text)) {
		return append(json.RawMessage(nil), text...)
	}
	raw, _ := json.Marshal(result.tool.Text)
	return raw
}
func runningCallbacks(t *turnState) int {
	n := 0
	if t != nil {
		for _, row := range t.callbacks {
			if row.running {
				n++
			}
		}
	}
	return n
}
func respondCancelled(r *callbackRequest, detail string) {
	out := callbackResult{}
	if r.kind == callbackTool {
		out.tool = driverproto.ToolResult{Text: detail, IsError: true}
	} else {
		out.resource = driverproto.ResourceResult{Error: detail}
	}
	select {
	case r.response <- out:
	default:
	}
}
func runtimeprotoToDriverTool(v runtimeproto.ToolResult) driverproto.ToolResult {
	return driverproto.ToolResult{Text: v.Text, IsError: v.IsError}
}

var _ runtimeproto.Runtime = (*engine)(nil)

// toDriverOrigin carries the screen a message came from across the driver seam.
func toDriverOrigin(o *runtimeproto.Origin) *driverproto.Origin {
	if o == nil {
		return nil
	}
	return &driverproto.Origin{Session: o.Session, Label: o.Label}
}
