package base

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

const (
	bootTimeout            = 30 * time.Second
	watchdogInitialTimeout = 10 * time.Minute
)

type loopState uint8

const (
	stateIdle loopState = iota
	stateStarting
	stateTurnActive
	stateInterrupting
)

type eventKind uint8

const (
	eventTurnStarted eventKind = iota
	eventTurnRejected
	eventTool
	eventTurnEnded
	eventControlDone
	eventProviderLost
)

type providerEvent struct {
	kind       eventKind
	op         OpID
	turnID     string
	code       string
	detail     string
	callID     string
	phase      string
	name       string
	toolStatus string
	status     TurnStatus
	finalText  string
	verdict    ControlVerdict
	cause      LostCause
}

type eventPort struct {
	life    context.Context
	events  chan<- providerEvent
	sys     actorbase.Sys
	closed  atomic.Bool
	persist persistCoordinator
}

func (p *eventPort) send(e providerEvent) {
	if p.closed.Load() {
		return
	}
	select {
	case p.events <- e:
	case <-p.life.Done():
	}
}
func (p *eventPort) TurnStarted(op OpID, turnID string) {
	p.send(providerEvent{kind: eventTurnStarted, op: op, turnID: turnID})
}
func (p *eventPort) TurnRejected(op OpID, code, detail string) {
	p.send(providerEvent{kind: eventTurnRejected, op: op, code: code, detail: detail})
}
func (p *eventPort) Tool(turnID, callID string, phase, name, status, detail string) {
	p.send(providerEvent{kind: eventTool, turnID: turnID, callID: callID, phase: phase, name: name, toolStatus: status, detail: detail})
}
func (p *eventPort) TurnEnded(turnID string, status TurnStatus, finalText string, errInfo string) {
	p.send(providerEvent{kind: eventTurnEnded, turnID: turnID, status: status, finalText: finalText, detail: errInfo})
}
func (p *eventPort) ControlDone(op OpID, verdict ControlVerdict, turnID, detail string) {
	p.send(providerEvent{kind: eventControlDone, op: op, verdict: verdict, turnID: turnID, detail: detail})
}
func (p *eventPort) ProviderLost(cause LostCause, detail string) {
	p.send(providerEvent{kind: eventProviderLost, cause: cause, detail: detail})
}
func (p *eventPort) Persist(key string, value []byte) { p.persist.submit(p.sys, key, value) }

type closureEvent struct{ id string }

type operation struct {
	kind string
	item *requestItem
}

type turnResult struct {
	status TurnStatus
	text   string
	err    string
}

type controlSlot struct {
	kind string
	item *requestItem
	op   OpID
}

type agentLoop struct {
	def definition
	sys actorbase.Sys
	eng Engine

	state      loopState
	settling   bool
	turnID     string
	turnIndex  int
	nextItem   int
	nextOp     uint64
	buffer     requestBuffer
	committing map[OpID]*operation
	active     *requestItem
	lastOwner  *requestItem
	result     *turnResult
	background []ContextItem

	pendingControl   *controlSlot
	executingControl *controlSlot
	controlExpiry    chan OpID
	controlTimer     *time.Timer
}

func (d definition) run(sys actorbase.Sys) error {
	events := make(chan providerEvent, 256)
	port := &eventPort{life: sys.Life(), events: events, sys: sys}
	eng, err := d.cfg.NewEngine(sys, readSeed(sys), port)
	if err != nil {
		return fmt.Errorf("agent/base: engine construct: %w", err)
	}
	defer func() {
		port.closed.Store(true)
		_ = eng.Close()
	}()

	bootCtx, cancel := context.WithTimeout(sys.Life(), bootTimeout)
	background := loadCatchup(bootCtx, sys)
	if err := eng.Boot(bootCtx, port); err != nil {
		cancel()
		return fmt.Errorf("agent/base: engine boot: %w", err)
	}
	cancel()

	l := &agentLoop{
		def: d, sys: sys, eng: eng, state: stateIdle, background: background,
		buffer:        requestBuffer{maxCount: d.cfg.BufferMaxCount, maxBytes: d.cfg.BufferMaxBytes},
		committing:    make(map[OpID]*operation),
		controlExpiry: make(chan OpID, 1),
	}
	defer l.stopControlDeadline()
	intake := make(chan actorbase.Msg)
	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := sys.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case intake <- msg:
			case <-sys.Life().Done():
				return
			}
		}
	}()
	closures := make(chan closureEvent, d.cfg.BufferMaxCount+32)
	watchdog := time.NewTimer(time.Hour)
	if !watchdog.Stop() {
		<-watchdog.C
	}
	for {
		select {
		case <-sys.Life().Done():
			return nil
		case err := <-recvErr:
			if sys.Life().Err() != nil {
				return nil
			}
			return err
		case msg := <-intake:
			if l.handleIntakeAndShouldArmWatchdog(msg, closures) || l.state == stateIdle {
				l.armWatchdog(watchdog)
			}
		case e := <-events:
			progress := l.isCurrentProgress(e)
			l.handleProviderEvent(e)
			if progress || l.state == stateIdle {
				l.armWatchdog(watchdog)
			}
		case c := <-closures:
			l.handleClosure(c.id)
		case op := <-l.controlExpiry:
			l.expireControl(op)
		case <-watchdog.C:
			_ = l.eng.Interrupt(l.opID())
			_ = l.eng.Terminate()
			l.providerLost(LostTimeout, "provider watchdog expired")
		}
	}
}

func (l *agentLoop) handleIntakeAndShouldArmWatchdog(msg actorbase.Msg, closures chan<- closureEvent) bool {
	wasIdle := l.state == stateIdle
	l.handleIntake(msg, closures)
	return wasIdle && l.state != stateIdle
}

func (l *agentLoop) isCurrentProgress(e providerEvent) bool {
	switch e.kind {
	case eventTurnStarted, eventTurnRejected:
		op := l.committing[e.op]
		return op != nil && op.kind == "start"
	case eventTool, eventTurnEnded:
		return e.turnID != "" && e.turnID == l.turnID
	default:
		return false
	}
}

func (l *agentLoop) armWatchdog(t *time.Timer) {
	if l.state == stateIdle {
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(watchdogInitialTimeout)
}

func (l *agentLoop) opID() OpID {
	l.nextOp++
	return OpID(fmt.Sprintf("op-%d", l.nextOp))
}

func (l *agentLoop) handleIntake(msg actorbase.Msg, closures chan<- closureEvent) {
	// Kind/self fencing is intentionally before all type dispatch.
	if msg.Kind != message.KindRequest || msg.Sender.ID == l.sys.Self() {
		return
	}
	if msg.Type == introspect.QueryDescribe {
		l.answerDescribe(msg)
		return
	}
	if strings.HasPrefix(msg.Type, "agent.") {
		if _, known := map[string]struct{}{TypeSteer: {}, TypeInterrupt: {}, TypeQueue: {}, TypeStop: {}, TypeTerminate: {}, TypeRestart: {}}[msg.Type]; !known || !l.def.supports(msg.Type) {
			_, _ = l.sys.Fail(msg, "type_unsupported", "agent does not support "+msg.Type)
			return
		}
	}
	l.nextItem++
	item := newRequestItem(msg, l.nextItem)
	if msg.Type == TypeSteer {
		var p struct {
			ExpectedTurnID string `json:"expected_turn_id"`
		}
		_ = json.Unmarshal(msg.Payload, &p)
		item.explicitCAS = strings.TrimSpace(p.ExpectedTurnID) != ""
	}
	go func(id string, ctx context.Context) {
		select {
		case <-ctx.Done():
			select {
			case closures <- closureEvent{id: id}:
			case <-l.sys.Life().Done():
			}
		case <-l.sys.Life().Done():
		}
	}(string(msg.ID), msg.Ctx())

	switch msg.Type {
	case TypeQueue:
		l.enqueue(item, true)
	case TypeSteer:
		l.acceptContent(item, true)
	case TypeInterrupt, TypeStop, TypeTerminate, TypeRestart:
		l.acceptControl(item)
	default:
		l.acceptContent(item, false)
	}
}

func (l *agentLoop) answerDescribe(msg actorbase.Msg) {
	req, err := introspect.ParseDescribeRequest(msg.Payload)
	if err != nil {
		_, _ = l.sys.Fail(msg, "payload_invalid", err.Error())
		return
	}
	d := l.eng.Describe()
	d.ActorID = string(l.sys.Self())
	if d.Types == nil {
		d.Types = map[string]introspect.TypeMeta{}
	}
	for typ := range l.def.controls {
		d.Types[typ] = introspect.TypeMeta{Description: "standard agent control", AllowedKinds: []string{string(message.KindRequest)}}
	}
	answer, ok := introspect.AnswerDescribe(d, req)
	if !ok {
		_, _ = l.sys.Fail(msg, "type_unsupported", "agent has no type "+req.Type)
		return
	}
	_, _ = l.sys.Reply(msg, answer)
}

func (l *agentLoop) enqueue(item *requestItem, progress bool) {
	if !l.buffer.push(item) {
		l.fail(item, errorOverloaded, "agent request buffer limit exceeded")
		return
	}
	if progress {
		_, _ = l.sys.Progress(item.msg, message.StatusQueued, map[string]any{"turn_index": item.trigger.Index})
	}
	if l.state == stateIdle && l.executingControl == nil && l.pendingControl == nil {
		l.startNext()
	}
}

func (l *agentLoop) acceptContent(item *requestItem, explicitSteer bool) {
	switch l.state {
	case stateIdle:
		if explicitSteer && item.explicitCAS {
			l.fail(item, errorCASMismatch, "no active turn")
			return
		}
		l.enqueue(item, false)
	case stateTurnActive:
		if !l.def.supports(TypeSteer) {
			l.enqueue(item, true)
			return
		}
		op := l.opID()
		l.committing[op] = &operation{kind: TypeSteer, item: item}
		_, _ = l.sys.Progress(item.msg, message.StatusProcessing, map[string]any{"turn_id": l.turnID})
		if err := l.eng.Steer(op, item.trigger); err != nil {
			delete(l.committing, op)
			l.fail(item, errorProviderCrash, err.Error())
		}
	default:
		l.enqueue(item, true)
	}
}

func (l *agentLoop) startNext() {
	if l.state != stateIdle || l.executingControl != nil || l.pendingControl != nil {
		return
	}
	batch := l.buffer.popBatch(l.def.cfg.BatchMaxCount)
	if len(batch) == 0 {
		return
	}
	tail := batch[len(batch)-1]
	for _, item := range batch[:len(batch)-1] {
		l.reply(item, map[string]any{"merged_into": tail.msg.ID})
	}
	if tail.closed {
		l.startNext()
		return
	}
	l.turnIndex++
	tail.trigger.Index = l.turnIndex
	triggers := make([]Trigger, 0, len(batch))
	for _, item := range batch {
		item.trigger.Index = l.turnIndex
		triggers = append(triggers, item.trigger)
	}
	op := l.opID()
	l.committing[op] = &operation{kind: "start", item: tail}
	l.state = stateStarting
	_, _ = l.sys.Progress(tail.msg, message.StatusProcessing, map[string]any{"turn_index": l.turnIndex})
	background := l.takeBackground()
	if err := l.eng.StartTurn(op, triggers, background); err != nil {
		delete(l.committing, op)
		l.state = stateIdle
		l.fail(tail, errorProviderCrash, err.Error())
		l.startNext()
	}
}

func (l *agentLoop) handleProviderEvent(e providerEvent) {
	switch e.kind {
	case eventTurnStarted:
		op := l.committing[e.op]
		if op == nil || op.kind != "start" || l.state != stateStarting || e.turnID == "" {
			return
		}
		delete(l.committing, e.op)
		l.turnID, l.state, l.lastOwner = e.turnID, stateTurnActive, op.item
		if !op.item.closed {
			l.active = op.item
		}
		l.logActivityError(registry.ActivityTurnStarted, l.emitTurnStarted())
		if l.active == nil {
			interruptOp := l.opID()
			l.committing[interruptOp] = &operation{kind: TypeInterrupt}
			l.state = stateInterrupting
			_ = l.eng.Interrupt(interruptOp)
		}
		l.maybeRunControl()
	case eventTurnRejected:
		op := l.committing[e.op]
		if op == nil || op.kind != "start" {
			return
		}
		delete(l.committing, e.op)
		code := e.code
		switch code {
		case errorInputTooLarge, errorProviderCrash, errorProviderFailed:
		default:
			code = errorProviderFailed
		}
		l.fail(op.item, code, e.detail)
		l.state, l.turnID = stateIdle, ""
		l.maybeRunControl()
		l.startNext()
	case eventTool:
		if e.turnID == l.turnID {
			activityType := registry.ActivityToolStarted
			if e.phase == "ended" {
				activityType = registry.ActivityToolEnded
			}
			l.logActivityError(activityType, l.emitTool(e))
		}
	case eventTurnEnded:
		if e.turnID != l.turnID || l.state == stateIdle {
			return
		}
		l.result = &turnResult{status: e.status, text: e.finalText, err: e.detail}
		if l.hasInFlightTurnControl() {
			l.settling = true
		} else {
			l.settleTurn()
		}
	case eventControlDone:
		l.controlDone(e)
	case eventProviderLost:
		l.providerLost(e.cause, e.detail)
	}
}

func (l *agentLoop) hasInFlightTurnControl() bool {
	for _, op := range l.committing {
		if op.kind == TypeSteer || op.kind == TypeInterrupt {
			return true
		}
	}
	return false
}

func (l *agentLoop) controlDone(e providerEvent) {
	op := l.committing[e.op]
	if op == nil {
		if l.executingControl != nil && l.executingControl.op == e.op {
			l.finishExecutingControl(e)
		}
		return
	}
	delete(l.committing, e.op)
	switch op.kind {
	case TypeSteer:
		if l.pendingControl != nil || l.executingControl != nil {
			l.fail(op.item, errorCancelled, "control pending")
		} else {
			switch e.verdict {
			case ControlAccepted:
				if !op.item.closed {
					if l.active != nil && l.active != op.item {
						l.reply(l.active, map[string]any{"preempted_by": op.item.msg.ID})
					}
					l.active, l.lastOwner = op.item, op.item
				}
			case ControlEmptyInput:
				l.fail(op.item, errorEmptyInput, e.detail)
			case ControlMismatch, ControlNoActiveTurn, ControlNotSteerable:
				if op.item.explicitCAS {
					l.fail(op.item, errorCASMismatch, e.detail)
				} else {
					l.enqueue(op.item, true)
				}
			default:
				l.fail(op.item, errorProviderCrash, e.detail)
			}
		}
	case TypeInterrupt:
		if e.verdict != ControlAccepted && e.verdict != ControlNoActiveTurn {
			if op.item != nil {
				l.fail(op.item, errorProviderCrash, e.detail)
			}
			l.state = stateTurnActive
		} else if e.verdict == ControlNoActiveTurn && l.result == nil {
			l.turnID, l.state = "", stateIdle
		}
	}
	if l.settling && !l.hasInFlightTurnControl() {
		l.settleTurn()
	} else if l.state == stateIdle {
		l.maybeRunControl()
		l.startNext()
	}
}

func (l *agentLoop) settleTurn() {
	if l.result == nil {
		return
	}
	r := l.result
	if l.active != nil && !l.active.closed {
		switch r.status {
		case TurnStatusOK:
			l.reply(l.active, map[string]any{"turn_index": l.turnIndex, "text": r.text})
		case TurnStatusInterrupted:
			l.fail(l.active, errorInterrupted, r.err)
		default:
			l.fail(l.active, errorProviderFailed, r.err)
		}
	}
	l.logActivityError(registry.ActivityTurnEnded, l.emitTurnEnded(r.status))
	l.result, l.active, l.turnID, l.settling, l.state = nil, nil, "", false, stateIdle
	if l.executingControl != nil && l.executingControl.kind == TypeInterrupt {
		l.stopControlDeadline()
		slot := l.executingControl
		l.executingControl = nil
		if controlHasContent(slot.item) {
			l.enqueue(slot.item, false)
		} else {
			l.reply(slot.item, map[string]any{"interrupted": r.status})
		}
	}
	l.maybeRunControl()
	if l.executingControl == nil && l.pendingControl == nil {
		l.startNext()
	}
}

func (l *agentLoop) handleClosure(id string) {
	if item := l.buffer.remove(id); item != nil {
		item.closed = true
		return
	}
	for _, op := range l.committing {
		if op.item != nil && string(op.item.msg.ID) == id {
			op.item.closed = true
		}
	}
	if l.active != nil && string(l.active.msg.ID) == id {
		l.active.closed = true
		l.active = nil
		if l.state == stateTurnActive {
			op := l.opID()
			l.committing[op] = &operation{kind: TypeInterrupt}
			l.state = stateInterrupting
			_ = l.eng.Interrupt(op)
		}
	}
}

func (l *agentLoop) providerLost(cause LostCause, detail string) {
	code := errorProviderCrash
	if cause == LostTimeout {
		code = errorProviderTimeout
	}
	if l.active != nil {
		l.fail(l.active, code, detail)
	}
	for opID, op := range l.committing {
		if op.item != nil {
			l.fail(op.item, code, detail)
		}
		delete(l.committing, opID)
	}
	if l.executingControl != nil {
		l.fail(l.executingControl.item, code, detail)
	}
	l.executingControl, l.active, l.lastOwner, l.turnID, l.result, l.settling, l.state = nil, nil, nil, "", nil, false, stateIdle
	l.maybeRunControl()
	l.startNext()
}

func (l *agentLoop) logActivityError(activityType registry.ActivityType, err error) {
	if err == nil {
		return
	}
	slog.Error("agent activity write failed", "actor", l.sys.Self(), "activity_type", activityType, "error", err)
}

var _ EventPort = (*eventPort)(nil)
var _ BootPort = (*eventPort)(nil)
