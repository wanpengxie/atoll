package base

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
)

type runtimeEventKind uint8

const (
	evTurnStarted runtimeEventKind = iota
	evTurnRejected
	evTool
	evTurnEnded
	evControlDone
	evReadyDone
	evProviderLost
	evSeed
)

type runtimeEvent struct {
	kind               runtimeEventKind
	op                 OpID
	turnID             TurnID
	code, detail, text string
	tool               ToolEvent
	status             TurnStatus
	verdict            ControlVerdict
	ready              ReadyResult
	cause              LostCause
	seed               []byte
}
type runtimePort struct {
	life    context.Context
	mu      sync.Mutex
	closed  bool
	items   []runtimeEvent
	wake    chan struct{}
	persist persistCoordinator
	sys     actorbase.Sys
}

func (p *runtimePort) send(v runtimeEvent) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.items = append(p.items, v)
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}
func (p *runtimePort) Seal() { p.mu.Lock(); p.closed = true; p.items = nil; p.mu.Unlock() }
func (p *runtimePort) pop() (runtimeEvent, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.items) == 0 {
		return runtimeEvent{}, false
	}
	v := p.items[0]
	p.items[0] = runtimeEvent{}
	p.items = p.items[1:]
	return v, true
}
func (p *runtimePort) TurnStarted(op OpID, id TurnID) {
	p.send(runtimeEvent{kind: evTurnStarted, op: op, turnID: id})
}
func (p *runtimePort) TurnRejected(op OpID, c, d string) {
	p.send(runtimeEvent{kind: evTurnRejected, op: op, code: c, detail: d})
}
func (p *runtimePort) Tool(id TurnID, t ToolEvent) {
	p.send(runtimeEvent{kind: evTool, turnID: id, tool: t})
}
func (p *runtimePort) TurnEnded(id TurnID, s TurnStatus, text, detail string) {
	p.send(runtimeEvent{kind: evTurnEnded, turnID: id, status: s, text: text, detail: detail})
}
func (p *runtimePort) ControlDone(op OpID, id TurnID, v ControlVerdict, d string) {
	p.send(runtimeEvent{kind: evControlDone, op: op, turnID: id, verdict: v, detail: d})
}
func (p *runtimePort) ReadyDone(op OpID, r ReadyResult) {
	p.send(runtimeEvent{kind: evReadyDone, op: op, ready: r})
}
func (p *runtimePort) ProviderLost(id TurnID, c LostCause, d string) {
	p.send(runtimeEvent{kind: evProviderLost, turnID: id, cause: c, detail: d})
}
func (p *runtimePort) ResumeSeedUpdated(v []byte) {
	p.send(runtimeEvent{kind: evSeed, seed: append([]byte(nil), v...)})
}

type closureEvent struct{ id string }
type agentLoop struct {
	def                                        definition
	sys                                        actorbase.Sys
	rt                                         Runtime
	port                                       *runtimePort
	book                                       baseBook
	background                                 []RuntimeContextItem
	nextOp, nextTurn, nextAction, nextRevision uint64
	intake                                     chan actorbase.Msg
	recvErr                                    chan error
	closures                                   chan closureEvent
	deadlines                                  chan deadlineFire
}

func (d definition) run(sys actorbase.Sys) error {
	port := &runtimePort{life: sys.Life(), wake: make(chan struct{}, 1), sys: sys}
	tools := newToolBridge(sys)
	resources := newResourceBridge(sys)
	rt, err := d.cfg.NewRuntime(RuntimeDeps{Parent: sys.Life(), Tools: tools, Resources: resources}, readSeed(sys), port)
	if err != nil {
		return fmt.Errorf("agent/base: runtime construct: %w", err)
	}
	defer func() { port.Seal(); rt.Close() }()
	l := &agentLoop{def: d, sys: sys, rt: rt, port: port, background: loadCatchup(sys.Life(), sys), intake: make(chan actorbase.Msg), recvErr: make(chan error, 1), closures: make(chan closureEvent, d.cfg.BufferMaxCount+32), deadlines: make(chan deadlineFire, 64)}
	l.book.buffer = requestBuffer{maxCount: d.cfg.BufferMaxCount, maxBytes: d.cfg.BufferMaxBytes}
	l.book.committing = map[OpID]*commitOp{}
	go l.receive()
	return l.runLoop()
}
func (l *agentLoop) receive() {
	for {
		m, err := l.sys.Recv()
		if err != nil {
			select {
			case l.recvErr <- err:
			case <-l.sys.Life().Done():
			}
			return
		}
		select {
		case l.intake <- m:
		case <-l.sys.Life().Done():
			return
		}
	}
}
func (l *agentLoop) runLoop() error {
	for {
		select {
		case <-l.sys.Life().Done():
			return nil
		case err := <-l.recvErr:
			if l.sys.Life().Err() != nil {
				return nil
			}
			return err
		case m := <-l.intake:
			l.handleIntake(m)
		case <-l.port.wake:
			for {
				e, ok := l.port.pop()
				if !ok {
					break
				}
				l.handleRuntimeEvent(e)
				if l.book.fault != nil {
					break
				}
			}
		case c := <-l.closures:
			l.handleClosure(c.id)
		case d := <-l.deadlines:
			if l.deadlineCurrent(d) {
				return l.runtimeFault("receipt_backstop", d.kind+" receipt deadline exceeded")
			}
		}
		if l.book.fault != nil {
			return fmt.Errorf("agent/base: runtime fault: %s", l.book.fault.detail)
		}
	}
}

func (l *agentLoop) opID() OpID { l.nextOp++; return OpID(fmt.Sprintf("op-%d", l.nextOp)) }
func (l *agentLoop) handleIntake(msg actorbase.Msg) {
	if msg.Kind != message.KindRequest || msg.Sender.ID == l.sys.Self() {
		return
	}
	if msg.Type == introspect.QueryDescribe {
		l.answerDescribe(msg)
		return
	}
	if strings.HasPrefix(msg.Type, "agent.") {
		known := msg.Type == TypeSteer || msg.Type == TypeInterrupt || msg.Type == TypeQueue || msg.Type == TypeStop || msg.Type == TypeTerminate || msg.Type == TypeRestart
		if !known || (msg.Type != TypeSteer && !l.def.supports(msg.Type)) {
			_, _ = l.sys.Fail(msg, "type_unsupported", "agent does not support "+msg.Type)
			return
		}
	}
	i := newRequestItem(msg)
	if msg.Type == TypeSteer {
		var p struct {
			Expected string `json:"expected_turn_id"`
		}
		_ = json.Unmarshal(msg.Payload, &p)
		i.explicitCAS = strings.TrimSpace(p.Expected) != ""
		i.expectedTurn = TurnID(strings.TrimSpace(p.Expected))
		i.input = steerInput(i)
		if strings.TrimSpace(messageText(msg.Payload)) == "" {
			l.fail(i, errorEmptyInput, "steer requires text input")
			return
		}
	}
	go func(id string, ctx context.Context) {
		select {
		case <-ctx.Done():
			select {
			case l.closures <- closureEvent{id}:
			case <-l.sys.Life().Done():
			}
		case <-l.sys.Life().Done():
		}
	}(string(msg.ID), msg.Ctx())
	switch msg.Type {
	case TypeQueue:
		i.input = steerInput(i)
		l.enqueue(i, true)
	case TypeSteer:
		l.acceptContent(i, true)
	case TypeInterrupt:
		l.acceptAction(actionInterrupt, i)
	case TypeStop:
		l.acceptAction(actionStop, i)
	case TypeTerminate:
		l.acceptAction(actionTerminate, i)
	case TypeRestart:
		l.acceptAction(actionRestart, i)
	default:
		l.acceptContent(i, false)
	}
}
func (l *agentLoop) answerDescribe(msg actorbase.Msg) {
	req, err := introspect.ParseDescribeRequest(msg.Payload)
	if err != nil {
		_, _ = l.sys.Fail(msg, "payload_invalid", err.Error())
		return
	}
	d := l.def.cfg.Runtime.Describe
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

func (l *agentLoop) enqueue(i *requestItem, progress bool) {
	if !l.book.buffer.push(i) {
		l.fail(i, errorOverloaded, "agent request buffer limit exceeded")
		return
	}
	if progress {
		_, _ = l.sys.Progress(i.msg, message.StatusQueued, map[string]any{})
	}
	l.startNext()
}
func (l *agentLoop) acceptContent(i *requestItem, steer bool) {
	t := l.book.turn
	if t == nil {
		if steer && i.explicitCAS {
			l.fail(i, errorCASMismatch, "no active turn")
			return
		}
		l.enqueue(i, false)
		return
	}
	if t.turnID == "" || t.terminal != nil || !l.def.cfg.Runtime.Capabilities.Steer {
		if steer && i.explicitCAS {
			l.fail(i, errorCASMismatch, "no steerable turn")
			return
		}
		l.enqueue(i, true)
		return
	}
	if i.explicitCAS && i.expectedTurn != t.turnID {
		l.fail(i, errorCASMismatch, "turn target mismatch")
		return
	}
	op := l.opID()
	l.book.committing[op] = &commitOp{kind: commitSteer, items: []*requestItem{i}, targetTurn: t.seq}
	t.ops[op] = &turnOp{kind: turnOpSteer, blocking: true}
	_, _ = l.sys.Progress(i.msg, message.StatusProcessing, map[string]any{"turn_id": t.turnID})
	cmd := ControlCommand{Op: op, Kind: RuntimeSteer, Target: t.turnID, Content: ptrInput(steerInput(i)), Scope: i.scope}
	if err := l.rt.Control(cmd); err != nil {
		l.runtimeFault("command_admission", err.Error())
		return
	}
	l.arm("turn-op", t.seq, uint64Op(op))
}
func ptrInput(v RuntimeInput) *RuntimeInput { return &v }
func (l *agentLoop) startNext() {
	if l.book.fault != nil || l.book.turn != nil || l.book.running != nil || l.book.pending != nil {
		return
	}
	batch := l.book.buffer.popBatch(l.def.cfg.BatchMaxCount)
	if len(batch) == 0 {
		return
	}
	tail := batch[len(batch)-1]
	for _, i := range batch[:len(batch)-1] {
		l.reply(i, map[string]any{"merged_into": tail.msg.ID})
		i.scope.Revoke()
	}
	if tail.closed {
		l.startNext()
		return
	}
	l.nextTurn++
	op := l.opID()
	t := &baseTurn{seq: l.nextTurn, startOp: op, owner: tail, anchor: tail, scope: tail.scope, ops: map[OpID]*turnOp{}}
	l.book.turn = t
	l.book.committing[op] = &commitOp{kind: commitStart, items: batch, targetTurn: t.seq}
	_, _ = l.sys.Progress(tail.msg, message.StatusProcessing, map[string]any{"turn_index": t.seq})
	msgs := make([]RuntimeInput, len(batch))
	for n, i := range batch {
		msgs[n] = i.input
	}
	cmd := StartCommand{Op: op, Input: TurnInput{Messages: msgs, Background: l.takeBackground()}, Scope: tail.scope}
	if err := l.rt.Start(cmd); err != nil {
		l.runtimeFault("command_admission", err.Error())
		return
	}
	l.arm("start", t.seq, uint64Op(op))
}

func (l *agentLoop) acceptAction(k actionKind, i *requestItem) {
	l.nextAction++
	a := &baseAction{id: l.nextAction, kind: k, item: i}
	if l.book.pending != nil {
		l.reply(l.book.pending.item, map[string]any{"superseded_by": i.msg.ID})
	}
	if l.book.running != nil {
		l.book.pending = a
		return
	}
	l.book.pending = a
	l.maybeRunAction()
}
func (l *agentLoop) maybeRunAction() {
	if l.book.running != nil || l.book.pending == nil {
		return
	}
	if l.book.turn != nil && l.book.turn.turnID == "" && l.book.pending.kind == actionInterrupt {
		return
	}
	a := l.book.pending
	l.book.pending = nil
	l.book.running = a
	t := l.book.turn
	switch a.kind {
	case actionStop:
		l.clearWork("stop", true)
		l.reply(a.item, map[string]any{"stopped": true})
		l.book.running = nil
		if t != nil && t.turnID != "" && l.def.cfg.Runtime.Capabilities.Interrupt {
			op := l.opID()
			t.ops[op] = &turnOp{kind: turnOpInterrupt, blocking: false}
			if err := l.rt.Control(ControlCommand{Op: op, Kind: RuntimeInterrupt, Target: t.turnID}); err != nil {
				l.runtimeFault("command_admission", err.Error())
				return
			}
		}
		l.maybeRunAction()
		l.startNext()
	case actionTerminate:
		l.clearWork("terminate", false)
		if err := l.rt.Terminate(); err != nil {
			l.runtimeFault("command_admission", err.Error())
			return
		}
		l.reply(a.item, map[string]any{"terminated": true})
		l.dropTurn()
		l.book.running = nil
		l.maybeRunAction()
		l.startNext()
	case actionRestart:
		l.clearWork("restart", false)
		if err := l.rt.Terminate(); err != nil {
			l.runtimeFault("command_admission", err.Error())
			return
		}
		l.dropTurn()
		a.op = l.opID()
		a.await = waitWorkerReady
		if err := l.rt.EnsureReady(a.op); err != nil {
			l.runtimeFault("command_admission", err.Error())
			return
		}
		l.arm("action", 0, a.id)
	case actionInterrupt:
		if t == nil || t.turnID == "" {
			l.book.running = nil
			if controlHasContent(a.item) {
				a.item.input = steerInput(a.item)
				a.item.msg.Type = ""
				l.enqueue(a.item, false)
			} else {
				l.reply(a.item, map[string]any{"interrupted": ""})
			}
			l.maybeRunAction()
			return
		}
		a.op = l.opID()
		a.targetTurn = t.seq
		a.await = waitRPC | waitTurnTerminal
		t.ops[a.op] = &turnOp{kind: turnOpInterrupt, blocking: true}
		if err := l.rt.Control(ControlCommand{Op: a.op, Kind: RuntimeInterrupt, Target: t.turnID}); err != nil {
			l.runtimeFault("command_admission", err.Error())
			return
		}
		l.arm("action", t.seq, a.id)
	}
}
func controlHasContent(i *requestItem) bool {
	return i != nil && strings.TrimSpace(messageText(i.msg.Payload)) != ""
}

func (l *agentLoop) handleRuntimeEvent(e runtimeEvent) {
	if l.book.fault != nil {
		return
	}
	t := l.book.turn
	switch e.kind {
	case evTurnStarted:
		if t == nil || t.startOp != e.op || t.turnID != "" || e.turnID == "" {
			return
		}
		t.turnID = e.turnID
		delete(l.book.committing, e.op)
		l.emitTurnStarted()
		if t.owner == nil && l.def.cfg.Runtime.Capabilities.Interrupt {
			op := l.opID()
			t.ops[op] = &turnOp{kind: turnOpInterrupt}
			if err := l.rt.Control(ControlCommand{Op: op, Kind: RuntimeInterrupt, Target: t.turnID}); err != nil {
				l.runtimeFault("command_admission", err.Error())
				return
			}
		}
		l.maybeRunAction()
	case evTurnRejected:
		if t == nil || t.startOp != e.op {
			return
		}
		if c := l.book.committing[e.op]; c != nil {
			for _, i := range c.items {
				if !i.closed {
					l.fail(i, normalizeStartCode(e.code), e.detail)
				}
				i.scope.Revoke()
			}
		}
		delete(l.book.committing, e.op)
		l.dropTurn()
		l.maybeRunAction()
		l.startNext()
	case evTool:
		if t != nil && t.turnID == e.turnID {
			l.emitTool(e.tool)
		}
	case evTurnEnded:
		if t == nil || t.turnID != e.turnID || t.terminal != nil {
			return
		}
		t.terminal = &turnTerminal{status: e.status, text: e.text, detail: e.detail}
		if a := l.book.running; a != nil && a.targetTurn == t.seq {
			a.await &^= waitTurnTerminal
		}
		l.trySettle()
	case evControlDone:
		l.controlDone(e)
	case evReadyDone:
		if a := l.book.running; a != nil && a.op == e.op && a.kind == actionRestart {
			a.await &^= waitWorkerReady
			if !e.ready.Ready {
				l.fail(a.item, errorProviderCrash, e.ready.Detail)
			}
			l.finishAction()
		}
	case evProviderLost:
		if t == nil || t.turnID != e.turnID {
			return
		}
		code := errorProviderCrash
		if e.cause == LostTimeout {
			code = errorProviderTimeout
		}
		l.failTurnOwned(code, e.detail)
		l.emitTurnEnded(TurnStatusFailed)
		l.dropTurn()
		l.finishAction()
		l.maybeRunAction()
		l.startNext()
	case evSeed:
		l.port.persist.submit(l.sys, ResumeSeedKey, e.seed)
	}
}
func (l *agentLoop) controlDone(e runtimeEvent) {
	t := l.book.turn
	if t == nil {
		return
	}
	op := t.ops[e.op]
	if op == nil {
		return
	}
	delete(t.ops, e.op)
	if op.kind == turnOpSteer {
		c := l.book.committing[e.op]
		delete(l.book.committing, e.op)
		if c != nil && len(c.items) > 0 {
			i := c.items[0]
			switch e.verdict {
			case ControlAccepted:
				if t.owner != nil && t.owner != i {
					l.reply(t.owner, map[string]any{"preempted_by": i.msg.ID})
					t.owner.scope.Revoke()
				}
				t.owner = i
				t.anchor = i
				t.scope = i.scope
			case ControlNotSteerable, ControlNoActiveTurn, ControlMismatch:
				if i.explicitCAS {
					l.fail(i, errorCASMismatch, e.detail)
					i.scope.Revoke()
				} else {
					l.enqueue(i, true)
				}
			case ControlEmptyInput:
				l.fail(i, errorEmptyInput, e.detail)
				i.scope.Revoke()
			case ControlInputTooLarge:
				l.fail(i, errorInputTooLarge, e.detail)
				i.scope.Revoke()
			default:
				l.fail(i, errorProviderCrash, e.detail)
				i.scope.Revoke()
			}
		}
	}
	if a := l.book.running; a != nil && a.op == e.op {
		a.await &^= waitRPC
		if e.verdict != ControlAccepted && e.verdict != ControlNoActiveTurn {
			l.fail(a.item, errorProviderCrash, e.detail)
			a.await = 0
		}
	}
	l.trySettle()
	l.finishAction()
}
func (l *agentLoop) trySettle() {
	t := l.book.turn
	if t == nil || t.terminal == nil {
		return
	}
	for _, op := range t.ops {
		if op.blocking {
			return
		}
	}
	if a := l.book.running; a != nil && a.targetTurn == t.seq && a.await != 0 {
		return
	}
	r := t.terminal
	if t.owner != nil && !t.owner.closed {
		switch r.status {
		case TurnStatusOK:
			l.reply(t.owner, map[string]any{"turn_index": t.seq, "text": r.text})
		case TurnStatusInterrupted:
			l.fail(t.owner, errorInterrupted, r.detail)
		default:
			l.fail(t.owner, errorProviderFailed, r.detail)
		}
		t.owner.scope.Revoke()
	}
	l.emitTurnEnded(r.status)
	l.dropTurn()
	l.finishAction()
	l.maybeRunAction()
	l.startNext()
}
func (l *agentLoop) finishAction() {
	a := l.book.running
	if a == nil || a.await != 0 {
		return
	}
	if !a.item.closed {
		switch a.kind {
		case actionInterrupt:
			if controlHasContent(a.item) {
				a.item.input = steerInput(a.item)
				a.item.msg.Type = ""
				l.book.running = nil
				l.enqueue(a.item, false)
				return
			}
			l.reply(a.item, map[string]any{"interrupted": true})
		case actionRestart:
			l.reply(a.item, map[string]any{"restarted": true})
		}
	}
	l.book.running = nil
	l.maybeRunAction()
	l.startNext()
}

func (l *agentLoop) clearWork(detail string, clearBuffer bool) {
	if t := l.book.turn; t != nil {
		if t.owner != nil {
			l.fail(t.owner, errorCancelled, detail)
			t.owner.scope.Revoke()
			t.owner = nil
		}
		for op, c := range l.book.committing {
			for _, i := range c.items {
				l.fail(i, errorCancelled, detail)
				i.scope.Revoke()
			}
			delete(l.book.committing, op)
			delete(t.ops, op)
		}
	}
	if clearBuffer {
		for _, i := range l.book.buffer.items {
			l.fail(i, errorCancelled, detail)
			i.scope.Revoke()
		}
		l.book.buffer.items = nil
		l.book.buffer.bytes = 0
	}
}
func (l *agentLoop) failTurnOwned(code, detail string) {
	if t := l.book.turn; t != nil {
		if t.owner != nil {
			l.fail(t.owner, code, detail)
			t.owner.scope.Revoke()
		}
		for op, c := range l.book.committing {
			for _, i := range c.items {
				l.fail(i, code, detail)
				i.scope.Revoke()
			}
			delete(l.book.committing, op)
		}
	}
	if a := l.book.running; a != nil && !a.item.closed {
		l.fail(a.item, code, detail)
		a.await = 0
	}
}
func (l *agentLoop) dropTurn() {
	if l.book.turn != nil {
		l.book.turn.scope.Revoke()
	}
	l.book.turn = nil
	l.book.committing = map[OpID]*commitOp{}
}
func (l *agentLoop) handleClosure(id string) {
	if i := l.book.buffer.remove(id); i != nil {
		i.closed = true
		i.scope.Revoke()
		return
	}
	if t := l.book.turn; t != nil {
		if t.owner != nil && string(t.owner.msg.ID) == id {
			t.owner.closed = true
			t.owner.scope.Revoke()
			t.owner = nil
			if t.turnID != "" && l.def.cfg.Runtime.Capabilities.Interrupt {
				op := l.opID()
				t.ops[op] = &turnOp{kind: turnOpInterrupt}
				if err := l.rt.Control(ControlCommand{Op: op, Kind: RuntimeInterrupt, Target: t.turnID}); err != nil {
					l.runtimeFault("command_admission", err.Error())
					return
				}
			}
		}
		for _, c := range l.book.committing {
			for _, i := range c.items {
				if string(i.msg.ID) == id {
					i.closed = true
					i.scope.Revoke()
				}
			}
		}
	}
}

func (l *agentLoop) runtimeFault(source, detail string) error {
	if l.book.fault != nil {
		return fmt.Errorf("%s", l.book.fault.detail)
	}
	l.book.fault = &baseFault{source: source, detail: detail}
	for _, i := range l.book.buffer.items {
		l.fail(i, errorAgentInternal, detail)
		i.scope.Revoke()
	}
	l.book.buffer.items = nil
	l.failTurnOwned(errorAgentInternal, detail)
	if l.book.pending != nil {
		l.fail(l.book.pending.item, errorAgentInternal, detail)
	}
	if l.book.running != nil {
		l.fail(l.book.running.item, errorAgentInternal, detail)
	}
	return fmt.Errorf("agent runtime %s: %s", source, detail)
}
func (l *agentLoop) arm(kind string, turn, owner uint64) {
	l.nextRevision++
	d := deadlineFire{turnSeq: turn, owner: owner, revision: l.nextRevision, kind: kind}
	rev := d.revision
	time.AfterFunc(l.def.cfg.ReceiptDeadline, func() {
		d.revision = rev
		select {
		case l.deadlines <- d:
		case <-l.sys.Life().Done():
		}
	})
}
func (l *agentLoop) deadlineCurrent(d deadlineFire) bool {
	switch d.kind {
	case "start":
		return l.book.turn != nil && l.book.turn.seq == d.turnSeq && l.book.turn.turnID == "" && uint64Op(l.book.turn.startOp) == d.owner
	case "turn-op":
		if l.book.turn == nil || l.book.turn.seq != d.turnSeq {
			return false
		}
		for op := range l.book.turn.ops {
			if uint64Op(op) == d.owner {
				return true
			}
		}
	case "action":
		return l.book.running != nil && l.book.running.id == d.owner && l.book.running.await != 0
	}
	return false
}
func uint64Op(op OpID) uint64 { var n uint64; _, _ = fmt.Sscanf(string(op), "op-%d", &n); return n }
func normalizeStartCode(c string) string {
	switch c {
	case errorInputTooLarge, errorProviderCrash, errorProviderFailed, errorProviderTimeout, errorOverloaded:
		return c
	}
	return errorProviderFailed
}
func (l *agentLoop) takeBackground() []RuntimeContextItem {
	x := l.background
	l.background = nil
	return x
}
func (l *agentLoop) logError(msg string, err error) {
	if err != nil {
		slog.Error(msg, "actor", l.sys.Self(), "error", err)
	}
}

var _ RuntimeEvents = (*runtimePort)(nil)
