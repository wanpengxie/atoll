package base

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base/internal/book"
	"github.com/wanpengxie/atoll/drivers/agents/effectcap"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
)

type intakeFact struct{ msg actorbase.Msg }
type receiveErrorFact struct{ err error }
type closureFact struct{ id book.RequestID }
type receiptFact struct {
	key      string
	revision uint64
}
type baseFaultFact struct{ code, detail string }

type loopEntry struct {
	seq   uint64
	value any
}
type loopInbox struct {
	mu               sync.Mutex
	sealed           bool
	next             uint64
	capacity         int
	reservedCapacity int
	items            []loopEntry
	reserved         []loopEntry
	fault            *loopEntry
	wake             chan struct{}
}

const runtimeEventReserve = 32

func newLoopInbox(capacity int) *loopInbox {
	if capacity < 64 {
		capacity = 64
	}
	return &loopInbox{capacity: capacity, reservedCapacity: runtimeEventReserve, wake: make(chan struct{}, 1)}
}
func (q *loopInbox) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
func (q *loopInbox) push(v any) bool {
	q.mu.Lock()
	if q.sealed || len(q.items) >= q.capacity {
		q.mu.Unlock()
		return false
	}
	q.next++
	q.items = append(q.items, loopEntry{seq: q.next, value: v})
	q.mu.Unlock()
	q.signal()
	return true
}
func (q *loopInbox) pushReserved(v any) bool {
	q.mu.Lock()
	if q.sealed || len(q.reserved) >= q.reservedCapacity {
		q.mu.Unlock()
		return false
	}
	q.next++
	q.reserved = append(q.reserved, loopEntry{seq: q.next, value: v})
	q.mu.Unlock()
	q.signal()
	return true
}
func (q *loopInbox) latchFault(code, detail string) {
	q.mu.Lock()
	if q.sealed || q.fault != nil {
		q.mu.Unlock()
		return
	}
	q.next++
	x := loopEntry{seq: q.next, value: baseFaultFact{code: code, detail: detail}}
	q.fault = &x
	q.mu.Unlock()
	q.signal()
}
func (q *loopInbox) pop() (any, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	source := 0
	var seq uint64
	if len(q.items) > 0 {
		source, seq = 1, q.items[0].seq
	}
	if len(q.reserved) > 0 && (source == 0 || q.reserved[0].seq < seq) {
		source, seq = 2, q.reserved[0].seq
	}
	if q.fault != nil && (source == 0 || q.fault.seq < seq) {
		source = 3
	}
	switch source {
	case 1:
		x := q.items[0]
		q.items[0] = loopEntry{}
		q.items = q.items[1:]
		return x.value, true
	case 2:
		x := q.reserved[0]
		q.reserved[0] = loopEntry{}
		q.reserved = q.reserved[1:]
		return x.value, true
	case 3:
		x := q.fault.value
		q.fault = nil
		return x, true
	}
	return nil, false
}
func (q *loopInbox) seal() {
	q.mu.Lock()
	q.sealed = true
	q.items = nil
	q.reserved = nil
	q.fault = nil
	q.mu.Unlock()
	q.signal()
}
func (q *loopInbox) isSealed() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.sealed }

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
	evRuntimeFault
)

type toolEvent = runtimeproto.ToolEvent
type runtimeEvent struct {
	kind               runtimeEventKind
	op                 runtimeproto.OpID
	turnID             runtimeproto.TurnID
	code, detail, text string
	tool               toolEvent
	status             runtimeproto.TurnStatus
	verdict            runtimeproto.ControlVerdict
	ready              runtimeproto.ReadyResult
	cause              runtimeproto.LostCause
	seed               []byte
}

type runtimePort struct {
	mu             sync.Mutex
	sealed         bool
	toolDropLogged bool
	queue          *loopInbox
}

func (p *runtimePort) send(v runtimeEvent) {
	p.mu.Lock()
	if p.sealed {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	if v.kind == evTool {
		if p.queue.push(v) {
			return
		}
		if p.queue.isSealed() {
			return
		}
		p.mu.Lock()
		first := !p.toolDropLogged
		p.toolDropLogged = true
		p.mu.Unlock()
		if first {
			slog.Warn("agent tool observation dropped under Base inbox pressure")
		}
		return
	}
	if p.queue.pushReserved(v) {
		return
	}
	p.mu.Lock()
	p.sealed = true
	p.mu.Unlock()
	p.queue.latchFault("runtime_events_overflow", "Base lifecycle event reserve exhausted")
}
func (p *runtimePort) Seal() { p.mu.Lock(); p.sealed = true; p.mu.Unlock() }
func (p *runtimePort) TurnStarted(op runtimeproto.OpID, id runtimeproto.TurnID) {
	p.send(runtimeEvent{kind: evTurnStarted, op: op, turnID: id})
}
func (p *runtimePort) TurnRejected(op runtimeproto.OpID, code, detail string) {
	p.send(runtimeEvent{kind: evTurnRejected, op: op, code: code, detail: detail})
}
func (p *runtimePort) Tool(id runtimeproto.TurnID, v runtimeproto.ToolEvent) {
	p.send(runtimeEvent{kind: evTool, turnID: id, tool: v})
}
func (p *runtimePort) TurnEnded(id runtimeproto.TurnID, status runtimeproto.TurnStatus, text, detail string) {
	p.send(runtimeEvent{kind: evTurnEnded, turnID: id, status: status, text: text, detail: detail})
}
func (p *runtimePort) ControlDone(op runtimeproto.OpID, id runtimeproto.TurnID, verdict runtimeproto.ControlVerdict, detail string) {
	p.send(runtimeEvent{kind: evControlDone, op: op, turnID: id, verdict: verdict, detail: detail})
}
func (p *runtimePort) ReadyDone(op runtimeproto.OpID, result runtimeproto.ReadyResult) {
	p.send(runtimeEvent{kind: evReadyDone, op: op, ready: result})
}
func (p *runtimePort) ProviderLost(id runtimeproto.TurnID, cause runtimeproto.LostCause, detail string) {
	p.send(runtimeEvent{kind: evProviderLost, turnID: id, cause: cause, detail: detail})
}
func (p *runtimePort) ResumeSeedUpdated(seed []byte) {
	p.send(runtimeEvent{kind: evSeed, seed: append([]byte(nil), seed...)})
}
func (p *runtimePort) RuntimeFault(code, detail string) {
	p.send(runtimeEvent{kind: evRuntimeFault, code: code, detail: detail})
}

type receiptRow struct{ revision uint64 }
type agentLoop struct {
	def                                       definition
	sys                                       actorbase.Sys
	rt                                        runtimeproto.Runtime
	port                                      *runtimePort
	vault                                     *effectcap.Vault
	exec                                      *executor
	state                                     book.State
	inbox                                     *loopInbox
	local                                     context.Context
	cancel                                    context.CancelFunc
	background                                []runtimeproto.ContextItem
	nextOp, nextTurn, nextAction, nextReceipt uint64
	receipts                                  map[string]receiptRow
	receiptTimers                             map[string]*time.Timer
	logger                                    *slog.Logger
	fault                                     error
	cleanupOnce                               sync.Once
}

func (d definition) run(sys actorbase.Sys) error {
	local, cancel := context.WithCancel(sys.Life())
	vault := effectcap.NewVault()
	capacity := d.cfg.RequestMaxCount + d.cfg.Runtime.Bounds.EventCapacity + 64
	inbox := newLoopInbox(capacity)
	port := &runtimePort{queue: inbox}
	exec := newExecutor(sys, vault)
	deps := runtimeproto.Deps{Parent: local, Tools: newToolBridge(sys, vault), Resources: newResourceBridge(sys, vault), Logger: slog.Default()}
	rt, err := d.cfg.NewRuntime(deps, readSeed(local, sys), port)
	if err != nil {
		cancel()
		vault.Seal()
		port.Seal()
		return fmt.Errorf("agent/base: runtime construct: %w", err)
	}
	exec.bindRuntime(rt)
	l := &agentLoop{def: d, sys: sys, rt: rt, port: port, vault: vault, exec: exec, state: book.New(), inbox: inbox, local: local, cancel: cancel, background: loadCatchup(local, sys), receipts: map[string]receiptRow{}, receiptTimers: map[string]*time.Timer{}, logger: slog.Default()}
	defer l.cleanup()
	go l.receive()
	return l.runLoop()
}

func (l *agentLoop) cleanup() {
	l.cleanupOnce.Do(func() {
		l.vault.Seal()
		l.port.Seal()
		l.rt.Close()
		l.cancel()
		for _, timer := range l.receiptTimers {
			timer.Stop()
		}
		l.inbox.seal()
	})
}

func (l *agentLoop) receive() {
	for {
		msg, err := l.sys.Recv()
		if err != nil {
			if !l.inbox.push(receiveErrorFact{err: err}) {
				l.inbox.latchFault("base_inbox_overflow", "receive completion could not be admitted")
			}
			return
		}
		if !l.inbox.push(intakeFact{msg: msg}) {
			l.inbox.latchFault("base_inbox_overflow", "channel intake could not be admitted")
			return
		}
		select {
		case <-l.local.Done():
			return
		default:
		}
	}
}

func (l *agentLoop) runLoop() error {
	for {
		select {
		case <-l.local.Done():
			if l.fault != nil {
				return l.fault
			}
			return nil
		case <-l.inbox.wake:
			for {
				fact, ok := l.inbox.pop()
				if !ok {
					break
				}
				l.handleFact(fact)
				if l.fault != nil {
					l.state.Faulted = true
					return l.fault
				}
			}
		}
	}
}

func (l *agentLoop) handleFact(v any) {
	switch x := v.(type) {
	case intakeFact:
		l.handleIntake(x.msg)
	case receiveErrorFact:
		if l.local.Err() == nil {
			l.faultNow("receive", x.err.Error())
		}
	case closureFact:
		l.handleClosure(x.id)
	case receiptFact:
		if row, ok := l.receipts[x.key]; ok && row.revision == x.revision {
			l.faultNow("receipt_timeout", x.key+" exceeded coarse receipt deadline")
		}
	case runtimeEvent:
		l.handleRuntimeEvent(x)
	case baseFaultFact:
		l.faultNow(x.code, x.detail)
	default:
		l.faultNow("unknown_fact", fmt.Sprintf("unknown Base fact %T", v))
	}
}

func (l *agentLoop) opID() runtimeproto.OpID {
	l.nextOp++
	if l.nextOp == 0 {
		l.faultNow("counter_overflow", "Base OpID overflow")
		return 0
	}
	return runtimeproto.OpID(l.nextOp)
}

func (l *agentLoop) handleIntake(msg actorbase.Msg) {
	if msg.Kind != message.KindRequest || msg.Sender.ID == l.sys.Self() {
		return
	}
	l.exec.install(msg)
	if msg.Type == introspect.QueryDescribe {
		l.answerDescribe(msg)
		return
	}
	if strings.HasPrefix(msg.Type, "agent.") {
		known := msg.Type == TypeSteer || msg.Type == TypeInterrupt || msg.Type == TypeQueue || msg.Type == TypeStop || msg.Type == TypeTerminate || msg.Type == TypeRestart
		if !known || !l.def.supports(msg.Type) {
			l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "type_unsupported", detail: "agent does not support " + msg.Type})
			return
		}
	}
	if len(l.state.Requests) >= l.def.cfg.RequestMaxCount {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: errorBaseCapacity, detail: "agent live request table capacity exceeded"})
		return
	}
	id := book.RequestID(msg.ID)
	corr := behavior.CorrelationID(msg.CorrelationID, msg.ID)
	row := &book.Request{ID: id, Input: runtimeproto.Input{SourceID: string(msg.ID), Type: msg.Type, Sender: string(msg.Sender.ID), Payload: append(json.RawMessage(nil), msg.Payload...), Text: messageText(msg.Payload)}, Bytes: len(msg.Payload), Sender: string(msg.Sender.ID), ParentID: string(msg.ID), CorrelationID: string(corr)}
	row.Scope = l.vault.Mint(row.ParentID, row.CorrelationID)
	if msg.Type == TypeSteer {
		var payload struct {
			Expected string `json:"expected_turn_id"`
		}
		_ = json.Unmarshal(msg.Payload, &payload)
		row.ExplicitCAS = strings.TrimSpace(payload.Expected) != ""
		row.ExpectedTurn = runtimeproto.TurnID(strings.TrimSpace(payload.Expected))
		row.Input = steerInput(row)
		if strings.TrimSpace(row.Input.Text) == "" {
			l.state.Requests[id] = row
			l.finish(id, terminalCandidate{fail: true, code: errorEmptyInput, detail: "steer requires text input"})
			return
		}
	}
	l.state.Requests[id] = row
	l.watchClosure(id, msg.Ctx())
	switch msg.Type {
	case TypeQueue:
		row.Input = steerInput(row)
		l.enqueue(id, true)
	case TypeSteer:
		l.acceptContent(id, true)
	case TypeInterrupt:
		l.scheduleAction(book.ActionInterrupt, id)
	case TypeStop:
		l.scheduleAction(book.ActionStop, id)
	case TypeTerminate:
		l.scheduleAction(book.ActionTerminate, id)
	case TypeRestart:
		l.scheduleAction(book.ActionRestart, id)
	default:
		l.acceptContent(id, false)
	}
}

func messageText(raw json.RawMessage) string {
	var p struct {
		Text *string `json:"text"`
	}
	if json.Unmarshal(raw, &p) == nil && p.Text != nil {
		return *p.Text
	}
	return string(raw)
}
func steerInput(row *book.Request) runtimeproto.Input {
	x := row.Input
	x.Type = ""
	x.Payload = nil
	x.Text = strings.TrimSpace(messageText(row.Input.Payload))
	return x
}

func (l *agentLoop) answerDescribe(msg actorbase.Msg) {
	req, err := introspect.ParseDescribeRequest(msg.Payload)
	if err != nil {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "payload_invalid", detail: err.Error()})
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
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "type_unsupported", detail: "agent has no type " + req.Type})
		return
	}
	l.exec.terminal(string(msg.ID), terminalCandidate{value: answer})
}

func (l *agentLoop) watchClosure(id book.RequestID, ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			if !l.inbox.push(closureFact{id: id}) {
				l.inbox.latchFault("base_inbox_overflow", "request closure could not be admitted")
			}
		case <-l.local.Done():
		}
	}()
}

func (l *agentLoop) acceptContent(id book.RequestID, explicitSteer bool) {
	row := l.state.Requests[id]
	if row == nil {
		return
	}
	t := l.state.Turn
	if t == nil || t.Phase != book.TurnActive || !l.def.cfg.Runtime.Capabilities.Steer {
		if explicitSteer && row.ExplicitCAS {
			l.finish(id, terminalCandidate{fail: true, code: errorCASMismatch, detail: "no steerable turn"})
			return
		}
		l.enqueue(id, t != nil)
		return
	}
	if row.ExplicitCAS && row.ExpectedTurn != t.ID {
		l.finish(id, terminalCandidate{fail: true, code: errorCASMismatch, detail: "turn target mismatch"})
		return
	}
	l.scheduleAction(book.ActionSteer, id)
}

func (l *agentLoop) enqueue(id book.RequestID, progress bool) {
	row := l.state.Requests[id]
	if row == nil {
		return
	}
	if len(l.state.Buffer) >= l.def.cfg.BufferMaxCount || l.state.BufferBytes+row.Bytes > l.def.cfg.BufferMaxBytes {
		l.finish(id, terminalCandidate{fail: true, code: errorBaseCapacity, detail: "agent request buffer capacity exceeded"})
		return
	}
	row.Location = book.Buffered
	l.state.Buffer = append(l.state.Buffer, id)
	l.state.BufferBytes += row.Bytes
	if progress {
		l.exec.progress(string(id), message.StatusQueued, map[string]any{})
	}
	l.startNext()
}

func (l *agentLoop) startNext() {
	if l.fault != nil || l.state.Turn != nil || l.state.Running != nil || l.state.Pending != nil || len(l.state.Buffer) == 0 {
		return
	}
	first := l.state.Requests[l.state.Buffer[0]]
	if first == nil {
		l.state.Buffer = l.state.Buffer[1:]
		l.startNext()
		return
	}
	batch := make([]book.RequestID, 0, l.def.cfg.BatchMaxCount)
	for len(l.state.Buffer) > 0 && len(batch) < l.def.cfg.BatchMaxCount {
		id := l.state.Buffer[0]
		row := l.state.Requests[id]
		if row == nil {
			l.state.Buffer = l.state.Buffer[1:]
			continue
		}
		if row.Sender != first.Sender {
			break
		}
		l.state.Buffer = l.state.Buffer[1:]
		l.state.BufferBytes -= row.Bytes
		batch = append(batch, id)
	}
	if len(batch) == 0 {
		return
	}
	tail := batch[len(batch)-1]
	messages := make([]runtimeproto.Input, 0, len(batch))
	for _, id := range batch {
		if row := l.state.Requests[id]; row != nil {
			messages = append(messages, runtimeproto.CloneInput(row.Input))
		}
	}
	for _, id := range batch[:len(batch)-1] {
		l.finish(id, terminalCandidate{value: map[string]any{"merged_into": tail}})
	}
	owner := l.state.Requests[tail]
	if owner == nil {
		l.startNext()
		return
	}
	l.nextTurn++
	if l.nextTurn == 0 {
		l.faultNow("counter_overflow", "Base turn serial overflow")
		return
	}
	op := l.opID()
	owner.Location = book.Starting
	l.state.Turn = &book.Turn{Serial: l.nextTurn, Phase: book.TurnStarting, StartOp: op, Owner: tail, Scope: owner.Scope, AnchorParent: owner.ParentID, AnchorCorrelation: owner.CorrelationID}
	cmd := runtimeproto.StartCommand{Op: op, Messages: messages, Background: l.takeBackground(), Scope: owner.Scope}
	if err := l.exec.runtimeStart(cmd); err != nil {
		l.faultNow("command_admission", err.Error())
		return
	}
	l.armReceipt(receiptKey("start", uint64(op)))
}

func (l *agentLoop) scheduleAction(kind book.ActionKind, id book.RequestID) {
	row := l.state.Requests[id]
	if row == nil {
		return
	}
	l.nextAction++
	if l.nextAction == 0 {
		l.faultNow("counter_overflow", "Base action serial overflow")
		return
	}
	a := &book.Action{Serial: l.nextAction, Kind: kind, Request: id}
	row.Location = book.ControlPending
	if old := l.state.Pending; old != nil {
		l.finish(old.Request, terminalCandidate{value: map[string]any{"superseded_by": id}})
	}
	l.state.Pending = a
	l.maybeRunAction()
}

func (l *agentLoop) scheduleCleanup() {
	if l.state.Running != nil || l.state.Pending != nil {
		return
	}
	l.nextAction++
	if l.nextAction == 0 {
		l.faultNow("counter_overflow", "Base action serial overflow")
		return
	}
	l.state.Pending = &book.Action{Serial: l.nextAction, Kind: book.ActionCleanup}
	l.maybeRunAction()
}

func (l *agentLoop) maybeRunAction() {
	if l.state.Running != nil || l.state.Pending == nil {
		return
	}
	a := l.state.Pending
	if (a.Kind == book.ActionSteer || a.Kind == book.ActionInterrupt) && l.state.Turn != nil && l.state.Turn.Phase == book.TurnStarting {
		return
	}
	l.state.Pending = nil
	l.state.Running = a
	if row := l.state.Requests[a.Request]; row != nil {
		row.Location = book.ControlRunning
	}
	switch a.Kind {
	case book.ActionSteer:
		l.runSteer(a)
	case book.ActionInterrupt:
		l.runInterrupt(a)
	case book.ActionStop:
		l.runStop(a)
	case book.ActionTerminate:
		l.runTerminate(a, false)
	case book.ActionRestart:
		l.runTerminate(a, true)
	case book.ActionCleanup:
		l.runCleanup(a)
	}
}

func (l *agentLoop) runSteer(a *book.Action) {
	row, turn := l.state.Requests[a.Request], l.state.Turn
	if row == nil {
		l.completeAction()
		return
	}
	if turn == nil || turn.Phase != book.TurnActive || (row.ExplicitCAS && row.ExpectedTurn != turn.ID) {
		if row.ExplicitCAS {
			l.finish(row.ID, terminalCandidate{fail: true, code: errorCASMismatch, detail: "steer target is no longer active"})
		} else {
			l.state.Running = nil
			l.enqueue(row.ID, true)
			l.maybeRunAction()
		}
		return
	}
	a.Op, a.Target = l.opID(), turn.ID
	content := runtimeproto.CloneInput(row.Input)
	if err := l.exec.runtimeControl(runtimeproto.ControlCommand{Op: a.Op, Kind: runtimeproto.ControlSteer, Target: turn.ID, Content: &content, Scope: row.Scope}); err != nil {
		l.faultNow("command_admission", err.Error())
		return
	}
	l.armReceipt(receiptKey("control", uint64(a.Op)))
}

func (l *agentLoop) runInterrupt(a *book.Action) {
	turn := l.state.Turn
	if turn == nil || turn.Phase != book.TurnActive {
		if a.Request != "" {
			l.finish(a.Request, terminalCandidate{value: map[string]any{"interrupted": false}})
		}
		l.completeAction()
		return
	}
	a.Op, a.Target = l.opID(), turn.ID
	if err := l.exec.runtimeControl(runtimeproto.ControlCommand{Op: a.Op, Kind: runtimeproto.ControlInterrupt, Target: turn.ID}); err != nil {
		l.faultNow("command_admission", err.Error())
		return
	}
	l.armReceipt(receiptKey("control", uint64(a.Op)))
}

func (l *agentLoop) runStop(a *book.Action) {
	l.revokeWork(true)
	turn := l.state.Turn
	if turn != nil && turn.Phase == book.TurnActive && l.def.cfg.Runtime.Capabilities.Interrupt {
		a.Kind, a.Op, a.Target = book.ActionCleanup, l.opID(), turn.ID
		if err := l.exec.runtimeControl(runtimeproto.ControlCommand{Op: a.Op, Kind: runtimeproto.ControlInterrupt, Target: turn.ID}); err != nil {
			l.faultNow("command_admission", err.Error())
			return
		}
		l.armReceipt(receiptKey("control", uint64(a.Op)))
		l.cancelWork(true, "stop")
		l.finish(a.Request, terminalCandidate{value: map[string]any{"stopped": true}})
		a.Request = ""
		return
	}
	l.cancelWork(true, "stop")
	l.finish(a.Request, terminalCandidate{value: map[string]any{"stopped": true}})
	a.Request = ""
	a.Kind = book.ActionCleanup
	l.runCleanup(a)
}

func (l *agentLoop) runCleanup(a *book.Action) {
	turn := l.state.Turn
	if turn == nil {
		l.completeAction()
		return
	}
	if !l.def.cfg.Runtime.Capabilities.Interrupt {
		l.clearTurn()
		l.completeAction()
		return
	}
	if turn.Phase == book.TurnStarting {
		return
	}
	a.Op, a.Target = l.opID(), turn.ID
	if err := l.exec.runtimeControl(runtimeproto.ControlCommand{Op: a.Op, Kind: runtimeproto.ControlInterrupt, Target: turn.ID}); err != nil {
		l.faultNow("command_admission", err.Error())
		return
	}
	l.armReceipt(receiptKey("control", uint64(a.Op)))
}

func (l *agentLoop) runTerminate(a *book.Action, restart bool) {
	l.revokeWork(false)
	if err := l.exec.runtimeTerminate(); err != nil {
		l.faultNow("command_admission", err.Error())
		return
	}
	l.cancelWork(false, map[bool]string{false: "terminate", true: "restart"}[restart])
	l.clearTurn()
	if !restart {
		l.finish(a.Request, terminalCandidate{value: map[string]any{"terminated": true}})
		l.completeAction()
		return
	}
	a.Op = l.opID()
	if err := l.exec.runtimeEnsureReady(a.Op); err != nil {
		l.faultNow("command_admission", err.Error())
		return
	}
	l.armReceipt(receiptKey("ready", uint64(a.Op)))
}

func (l *agentLoop) cancelWork(clearBuffer bool, detail string) {
	if turn := l.state.Turn; turn != nil && turn.Owner != "" {
		l.finish(turn.Owner, terminalCandidate{fail: true, code: errorCancelled, detail: detail})
		turn.Owner = ""
	}
	if clearBuffer {
		ids := append([]book.RequestID(nil), l.state.Buffer...)
		for _, id := range ids {
			l.finish(id, terminalCandidate{fail: true, code: errorCancelled, detail: detail})
		}
		l.state.Buffer = nil
		l.state.BufferBytes = 0
	}
}

func (l *agentLoop) revokeWork(includeBuffer bool) {
	if turn := l.state.Turn; turn != nil && turn.Owner != "" {
		if row := l.state.Requests[turn.Owner]; row != nil {
			l.exec.revoke(row.Scope)
		}
	}
	if includeBuffer {
		for _, id := range l.state.Buffer {
			if row := l.state.Requests[id]; row != nil {
				l.exec.revoke(row.Scope)
			}
		}
	}
}

func (l *agentLoop) handleRuntimeEvent(e runtimeEvent) {
	switch e.kind {
	case evTurnStarted:
		l.onTurnStarted(e)
	case evTurnRejected:
		l.onTurnRejected(e)
	case evTool:
		if t := l.state.Turn; t != nil && t.ID == e.turnID {
			l.emitTool(e.tool)
		} else {
			l.logLate("Tool", e.turnID)
		}
	case evTurnEnded:
		l.onTurnEnded(e)
	case evControlDone:
		l.onControlDone(e)
	case evReadyDone:
		l.onReadyDone(e)
	case evProviderLost:
		l.onProviderLost(e)
	case evSeed:
		l.exec.persistSeed(e.seed)
	case evRuntimeFault:
		l.faultNow(e.code, e.detail)
	}
}

func (l *agentLoop) onTurnStarted(e runtimeEvent) {
	t := l.state.Turn
	if t == nil || t.Phase != book.TurnStarting || t.StartOp != e.op || e.turnID == "" {
		l.logLate("TurnStarted", e.turnID)
		return
	}
	l.clearReceipt(receiptKey("start", uint64(e.op)))
	t.Phase, t.ID = book.TurnActive, e.turnID
	if row := l.state.Requests[t.Owner]; row != nil {
		row.Location = book.Workspace
		l.exec.progress(string(row.ID), message.StatusProcessing, map[string]any{"turn_id": e.turnID})
	}
	l.emitTurnStarted()
	if l.state.Running != nil && l.state.Running.Kind == book.ActionCleanup && l.state.Running.Op == 0 {
		l.runCleanup(l.state.Running)
		return
	}
	if t.Owner == "" {
		l.scheduleCleanup()
	}
	l.maybeRunAction()
}

func (l *agentLoop) onTurnRejected(e runtimeEvent) {
	t := l.state.Turn
	if t == nil || t.Phase != book.TurnStarting || t.StartOp != e.op {
		l.logLate("TurnRejected", e.turnID)
		return
	}
	l.clearReceipt(receiptKey("start", uint64(e.op)))
	if t.Owner != "" {
		l.finish(t.Owner, terminalCandidate{fail: true, code: normalizeStartCode(e.code), detail: e.detail})
	}
	l.clearTurn()
	if a := l.state.Running; a != nil && a.Kind == book.ActionCleanup && a.Op == 0 {
		l.completeAction()
		return
	}
	l.maybeRunAction()
	l.startNext()
}

func (l *agentLoop) onTurnEnded(e runtimeEvent) {
	t := l.state.Turn
	if t == nil || t.Phase != book.TurnActive || t.ID != e.turnID {
		l.logLate("TurnEnded", e.turnID)
		return
	}
	if t.Owner != "" {
		switch e.status {
		case runtimeproto.TurnStatusOK:
			l.finish(t.Owner, terminalCandidate{value: map[string]any{"turn_index": t.Serial, "text": e.text}})
		case runtimeproto.TurnStatusInterrupted:
			l.finish(t.Owner, terminalCandidate{fail: true, code: errorInterrupted, detail: e.detail})
		default:
			l.finish(t.Owner, terminalCandidate{fail: true, code: errorProviderFailed, detail: e.detail})
		}
	}
	l.emitTurnEnded(string(e.status))
	l.clearTurn()
	if a := l.state.Running; a != nil && a.Target == e.turnID {
		l.clearReceipt(receiptKey("terminal", a.Serial))
		a.TerminalSeen = true
		if a.ControlDone {
			l.finishControlAction(a)
		}
	}
	l.maybeRunAction()
	l.startNext()
}

func (l *agentLoop) onProviderLost(e runtimeEvent) {
	t := l.state.Turn
	if t == nil || t.ID != e.turnID {
		l.logLate("ProviderLost", e.turnID)
		return
	}
	code := errorProviderCrash
	if e.cause == runtimeproto.LostTimeout {
		code = errorProviderTimeout
	}
	if t.Owner != "" {
		l.finish(t.Owner, terminalCandidate{fail: true, code: code, detail: e.detail})
	}
	l.emitTurnEnded(string(runtimeproto.TurnStatusFailed))
	l.clearTurn()
	if a := l.state.Running; a != nil && a.Target == e.turnID {
		l.clearReceipt(receiptKey("terminal", a.Serial))
		a.TerminalSeen = true
		if a.ControlDone {
			l.finishControlAction(a)
		}
	}
	l.maybeRunAction()
	l.startNext()
}

func (l *agentLoop) onControlDone(e runtimeEvent) {
	a := l.state.Running
	if a == nil || a.Op != e.op || a.Target != e.turnID {
		l.logLate("ControlDone", e.turnID)
		return
	}
	l.clearReceipt(receiptKey("control", uint64(e.op)))
	a.ControlDone = true
	switch a.Kind {
	case book.ActionSteer:
		l.settleSteer(a, e)
		l.completeAction()
	case book.ActionInterrupt:
		if e.verdict != runtimeproto.ControlAccepted {
			if a.Request != "" {
				l.finish(a.Request, terminalCandidate{fail: true, code: controlErrorCode(e.verdict), detail: e.detail})
			}
			l.completeAction()
			return
		}
		if a.TerminalSeen {
			l.finishControlAction(a)
		} else {
			l.armReceipt(receiptKey("terminal", a.Serial))
		}
	case book.ActionCleanup:
		if e.verdict != runtimeproto.ControlAccepted {
			l.clearTurn()
			l.completeAction()
			return
		}
		if a.TerminalSeen {
			l.completeAction()
		} else {
			l.armReceipt(receiptKey("terminal", a.Serial))
		}
	}
}

func (l *agentLoop) settleSteer(a *book.Action, e runtimeEvent) {
	row := l.state.Requests[a.Request]
	if row == nil {
		return
	}
	if e.verdict == runtimeproto.ControlAccepted {
		turn := l.state.Turn
		if turn == nil || turn.ID != a.Target {
			l.finish(row.ID, terminalCandidate{value: map[string]any{"merged_into": a.Target}})
			return
		}
		if turn.Owner != "" && turn.Owner != row.ID {
			old := turn.Owner
			l.finish(old, terminalCandidate{value: map[string]any{"preempted_by": row.ID}})
		}
		turn.Owner, turn.Scope = row.ID, row.Scope
		turn.AnchorParent, turn.AnchorCorrelation = row.ParentID, row.CorrelationID
		row.Location = book.Workspace
		l.exec.progress(string(row.ID), message.StatusProcessing, map[string]any{"turn_id": turn.ID})
		return
	}
	if e.verdict == runtimeproto.ControlTimeout {
		l.finish(row.ID, terminalCandidate{fail: true, code: errorControlTimeout, detail: e.detail})
		return
	}
	if row.ExplicitCAS {
		l.finish(row.ID, terminalCandidate{fail: true, code: errorCASMismatch, detail: e.detail})
		return
	}
	l.enqueue(row.ID, true)
}

func (l *agentLoop) finishControlAction(a *book.Action) {
	if a.Kind == book.ActionInterrupt && a.Request != "" {
		l.finish(a.Request, terminalCandidate{value: map[string]any{"interrupted": true}})
	}
	l.completeAction()
}

func (l *agentLoop) onReadyDone(e runtimeEvent) {
	a := l.state.Running
	if a == nil || a.Kind != book.ActionRestart || a.Op != e.op {
		l.logLate("ReadyDone", "")
		return
	}
	l.clearReceipt(receiptKey("ready", uint64(e.op)))
	if e.ready.Ready {
		l.finish(a.Request, terminalCandidate{value: map[string]any{"restarted": true}})
	} else {
		code := e.ready.Code
		if code == "" {
			code = errorProviderCrash
		}
		l.finish(a.Request, terminalCandidate{fail: true, code: normalizeStartCode(code), detail: e.ready.Detail})
	}
	l.completeAction()
}

func (l *agentLoop) completeAction() {
	l.state.Running = nil
	if t := l.state.Turn; t != nil && t.Phase == book.TurnActive && t.Owner == "" && l.state.Pending == nil {
		l.scheduleCleanup()
		return
	}
	l.maybeRunAction()
	l.startNext()
}

func (l *agentLoop) finish(id book.RequestID, candidate terminalCandidate) {
	row := l.state.RemoveRequest(id)
	if row == nil {
		l.logger.Error("agent late terminal candidate for missing request", "request", id)
		return
	}
	l.exec.revoke(row.Scope)
	l.exec.terminal(string(id), candidate)
}

func (l *agentLoop) clearTurn() {
	if l.state.Turn != nil {
		l.exec.revoke(l.state.Turn.Scope)
	}
	l.state.Turn = nil
}

func (l *agentLoop) handleClosure(id book.RequestID) {
	row := l.state.Requests[id]
	if row == nil {
		l.logger.Debug("agent late request closure", "request", id)
		return
	}
	l.exec.revoke(row.Scope)
	switch row.Location {
	case book.Buffered:
		l.state.RemoveFromBuffer(id)
	case book.Workspace, book.Starting:
		if l.state.Turn != nil && l.state.Turn.Owner == id {
			l.state.Turn.Owner = ""
			if l.state.Turn.Phase == book.TurnActive {
				l.scheduleCleanup()
			}
		}
	case book.ControlPending:
		if l.state.Pending != nil && l.state.Pending.Request == id {
			l.state.Pending = nil
		}
	case book.ControlRunning:
		if l.state.Running != nil && l.state.Running.Request == id {
			l.state.Running.Request = ""
		}
	}
	delete(l.state.Requests, id)
	l.exec.release(string(id))
	l.maybeRunAction()
	l.startNext()
}

func (l *agentLoop) armReceipt(key string) {
	l.nextReceipt++
	if l.nextReceipt == 0 {
		l.faultNow("counter_overflow", "Base receipt revision overflow")
		return
	}
	rev := l.nextReceipt
	l.receipts[key] = receiptRow{revision: rev}
	if old := l.receiptTimers[key]; old != nil {
		old.Stop()
	}
	l.receiptTimers[key] = time.AfterFunc(l.def.cfg.ReceiptDeadline, func() {
		if !l.inbox.push(receiptFact{key: key, revision: rev}) {
			l.inbox.latchFault("base_inbox_overflow", "receipt timer could not be admitted")
		}
	})
}
func (l *agentLoop) clearReceipt(key string) {
	delete(l.receipts, key)
	if timer := l.receiptTimers[key]; timer != nil {
		timer.Stop()
		delete(l.receiptTimers, key)
	}
}
func receiptKey(kind string, id uint64) string { return fmt.Sprintf("%s/%d", kind, id) }

func (l *agentLoop) faultNow(code, detail string) {
	if l.fault != nil {
		return
	}
	l.state.Faulted = true
	l.fault = fmt.Errorf("agent/base: runtime contract fault %s: %s", code, detail)
	// Fault has no terminal effects. cleanup() performs the fixed
	// Seal-Vault -> Seal-Events -> Close-Runtime -> cancel sequence.
}

func (l *agentLoop) takeBackground() []runtimeproto.ContextItem {
	x := l.background
	l.background = nil
	return x
}
func (l *agentLoop) logLate(kind string, turn runtimeproto.TurnID) {
	l.logger.Error("agent contradictory or late runtime fact", "kind", kind, "turn", turn)
}
func normalizeStartCode(code string) string {
	switch code {
	case errorInputTooLarge, errorProviderCrash, errorProviderFailed, errorProviderTimeout, errorBaseCapacity:
		return code
	}
	return errorProviderFailed
}
func controlErrorCode(v runtimeproto.ControlVerdict) string {
	if v == runtimeproto.ControlTimeout {
		return errorControlTimeout
	}
	return errorProviderFailed
}

var _ runtimeproto.Events = (*runtimePort)(nil)
