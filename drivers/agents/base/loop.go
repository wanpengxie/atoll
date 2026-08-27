package base

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base/internal/book"
	"github.com/wanpengxie/atoll/drivers/agents/effectcap"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/schedule"
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
	evProgress
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
	usage              runtimeproto.TurnUsage
	progress           runtimeproto.ProgressEvent
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
	if v.kind == evTool || v.kind == evProgress {
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
func (p *runtimePort) Progress(id runtimeproto.TurnID, v runtimeproto.ProgressEvent) {
	p.send(runtimeEvent{kind: evProgress, turnID: id, progress: v})
}
func (p *runtimePort) TurnEnded(id runtimeproto.TurnID, status runtimeproto.TurnStatus, text, detail string, usage runtimeproto.TurnUsage) {
	p.send(runtimeEvent{kind: evTurnEnded, turnID: id, status: status, text: text, detail: detail, usage: usage})
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

type freezeSource uint8

const (
	freezeSourceNone freezeSource = iota
	freezeSourceHold
	freezeSourceInterrupt
)

var interruptFrozenUntil = time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)

type steerBatch struct {
	IDs     []book.RequestID
	Indices []int
	Content runtimeproto.Input
}

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
	options                                   runtimeproto.TurnOptions
	lastUsage                                 runtimeproto.TurnUsage
	hasUsage                                  bool
	frozenUntil                               time.Time
	heldBy                                    book.RequestID
	freezeSource                              freezeSource
	restoreInterrupt                          bool
	restoreInterruptBy                        book.RequestID
	holdTimer                                 schedule.TimerID
	steerBatches                              map[uint64]steerBatch
	// selectSlot is agent.select's bypass lane (design §8): an exclusive
	// 0-or-1 slot BESIDE the buffer — never in the wait queue, never counted
	// against capacity, never touched by hold/interrupt freezes or steer/replace.
	// A newer select supersedes the held one; the slot runs immediately when no
	// turn is active, otherwise right after the current turn, before anything
	// queued.
	selectSlot book.RequestID
	nowFn      func() time.Time
}

func (d definition) run(sys actorbase.Sys) error {
	local, cancel := context.WithCancel(sys.Life())
	vault := effectcap.NewVault()
	capacity := d.cfg.RequestMaxCount + d.cfg.Runtime.Bounds.EventCapacity + 64
	inbox := newLoopInbox(capacity)
	port := &runtimePort{queue: inbox}
	exec := newExecutor(sys, vault)
	deps := runtimeproto.Deps{Parent: local, Tools: newToolBridge(sys, vault), Resources: newResourceBridge(sys, vault), Logger: slog.Default()}
	seed := readSeed(local, sys)
	options := readSelection(local, sys, d.cfg.Runtime)
	rt, err := d.cfg.NewRuntime(deps, seed, options, port)
	if err != nil {
		cancel()
		vault.Seal()
		port.Seal()
		return fmt.Errorf("agent/base: runtime construct: %w", err)
	}
	exec.bindRuntime(rt)
	l := &agentLoop{def: d, sys: sys, rt: rt, port: port, vault: vault, exec: exec, state: book.New(), inbox: inbox, local: local, cancel: cancel, background: loadCatchup(local, sys), receipts: map[string]receiptRow{}, receiptTimers: map[string]*time.Timer{}, logger: slog.Default(), options: options, nowFn: time.Now}
	if options.Model != "" || options.Effort != "" {
		l.lastUsage = runtimeproto.TurnUsage{Model: options.Model, Effort: options.Effort}
		l.hasUsage = true
	}
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

const defaultHoldDuration = 30 * time.Minute

type replacePayload struct {
	target      book.RequestID
	oldText     string
	newText     string
	attachments []runtimeproto.Attachment
	bytes       int
}

func (l *agentLoop) now() time.Time {
	if l.nowFn != nil {
		return l.nowFn()
	}
	return time.Now()
}

func (l *agentLoop) frozen(now time.Time) bool { return now.Before(l.frozenUntil) }

func (l *agentLoop) handleIntake(msg actorbase.Msg) {
	if msg.Kind == message.KindEvent {
		switch {
		case msg.Type == typeHoldExpired:
			l.handleHoldExpired(msg.Payload)
		case l.isTimerFire(msg):
			l.postTimerWake(msg)
		}
		return
	}
	if msg.Kind != message.KindRequest {
		return
	}
	// A request this actor sent itself is normally noise and is ignored. The
	// one exception is the alarm commission the loop Posts to itself: it is
	// self-addressed BY CONSTRUCTION (that is what gives the alarm turn an
	// owner). The exception runs BOTH ways — a commission is by definition
	// from yourself, so one arriving from anyone else is not an alarm and is
	// refused the same way the word is left out of the advertised vocabulary.
	if self := msg.Sender.ID == l.sys.Self(); self != (msg.Type == TypeTimerWake) {
		return
	}
	l.exec.install(msg)
	if msg.Type == TypeContext {
		value := map[string]any{}
		if l.hasUsage {
			value = usageValue(l.lastUsage)
		}
		if l.frozen(l.now()) {
			value["frozen"] = map[string]any{"held_by": l.heldBy, "until": l.frozenUntil.UnixMilli()}
		}
		l.exec.terminal(string(msg.ID), terminalCandidate{value: value})
		return
	}
	if !l.def.supports(msg.Type) {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "type_unsupported",
			detail: "agent does not support " + msg.Type + "; it accepts " + strings.Join(l.def.accepted(), ", ")})
		return
	}
	if msg.Type == TypeFork {
		l.handleFork(msg)
		return
	}
	switch msg.Type {
	case TypeHold:
		l.handleHold(msg)
		return
	case TypeUnhold:
		l.handleUnhold(msg)
		return
	case TypeInterrupt:
		l.handleInterrupt(msg)
		return
	}
	if msg.Type == TypeSteer {
		form, target, err := parseSteerTarget(msg.Payload)
		if err != nil {
			l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "invalid_args", detail: "invalid agent.steer payload: " + err.Error()})
			return
		}
		switch form {
		case steerTarget:
			l.handleSteerTarget(msg, target)
			return
		case steerAll:
			l.handleSteerAll(msg)
			return
		}
	}
	if msg.Type == TypeNew {
		var payload struct{}
		if err := decodeStrict(msg.Payload, &payload); err != nil {
			l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "invalid_args", detail: "agent.new payload must be an empty object"})
			return
		}
	}
	if msg.Type != TypeReplace && len(l.state.Requests) >= l.def.cfg.RequestMaxCount {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: errorBaseCapacity, detail: "agent live request table capacity exceeded"})
		return
	}
	id := book.RequestID(msg.ID)
	corr := message.CorrelationID(msg.CorrelationID, msg.ID)
	caller := actorbase.EffectiveCaller(msg)
	input := runtimeproto.Input{SourceID: string(msg.ID), Type: msg.Type, Sender: string(msg.Sender.ID), Caller: caller, Payload: append(json.RawMessage(nil), msg.Payload...), Text: messageText(msg.Payload)}
	var replacement replacePayload
	if msg.Type == TypeReplace {
		var code, detail string
		replacement, code, detail = l.validateReplace(msg)
		if code != "" {
			l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: code, detail: detail})
			return
		}
		input.Text = replacement.newText
		input.Attachments = replacement.attachments
	}
	if msg.Type == TypeAsk {
		var ask struct {
			Text        string            `json:"text"`
			Attachments []json.RawMessage `json:"attachments,omitempty"`
		}
		if err := decodeStrict(msg.Payload, &ask); err != nil || strings.TrimSpace(ask.Text) == "" {
			detail := "agent.ask requires text"
			if err != nil {
				detail = "invalid agent.ask payload: " + err.Error()
			}
			l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "invalid_args", detail: detail})
			return
		}
		attachments := make([]runtimeproto.Attachment, 0, len(ask.Attachments))
		for _, raw := range ask.Attachments {
			var attachment runtimeproto.Attachment
			if json.Unmarshal(raw, &attachment) != nil || strings.TrimSpace(attachment.Address) == "" {
				continue
			}
			attachments = append(attachments, attachment)
		}
		input.Text = ask.Text
		input.Attachments = attachments
	}
	rowBytes := len(msg.Payload)
	if msg.Type == TypeReplace {
		rowBytes = replacement.bytes
	}
	row := &book.Request{ID: id, Input: input, Bytes: rowBytes, Sender: string(msg.Sender.ID), ParentID: string(msg.ID), CorrelationID: string(corr)}
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
	if msg.Type == TypeCompact {
		row.TurnKind = runtimeproto.TurnCompact
		row.Input.Text = ""
	}
	if msg.Type == TypeNew {
		row.TurnKind = runtimeproto.TurnNew
		row.Input.Text = ""
	}
	if msg.Type == TypeSelect {
		options, code, detail := l.validateSelection(msg.Payload)
		if code != "" {
			l.state.Requests[id] = row
			l.finish(id, terminalCandidate{fail: true, code: code, detail: detail})
			return
		}
		row.TurnKind, row.Options, row.Input.Text = runtimeproto.TurnSelect, options, ""
	}
	l.state.Requests[id] = row
	l.watchClosure(id, msg.Ctx())
	switch msg.Type {
	case TypeQueue:
		row.Input = steerInput(row)
		l.enqueue(id)
	case TypeCompact, TypeNew:
		l.enqueue(id)
	case TypeSelect:
		l.admitSelectSlot(id)
	case TypeSteer:
		l.acceptContent(id, true)
	case TypeReplace:
		target := l.state.Requests[replacement.target]
		idx := l.state.IndexInBuffer(replacement.target)
		resumed := target.Resumed
		l.finish(replacement.target, terminalCandidate{value: map[string]any{"replaced_by": id}})
		l.admitBufferedAt(id, idx, resumed, false)
	default:
		l.acceptContent(id, false)
	}
}

type steerForm uint8

const (
	steerText steerForm = iota
	steerTarget
	steerAll
)

func parseSteerTarget(raw json.RawMessage) (steerForm, book.RequestID, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return steerText, "", errors.New("payload must be an object")
	}
	_, hasText := fields["text"]
	targetRaw, hasTarget := fields["target"]
	allRaw, hasAll := fields["all"]
	forms := 0
	for _, present := range []bool{hasText, hasTarget, hasAll} {
		if present {
			forms++
		}
	}
	if forms != 1 {
		return steerText, "", errors.New("exactly one of text, target, or all is required")
	}
	if hasTarget {
		if len(fields) != 1 {
			return steerTarget, "", errors.New("target form must contain only target")
		}
		var target string
		if json.Unmarshal(targetRaw, &target) != nil || strings.TrimSpace(target) == "" {
			return steerTarget, "", errors.New("target must be a non-empty string")
		}
		return steerTarget, book.RequestID(target), nil
	}
	if hasAll {
		if len(fields) != 1 || !bytes.Equal(bytes.TrimSpace(allRaw), []byte("true")) {
			return steerAll, "", errors.New("all form must contain only literal true")
		}
		return steerAll, "", nil
	}
	if len(fields) > 2 {
		return steerText, "", errors.New("text form may contain only text and expected_turn_id")
	}
	for name := range fields {
		if name != "text" && name != "expected_turn_id" {
			return steerText, "", errors.New("text form may contain only text and expected_turn_id")
		}
	}
	var text string
	if json.Unmarshal(fields["text"], &text) != nil {
		return steerText, "", errors.New("text must be a string")
	}
	if expected, ok := fields["expected_turn_id"]; ok {
		var turnID string
		if json.Unmarshal(expected, &turnID) != nil {
			return steerText, "", errors.New("expected_turn_id must be a string")
		}
	}
	return steerText, "", nil
}

func (l *agentLoop) handleSteerTarget(msg actorbase.Msg, targetID book.RequestID) {
	row := l.state.Requests[targetID]
	if row == nil || row.Location != book.Buffered || l.state.IndexInBuffer(targetID) < 0 {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: errorCASMismatch, detail: "steer target is not buffered"})
		return
	}
	if row.Sender != string(msg.Sender.ID) {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "target_not_owned", detail: "steer target belongs to a different sender"})
		return
	}
	turn := l.state.Turn
	if turn == nil || turn.Phase != book.TurnActive {
		l.state.RemoveFromBuffer(targetID)
		l.state.InsertAt(0, targetID)
		l.state.BufferBytes += row.Bytes
		l.clearFreeze()
		l.exec.terminal(string(msg.ID), terminalCandidate{value: map[string]any{}})
		l.startNext()
		return
	}
	idx := l.state.IndexInBuffer(targetID)
	l.state.RemoveFromBuffer(targetID)
	l.scheduleSteerTargets([]book.RequestID{targetID}, []int{idx})
	l.exec.terminal(string(msg.ID), terminalCandidate{value: map[string]any{}})
}

func (l *agentLoop) handleSteerAll(msg actorbase.Msg) {
	ids, indices := l.ownedBuffered(string(msg.Sender.ID))
	if len(ids) == 0 {
		l.exec.terminal(string(msg.ID), terminalCandidate{value: map[string]any{}})
		return
	}
	for _, id := range ids {
		l.state.RemoveFromBuffer(id)
	}
	turn := l.state.Turn
	if turn == nil || turn.Phase != book.TurnActive {
		for idx := len(ids) - 1; idx >= 0; idx-- {
			id := ids[idx]
			l.state.InsertAt(0, id)
			if row := l.state.Requests[id]; row != nil {
				l.state.BufferBytes += row.Bytes
			}
		}
		l.clearFreeze()
		l.exec.terminal(string(msg.ID), terminalCandidate{value: map[string]any{}})
		l.startNext()
		return
	}
	l.scheduleSteerTargets(ids, indices)
	l.exec.terminal(string(msg.ID), terminalCandidate{value: map[string]any{}})
}

func (l *agentLoop) ownedBuffered(sender string) ([]book.RequestID, []int) {
	ids := make([]book.RequestID, 0)
	indices := make([]int, 0)
	for idx, id := range l.state.Buffer {
		row := l.state.Requests[id]
		if row == nil || row.Location != book.Buffered || row.Sender != sender {
			continue
		}
		// Command rows (compact) are instructions, not content — sweeping one
		// into a turn would turn a directive into a meaningless empty injection.
		if isCommandRequest(row) {
			continue
		}
		ids = append(ids, id)
		indices = append(indices, idx)
	}
	return ids, indices
}

func parseHoldPayload(raw json.RawMessage) (string, time.Duration, error) {
	var payload struct {
		Target     json.RawMessage `json:"target"`
		DurationMS json.RawMessage `json:"duration_ms"`
	}
	if err := decodeStrict(raw, &payload); err != nil {
		return "", 0, err
	}
	target := ""
	if len(payload.Target) != 0 {
		if bytes.Equal(bytes.TrimSpace(payload.Target), []byte("null")) || json.Unmarshal(payload.Target, &target) != nil {
			return "", 0, errors.New("target must be a string")
		}
	}
	duration := defaultHoldDuration
	if len(payload.DurationMS) != 0 {
		if bytes.Equal(bytes.TrimSpace(payload.DurationMS), []byte("null")) {
			return "", 0, errors.New("duration_ms must be an integer")
		}
		var milliseconds int64
		if json.Unmarshal(payload.DurationMS, &milliseconds) != nil || milliseconds < 1 || milliseconds > defaultHoldDuration.Milliseconds() {
			return "", 0, errors.New("duration_ms must be an integer between 1 and 1800000")
		}
		duration = time.Duration(milliseconds) * time.Millisecond
	}
	return target, duration, nil
}

func (l *agentLoop) handleHold(msg actorbase.Msg) {
	targetText, duration, err := parseHoldPayload(msg.Payload)
	if err != nil {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "invalid_args", detail: "invalid agent.hold payload: " + err.Error()})
		return
	}
	targetID := book.RequestID(targetText)
	if targetID != "" {
		row := l.state.Requests[targetID]
		turnOwner := l.state.Turn != nil && l.state.Turn.Owner == targetID
		if row == nil {
			l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: errorCASMismatch, detail: "hold target is not buffered or the current turn owner"})
			return
		}
		if turnOwner && l.interruptBusy() {
			l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "busy", detail: "current turn cannot be interrupted while it is starting or another control action is running"})
			return
		}
		activeOwner := turnOwner && l.state.Turn.Phase == book.TurnActive && row.Location == book.Workspace
		if row.Location != book.Buffered && !activeOwner {
			l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: errorCASMismatch, detail: "hold target is not buffered or the current active turn owner"})
			return
		}
		if row.Sender != string(msg.Sender.ID) {
			l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "target_not_owned", detail: "hold target belongs to a different sender"})
			return
		}
		if isCommandRequest(row) {
			l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "invalid_args", detail: "compact and select requests cannot be edited"})
			return
		}
	}
	id := book.RequestID(msg.ID)
	if l.freezeSource == freezeSourceInterrupt {
		l.restoreInterrupt = true
		l.restoreInterruptBy = l.heldBy
	}
	l.freeze(id, duration)
	if targetID != "" && l.state.Turn != nil && l.state.Turn.Owner == targetID && l.def.cfg.Runtime.Capabilities[runtimeproto.CapabilityInterrupt] {
		l.runAdmittedInterrupt(id, book.DispRebufferOwner, id, targetID)
	}
	l.exec.terminal(string(msg.ID), terminalCandidate{value: map[string]any{}})
}

func (l *agentLoop) handleUnhold(msg actorbase.Msg) {
	l.releaseHold()
	l.exec.terminal(string(msg.ID), terminalCandidate{value: map[string]any{}})
}

func (l *agentLoop) handleInterrupt(msg actorbase.Msg) {
	var payload struct{}
	if err := decodeStrict(msg.Payload, &payload); err != nil {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "invalid_args", detail: "agent.interrupt payload must be an empty object"})
		return
	}
	if l.interruptBusy() {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "busy", detail: "current turn cannot be interrupted while it is starting or another control action is running"})
		return
	}
	id := book.RequestID(msg.ID)
	l.freezeInterrupt(id)
	if l.state.Turn != nil && l.def.cfg.Runtime.Capabilities[runtimeproto.CapabilityInterrupt] {
		l.runAdmittedInterrupt(id, book.DispFailOwner, id, "")
	}
	l.exec.terminal(string(msg.ID), terminalCandidate{value: map[string]any{}})
}

func (l *agentLoop) validateReplace(msg actorbase.Msg) (replacePayload, string, string) {
	var payload struct {
		Target      *string         `json:"target"`
		OldText     *string         `json:"old_text"`
		NewText     *string         `json:"new_text"`
		Attachments json.RawMessage `json:"attachments"`
	}
	if err := decodeStrict(msg.Payload, &payload); err != nil || payload.Target == nil || strings.TrimSpace(*payload.Target) == "" || payload.OldText == nil || payload.NewText == nil {
		detail := "target, old_text, and new_text are required strings"
		if err != nil {
			detail = err.Error()
		}
		return replacePayload{}, "invalid_args", "invalid agent.replace payload: " + detail
	}
	targetID := book.RequestID(*payload.Target)
	target := l.state.Requests[targetID]
	if target == nil {
		return replacePayload{}, errorCASMismatch, "replace target does not exist"
	}
	if target.Sender != string(msg.Sender.ID) {
		return replacePayload{}, "target_not_owned", "replace target belongs to a different sender"
	}
	if target.Location != book.Buffered || l.state.IndexInBuffer(targetID) < 0 || target.Input.Text != *payload.OldText {
		return replacePayload{}, errorCASMismatch, "replace target is not buffered at the expected text"
	}
	if isCommandRequest(target) {
		return replacePayload{}, "invalid_args", "replace target is an instruction, not an editable message"
	}
	attachments := []runtimeproto.Attachment(nil)
	newBytes := len(*payload.NewText)
	if len(payload.Attachments) != 0 {
		if bytes.Equal(bytes.TrimSpace(payload.Attachments), []byte("null")) {
			return replacePayload{}, "invalid_args", "invalid agent.replace payload: attachments must be an array"
		}
		var rawAttachments []json.RawMessage
		if json.Unmarshal(payload.Attachments, &rawAttachments) != nil {
			return replacePayload{}, "invalid_args", "invalid agent.replace payload: attachments must be an array"
		}
		attachments = make([]runtimeproto.Attachment, 0, len(rawAttachments))
		for _, raw := range rawAttachments {
			newBytes += len(raw)
			var attachment runtimeproto.Attachment
			if json.Unmarshal(raw, &attachment) != nil || strings.TrimSpace(attachment.Address) == "" {
				continue
			}
			attachments = append(attachments, attachment)
		}
	}
	if l.state.BufferBytes-target.Bytes+newBytes > l.def.cfg.BufferMaxBytes {
		return replacePayload{}, errorBaseCapacity, "replacement exceeds agent request buffer capacity"
	}
	return replacePayload{target: targetID, oldText: *payload.OldText, newText: *payload.NewText, attachments: attachments, bytes: newBytes}, "", ""
}

func (l *agentLoop) freeze(holdID book.RequestID, duration time.Duration) {
	l.clearHoldTimer()
	l.frozenUntil = l.now().Add(duration)
	l.heldBy = holdID
	l.freezeSource = freezeSourceHold
	if l.sys != nil {
		l.holdTimer, _ = l.sys.After(duration, typeHoldExpired, map[string]any{"hold_id": string(holdID)}, schedule.TimerHomeMemory)
	}
}

func (l *agentLoop) freezeInterrupt(holdID book.RequestID) {
	l.clearHoldTimer()
	l.frozenUntil = interruptFrozenUntil
	l.heldBy = holdID
	l.freezeSource = freezeSourceInterrupt
	l.restoreInterrupt = false
	l.restoreInterruptBy = ""
}

func (l *agentLoop) clearFreeze() {
	l.frozenUntil = time.Time{}
	l.heldBy = ""
	l.freezeSource = freezeSourceNone
	l.restoreInterrupt = false
	l.restoreInterruptBy = ""
	l.clearHoldTimer()
}

func (l *agentLoop) releaseHold() {
	restore, holder := l.restoreInterrupt, l.restoreInterruptBy
	l.clearFreeze()
	if restore {
		l.freezeInterrupt(holder)
		return
	}
	l.startNext()
}

func (l *agentLoop) clearHoldTimer() {
	if l.holdTimer == "" {
		return
	}
	timer := l.holdTimer
	l.holdTimer = ""
	if l.sys != nil {
		_ = l.sys.CancelTimer(timer)
	}
}

func (l *agentLoop) handleHoldExpired(raw json.RawMessage) {
	var payload struct {
		HoldID string `json:"hold_id"`
	}
	_ = json.Unmarshal(raw, &payload)
	if book.RequestID(payload.HoldID) != l.heldBy {
		return
	}
	now := l.now()
	if !l.frozen(now) {
		l.releaseHold()
		return
	}
	if l.sys != nil {
		l.holdTimer, _ = l.sys.After(l.frozenUntil.Sub(now), typeHoldExpired, map[string]any{"hold_id": string(l.heldBy)}, schedule.TimerHomeMemory)
	}
}

func (l *agentLoop) handleFork(msg actorbase.Msg) {
	var payload struct{}
	if err := decodeStrict(msg.Payload, &payload); err != nil {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "invalid_args", detail: "agent.fork payload must be an object"})
		return
	}
	parts := strings.Split(string(l.sys.Self()), ":")
	if len(parts) != 3 || parts[1] == "" {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "internal_error", detail: "agent identity has no declaration seed"})
		return
	}
	if kind, ok := actor.ParseKind(parts[0]); !ok || kind != actor.KindAgent {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "internal_error", detail: "agent identity is malformed"})
		return
	}
	pending, err := l.sys.Call(msg.Cause(), actor.SystemActorID, "system.member.create", map[string]any{"decl_id": parts[1]})
	if err != nil {
		l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "internal_error", detail: err.Error()})
		return
	}
	go func() {
		terminal, waitErr := pending.Wait(msg.Ctx(), 0)
		if waitErr != nil {
			l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: "internal_error", detail: waitErr.Error()})
			return
		}
		var outcome struct {
			Status string `json:"status"`
			message.Failure
		}
		if json.Unmarshal(terminal.Payload, &outcome) != nil {
			outcome = struct {
				Status string `json:"status"`
				message.Failure
			}{Status: message.StatusFailed, Failure: message.Failure{ErrorCode: "internal_error", Detail: "member creation returned an invalid terminal"}}
		}
		if outcome.Status == message.StatusFailed {
			if outcome.ErrorCode == "" {
				outcome.Failure = message.Failure{ErrorCode: "internal_error", Detail: "member creation failed"}
			}
			l.exec.terminal(string(msg.ID), terminalCandidate{fail: true, code: outcome.ErrorCode, detail: outcome.Detail})
			return
		}
		l.exec.terminal(string(msg.ID), terminalCandidate{value: append(json.RawMessage(nil), terminal.Payload...)})
	}()
}

func decodeStrict(raw json.RawMessage, out any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage(`{}`)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
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

func (l *agentLoop) validateSelection(raw json.RawMessage) (runtimeproto.TurnOptions, string, string) {
	if len(l.def.cfg.Runtime.Selections) == 0 {
		return runtimeproto.TurnOptions{}, "type_unsupported", "agent does not support " + TypeSelect
	}
	var requested runtimeproto.TurnOptions
	if len(raw) != 0 && json.Unmarshal(raw, &requested) != nil {
		return runtimeproto.TurnOptions{}, "invalid_args", "selection payload must be an object"
	}
	requested.Model = strings.TrimSpace(requested.Model)
	requested.Effort = strings.TrimSpace(requested.Effort)
	if requested.Model == "" && requested.Effort != "" {
		requested.Model = l.options.Model
	}
	for _, candidate := range l.def.cfg.Runtime.Selections {
		if requested.Model != candidate.Model {
			continue
		}
		if requested.Effort == "" || requested.Effort == candidate.Effort {
			return candidate, "", ""
		}
	}
	return runtimeproto.TurnOptions{}, "invalid_args", "selection is not in provider selections"
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
	if !explicitSteer {
		l.enqueue(id)
		return
	}
	t := l.state.Turn
	if t == nil || t.Phase != book.TurnActive || !l.def.cfg.Runtime.Capabilities[runtimeproto.CapabilitySteer] {
		if explicitSteer && row.ExplicitCAS {
			l.finish(id, terminalCandidate{fail: true, code: errorCASMismatch,
				detail: "no steerable turn: no turn is active, so there is nothing to steer into. Send agent.ask to start work, or resend without expected_turn_id to queue behind whatever runs next"})
			return
		}
		l.enqueue(id)
		return
	}
	if row.ExplicitCAS && row.ExpectedTurn != t.ID {
		l.finish(id, terminalCandidate{fail: true, code: errorCASMismatch,
			detail: fmt.Sprintf("turn target mismatch: expected_turn_id %q, but the active turn is %q. Re-read turn_id from the most recent processing reply and resend; the turn you named has already ended", row.ExpectedTurn, t.ID)})
		return
	}
	l.scheduleSteer(id)
}

func (l *agentLoop) enqueue(id book.RequestID) {
	l.admitBufferedAt(id, -1, false, true)
}

func (l *agentLoop) admitBufferedAt(id book.RequestID, idx int, resumed, releaseFreeze bool) {
	row := l.state.Requests[id]
	if row == nil {
		return
	}
	if releaseFreeze {
		l.clearFreeze()
	}
	if len(l.state.Buffer) >= l.def.cfg.BufferMaxCount || l.state.BufferBytes+row.Bytes > l.def.cfg.BufferMaxBytes {
		l.finish(id, terminalCandidate{fail: true, code: errorBaseCapacity, detail: "agent request buffer capacity exceeded"})
		l.startNext()
		return
	}
	row.Resumed = resumed
	row.Location = book.Buffered
	if idx < 0 {
		l.state.Buffer = append(l.state.Buffer, id)
	} else {
		l.state.InsertAt(idx, id)
	}
	l.state.BufferBytes += row.Bytes
	l.startNext()
	row = l.state.Requests[id]
	if row == nil {
		return
	}
	if row.Location == book.Buffered {
		row.LastHeartbeatMs = l.now().UnixMilli()
		l.exec.progress(string(id), message.StatusQueued, map[string]any{"controls": l.queuedControls()})
	}
}

// queuedHeartbeatEvery is the floor between two queued heartbeats for the
// same buffered request.
//
// A request waiting behind a long turn hears nothing after its admission
// frame, and the caller's sliding deadline (actorbase.DefaultTimeout, 5 min)
// reads that silence as "no progress" — the request times out and is dropped
// before the turn ever reaches it. The heartbeat is deliberately NOT a timer
// and NOT an engine mechanism: it is written only when the running turn has
// just produced a real runtime event (touchQueued is called from the tool /
// stage progress sites), so a stuck runtime stops the heartbeat and the
// waiting caller times out exactly as before. It says "I am processing
// someone else, you are behind them, and I am demonstrably moving" — a claim
// only the loop can make. 60 s against a 5 min window leaves four beats of
// slack; a 30 min turn adds at most 30 rows per waiting request.
const queuedHeartbeatEvery = 60 * time.Second

// touchQueued re-affirms every buffered request whose last queued frame is
// older than queuedHeartbeatEvery. Same frame shape as admission (status +
// controls), plus the position in line, when the turn ahead started, and a
// heartbeat marker so the front end can tell a re-affirmation from a move.
func (l *agentLoop) touchQueued() {
	if len(l.state.Buffer) == 0 {
		return
	}
	nowMs := l.now().UnixMilli()
	for pos, id := range l.state.Buffer {
		row := l.state.Requests[id]
		if row == nil || row.Location != book.Buffered {
			continue
		}
		if nowMs-row.LastHeartbeatMs < queuedHeartbeatEvery.Milliseconds() {
			continue
		}
		row.LastHeartbeatMs = nowMs
		value := map[string]any{
			"controls":  l.queuedControls(),
			"position":  pos + 1,
			"heartbeat": true,
		}
		if t := l.state.Turn; t != nil && t.StartedAtMs != 0 {
			value["current_turn_since"] = t.StartedAtMs
		}
		l.exec.progress(string(id), message.StatusQueued, value)
	}
}

// progress 契约：凡带 status 的进度帧必带 controls——受理方在这条消息自己的账上
// 宣告"此刻可以对它用哪些控制词"。全量快照、后帧覆盖前帧、终态帧恒不带。
// 前端据此画按钮，不查任何表；能力差异（steer/interrupt）只在这里收敛。
func (l *agentLoop) queuedControls() []map[string]any {
	controls := []map[string]any{{"word": TypeReplace}}
	if l.def.cfg.Runtime.Capabilities[runtimeproto.CapabilitySteer] {
		controls = append(controls, map[string]any{"word": TypeSteer})
	}
	return controls
}

func (l *agentLoop) processingControls() []map[string]any {
	if !l.def.cfg.Runtime.Capabilities[runtimeproto.CapabilityInterrupt] {
		return []map[string]any{}
	}
	// 运行中编辑 = 先打断，故 processing 位置的 replace 依赖 interrupt 能力——
	// 这条规则只活在受理方，前端恒不重演。
	return []map[string]any{{"word": TypeInterrupt}, {"word": TypeReplace}}
}

// admitSelectSlot places one agent.select into the bypass slot. Supersession is
// the slot's whole concurrency story: the previous occupant (if any) fails with
// "superseded" — it never took effect, so a success terminal would lie.
func (l *agentLoop) admitSelectSlot(id book.RequestID) {
	if l.selectSlot != "" && l.selectSlot != id {
		l.finish(l.selectSlot, terminalCandidate{fail: true, code: "superseded", detail: "superseded by a newer agent.select"})
	}
	l.selectSlot = id
	// Slot registration receipt. Controls are empty: the slot is outside the
	// buffer, so replace/steer can never address it — cancel rides the request
	// closure like any request.
	l.exec.progress(string(id), message.StatusQueued, map[string]any{"controls": []map[string]any{}})
	l.maybeRunSelectSlot()
}

// maybeRunSelectSlot runs the bypass slot when no turn is active. Deliberately
// NO freeze check: switching a model must neither wake a stopped agent's queue
// nor be blocked by it — the select turn runs alone, and the queue's own gate
// (startNext's frozen guard) keeps everything else asleep afterwards.
func (l *agentLoop) maybeRunSelectSlot() {
	if l.selectSlot == "" || l.fault != nil || l.state.Turn != nil || l.state.Running != nil || l.state.Pending != nil {
		return
	}
	id := l.selectSlot
	l.selectSlot = ""
	if l.state.Requests[id] == nil {
		return // closed (cancelled) while waiting in the slot
	}
	l.beginTurn(id, nil)
}

func (l *agentLoop) startNext() {
	// The bypass slot goes before anything queued: "after the current turn,
	// before the next instruction" holds on every resume path because they all
	// funnel through here.
	l.maybeRunSelectSlot()
	if l.frozen(l.now()) || l.fault != nil || l.state.Turn != nil || l.state.Running != nil || l.state.Pending != nil || len(l.state.Buffer) == 0 {
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
		if row.Input.Caller != first.Input.Caller {
			break
		}
		if len(batch) > 0 && (isCommandRequest(row) || row.Resumed) {
			break
		}
		l.state.Buffer = l.state.Buffer[1:]
		l.state.BufferBytes -= row.Bytes
		batch = append(batch, id)
		if isCommandRequest(row) || row.Resumed {
			break
		}
	}
	if len(batch) == 0 {
		return
	}
	tail := batch[len(batch)-1]
	messages := make([]runtimeproto.Input, 0, len(batch))
	for _, id := range batch {
		if row := l.state.Requests[id]; row != nil {
			if isCommandRequest(row) {
				continue
			}
			input := runtimeproto.CloneInput(row.Input)
			if row.Resumed {
				input.Text = resumedInputText(row)
			}
			messages = append(messages, input)
		}
	}
	for _, id := range batch[:len(batch)-1] {
		l.finish(id, terminalCandidate{value: map[string]any{"merged_into": tail}})
	}
	if l.state.Requests[tail] == nil {
		l.startNext()
		return
	}
	l.beginTurn(tail, messages)
}

// beginTurn opens one turn owned by tail. Shared by the queue path (startNext,
// with the batch's messages) and the select bypass slot (nil messages — a
// select turn carries no content, it only registers options).
func (l *agentLoop) beginTurn(tail book.RequestID, messages []runtimeproto.Input) {
	owner := l.state.Requests[tail]
	if owner == nil {
		return
	}
	l.nextTurn++
	if l.nextTurn == 0 {
		l.faultNow("counter_overflow", "Base turn serial overflow")
		return
	}
	op := l.opID()
	owner.Location = book.Starting
	l.state.Turn = &book.Turn{Serial: l.nextTurn, Phase: book.TurnStarting, StartOp: op, Owner: tail, Scope: owner.Scope, AnchorParent: owner.ParentID, AnchorCorrelation: owner.CorrelationID, StartedAtMs: l.now().UnixMilli()}
	l.exec.progress(string(owner.ID), message.StatusProcessing, map[string]any{"controls": l.processingControls()})
	cmd := runtimeproto.StartCommand{Op: op, Messages: messages, Background: l.takeBackground(), Scope: owner.Scope, Kind: owner.TurnKind, Options: owner.Options}
	if err := l.exec.runtimeStart(cmd); err != nil {
		l.faultNow("command_admission", err.Error())
		return
	}
	l.armReceipt(receiptKey("start", uint64(op)))
}

func isCommandRequest(row *book.Request) bool {
	return row != nil && (row.TurnKind == runtimeproto.TurnCompact || row.TurnKind == runtimeproto.TurnSelect || row.TurnKind == runtimeproto.TurnNew)
}

func resumedInputText(row *book.Request) string {
	if row != nil && row.Input.Type == TypeReplace {
		var payload struct {
			OldText string `json:"old_text"`
			NewText string `json:"new_text"`
		}
		if json.Unmarshal(row.Input.Payload, &payload) == nil {
			return fmt.Sprintf("用户明确将 %q 修改为 %q，请遵循更新之后的指令或信息，其余保持不变。", payload.OldText, payload.NewText)
		}
	}
	return "请继续刚才被打断的工作。"
}

func (l *agentLoop) scheduleSteer(id book.RequestID) {
	l.scheduleSteerAt(id, false, -1, nil)
}

func (l *agentLoop) interruptBusy() bool {
	turn := l.state.Turn
	if turn == nil {
		return false
	}
	return turn.Phase == book.TurnStarting || l.state.Running != nil
}

// Interrupt actions are admitted only when they can run immediately. Keeping
// them out of Pending makes the no-queue rule structural instead of relying on
// a later unfreeze or turn-start path to detach them.
func (l *agentLoop) runAdmittedInterrupt(id book.RequestID, disposition book.ActionDisposition, holder, ownerAtAdmit book.RequestID) {
	turn := l.state.Turn
	if turn == nil || turn.Phase != book.TurnActive || l.state.Running != nil {
		return
	}
	l.nextAction++
	if l.nextAction == 0 {
		l.faultNow("counter_overflow", "Base action serial overflow")
		return
	}
	a := &book.Action{Serial: l.nextAction, Kind: book.ActionInterrupt, Request: id, Disposition: disposition, HolderID: holder, OwnerAtAdmit: ownerAtAdmit}
	l.state.Running = a
	l.runInterrupt(a)
}

func (l *agentLoop) scheduleSteerTargets(ids []book.RequestID, indices []int) {
	if len(ids) == 0 || len(ids) != len(indices) {
		return
	}
	content, ok := l.steerBatchInput(ids)
	if !ok {
		return
	}
	tail := ids[len(ids)-1]
	batch := &steerBatch{IDs: append([]book.RequestID(nil), ids...), Indices: append([]int(nil), indices...), Content: content}
	l.scheduleSteerAt(tail, true, indices[len(indices)-1], batch)
}

func (l *agentLoop) steerBatchInput(ids []book.RequestID) (runtimeproto.Input, bool) {
	texts := make([]string, 0, len(ids))
	attachments := make([]runtimeproto.Attachment, 0)
	var content runtimeproto.Input
	for _, id := range ids {
		row := l.state.Requests[id]
		if row == nil {
			continue
		}
		input := runtimeproto.CloneInput(row.Input)
		if row.Resumed {
			input.Text = resumedInputText(row)
		}
		texts = append(texts, input.Text)
		attachments = append(attachments, input.Attachments...)
		content = input
	}
	if len(texts) == 0 {
		return runtimeproto.Input{}, false
	}
	content.Text = strings.Join(texts, "\n\n")
	content.Attachments = attachments
	return content, true
}

func (l *agentLoop) scheduleSteerAt(id book.RequestID, steerTarget bool, bufferIndex int, batch *steerBatch) {
	row := l.state.Requests[id]
	if row == nil {
		return
	}
	l.nextAction++
	if l.nextAction == 0 {
		l.faultNow("counter_overflow", "Base action serial overflow")
		return
	}
	a := &book.Action{Serial: l.nextAction, Kind: book.ActionSteer, Request: id, SteerTarget: steerTarget, BufferIndex: bufferIndex}
	if batch != nil {
		if l.steerBatches == nil {
			l.steerBatches = make(map[uint64]steerBatch)
		}
		l.steerBatches[a.Serial] = *batch
		for _, batchID := range batch.IDs {
			if batchRow := l.state.Requests[batchID]; batchRow != nil {
				batchRow.Location = book.ControlPending
			}
		}
	} else {
		row.Location = book.ControlPending
	}
	if old := l.state.Pending; old != nil {
		if old.Kind == book.ActionSteer && old.SteerTarget {
			l.returnSteerTarget(old)
			delete(l.steerBatches, old.Serial)
		} else if l.state.Requests[old.Request] != nil {
			l.finish(old.Request, terminalCandidate{value: map[string]any{"superseded_by": id}})
		}
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
	if a.Kind == book.ActionSteer && l.state.Turn != nil && l.state.Turn.Phase == book.TurnStarting {
		return
	}
	l.state.Pending = nil
	l.state.Running = a
	if batch, ok := l.steerBatches[a.Serial]; ok {
		for _, id := range batch.IDs {
			if row := l.state.Requests[id]; row != nil {
				row.Location = book.ControlRunning
			}
		}
	} else if row := l.state.Requests[a.Request]; row != nil {
		row.Location = book.ControlRunning
	}
	switch a.Kind {
	case book.ActionSteer:
		l.runSteer(a)
	case book.ActionInterrupt:
		l.runInterrupt(a)
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
		if a.SteerTarget {
			l.returnSteerTarget(a)
			l.completeAction()
		} else if row.ExplicitCAS {
			l.finish(row.ID, terminalCandidate{fail: true, code: errorCASMismatch,
				detail: fmt.Sprintf("steer target is no longer active: turn %q ended before this steer could be applied, so the work it aimed at is already finished. Read that turn's result before deciding whether anything still needs saying", row.ExpectedTurn)})
		} else {
			l.state.Running = nil
			l.enqueue(row.ID)
			l.maybeRunAction()
		}
		return
	}
	a.Op, a.Target = l.opID(), turn.ID
	content := runtimeproto.CloneInput(row.Input)
	if batch, ok := l.steerBatches[a.Serial]; ok {
		content = runtimeproto.CloneInput(batch.Content)
	}
	if err := l.exec.runtimeControl(runtimeproto.ControlCommand{Op: a.Op, Kind: runtimeproto.ControlSteer, Target: turn.ID, Content: &content, Scope: row.Scope}); err != nil {
		l.faultNow("command_admission", err.Error())
		return
	}
	l.armReceipt(receiptKey("control", uint64(a.Op)))
}

func (l *agentLoop) runInterrupt(a *book.Action) {
	turn := l.state.Turn
	if turn == nil || turn.Phase != book.TurnActive {
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

func (l *agentLoop) runCleanup(a *book.Action) {
	turn := l.state.Turn
	if turn == nil {
		l.completeAction()
		return
	}
	if !l.def.cfg.Runtime.Capabilities[runtimeproto.CapabilityInterrupt] {
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

func (l *agentLoop) handleRuntimeEvent(e runtimeEvent) {
	switch e.kind {
	case evTurnStarted:
		l.onTurnStarted(e)
	case evTurnRejected:
		l.onTurnRejected(e)
	case evTool:
		if t := l.state.Turn; t != nil && t.ID == e.turnID {
			l.progressTool(e.tool)
			l.touchQueued()
		} else {
			l.logLate("Tool", e.turnID)
		}
	case evProgress:
		// 过程读数落成 owner 请求的 provisional 进度（与 turn started 的
		// processing 回执同一机制），条目原样交给前端自行展示；没有 owner
		// （自发回合）就无人可告，丢弃。
		if t := l.state.Turn; t != nil && t.ID == e.turnID && t.Owner != "" {
			if row := l.state.Requests[t.Owner]; row != nil {
				l.progressStage(e.progress.Kind, e.progress.Text)
				l.touchQueued()
			}
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
	}
	l.progressTurnStarted()
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
	ownerKind := runtimeproto.TurnChat
	if row := l.state.Requests[t.Owner]; row != nil {
		ownerKind = row.TurnKind
	}
	matchingAction := l.state.Running
	if matchingAction == nil || matchingAction.Target != e.turnID {
		matchingAction = nil
	}
	if t.Owner != "" {
		switch e.status {
		case runtimeproto.TurnStatusOK:
			value := map[string]any{"turn_index": t.Serial, "usage": usageValue(e.usage)}
			if e.text != "" {
				value["text"] = e.text
			}
			if ownerKind == runtimeproto.TurnCompact || ownerKind == runtimeproto.TurnNew {
				value["status"] = "completed"
			}
			l.finish(t.Owner, terminalCandidate{value: value})
		case runtimeproto.TurnStatusInterrupted:
			if matchingAction != nil && matchingAction.Disposition == book.DispRebufferOwner && matchingAction.OwnerAtAdmit == t.Owner {
				owner := t.Owner
				if row := l.state.Requests[owner]; row != nil {
					row.Location = book.Buffered
					row.Resumed = true
					row.Scope = l.vault.Mint(row.ParentID, row.CorrelationID)
					l.state.InsertAt(0, owner)
					l.state.BufferBytes += row.Bytes
					l.exec.progress(string(owner), message.StatusQueued, map[string]any{"resumed": true, "held_by": matchingAction.HolderID, "controls": l.queuedControls()})
				}
				t.Owner = ""
			} else {
				l.finish(t.Owner, terminalCandidate{fail: true, code: errorInterrupted, detail: e.detail})
			}
		default:
			l.finish(t.Owner, terminalCandidate{fail: true, code: errorProviderFailed, detail: e.detail})
		}
	}
	l.lastUsage, l.hasUsage = e.usage, true
	if e.status == runtimeproto.TurnStatusOK && ownerKind == runtimeproto.TurnSelect {
		l.options = runtimeproto.TurnOptions{Model: e.usage.Model, Effort: e.usage.Effort}
		l.exec.persistSelection(l.options)
	}
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

func usageValue(usage runtimeproto.TurnUsage) map[string]any {
	return map[string]any{"context_tokens": usage.ContextTokens, "context_window": usage.ContextWindow, "model": usage.Model, "effort": usage.Effort}
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
		if a.SteerTarget && e.verdict != runtimeproto.ControlAccepted {
			l.returnSteerTarget(a)
		}
		return
	}
	batch, batched := l.steerBatches[a.Serial]
	if e.verdict == runtimeproto.ControlAccepted {
		if batched {
			for _, id := range batch.IDs[:len(batch.IDs)-1] {
				if l.state.Requests[id] != nil {
					l.finish(id, terminalCandidate{value: map[string]any{"merged_into": row.ID}})
				}
			}
		}
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
		l.exec.progress(string(row.ID), message.StatusProcessing, map[string]any{"turn_id": turn.ID, "controls": l.processingControls()})
		return
	}
	if a.SteerTarget {
		l.returnSteerTarget(a)
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
	l.enqueue(row.ID)
}

func (l *agentLoop) returnSteerTarget(a *book.Action) {
	if batch, ok := l.steerBatches[a.Serial]; ok {
		for idx, id := range batch.IDs {
			row := l.state.Requests[id]
			if row == nil || row.Location == book.Buffered {
				continue
			}
			row.Location = book.Buffered
			l.state.InsertAt(batch.Indices[idx], id)
			l.state.BufferBytes += row.Bytes
		}
		return
	}
	row := l.state.Requests[a.Request]
	if row == nil || row.Location == book.Buffered {
		return
	}
	row.Location = book.Buffered
	l.state.InsertAt(a.BufferIndex, row.ID)
	l.state.BufferBytes += row.Bytes
}

func (l *agentLoop) finishControlAction(a *book.Action) {
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
	if a := l.state.Running; a != nil {
		delete(l.steerBatches, a.Serial)
	}
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
	if l.selectSlot == id {
		l.selectSlot = ""
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
