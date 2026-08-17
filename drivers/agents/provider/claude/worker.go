package claude

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/provider/internal/emit"
)

const initialContextUsageTimeout = 5 * time.Second

type connection struct {
	process      *childProcess
	wire         *wireClient
	retired      atomic.Bool
	terminalOnce sync.Once
	onTerminal   func(driverproto.WorkerEndCause, string)
	wireClosed   chan error
}

func (c *connection) startTerminalMonitor() {
	go func() {
		select {
		case err, ok := <-c.process.exit:
			if !ok {
				return
			}
			<-c.wire.pumpDone
			c.processExit(err)
		case wireErr := <-c.wireClosed:
			select {
			case err, ok := <-c.process.exit:
				if ok {
					c.processExit(err)
					return
				}
			default:
			}
			c.terminal(driverproto.WorkerTransportEnded, wireErr.Error())
		}
	}()
}

func (c *connection) processExit(err error) {
	if err != nil {
		c.terminal(driverproto.WorkerCrash, err.Error())
		return
	}
	c.terminal(driverproto.WorkerTransportEnded, "claude process exited")
}

func (c *connection) terminal(cause driverproto.WorkerEndCause, detail string) {
	c.terminalOnce.Do(func() {
		if c.process.stoppedByUs.Load() || c.retired.Load() {
			return
		}
		if c.onTerminal != nil {
			c.onTerminal(cause, detail)
		}
	})
}

func (c *connection) Retire() {
	if c != nil && !c.retired.Swap(true) {
		c.wire.retire()
		c.process.stop()
	}
}

type workerPhase uint8

const (
	phaseConstructed workerPhase = iota
	phaseOpening
	phaseReady
	phaseStarting
	phaseActive
	phaseRetiring
	phaseReaped
)

type steerState struct {
	action   driverproto.ActionToken
	accepted bool
	started  bool
	done     bool
}
type interruptState struct {
	inflight  bool
	requestID string
}
type turnState struct {
	U             string
	kind          driverproto.TurnKind
	options       driverproto.TurnOptions
	settling      bool
	compactResult string
	compactError  string
	compactPost   int64
	compactMeta   bool
	steers        map[string]steerState
	interrupt     interruptState
	seen          map[string]map[string]bool
}

type worker struct {
	cfg               Config
	gate              *emit.Gate
	mu                sync.Mutex
	phase             workerPhase
	conn              *connection
	session           string
	resume            bool
	attempt           driverproto.AttemptToken
	target            driverproto.WorkerTurnTarget
	turn              *turnState
	leases            sync.WaitGroup
	reaped            chan struct{}
	retireOnce        sync.Once
	terminalOnce      sync.Once
	initSeen          bool
	unsolicitedWarned bool
	debugSeen         map[string]bool
	options           driverproto.TurnOptions
	lastModel         string
	usage             driverproto.TurnUsage
}

func newWorker(cfg Config, host driverproto.WorkerHost) *worker {
	return &worker{cfg: cfg, gate: emit.New(host.Events()), phase: phaseConstructed, reaped: make(chan struct{}), debugSeen: map[string]bool{}}
}

func (w *worker) begin(allowed ...workerPhase) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, p := range allowed {
		if w.phase == p {
			w.leases.Add(1)
			return true
		}
	}
	return false
}
func (w *worker) end() { w.leases.Done() }

func (w *worker) Open(ctx context.Context, req driverproto.OpenRequest) {
	if !w.begin(phaseConstructed) {
		return
	}
	defer w.end()
	w.mu.Lock()
	w.phase = phaseOpening
	w.options = req.Options
	if w.options.Model == "" {
		w.options.Model = w.cfg.Model
	}
	if len(req.ResumeSeed) > 0 {
		w.session, w.resume = string(req.ResumeSeed), true
	} else {
		w.session, w.resume = uuid.NewString(), false
	}
	session, resume := w.session, w.resume
	w.mu.Unlock()
	p, err := w.cfg.processFactory(ctx, w.cfg, spawnArgs(w.cfg, session, resume, req.Options))
	if err != nil {
		w.terminal(driverproto.OpenRejected{Class: driverproto.FailureTransport, Detail: err.Error(), Disposition: driverproto.RetireWorker})
		return
	}
	c := &connection{process: p, wireClosed: make(chan error, 1)}
	c.wire = newWire(p)
	c.onTerminal = func(cause driverproto.WorkerEndCause, detail string) { w.connectionEnded(c, cause, detail) }
	c.wire.onLifecycle = func(id, state string) {
		if !c.retired.Load() {
			w.onLifecycle(c, id, state)
		}
	}
	c.wire.onFrame = func(typ, subtype string, raw json.RawMessage) {
		if !c.retired.Load() {
			w.onFrame(c, typ, subtype, raw)
		}
	}
	c.wire.onServerRequest = handleServerRequest
	c.wire.onDebug = func(code, detail string) { w.debug(code, detail) }
	c.wire.onClose = func(err error) {
		select {
		case c.wireClosed <- err:
		default:
		}
	}
	w.mu.Lock()
	if w.phase != phaseOpening {
		w.mu.Unlock()
		c.Retire()
		return
	}
	w.conn = c
	w.mu.Unlock()
	c.wire.start()
	c.startTerminalMonitor()
	_, err = c.wire.sendControl("initialize", nil, func(reply controlReply) { w.afterInitialize(c, reply) })
	if err != nil {
		w.terminal(driverproto.OpenRejected{Class: driverproto.FailureTransport, Detail: err.Error(), Disposition: driverproto.RetireWorker})
	}
}

func (w *worker) afterInitialize(c *connection, reply controlReply) {
	w.mu.Lock()
	opening := w.phase == phaseOpening && w.conn == c
	w.mu.Unlock()
	if !opening {
		return
	}
	if !reply.Success {
		class := driverproto.FailureProvider
		if reply.Error == "wire closed" {
			class = driverproto.FailureTransport
		}
		detail := strings.TrimSpace(reply.Error)
		if detail == "" {
			detail = "initialize rejected"
		}
		w.terminal(driverproto.OpenRejected{Class: class, Detail: detail, Disposition: driverproto.RetireWorker})
		return
	}
	w.initializeDiagnostic(reply.Response)
	w.fetchInitialContextWindow(c)
}

func (w *worker) fetchInitialContextWindow(c *connection) {
	var once sync.Once
	finish := func(reply controlReply, timeout bool) {
		once.Do(func() {
			if timeout {
				w.debug("context_usage_timeout", "initial context window unavailable after 5s")
			} else if !w.cacheContextWindow(c, reply) {
				w.debug("context_usage_failed", "initial context window unavailable")
			}
			w.finishOpen(c)
		})
	}
	id, err := c.wire.sendControl("get_context_usage", nil, func(reply controlReply) { finish(reply, false) })
	if err != nil {
		finish(controlReply{Error: err.Error()}, false)
		return
	}
	time.AfterFunc(initialContextUsageTimeout, func() {
		c.wire.take(id)
		finish(controlReply{}, true)
	})
}

func (w *worker) finishOpen(c *connection) {
	w.mu.Lock()
	if w.phase != phaseOpening || w.conn != c {
		w.mu.Unlock()
		return
	}
	resume, session := w.resume, w.session
	w.phase = phaseReady
	w.mu.Unlock()
	if !resume && !w.publish(driverproto.SeedUpdated{Value: []byte(session)}) {
		return
	}
	w.publish(driverproto.WorkerReady{})
}

func (w *worker) refreshContextWindow(c *connection) {
	_, err := c.wire.sendControl("get_context_usage", nil, func(reply controlReply) {
		if !w.cacheContextWindow(c, reply) && reply.Error != "wire closed" {
			w.debug("context_usage_failed", "selected model context window unavailable")
		}
	})
	if err != nil {
		w.debug("context_usage_failed", "selected model context window unavailable")
	}
}

func (w *worker) cacheContextWindow(c *connection, reply controlReply) bool {
	if !reply.Success {
		return false
	}
	var frame contextUsageFrame
	if json.Unmarshal(reply.Response, &frame) != nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn != c || w.phase == phaseRetiring || w.phase == phaseReaped {
		return false
	}
	w.usage.ContextWindow = frame.MaxTokens
	return true
}

func (w *worker) initializeDiagnostic(raw json.RawMessage) {
	var response map[string]json.RawMessage
	if json.Unmarshal(raw, &response) != nil {
		return
	}
	delete(response, "account")
	w.publish(driverproto.Diagnostic{Level: driverproto.DiagnosticDebug, Code: "initialize", Detail: boundedSummary(response)})
}

func (w *worker) Start(_ context.Context, req driverproto.StartRequest) {
	if !w.begin(phaseReady) {
		return
	}
	defer w.end()
	content := buildContent(req.Messages, req.Background)
	if req.Kind == driverproto.TurnCompact {
		content = []map[string]any{{"type": "text", "text": "/compact"}}
	}
	if req.Kind == driverproto.TurnChat && len(content) == 0 {
		w.publish(driverproto.SubmissionRejected{Attempt: req.Attempt, Class: driverproto.FailureInvalidInput, Detail: "empty input", Disposition: driverproto.KeepWorker})
		return
	}
	u := newTurnUUID()
	w.mu.Lock()
	if w.phase != phaseReady || w.conn == nil {
		w.mu.Unlock()
		return
	}
	c, session := w.conn, w.session
	w.phase, w.attempt = phaseStarting, req.Attempt
	w.target = driverproto.WorkerTurnTarget{Attempt: req.Attempt}
	selected := req.Options
	if selected.Model == "" {
		selected.Model = w.options.Model
	}
	if selected.Effort == "" {
		selected.Effort = w.options.Effort
	}
	w.turn = &turnState{U: u, kind: req.Kind, options: selected, steers: map[string]steerState{}, seen: map[string]map[string]bool{}}
	w.mu.Unlock()
	if req.Kind == driverproto.TurnSelect {
		w.startSelect(c, session, req, u)
		return
	}
	if err := c.wire.writeFrame(userFrame(u, session, content)); err != nil {
		w.terminal(driverproto.SubmissionRejected{Attempt: req.Attempt, Class: driverproto.FailureTransport, Detail: err.Error(), Disposition: driverproto.RetireWorker})
	}
}

func (w *worker) startSelect(c *connection, session string, req driverproto.StartRequest, u string) {
	afterModel := func(reply controlReply) {
		if !reply.Success {
			detail := strings.TrimSpace(reply.Error)
			if detail == "" {
				detail = "set_model rejected"
			}
			w.rejectSelection(req.Attempt, detail)
			return
		}
		w.mu.Lock()
		if w.attempt != req.Attempt || w.turn == nil {
			w.mu.Unlock()
			return
		}
		if req.Options.Model != "" {
			w.options.Model = req.Options.Model
			w.lastModel = ""
		}
		w.mu.Unlock()
		if req.Options.Effort == "" {
			w.finishModelOnlySelection(req.Attempt)
			w.refreshContextWindow(c)
			return
		}
		if err := c.wire.writeFrame(userFrame(u, session, []map[string]any{{"type": "text", "text": "/effort " + req.Options.Effort}})); err != nil {
			w.terminal(driverproto.SubmissionRejected{Attempt: req.Attempt, Class: driverproto.FailureTransport, Detail: err.Error(), Disposition: driverproto.RetireWorker})
			return
		}
		w.refreshContextWindow(c)
	}
	if req.Options.Model == "" {
		afterModel(controlReply{Success: true})
		return
	}
	if _, err := c.wire.sendControl("set_model", map[string]any{"model": req.Options.Model}, afterModel); err != nil {
		w.terminal(driverproto.SubmissionRejected{Attempt: req.Attempt, Class: driverproto.FailureTransport, Detail: err.Error(), Disposition: driverproto.RetireWorker})
	}
}

func (w *worker) rejectSelection(attempt driverproto.AttemptToken, detail string) {
	w.mu.Lock()
	if w.attempt != attempt || w.phase != phaseStarting {
		w.mu.Unlock()
		return
	}
	w.attempt, w.target, w.turn, w.phase = 0, driverproto.WorkerTurnTarget{}, nil, phaseReady
	w.mu.Unlock()
	w.publish(driverproto.SubmissionRejected{Attempt: attempt, Class: driverproto.FailureProvider, Detail: detail, Disposition: driverproto.KeepWorker})
}

func (w *worker) finishModelOnlySelection(attempt driverproto.AttemptToken) {
	w.mu.Lock()
	if w.attempt != attempt || w.phase != phaseStarting || w.turn == nil {
		w.mu.Unlock()
		return
	}
	w.target.Native = driverproto.WorkerTurnRef(fmt.Sprintf("select-%d", attempt))
	target := w.target
	usage := w.currentUsageLocked()
	w.options = w.turn.options
	usage.Model, usage.Effort = w.options.Model, w.options.Effort
	w.usage = usage
	w.phase = phaseActive
	w.mu.Unlock()
	w.publish(driverproto.TurnStarted{Target: target})
	w.mu.Lock()
	if w.attempt == attempt {
		w.attempt, w.target, w.turn, w.phase = 0, driverproto.WorkerTurnTarget{}, nil, phaseReady
	}
	w.mu.Unlock()
	w.publish(driverproto.TurnEnded{Target: target, Status: driverproto.TurnOK, Usage: usage})
}

func (w *worker) currentUsageLocked() driverproto.TurnUsage {
	usage := w.usage
	usage.Model = w.lastModel
	if usage.Model == "" {
		usage.Model = w.options.Model
	}
	usage.Effort = w.options.Effort
	return usage
}

func userFrame(id, session string, content []map[string]any) map[string]any {
	return map[string]any{"type": "user", "uuid": id, "message": map[string]any{"role": "user", "content": content}, "parent_tool_use_id": nil, "session_id": session}
}

func (w *worker) Control(_ context.Context, req driverproto.ControlRequest) {
	if !w.begin(phaseActive) {
		return
	}
	defer w.end()
	w.mu.Lock()
	if w.turn == nil || w.target != req.Target || !w.target.Valid() {
		w.mu.Unlock()
		w.publish(driverproto.ControlOutcome{Action: req.Action, Target: req.Target, Verdict: driverproto.ControlTargetGone, Detail: "target is not active", Disposition: driverproto.KeepWorker})
		return
	}
	c, session, target := w.conn, w.session, w.target
	if req.Kind == driverproto.ControlSteer {
		if req.Message == nil || req.Message.Text == "" {
			w.mu.Unlock()
			w.publish(driverproto.ControlOutcome{Action: req.Action, Target: target, Verdict: driverproto.ControlRejected, Detail: "empty steer", Disposition: driverproto.KeepWorker})
			return
		}
		s := newTurnUUID()
		w.turn.steers[s] = steerState{action: req.Action}
		w.mu.Unlock()
		if err := c.wire.writeFrame(userFrame(s, session, buildContent([]driverproto.DriverMessage{*req.Message}, nil))); err != nil {
			w.mu.Lock()
			if w.turn != nil {
				delete(w.turn.steers, s)
			}
			w.mu.Unlock()
			w.terminal(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
		}
		return
	}
	w.turn.interrupt.inflight = true
	w.mu.Unlock()
	id, err := c.wire.sendControl("interrupt", map[string]any{"cancel_queued": true}, func(reply controlReply) { w.afterInterrupt(c, req, reply) })
	if err != nil {
		w.mu.Lock()
		if w.turn != nil {
			w.turn.interrupt = interruptState{}
		}
		w.mu.Unlock()
		w.terminal(driverproto.WorkerEnded{Cause: driverproto.WorkerTransportEnded, Detail: err.Error()})
		return
	}
	w.mu.Lock()
	if w.turn != nil && w.target == target && w.turn.interrupt.inflight {
		w.turn.interrupt.requestID = id
	}
	w.mu.Unlock()
}

func (w *worker) afterInterrupt(c *connection, req driverproto.ControlRequest, reply controlReply) {
	w.mu.Lock()
	if w.phase != phaseActive || w.conn != c || w.turn == nil || w.target != req.Target || !w.turn.interrupt.inflight {
		w.mu.Unlock()
		return
	}
	if !reply.Success {
		w.turn.interrupt = interruptState{}
	}
	w.mu.Unlock()
	if reply.Error == "wire closed" {
		return
	}
	w.publish(classifyInterruptReply(req.Action, req.Target, reply))
}

func (w *worker) publish(v driverproto.DriverEvent) bool { return w.gate.Publish(v) }

func (w *worker) terminal(ev driverproto.DriverEvent) {
	w.terminalOnce.Do(func() {
		w.mu.Lock()
		w.phase = phaseRetiring
		w.mu.Unlock()
		w.publish(ev)
		w.gate.Close()
	})
}

func (w *worker) connectionEnded(c *connection, cause driverproto.WorkerEndCause, detail string) {
	w.mu.Lock()
	if w.conn != c || w.phase == phaseRetiring || w.phase == phaseReaped {
		w.mu.Unlock()
		return
	}
	phase, attempt := w.phase, w.attempt
	w.mu.Unlock()
	switch phase {
	case phaseOpening:
		w.terminal(driverproto.OpenRejected{Class: driverproto.FailureTransport, Detail: detail, Disposition: driverproto.RetireWorker})
	case phaseStarting:
		w.terminal(driverproto.SubmissionRejected{Attempt: attempt, Class: driverproto.FailureTransport, Detail: detail, Disposition: driverproto.RetireWorker})
	default:
		w.terminal(driverproto.WorkerEnded{Cause: cause, Detail: detail})
	}
}

func (w *worker) Retire() {
	w.retireOnce.Do(func() {
		w.gate.Close()
		w.mu.Lock()
		w.phase = phaseRetiring
		c := w.conn
		w.mu.Unlock()
		if c != nil {
			c.Retire()
		}
		go func() {
			w.leases.Wait()
			if c != nil {
				<-c.process.reaped
				<-c.wire.pumpDone
			}
			w.mu.Lock()
			w.phase = phaseReaped
			w.mu.Unlock()
			close(w.reaped)
		}()
	})
}
func (w *worker) Reaped() <-chan struct{} { return w.reaped }

func (w *worker) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return fmt.Sprintf("claude worker session=%s", w.session)
}

func handleServerRequest(subtype string, _ json.RawMessage) (any, bool) {
	if subtype == "can_use_tool" {
		return map[string]any{"behavior": "deny", "message": "permission escalation unavailable"}, false
	}
	return "unsupported: " + subtype, true
}

var _ driverproto.Worker = (*worker)(nil)
