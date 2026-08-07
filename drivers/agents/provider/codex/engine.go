package codex

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
)

type controlCall struct {
	kind string
	op   base.OpID
	item base.Trigger
}
type engine struct {
	cfg             Config
	events          base.EventPort
	life            context.Context
	seed            string
	mu              sync.Mutex
	current         *connection
	threadID        string
	turnID          string
	startOp         base.OpID
	serviceEpoch    uint64
	closed          bool
	nextConnection  atomic.Uint64
	controlInit     sync.Once
	controlMu       sync.Mutex
	controlQueue    []controlCall
	controlsClosed  bool
	controlWake     chan struct{}
	controlStop     chan struct{}
	controlStopOnce sync.Once
	final           map[string]string
	watchdog        *time.Timer
}

func newEngineFn(cfg Config) base.NewEngine {
	return func(sys actorbase.Sys, seed []byte, events base.EventPort) (base.Engine, error) {
		e := &engine{cfg: cfg, events: events, life: sys.Life(), seed: string(seed), final: map[string]string{}}
		e.initControlQueue()
		go e.controlWorker()
		return e, nil
	}
}

func (e *engine) Boot(ctx context.Context, port base.BootPort) error {
	c, err := e.openConnection(ctx)
	if err != nil {
		return err
	}
	thread, err := e.establishSession(ctx, c, e.seed)
	if err != nil {
		c.retire()
		return err
	}
	e.mu.Lock()
	closed := e.closed
	if closed || c.dead.Load() {
		e.mu.Unlock()
		c.retire()
		if closed {
			return errors.New("codex: closed")
		}
		return errors.New("codex: connection closed before ready")
	}
	e.current = c
	e.threadID = thread
	e.mu.Unlock()
	port.Persist(base.ResumeSeedKey, []byte(thread))
	return nil
}

func (e *engine) StartTurn(op base.OpID, batch []base.Trigger, background []base.ContextItem) error {
	input, err := buildInput(batch, background)
	if err != nil {
		e.events.TurnRejected(op, "input_too_large", err.Error())
		return nil
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errors.New("codex: closed")
	}
	if e.startOp != "" || e.turnID != "" {
		e.mu.Unlock()
		return errors.New("codex: turn already in flight")
	}
	e.startOp = op
	e.mu.Unlock()
	go e.startTurn(op, input)
	return nil
}
func (e *engine) startTurn(op base.OpID, input []map[string]any) {
	c, thread, err := e.ensureService(e.life)
	if err != nil {
		e.clearStart(op)
		e.events.TurnRejected(op, "provider_crash", err.Error())
		return
	}
	_, err = c.rpc.call(e.life, "turn/start", map[string]any{"threadId": thread, "input": input}, rpcTimeout)
	if err != nil {
		e.clearStart(op)
		if e.isCurrent(c) {
			e.events.TurnRejected(op, "provider_crash", err.Error())
		}
	}
}
func (e *engine) clearStart(op base.OpID) {
	e.mu.Lock()
	if e.startOp == op {
		e.startOp = ""
	}
	e.mu.Unlock()
}

func (e *engine) Steer(op base.OpID, item base.Trigger) error {
	return e.enqueueControl(controlCall{kind: base.TypeSteer, op: op, item: item})
}
func (e *engine) Interrupt(op base.OpID) error {
	return e.enqueueControl(controlCall{kind: base.TypeInterrupt, op: op})
}
func (e *engine) initControlQueue() {
	e.controlInit.Do(func() {
		e.controlWake = make(chan struct{}, 1)
		e.controlStop = make(chan struct{})
	})
}
func (e *engine) enqueueControl(call controlCall) error {
	e.initControlQueue()
	e.mu.Lock()
	closed := e.closed
	e.mu.Unlock()
	if closed {
		return errors.New("codex: closed")
	}
	e.controlMu.Lock()
	if e.controlsClosed {
		e.controlMu.Unlock()
		return errors.New("codex: closed")
	}
	e.controlQueue = append(e.controlQueue, call)
	e.controlMu.Unlock()
	select {
	case e.controlWake <- struct{}{}:
	default:
	}
	return nil
}
func (e *engine) popControl() (controlCall, bool) {
	e.controlMu.Lock()
	defer e.controlMu.Unlock()
	if len(e.controlQueue) == 0 {
		return controlCall{}, false
	}
	call := e.controlQueue[0]
	e.controlQueue[0] = controlCall{}
	if len(e.controlQueue) == 1 {
		e.controlQueue = nil
	} else {
		e.controlQueue = e.controlQueue[1:]
	}
	return call, true
}
func (e *engine) controlWorker() {
	e.initControlQueue()
	for {
		select {
		case <-e.life.Done():
			return
		case <-e.controlStop:
			return
		case <-e.controlWake:
			for {
				call, ok := e.popControl()
				if !ok {
					break
				}
				e.executeControl(call)
			}
		}
	}
}
func (e *engine) executeControl(call controlCall) {
	e.mu.Lock()
	c, thread, turn := e.current, e.threadID, e.turnID
	e.mu.Unlock()
	if c == nil || c.retired.Load() {
		e.events.ControlDone(call.op, base.ControlRPCError, turn, "provider unavailable")
		return
	}
	var method string
	var params map[string]any
	switch call.kind {
	case base.TypeSteer:
		if strings.TrimSpace(triggerBody(call.item)) == "" {
			e.events.ControlDone(call.op, base.ControlEmptyInput, turn, "steer input is empty")
			return
		}
		input, err := buildInput([]base.Trigger{call.item}, nil)
		if err != nil {
			e.events.ControlDone(call.op, base.ControlEmptyInput, turn, err.Error())
			return
		}
		method = "turn/steer"
		params = map[string]any{"threadId": thread, "expectedTurnId": steerExpected(call.item, turn), "input": input}
	case base.TypeInterrupt:
		if turn == "" {
			e.events.ControlDone(call.op, base.ControlNoActiveTurn, "", "")
			return
		}
		method = "turn/interrupt"
		params = map[string]any{"threadId": thread, "turnId": turn}
	}
	_, err := c.rpc.call(e.life, method, params, rpcTimeout)
	if e.isCurrent(c) {
		e.events.ControlDone(call.op, controlVerdict(err), turn, errorString(err))
	}
}

func (e *engine) Terminate() error {
	e.mu.Lock()
	c := e.current
	e.serviceEpoch++
	e.current = nil
	e.turnID = ""
	e.startOp = ""
	e.mu.Unlock()
	if c != nil {
		c.retire()
	}
	return nil
}
func (e *engine) EnsureAlive(op base.OpID) error {
	go func() {
		_, _, err := e.ensureService(e.life)
		if err != nil {
			e.events.ControlDone(op, base.ControlRPCError, "", err.Error())
		} else {
			e.events.ControlDone(op, base.ControlAccepted, "", "")
		}
	}()
	return nil
}
func (e *engine) ensureService(ctx context.Context) (*connection, string, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, "", errors.New("codex: closed")
	}
	if e.current != nil && !e.current.retired.Load() && !e.current.dead.Load() {
		c, t := e.current, e.threadID
		e.mu.Unlock()
		return c, t, nil
	}
	seed := e.threadID
	epoch := e.serviceEpoch
	if seed == "" {
		seed = e.seed
	}
	e.mu.Unlock()
	c, err := e.openConnection(ctx)
	if err != nil {
		return nil, "", err
	}
	thread, err := e.establishSession(ctx, c, seed)
	if err != nil {
		c.retire()
		return nil, "", err
	}
	e.mu.Lock()
	retired := e.serviceEpoch != epoch
	closed := e.closed
	if closed || c.dead.Load() || retired {
		e.mu.Unlock()
		c.retire()
		if closed {
			return nil, "", errors.New("codex: closed")
		}
		if retired {
			return nil, "", errors.New("codex: service retired")
		}
		return nil, "", errors.New("codex: connection closed before ready")
	}
	e.current = c
	e.threadID = thread
	e.mu.Unlock()
	e.events.Persist(base.ResumeSeedKey, []byte(thread))
	return c, thread, nil
}
func (e *engine) isCurrent(c *connection) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.current == c && !c.retired.Load() && !c.dead.Load()
}
func (e *engine) isCurrentObject(c *connection) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.current == c
}
func (*engine) Describe() introspect.Describe {
	return introspect.Describe{Description: "Codex workspace agent backed by a dedicated local app-server.", SkillDoc: agentSkillDoc}
}
func (e *engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	if e.watchdog != nil {
		e.watchdog.Stop()
		e.watchdog = nil
	}
	c := e.current
	e.current = nil
	e.mu.Unlock()
	e.initControlQueue()
	e.controlMu.Lock()
	e.controlsClosed = true
	e.controlQueue = nil
	e.controlMu.Unlock()
	e.controlStopOnce.Do(func() { close(e.controlStop) })
	if c != nil {
		c.retire()
	}
	return nil
}
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func (e *engine) String() string { return fmt.Sprintf("codex engine thread=%s", e.threadID) }

var _ base.Engine = (*engine)(nil)
