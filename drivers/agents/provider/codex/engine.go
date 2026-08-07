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

// controlCall is a control verb bound, AT ENQUEUE TIME, to the generation and
// turn it was meant for. The binding is what makes the queue safe: a call can
// wait behind a blocked RPC for as long as it likes, and if the world moved on
// while it waited it is refused instead of landing on whoever is current now —
// a provider-side side effect (content injected, a turn killed) cannot be taken
// back by any later bookkeeping.
type controlCall struct {
	kind  string
	op    base.OpID
	item  base.Trigger
	gen   *connection
	turn  string
	epoch uint64
}

// startIntent is an accepted StartTurn that has not yet been bound to a
// connection, stamped with the service epoch it was accepted under.
type startIntent struct {
	op    base.OpID
	epoch uint64
}

// errServiceRetired says the caller's epoch is no longer in service: whatever
// it was about to do was cancelled before it could reach a provider.
var errServiceRetired = errors.New("codex: service retired")

type engine struct {
	cfg    Config
	events base.EventPort
	life   context.Context
	seed   string
	mu     sync.Mutex
	// current is the live generation. All turn bookkeeping lives ON it
	// (connection.startOp/turnOp/turnID/final): a generation's account is
	// unreachable the moment the generation is detached, so "process died but
	// its turn blocks the next one" is structurally unrepresentable.
	current  *connection
	threadID string
	// pending is the accepted-but-unbound start intent covering the window
	// before a connection exists to hang it on. It carries the service epoch it
	// was accepted under: a fence (Terminate, an observed death, Close) makes it
	// void by construction, so no fencing path has to remember to clear it.
	pending *startIntent
	// serviceEpoch counts service fences. A bump means "everything accepted
	// under the previous epoch is void" — that is the whole meaning of the
	// counter, and the only thing intents need to consult.
	serviceEpoch uint64
	// establishMu serializes bringing a generation into service, so two
	// establishments can never both promote (the loser would otherwise be
	// overwritten with its process left unreaped).
	establishMu     sync.Mutex
	closed          bool
	nextConnection  atomic.Uint64
	controlInit     sync.Once
	controlMu       sync.Mutex
	controlQueue    []controlCall
	controlsClosed  bool
	controlWake     chan struct{}
	controlStop     chan struct{}
	controlStopOnce sync.Once
	watchdog        *time.Timer
}

func newEngineFn(cfg Config) base.NewEngine {
	return func(sys actorbase.Sys, seed []byte, events base.EventPort) (base.Engine, error) {
		e := &engine{cfg: cfg, events: events, life: sys.Life(), seed: string(seed)}
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
	// Only an intent from the LIVE epoch occupies the engine. One left over from
	// a fenced epoch is already void; letting it block admission would turn a
	// cancelled turn into a gate on every request that follows.
	if e.pending != nil && e.pending.epoch == e.serviceEpoch {
		e.mu.Unlock()
		return errors.New("codex: turn already in flight")
	}
	if c := e.current; c != nil && (c.startOp != "" || c.turnID != "") {
		e.mu.Unlock()
		return errors.New("codex: turn already in flight")
	}
	epoch := e.serviceEpoch
	e.pending = &startIntent{op: op, epoch: epoch}
	e.mu.Unlock()
	go e.startTurn(op, epoch, input)
	return nil
}
func (e *engine) startTurn(op base.OpID, epoch uint64, input []map[string]any) {
	c, thread, err := e.ensureService(e.life, epoch)
	if err != nil {
		e.clearPending(op)
		if errors.Is(err, errServiceRetired) {
			e.voidStart(op, epoch)
			return
		}
		e.events.TurnRejected(op, "provider_crash", err.Error())
		return
	}
	e.mu.Lock()
	if e.current != c || e.serviceEpoch != epoch {
		e.mu.Unlock()
		e.clearPending(op)
		e.voidStart(op, epoch)
		return
	}
	c.startOp = op
	e.pending = nil
	e.mu.Unlock()
	_, err = c.rpc.call(e.life, "turn/start", map[string]any{"threadId": thread, "input": input}, rpcTimeout)
	if err == nil {
		return
	}
	var providerErr *rpcError
	if errors.As(err, &providerErr) {
		// The provider answered: this turn did not start. Clean rejection.
		e.mu.Lock()
		current := e.current == c
		if current && c.startOp == op {
			c.startOp = ""
		}
		e.mu.Unlock()
		if current {
			e.events.TurnRejected(op, "provider_crash", err.Error())
		}
		return
	}
	e.fenceFailedStart(c, op, err)
}

// fenceFailedStart handles a turn/start whose transport failed: whether the
// turn started is unknowable, so the whole generation is fenced. It goes
// through the same detach CAS as every other death observer — the winner
// reports, and `started` is read in the same critical section so the report
// form matches the account that actually existed at fencing time. If an EOF
// observer already fenced this generation, the loss is already reported and
// this call stays silent.
func (e *engine) fenceFailedStart(c *connection, op base.OpID, cause error) {
	e.mu.Lock()
	started := c.turnOp == op
	won := e.detachLocked(c)
	e.mu.Unlock()
	if !won {
		return
	}
	e.stopWatchdog()
	c.retireAsync()
	if started {
		e.events.ProviderLost(base.LostCrash, cause.Error())
	} else {
		e.events.TurnRejected(op, "provider_crash", cause.Error())
	}
}

// voidStart reports a start that a service fence cancelled before any provider
// saw it. It reports rather than staying silent on purpose: the base loop parks
// in `starting` until an accepted start reaches an outcome, so a swallowed one
// wedges the agent for good. Every fencing path also settles the request on its
// own, and the base ignores outcomes for ops it has already closed — so the
// duplicate is free while the silence would be fatal.
func (e *engine) voidStart(op base.OpID, epoch uint64) {
	e.cfg.Logger.Info("codex.start_intent_voided", "op", op, "epoch", epoch)
	e.events.TurnRejected(op, "provider_crash", "start cancelled by a service fence")
}

func (e *engine) clearPending(op base.OpID) {
	e.mu.Lock()
	if e.pending != nil && e.pending.op == op {
		e.pending = nil
	}
	e.mu.Unlock()
}

func (e *engine) Steer(op base.OpID, item base.Trigger) error {
	return e.enqueueControl(e.bindControl(controlCall{kind: base.TypeSteer, op: op, item: item}))
}
func (e *engine) Interrupt(op base.OpID) error {
	return e.enqueueControl(e.bindControl(controlCall{kind: base.TypeInterrupt, op: op}))
}

// bindControl stamps the call with the generation and turn live at submission.
func (e *engine) bindControl(call controlCall) controlCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	call.gen, call.epoch = e.current, e.serviceEpoch
	if call.gen != nil {
		call.turn = call.gen.turnID
	}
	return call
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
	stale := call.gen == nil || e.current != call.gen || e.serviceEpoch != call.epoch ||
		(call.turn != "" && call.gen.turnID != call.turn)
	c, thread := call.gen, e.threadID
	turn := call.turn
	e.mu.Unlock()
	if stale {
		// The generation or turn this verb was aimed at is gone. Report it as
		// "no such active turn" — the honest description, and the verdict the
		// base already knows how to settle (re-queue the content, or fail an
		// explicit CAS).
		e.events.ControlDone(call.op, base.ControlNoActiveTurn, "", "target turn is no longer live")
		return
	}
	if c.retired.Load() {
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

// currentEpoch reports the live service epoch (test and caller support).
func (e *engine) currentEpoch() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.serviceEpoch
}

// Terminate detaches whatever generation is current, silently: a user-ordered
// retirement is not a loss, so no ProviderLost is reported. It never blocks —
// physical reaping is retireAsync's internals — which is what lets the base
// arbiter loop call it inline (Engine interface obligation).
func (e *engine) Terminate() error {
	e.mu.Lock()
	c := e.current
	// Fence unconditionally, even with no current generation: terminate must
	// also stop a connection still becoming ready (ensureService re-checks the
	// epoch before promoting), so nothing spawned before this order can enter
	// service afterwards.
	e.fenceServiceLocked()
	e.mu.Unlock()
	if c != nil {
		e.stopWatchdog()
		c.retireAsync()
	}
	return nil
}

// fenceServiceLocked retires the service epoch: the current generation leaves
// service and any connection still being established loses its promotion.
func (e *engine) fenceServiceLocked() {
	e.serviceEpoch++
	e.current = nil
}

// detachLocked is the ONLY transition that removes a generation from service
// (caller holds e.mu). Death has many observers — EOF, failed-start fencing,
// the turn watchdog, explicit Terminate/Close — but exactly one transition:
// whoever wins this CAS owns every consequence (loss report, teardown); losers
// must stay silent. Exactly-once is carried by this linearization, not by
// observers remembering to check flags. The generation's turn account needs no
// clearing here: it lives on the connection and dies with its reachability.
func (e *engine) detachLocked(c *connection) bool {
	if c == nil || e.current != c {
		return false
	}
	e.fenceServiceLocked()
	return true
}

func (e *engine) detach(c *connection) bool {
	e.mu.Lock()
	won := e.detachLocked(c)
	e.mu.Unlock()
	if won {
		e.stopWatchdog()
		c.retireAsync()
	}
	return won
}
func (e *engine) EnsureAlive(op base.OpID) error {
	e.mu.Lock()
	epoch := e.serviceEpoch
	e.mu.Unlock()
	go func() {
		_, _, err := e.ensureService(e.life, epoch)
		if err != nil {
			e.events.ControlDone(op, base.ControlRPCError, "", err.Error())
		} else {
			e.events.ControlDone(op, base.ControlAccepted, "", "")
		}
	}()
	return nil
}

// ensureService returns the live generation, bringing one up if needed. It
// honours the epoch its CALLER was accepted under — never a freshly read one —
// so an intent that a fence already voided can neither spawn a process nor
// bind to the generation that replaced it.
func (e *engine) ensureService(ctx context.Context, wantEpoch uint64) (*connection, string, error) {
	if c, thread, err, done := e.serviceFastPath(wantEpoch); done {
		return c, thread, err
	}
	// Establishment is serialized: two callers must not both promote, or the
	// loser's process would be overwritten and never reaped.
	e.establishMu.Lock()
	defer e.establishMu.Unlock()
	if c, thread, err, done := e.serviceFastPath(wantEpoch); done {
		return c, thread, err
	}

	e.mu.Lock()
	seed := e.threadID
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
	closed, retired := e.closed, e.serviceEpoch != wantEpoch
	if closed || c.dead.Load() || retired {
		e.mu.Unlock()
		c.retire()
		switch {
		case closed:
			return nil, "", errors.New("codex: closed")
		case retired:
			return nil, "", errServiceRetired
		default:
			return nil, "", errors.New("codex: connection closed before ready")
		}
	}
	e.current = c
	e.threadID = thread
	e.mu.Unlock()
	e.events.Persist(base.ResumeSeedKey, []byte(thread))
	return c, thread, nil
}

// serviceFastPath answers without establishing anything. done=false means the
// caller must go on to bring a generation up.
func (e *engine) serviceFastPath(wantEpoch uint64) (*connection, string, error, bool) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, "", errors.New("codex: closed"), true
	}
	if e.serviceEpoch != wantEpoch {
		e.mu.Unlock()
		return nil, "", errServiceRetired, true
	}
	c := e.current
	if c == nil {
		e.mu.Unlock()
		return nil, "", nil, false
	}
	if !c.retired.Load() && !c.dead.Load() {
		thread := e.threadID
		e.mu.Unlock()
		return c, thread, nil, true
	}
	// We found an in-service generation already dead. Normally its own onClose
	// detaches and reports it; racing ahead of that observer would leave the
	// death reported zero times. So whoever wins the detach owes the loss
	// report — that duty rides with the transition, never with who noticed.
	won := e.detachLocked(c)
	e.mu.Unlock()
	if won {
		e.cfg.Logger.Warn("codex.dead_generation_reaped_by_caller", "connection", c.id)
		e.stopWatchdog()
		c.retireAsync()
		e.events.ProviderLost(base.LostCrash, "provider connection found dead")
	}
	return nil, "", errServiceRetired, true
}
func (e *engine) isCurrent(c *connection) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.current == c && !c.retired.Load() && !c.dead.Load()
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
	_ = e.detachLocked(c)
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
