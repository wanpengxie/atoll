package actorrt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

var (
	ErrInvalidUnitConfig = errors.New("actorrt: invalid unit config")
	ErrUnitNotPrepared   = errors.New("actorrt: unit is not prepared")
)

// UnitState reports one exact Unit's physical lifecycle.
type UnitState uint8

const (
	UnitPrepared UnitState = iota
	UnitRunning
	UnitStopping
	UnitDone
)

// UnitConfig contains substrate-owned metadata for one local incarnation.
type UnitConfig struct {
	Parent  context.Context
	ActorID actor.ActorID
	Kind    actor.Kind
	Mailbox int
	Clock   func() time.Time
	Logger  *slog.Logger
}

// ExitedEvent is the exact, process-local edge emitted after a started Unit
// stops admitting work. It is a wake hint; the Host still repairs from level
// state.
type ExitedEvent struct {
	Unit  *Unit
	Self  Incarnation
	Cause error
}

// UnitObsEvent carries an actor-produced immutable observation from one exact
// Unit.
type UnitObsEvent struct {
	Unit  *Unit
	Self  Incarnation
	Kind  ObsKind
	Value ObsValue
}

// UnitEventSink consumes exact Unit events. Implementations must return
// promptly; Unit isolates consumer panics.
type UnitEventSink interface {
	OnExited(ExitedEvent)
	OnObs(UnitObsEvent)
}

// Unit is one local actor incarnation. It owns no ActorID registry and cannot
// resolve, replace, or stop any other Unit.
type Unit struct {
	id     actor.ActorID
	kindV  actor.Kind
	clock  func() time.Time
	logger *slog.Logger
	sink   UnitEventSink

	ctx    context.Context
	cancel context.CancelFunc
	inbox  chan *message.Envelope
	done   chan struct{}

	impl Actor
	self Incarnation

	mu            sync.Mutex
	state         UnitState
	startedAtV    time.Time
	stopRequested bool
	admissionOpen bool

	alive atomic.Bool

	cancelQ chan message.ID
	organs  sync.WaitGroup

	finishOnce sync.Once
}

type unitActorContext struct{ unit *Unit }

func (c unitActorContext) Self() actor.ActorID { return c.unit.id }
func (c unitActorContext) PublishObs(kind ObsKind, value ObsValue) {
	c.unit.publishObs(kind, value)
}

// Prepare allocates the exact Unit shell before invoking build. The builder
// receives the same read-only Incarnation later carried by events.
func Prepare(cfg UnitConfig, build func(Incarnation) Actor, sink UnitEventSink) (*Unit, error) {
	if cfg.ActorID == "" || build == nil {
		return nil, ErrInvalidUnitConfig
	}
	if _, ok := actor.ParseKind(string(cfg.Kind)); !ok {
		return nil, ErrInvalidUnitConfig
	}
	parent := cfg.Parent
	if parent == nil {
		parent = context.Background()
	}
	mailbox := cfg.Mailbox
	if mailbox <= 0 {
		mailbox = 64
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(parent)
	u := &Unit{
		id:            cfg.ActorID,
		kindV:         cfg.Kind,
		clock:         clock,
		logger:        logger,
		sink:          sink,
		ctx:           ctx,
		cancel:        cancel,
		inbox:         make(chan *message.Envelope, mailbox),
		done:          make(chan struct{}),
		state:         UnitPrepared,
		admissionOpen: false,
		cancelQ:       make(chan message.ID, cancelSetCap),
	}
	u.self = Incarnation{id: cfg.ActorID, unit: u}

	impl, err := buildActor(build, u.self)
	if err != nil {
		cancel()
		u.state = UnitDone
		close(u.done)
		return nil, err
	}
	u.impl = impl
	return u, nil
}

// Start publishes this exact Unit as locally alive and starts its one receive
// loop. It may be called successfully once.
func (u *Unit) Start() error {
	if u == nil {
		return ErrUnitNotPrepared
	}
	u.mu.Lock()
	if u.state != UnitPrepared {
		u.mu.Unlock()
		return ErrUnitNotPrepared
	}
	u.state = UnitRunning
	u.startedAtV = u.clock()
	u.admissionOpen = true
	u.alive.Store(true)
	u.mu.Unlock()

	if _, ok := u.impl.(RequestCanceller); ok {
		u.organs.Add(1)
		go u.runCancelOrgan()
	}
	go u.run()
	return nil
}

// Stop is an idempotent, non-blocking begin-stop for this exact Unit.
func (u *Unit) Stop() {
	if u == nil {
		return
	}
	u.mu.Lock()
	switch u.state {
	case UnitDone, UnitStopping:
		u.mu.Unlock()
		return
	case UnitPrepared:
		u.state = UnitStopping
		u.stopRequested = true
		u.admissionOpen = false
		u.alive.Store(false)
		u.cancel()
		u.mu.Unlock()
		go u.finish(nil, false)
		return
	case UnitRunning:
		u.state = UnitStopping
		u.stopRequested = true
		u.admissionOpen = false
		u.alive.Store(false)
		u.cancel()
		u.mu.Unlock()
		return
	default:
		u.mu.Unlock()
		return
	}
}

// Done closes only after the receive loop, Unit-owned organs, and actor Stop
// have actually returned.
func (u *Unit) Done() <-chan struct{} {
	if u == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return u.done
}

// IsAlive is a lock-free exact-Unit liveness check.
func (u *Unit) IsAlive() bool { return u != nil && u.alive.Load() }

// State reports this exact Unit's physical state.
func (u *Unit) State() UnitState {
	if u == nil {
		return UnitDone
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.state
}

// Self returns the exact read-only identity allocated before the actor builder.
func (u *Unit) Self() Incarnation {
	if u == nil {
		return Incarnation{}
	}
	return u.self
}

// Stat returns substrate-owned facts for this exact Unit.
func (u *Unit) Stat() UnitStat {
	if u == nil {
		return UnitStat{}
	}
	u.mu.Lock()
	started := u.startedAtV
	u.mu.Unlock()
	return UnitStat{StartedAt: started, Kind: u.kindV}
}

// Deliver is the bounded, non-blocking local endpoint.
func (u *Unit) Deliver(env *message.Envelope) error {
	if u == nil || env == nil {
		return ErrCellStopped
	}
	u.mu.Lock()
	if !u.admissionOpen || !u.alive.Load() {
		u.mu.Unlock()
		return ErrCellStopped
	}
	select {
	case u.inbox <- env:
		u.mu.Unlock()
		return nil
	default:
		u.mu.Unlock()
		return ErrMailboxFull
	}
}

// CancelRequest is a best-effort signal to this exact Unit.
func (u *Unit) CancelRequest(id message.ID) {
	if u == nil || !u.IsAlive() {
		return
	}
	if _, ok := u.impl.(RequestCanceller); !ok {
		return
	}
	select {
	case u.cancelQ <- id:
	default:
		u.logger.Warn("actorrt.cancel_queue_full", "actor", string(u.id))
	}
}

func (u *Unit) run() {
	var cause error
	defer func() {
		if recovered := recover(); recovered != nil {
			cause = fmt.Errorf("actorrt: unit %s panicked: %v", u.id, recovered)
		}
		u.finish(cause, true)
	}()

	if starter, ok := u.impl.(Starter); ok {
		if err := starter.Start(u.ctx, unitActorContext{unit: u}); err != nil {
			cause = fmt.Errorf("actorrt: unit %s Start failed: %w", u.id, err)
			return
		}
	}

	var dying <-chan error
	if reporter, ok := u.impl.(DownReporter); ok {
		dying = reporter.Dying()
	}
	for {
		select {
		case <-u.ctx.Done():
			if err, ok := drainDying(dying); ok {
				cause = err
			}
			return
		case err := <-dying:
			cause = err
			return
		case env := <-u.inbox:
			if err := u.impl.Receive(u.ctx, env); err != nil {
				u.logger.Debug("actorrt.unit.receive_failed", "actor", string(u.id), "err", err)
			}
		}
	}
}

func (u *Unit) runCancelOrgan() {
	defer u.organs.Done()
	canceller := u.impl.(RequestCanceller)
	for {
		select {
		case <-u.ctx.Done():
			return
		case id := <-u.cancelQ:
			canceller.CancelRequest(id)
		}
	}
}

func (u *Unit) finish(cause error, emitExited bool) {
	u.finishOnce.Do(func() {
		u.mu.Lock()
		started := u.startedAtV != (time.Time{})
		if u.stopRequested {
			cause = nil
		}
		u.state = UnitStopping
		u.admissionOpen = false
		u.alive.Store(false)
		u.cancel()
		u.mu.Unlock()

		if emitExited && started {
			u.emitExited(cause)
		}
		u.organs.Wait()
		if stopper, ok := u.impl.(Stopper); ok {
			_ = safeStop(u.logger, u.id, stopper)
		}
		u.mu.Lock()
		u.state = UnitDone
		u.mu.Unlock()
		close(u.done)
	})
}

func (u *Unit) emitExited(cause error) {
	if u.sink == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			u.logger.Error("actorrt.unit.exited_sink_panicked", "actor", string(u.id), "panic", recovered)
		}
	}()
	u.sink.OnExited(ExitedEvent{Unit: u, Self: u.self, Cause: cause})
}

func (u *Unit) publishObs(kind ObsKind, value ObsValue) {
	if u.sink == nil || !u.IsAlive() {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			u.logger.Error("actorrt.unit.obs_sink_panicked", "actor", string(u.id), "panic", recovered)
		}
	}()
	u.sink.OnObs(UnitObsEvent{Unit: u, Self: u.self, Kind: kind, Value: value})
}
