package home

import (
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

type EnsureTicket string
type occupancy uint8

const (
	occNone occupancy = iota
	occStarting
	occRunning
	occDetached
)

type transitionVerdict uint8

const (
	transitionApplied transitionVerdict = iota + 1
	transitionInFlight
	transitionStaleTicket
	transitionInvalid
)

type deliveryCarrier interface{ Enqueue(*message.Envelope) error }

type carrierKind uint8

const (
	carrierLocal carrierKind = iota + 1
	carrierPort
)

type carrier struct {
	kind  carrierKind
	queue deliveryCarrier
}

type deliveryDropReason string

const (
	deliveryDropNoCarrier       deliveryDropReason = "no_carrier"
	deliveryDropCarrierRejected deliveryDropReason = "carrier_rejected"
)

type deliveryDropObserver func(actor.ActorID, *message.Envelope, deliveryDropReason, error)

type lstate struct {
	occ     occupancy
	carrier carrier
	dirty   bool
	restart bool
	ticket  EnsureTicket
	version int64
}

type livenessLedger struct {
	mu          sync.Mutex
	rows        map[actor.ActorID]lstate
	closed      bool
	observeDrop deliveryDropObserver
}

var errLivenessClosed = errors.New("home: liveness ledger closed")

func newLivenessLedger(observers ...deliveryDropObserver) *livenessLedger {
	l := &livenessLedger{rows: make(map[actor.ActorID]lstate)}
	if len(observers) > 0 {
		l.observeDrop = observers[0]
	}
	return l
}

func (l *livenessLedger) Bootstrap(ids []actor.ActorID) transitionVerdict {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return transitionInvalid
	}
	next := make(map[actor.ActorID]lstate, len(ids))
	for _, id := range ids {
		if id == "" {
			return transitionInvalid
		}
		next[id] = lstate{occ: occNone}
	}
	l.rows = next
	return transitionApplied
}

// AdmitIdentity installs one newly admitted identity without disturbing any
// concurrent liveness row. Birth is dormant; only a request/fired anchor can
// create wake debt for a finite-idle actor.
func (l *livenessLedger) AdmitIdentity(id actor.ActorID) transitionVerdict {
	if id == "" {
		return transitionInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return transitionInvalid
	}
	if _, exists := l.rows[id]; exists {
		return transitionApplied
	}
	l.rows[id] = lstate{occ: occNone}
	return transitionApplied
}

// EndIdentity removes the volatile row. A late delivery or down edge therefore
// fails closed and can never recreate a terminal identity.
func (l *livenessLedger) EndIdentity(id actor.ActorID) (carrier, transitionVerdict) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok {
		return carrier{}, transitionApplied
	}
	delete(l.rows, id)
	return s.carrier, transitionApplied
}

// AcceptDelivery never stores env. A request presented without a carrier sets
// the one-bit wake debt; an event is a best-effort drop and leaves dirty false.
func (l *livenessLedger) AcceptDelivery(id actor.ActorID, env *message.Envelope) (transitionVerdict, error) {
	return l.acceptDelivery(id, env, false)
}

// AcceptFiredDelivery is the fired-timer level path. Unlike an ordinary event,
// a fired row has no caller that can re-submit it, so absence creates the same
// one-bit wake debt as a request. The envelope itself is still never buffered.
func (l *livenessLedger) AcceptFiredDelivery(id actor.ActorID, env *message.Envelope) (transitionVerdict, error) {
	return l.acceptDelivery(id, env, true)
}

func (l *livenessLedger) acceptDelivery(id actor.ActorID, env *message.Envelope, fired bool) (transitionVerdict, error) {
	if env == nil {
		return transitionInvalid, errors.New("home: nil delivery")
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return transitionInvalid, errLivenessClosed
	}
	s, ok := l.rows[id]
	if !ok {
		l.mu.Unlock()
		return transitionInvalid, nil
	}
	if s.occ != occRunning || s.carrier.queue == nil {
		if env.Kind == message.KindRequest || fired {
			s.dirty = true
			l.rows[id] = s
			l.mu.Unlock()
			return transitionApplied, nil
		}
		l.mu.Unlock()
		if l.observeDrop != nil {
			l.observeDrop(id, env, deliveryDropNoCarrier, nil)
		}
		return transitionApplied, nil
	}
	// Carrier enqueue is the liveness linearization point. Keeping it inside
	// the ledger critical section makes delivery and idle/retire one total
	// order: an accepted delivery is physically ahead of the retirement
	// command, while a later request observes no carrier and creates wake debt.
	err := s.carrier.queue.Enqueue(env)
	l.mu.Unlock()
	if err != nil && l.observeDrop != nil {
		l.observeDrop(id, env, deliveryDropCarrierRejected, err)
	}
	return transitionApplied, err
}

// MarkFiredWake records wake debt before a due durable timer can be committed
// for a daemon-placed dormant identity. It does not create an envelope buffer;
// the durable timer row remains the sole retry source.
func (l *livenessLedger) MarkFiredWake(id actor.ActorID) transitionVerdict {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok || l.closed {
		return transitionInvalid
	}
	if s.occ != occRunning || s.carrier.queue == nil {
		s.dirty = true
		l.rows[id] = s
	}
	return transitionApplied
}

func (l *livenessLedger) ApproveIdle(id actor.ActorID) (carrier, transitionVerdict) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok || s.occ != occRunning || s.carrier.queue == nil {
		return carrier{}, transitionInvalid
	}
	old := s.carrier
	s.occ, s.carrier, s.ticket = occNone, carrier{}, ""
	l.rows[id] = s
	return old, transitionApplied
}

func (l *livenessLedger) ObserveDown(id actor.ActorID, port bool, voluntary bool) transitionVerdict {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok || s.occ != occRunning {
		return transitionInvalid
	}
	s.carrier = carrier{}
	if port {
		s.occ = occDetached
	} else {
		s.occ = occNone
		s.ticket = ""
		s.restart = !voluntary
	}
	l.rows[id] = s
	return transitionApplied
}

func (l *livenessLedger) Retire(id actor.ActorID, restartIntent bool) (carrier, transitionVerdict) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok {
		return carrier{}, transitionInvalid
	}
	if s.occ == occNone && s.carrier.queue == nil {
		return carrier{}, transitionApplied
	}
	old := s.carrier
	s.occ, s.carrier, s.ticket = occNone, carrier{}, ""
	s.restart = restartIntent
	l.rows[id] = s
	return old, transitionApplied
}

func (l *livenessLedger) BeginEnsure(id actor.ActorID, version int64) (EnsureTicket, transitionVerdict) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok || l.closed || version <= 0 {
		return "", transitionInvalid
	}
	if s.ticket != "" && (s.occ == occStarting || s.occ == occRunning || s.occ == occDetached) {
		if s.version != version {
			return s.ticket, transitionInvalid
		}
		return s.ticket, transitionInFlight
	}
	if s.occ != occNone {
		return "", transitionInvalid
	}
	s.ticket, s.occ, s.version = EnsureTicket(uuid.NewString()), occStarting, version
	l.rows[id] = s
	return s.ticket, transitionApplied
}

func (l *livenessLedger) publish(id actor.ActorID, ticket EnsureTicket, c carrier, attach bool) transitionVerdict {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok || c.queue == nil {
		return transitionInvalid
	}
	if s.ticket != ticket || ticket == "" {
		return transitionStaleTicket
	}
	if attach {
		if s.occ != occStarting && s.occ != occDetached {
			return transitionInvalid
		}
	} else if s.occ != occStarting {
		return transitionInvalid
	}
	s.occ, s.carrier, s.dirty, s.restart = occRunning, c, false, false
	l.rows[id] = s
	return transitionApplied
}

func (l *livenessLedger) PublishLocal(id actor.ActorID, ticket EnsureTicket, q deliveryCarrier) transitionVerdict {
	return l.publish(id, ticket, carrier{kind: carrierLocal, queue: q}, false)
}
func (l *livenessLedger) Attach(id actor.ActorID, ticket EnsureTicket, q deliveryCarrier) transitionVerdict {
	return l.publish(id, ticket, carrier{kind: carrierPort, queue: q}, true)
}

func (l *livenessLedger) AbortEnsure(id actor.ActorID, ticket EnsureTicket) transitionVerdict {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok {
		return transitionInvalid
	}
	if s.ticket != ticket {
		return transitionStaleTicket
	}
	if s.occ != occStarting {
		return transitionInvalid
	}
	s.occ, s.ticket = occNone, ""
	l.rows[id] = s
	return transitionApplied
}

func (l *livenessLedger) Close() transitionVerdict {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return transitionApplied
	}
	l.closed = true
	for id, s := range l.rows {
		s.occ, s.carrier, s.ticket = occNone, carrier{}, ""
		l.rows[id] = s
	}
	return transitionApplied
}

func (l *livenessLedger) snapshot(id actor.ActorID) (lstate, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	return s, ok
}
