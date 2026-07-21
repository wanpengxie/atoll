package home

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
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
	kind carrierKind
	// inc is the published body's identity token — the write-side
	// self-validation key for down edges (值范式: a late down edge from a
	// replaced predecessor carries a token that no longer matches and is
	// REJECTED instead of wiping the successor's account). It is a comparable
	// identity value, never a capability: nothing can be enqueued through it.
	inc   actorrt.Incarnation
	queue deliveryCarrier
}

type attachmentFenceWord struct {
	ticket  EnsureTicket
	version int64
	alive   bool
}

type attachmentFenceCell struct {
	current atomic.Pointer[attachmentFenceWord]
}

type attachmentFence struct {
	cell     *attachmentFenceCell
	expected *attachmentFenceWord
}

func (f attachmentFence) Valid() bool {
	return f.cell != nil && f.expected != nil && f.cell.current.Load() == f.expected
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
	fence   *attachmentFenceCell
}

type attachmentIntent struct {
	Ticket  EnsureTicket
	Version int64
	Present bool
}

type wakeStanding struct {
	Occ        occupancy
	Dirty      bool
	Restart    bool
	HasCarrier bool
	Port       bool
	// CarrierInc is the published body's identity token — exposed so the
	// reconcile repair path can hand ObserveDown the token of the exact
	// carrier it observed absent (identity value only, not a capability).
	CarrierInc actorrt.Incarnation
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

func newDormantState() lstate {
	cell := new(attachmentFenceCell)
	cell.current.Store(&attachmentFenceWord{})
	return lstate{occ: occNone, fence: cell}
}

func publishFence(s *lstate, ticket EnsureTicket, version int64, alive bool) {
	if s.fence == nil {
		s.fence = new(attachmentFenceCell)
	}
	s.fence.current.Store(&attachmentFenceWord{ticket: ticket, version: version, alive: alive})
}

func invalidateFence(s *lstate) { publishFence(s, "", 0, false) }

func (l *livenessLedger) Bootstrap(ids []actor.ActorID) transitionVerdict {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return transitionInvalid
	}
	for _, id := range ids {
		if id == "" {
			return transitionInvalid
		}
	}
	for id, s := range l.rows {
		invalidateFence(&s)
		l.rows[id] = s
	}
	next := make(map[actor.ActorID]lstate, len(ids))
	for _, id := range ids {
		next[id] = newDormantState()
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
	l.rows[id] = newDormantState()
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
	invalidateFence(&s)
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

func (l *livenessLedger) ApproveIdle(id actor.ActorID) (carrier, transitionVerdict) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok || s.occ != occRunning || s.carrier.queue == nil {
		return carrier{}, transitionInvalid
	}
	old := s.carrier
	invalidateFence(&s)
	s.occ, s.carrier, s.ticket = occNone, carrier{}, ""
	l.rows[id] = s
	return old, transitionApplied
}

// ObserveDown is write-side self-validating (§2.6 组装纪律): the down edge
// must carry the incarnation token of the body it reports dead. A late edge
// from a predecessor that has already been replaced by a published successor
// carries a stale token and is rejected — it must never wipe the successor's
// carrier/ticket or charge it a spurious restart/backoff. (The local half of
// the same discipline the attach fence gives the port half.)
func (l *livenessLedger) ObserveDown(id actor.ActorID, inc actorrt.Incarnation, port bool, voluntary bool) transitionVerdict {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok || s.occ != occRunning {
		return transitionInvalid
	}
	if s.carrier.inc != inc {
		return transitionInvalid
	}
	s.carrier = carrier{}
	if port {
		s.occ = occDetached
	} else {
		invalidateFence(&s)
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
	invalidateFence(&s)
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
	publishFence(&s, s.ticket, version, true)
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

func (l *livenessLedger) PublishLocal(id actor.ActorID, ticket EnsureTicket, inc actorrt.Incarnation, q deliveryCarrier) transitionVerdict {
	return l.publish(id, ticket, carrier{kind: carrierLocal, inc: inc, queue: q}, false)
}
func (l *livenessLedger) Attach(id actor.ActorID, ticket EnsureTicket, birthVersion int64, inc actorrt.Incarnation, q deliveryCarrier) transitionVerdict {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok || q == nil || birthVersion <= 0 {
		return transitionInvalid
	}
	if s.ticket != ticket || ticket == "" || s.version != birthVersion {
		return transitionStaleTicket
	}
	if s.occ != occStarting && s.occ != occDetached && s.occ != occRunning {
		return transitionInvalid
	}
	s.occ, s.carrier, s.dirty, s.restart = occRunning, carrier{kind: carrierPort, inc: inc, queue: q}, false, false
	l.rows[id] = s
	return transitionApplied
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
	invalidateFence(&s)
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
		invalidateFence(&s)
		s.occ, s.carrier, s.ticket = occNone, carrier{}, ""
		l.rows[id] = s
	}
	return transitionApplied
}

func (l *livenessLedger) AttachmentIntent(id actor.ActorID) attachmentIntent {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok {
		return attachmentIntent{}
	}
	present := s.ticket != "" && (s.occ == occStarting || s.occ == occRunning || s.occ == occDetached)
	return attachmentIntent{Ticket: s.ticket, Version: s.version, Present: present}
}

func (l *livenessLedger) WakeStanding(id actor.ActorID) (wakeStanding, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok {
		return wakeStanding{}, false
	}
	return wakeStanding{
		Occ: s.occ, Dirty: s.dirty, Restart: s.restart,
		HasCarrier: s.carrier.queue != nil, Port: s.carrier.kind == carrierPort,
		CarrierInc: s.carrier.inc,
	}, true
}

func (l *livenessLedger) prepareAttachmentFence(id actor.ActorID, ticket EnsureTicket, birthVersion int64) (attachmentFence, transitionVerdict) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok || l.closed || s.fence == nil || ticket == "" || birthVersion <= 0 {
		return attachmentFence{}, transitionInvalid
	}
	word := s.fence.current.Load()
	if word == nil || !word.alive || word.ticket != ticket || word.version != birthVersion ||
		s.ticket != ticket || s.version != birthVersion {
		return attachmentFence{}, transitionStaleTicket
	}
	return attachmentFence{cell: s.fence, expected: word}, transitionApplied
}

// RetireIfVersionSkew closes the read→retire race: the version comparison and
// retirement happen under the same ledger lock. A live attempt is retired when
// the account's declaration version has migrated away from the one this
// attempt welded.
func (l *livenessLedger) RetireIfVersionSkew(id actor.ActorID, declVersion int64) (carrier, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok || declVersion <= 0 || s.occ == occNone || s.version == 0 {
		return carrier{}, false
	}
	if s.version == declVersion {
		return carrier{}, false
	}
	old := s.carrier
	invalidateFence(&s)
	s.occ, s.carrier, s.ticket = occNone, carrier{}, ""
	s.restart = true
	l.rows[id] = s
	return old, true
}

func (l *livenessLedger) RetireIfTicketMatches(id actor.ActorID, expected EnsureTicket, restartIntent bool) (carrier, bool) {
	if expected == "" {
		return carrier{}, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.rows[id]
	if !ok || s.ticket != expected {
		return carrier{}, false
	}
	old := s.carrier
	invalidateFence(&s)
	s.occ, s.carrier, s.ticket = occNone, carrier{}, ""
	s.restart = restartIntent
	l.rows[id] = s
	return old, true
}
