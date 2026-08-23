package runtime

import (
	"sync"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type ingressClass uint8

const (
	classGeneral ingressClass = iota
	classObservation
	classCommand
	classCallback
	classCompletion
	classTimer
	classFault
)

const criticalIngressCapacity = 16

type ingressEntry struct {
	seq         uint64
	class       ingressClass
	value       any
	activityKey string
}

type protocolFault struct {
	generation   uint64
	code, detail string
}

// inbox is a bounded MPSC admission ledger. Every physical container shares
// one sequence generator and pop always consumes the smallest admitted seq.
type inbox struct {
	mu                                                              sync.Mutex
	sealed                                                          bool
	next                                                            uint64
	items                                                           []*ingressEntry
	wake                                                            chan struct{}
	general, observations, commands, callbacks, completions, timers int
	generalCap, observationCap, commandCap, callbackCap             int
	activity                                                        map[string]*ingressEntry
	fault                                                           *ingressEntry
}

func newInbox(p Policy) *inbox {
	return &inbox{wake: make(chan struct{}, 1), generalCap: criticalIngressCapacity, observationCap: p.IngressCapacity, commandCap: p.CommandCapacity, callbackCap: p.CallbackCapacity, activity: map[string]*ingressEntry{}}
}

func (q *inbox) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *inbox) push(class ingressClass, value any) bool {
	q.mu.Lock()
	if q.sealed {
		q.mu.Unlock()
		return false
	}
	switch class {
	case classGeneral:
		if q.general >= q.generalCap {
			q.mu.Unlock()
			return false
		}
		q.general++
	case classObservation:
		if q.observations >= q.observationCap {
			q.mu.Unlock()
			return false
		}
		q.observations++
	case classCommand:
		if q.commands >= q.commandCap {
			q.mu.Unlock()
			return false
		}
		q.commands++
	case classCallback:
		if q.callbacks >= q.callbackCap {
			q.mu.Unlock()
			return false
		}
		q.callbacks++
	case classCompletion:
		// Completion has its own fixed burst capacity. This is transport
		// buffering, not a per-turn callback budget.
		if q.completions >= q.callbackCap+1 {
			q.mu.Unlock()
			return false
		}
		q.completions++
	case classTimer:
		// At most one open/start/control/watchdog/reap timer is live per
		// identity. Eight slots is a structural upper bound for one Runtime.
		if q.timers >= 8 {
			q.mu.Unlock()
			return false
		}
		q.timers++
	}
	q.next++
	q.items = append(q.items, &ingressEntry{seq: q.next, class: class, value: value})
	q.mu.Unlock()
	q.signal()
	return true
}

func (q *inbox) pushActivity(generation uint64, target driverproto.WorkerTurnTarget) bool {
	key := activityKey(generation, target)
	q.mu.Lock()
	if q.sealed {
		q.mu.Unlock()
		return false
	}
	if old := q.activity[key]; old != nil {
		// Activity has no content. Retaining the first seq while replacing the
		// value is therefore order-preserving coalescence.
		old.value = driverFact{generation: generation, event: driverproto.Activity{Target: target}}
		q.mu.Unlock()
		return true
	}
	if q.observations >= q.observationCap {
		q.mu.Unlock()
		return false
	}
	q.observations++
	q.next++
	e := &ingressEntry{seq: q.next, class: classObservation, value: driverFact{generation: generation, event: driverproto.Activity{Target: target}}, activityKey: key}
	q.items = append(q.items, e)
	q.activity[key] = e
	q.mu.Unlock()
	q.signal()
	return true
}

func activityKey(g uint64, t driverproto.WorkerTurnTarget) string {
	return fmtUint(g) + "/" + fmtUint(uint64(t.Attempt)) + "/" + string(t.Native)
}

func fmtUint(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

func (q *inbox) latchFault(v protocolFault) {
	q.mu.Lock()
	if q.sealed || q.fault != nil {
		q.mu.Unlock()
		return
	}
	q.next++
	q.fault = &ingressEntry{seq: q.next, class: classFault, value: v}
	q.mu.Unlock()
	q.signal()
}

func (q *inbox) pop() (*ingressEntry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var e *ingressEntry
	if len(q.items) > 0 && (q.fault == nil || q.items[0].seq < q.fault.seq) {
		e = q.items[0]
		q.items[0] = nil
		q.items = q.items[1:]
	} else if q.fault != nil {
		e = q.fault
		q.fault = nil
	} else {
		return nil, false
	}
	switch e.class {
	case classGeneral:
		q.general--
	case classObservation:
		q.observations--
	case classCommand:
		q.commands--
	case classCallback:
		q.callbacks--
	case classCompletion:
		q.completions--
	case classTimer:
		q.timers--
	}
	if e.activityKey != "" {
		delete(q.activity, e.activityKey)
	}
	return e, true
}

func (q *inbox) seal() {
	q.mu.Lock()
	q.sealed = true
	q.items = nil
	q.activity = nil
	q.fault = nil
	q.mu.Unlock()
	q.signal()
}

func (q *inbox) isSealed() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.sealed }
