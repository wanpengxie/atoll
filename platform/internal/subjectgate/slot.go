package subjectgate

import (
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// Level is a layer-3 presence value the gateway writes into a slot as its own
// device-aggregate testimony (NOT derived from layer-2 bound/unbound — 禁互训).
type Level string

const (
	LevelOnline  Level = "online"
	LevelOffline Level = "offline"
)

// PresenceUpdate is one edge delivered to a slot observer. Live=false is a
// REVOCATION (a new gateway epoch supersedes the prior testimony, or the slot's
// testimony is Forgotten): the observer must stop asserting the old value.
type PresenceUpdate struct {
	Epoch   int64
	EdgeSeq int64
	Level   Level
	Live    bool
}

// presenceState is the slot's layer-3 register: the gateway epoch + edge cursor +
// level, and whether any value is present. `set=false` folds to the honest
// unknown (no value published → observer asserts nothing).
type presenceState struct {
	epoch   int64
	edgeSeq int64
	level   Level
	set     bool
}

// Slot is the per-identity binding slot (build spec §S2): the four-tuple
// {绑定世代(bindingGen), gateway epoch(presence.epoch), 帧递交端(frames), presence
// level} with the独立性不变式 — level's ONLY writer is PublishLevel (layer-3),
// never co-written with SetBinding (layer-2). The frame delivery端 is a
// synchronous request/reply surface: the gateway calls Deliver and blocks for
// the cell's interpreter goroutine to answer (零队列零 ack 关联器 — the reply
// channel IS the correlation).
type Slot struct {
	id actor.ActorID
	mu sync.Mutex

	// Layer-2 (binding axis), written ONLY by SetBinding.
	bindingGen int64

	// Layer-3 (presence axis), written ONLY by PublishLevel/Forget.
	presence presenceState

	// Observers keyed by an incarnation token (the cell's own uuid). A stale
	// remove carrying an old token can never摘 a fresh cell's registration —
	// the tokens differ, so RemoveObserver(oldToken) misses the new entry.
	observers map[string]func(PresenceUpdate)

	// frames is the帧递交端, one per identity (outlives incarnations). Unbuffered:
	// Deliver hands a job and blocks on its reply, so no persistent queue accrues.
	frames chan Job
	// live is true while an interpreter is attached; dead is closed when the
	// current interpreter detaches (so a blocked Deliver unblocks). dead is
	// re-created on each AttachInterpreter (per incarnation). incarnation is the
	// current interpreter's token: a stale incarnation's delayed release closes its
	// OWN dead channel but must NOT flip live/dead for the successor that already
	// took over (旧 incarnation 延迟 release 摘不掉新 interpreter, straddle gate).
	live        bool
	dead        chan struct{}
	incarnation uint64
}

// Job is one upstream frame handed to the interpreter goroutine. The interpreter
// answers it exactly once via Reply — the reply channel is buffered(1) so a
// Reply never blocks even if the delivering gateway goroutine has already given
// up (its Deliver saw the slot go dead).
//
// BindingGen is the绑定世代 Deliver was invoked with, carried WITH the job so the
// interpreter can re-verify it against the slot's current层2世代 at the真线性化点
// (the commit / pen 落账, north of this queue): the enqueue-time check under the
// slot lock is only a fast reject — a rebind (SetBinding) can still land AFTER the
// check but BEFORE the interpreter commits, so the authoritative gate is the
// interpreter's commit-point recheck of this carried gen. DeliverAnyGen rides
// through unchanged (trusted platform-internal shim → exempt at the commit point).
type Job struct {
	Frame      Frame
	BindingGen int64
	reply      chan FrameResult
}

// Reply answers this job's frame with its receipt-or-error result.
func (j Job) Reply(r FrameResult) { j.reply <- r }

// FrameResult is what Deliver returns to the gateway: the receipt-or-error frame
// the cell's interpreter produced for one upstream frame.
type FrameResult struct {
	Frame Frame
}

// ErrNoOccupant is Deliver's verdict when no interpreter is currently attached
// (the cell is mid-re-mint, or torn down). The gateway maps it to the transient
// unavailable code.
var ErrNoOccupant = errors.New("subjectgate: no live occupant for slot")

// ErrStaleBinding is Deliver's verdict when the frame's binding generation no
// longer matches the slot's current layer-2 generation — a rebind (seal → fresh
// arm → SetBinding) superseded the binding AFTER the gateway's upstream初验 but
// BEFORE this delivery reached the linearization point. Comparing under the slot
// lock (atomic with the live check) closes the初验→seal→rebind→Deliver TOCTOU
// window: the gateway side初验 and the slot side re-verify form the two ends of
// the双向世代 gate. The gateway maps it to the stale_binding边界 code.
var ErrStaleBinding = errors.New("subjectgate: stale binding generation")

// DeliverAnyGen bypasses the binding-generation assertion in Deliver. It is for
// trusted platform-internal delivery paths (the app control shim) that carry NO
// gateway binding — there is no gateway session/arm behind them, so a stale
// gateway binding is not a possible fault. The gateway ALWAYS passes the
// session's own granted gen (≥1), so a superseded binding is refused at the
// delivery linearization point.
const DeliverAnyGen int64 = -1

func newSlot(id actor.ActorID) *Slot {
	return &Slot{
		id:        id,
		observers: map[string]func(PresenceUpdate){},
		frames:    make(chan Job),
	}
}

// SetBinding writes the layer-2 binding generation. Per the独立性不变式 it NEVER
// touches the presence register — a pure rebind produces zero presence side
// effect (build spec §2 pair B×E).
func (s *Slot) SetBinding(gen int64) {
	s.mu.Lock()
	s.bindingGen = gen
	s.mu.Unlock()
}

// BindingGen reads the current layer-2 generation (receipts stamp it downstream).
func (s *Slot) BindingGen() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bindingGen
}

// PublishLevel is the ONLY layer-3 writer. Dedup/ordering (build spec §S2):
//   - same epoch: edgeSeq must strictly increase, else the edge is dropped;
//   - new (greater) epoch: the old testimony is REVOKED (observers see Live=false)
//     then the new value is snapshotted and delivered;
//   - lesser epoch: dropped (stale gateway).
//
// It returns whether the edge was applied (a dropped duplicate/stale returns
// false). Observers are notified under the slot lock so edges are totally ordered.
func (s *Slot) PublishLevel(epoch, edgeSeq int64, level Level) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.presence
	switch {
	case !cur.set:
		// first testimony.
	case epoch == cur.epoch:
		if edgeSeq <= cur.edgeSeq {
			return false // duplicate/reorder within the same epoch — dropped.
		}
	case epoch > cur.epoch:
		// new epoch: revoke the prior testimony before snapshotting the new one.
		s.notifyLocked(PresenceUpdate{Epoch: cur.epoch, EdgeSeq: cur.edgeSeq, Level: cur.level, Live: false})
	default:
		return false // epoch < cur.epoch — stale gateway, dropped.
	}
	s.presence = presenceState{epoch: epoch, edgeSeq: edgeSeq, level: level, set: true}
	s.notifyLocked(PresenceUpdate{Epoch: epoch, EdgeSeq: edgeSeq, Level: level, Live: true})
	return true
}

// Forget clears the slot's layer-3 testimony (证词账清洁边 — the容器 owner清账,
// NOT a produced证词; build spec §S2 / design §5.4). Observers see a revocation
// and the register folds back to unknown (no value). Used by the gateway epoch
// teardown / 户籍级联 (S4).
func (s *Slot) Forget() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.presence.set {
		s.notifyLocked(PresenceUpdate{Epoch: s.presence.epoch, EdgeSeq: s.presence.edgeSeq, Level: s.presence.level, Live: false})
	}
	s.presence = presenceState{}
}

// Snapshot reads the current layer-3 register atomically (the cell Start read).
// ok=false = unknown (nothing published) — the cell self-reports nothing.
func (s *Slot) Snapshot() (level Level, epoch, edgeSeq int64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.presence.level, s.presence.epoch, s.presence.edgeSeq, s.presence.set
}

// RegisterObserver registers fn under token (the cell's incarnation token). A
// re-registration under the same token replaces. The registration is摘除 by
// RemoveObserver(token); a stale token misses a newer cell's entry.
func (s *Slot) RegisterObserver(token string, fn func(PresenceUpdate)) {
	s.mu.Lock()
	s.observers[token] = fn
	s.mu.Unlock()
}

// RemoveObserver摘除 the registration for token IFF it is still the one present
// (a newer incarnation registered under a different token is never touched).
func (s *Slot) RemoveObserver(token string) {
	s.mu.Lock()
	delete(s.observers, token)
	s.mu.Unlock()
}

func (s *Slot) notifyLocked(u PresenceUpdate) {
	for _, fn := range s.observers {
		fn(u)
	}
}

// AttachInterpreter marks the slot occupied and returns the frame job stream the
// interpreter goroutine consumes, this incarnation's token, and a release func it
// defers. release closes THIS incarnation's dead channel so any Deliver blocked on
// it unblocks (解阻), but it only flips the slot's live flag if this incarnation is
// still the current one — a stale incarnation whose release runs AFTER a successor
// has taken over never摘 the successor's liveness (incarnation gate, 照 observer
// token). A fresh AttachInterpreter overrides: it stamps a new incarnation + dead
// channel, so a subsequent Deliver sees the successor.
func (s *Slot) AttachInterpreter() (<-chan Job, uint64, func()) {
	s.mu.Lock()
	s.incarnation++
	token := s.incarnation
	s.live = true
	s.dead = make(chan struct{})
	dead := s.dead
	frames := s.frames
	s.mu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			s.mu.Lock()
			if s.incarnation == token {
				s.live = false // still the current incarnation → the slot goes idle
			}
			close(dead) // always unblock THIS incarnation's stranded Delivers
			s.mu.Unlock()
		})
	}
	return frames, token, release
}

// Deliver hands one upstream frame to the attached interpreter and blocks for
// its reply (synchronous, 零 ack 关联器). No interpreter, or the interpreter
// detaches mid-flight → ErrNoOccupant.
//
// bindingGen is the递交 gate. The enqueue-time comparison against the slot's
// current layer-2 generation UNDER the slot lock (atomic with the live check) is a
// FAST reject only — it is NOT the linearization point: a rebind (SetBinding) can
// still land between this check and the interpreter's actual commit, so bindingGen
// is ALSO carried into the Job and re-verified by the interpreter immediately
// before it 落账 (the真线性化点, north of this queue). A stale frame that slips the
// fast check is then refused stale_binding at commit, so its queue traversal is
// harmless. Pass DeliverAnyGen from trusted platform-internal paths that carry no
// gateway binding (exempt at both the fast check and the commit-point recheck).
func (s *Slot) Deliver(f Frame, bindingGen int64) (FrameResult, error) {
	s.mu.Lock()
	if !s.live {
		s.mu.Unlock()
		return FrameResult{}, ErrNoOccupant
	}
	if bindingGen != DeliverAnyGen && bindingGen != s.bindingGen {
		s.mu.Unlock()
		return FrameResult{}, ErrStaleBinding
	}
	dead := s.dead
	frames := s.frames
	s.mu.Unlock()

	reply := make(chan FrameResult, 1)
	select {
	case frames <- Job{Frame: f, BindingGen: bindingGen, reply: reply}:
	case <-dead:
		return FrameResult{}, ErrNoOccupant
	}
	select {
	case r := <-reply:
		return r, nil
	case <-dead:
		return FrameResult{}, ErrNoOccupant
	}
}

// Registry is the per-Home binding registry (built at Home.Open, step①). It owns
// every subject's slot; slots are ensured at attach (step②, before any cell
// construction path) and read by the cell factory (step③).
type Registry struct {
	mu    sync.Mutex
	slots map[actor.ActorID]*Slot
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{slots: map[actor.ActorID]*Slot{}}
}

// EnsureSlot returns id's slot, creating it on first call (idempotent). The
// gateway calls this at attach BEFORE the cell is constructed (step②), so the
// factory's step③ lookup never races an absent slot.
func (r *Registry) EnsureSlot(id actor.ActorID) *Slot {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.slots[id]
	if s == nil {
		s = newSlot(id)
		r.slots[id] = s
	}
	return s
}

// Slot returns id's slot if one exists (the factory's step③ lookup — no create).
func (r *Registry) Slot(id actor.ActorID) (*Slot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.slots[id]
	return s, ok
}

// Remove drops id's slot (户籍级联, S4 consumes). Forgets its testimony first so
// no observer keeps a stale value.
func (r *Registry) Remove(id actor.ActorID) {
	r.mu.Lock()
	s := r.slots[id]
	delete(r.slots, id)
	r.mu.Unlock()
	if s != nil {
		s.Forget()
	}
}
