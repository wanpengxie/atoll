package actorrt

import (
	"context"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// The zombie ledger is the substrate's name for the ALREADY-JUDGED-DEAD-BUT-NOT-
// YET-REAPED state — the missing state in the embodiment lifecycle machine. Go
// gives no strong-kill: judging an actor dead (flip live off + retract from the
// addressing map — microseconds) and its goroutine actually exiting (unbounded:
// a blocked Receive / a worker-joining Stop) are two events, and every external
// termination entry used to fake a zero-width window by BLOCKING on the join.
// That unbounded wait is the disease. The cure (Unix zombie/wait split, direct):
// give the intermediate state a name and an account.
//
//	live ─judge dead→ zombie(enrolled) ─exit within grace→ reaped(struck)
//	                                    └─grace elapsed→ leaked(kept + observed) ─late exit→ late-reaped
//
// The ledger is pure in-memory (a corpse is an incarnation-level entity, and
// incarnation never persists — the same axiom the embodiments map obeys). It is
// keyed by embodiment POINTER, not id: a replaced predecessor and its live
// same-id successor coexist (the successor in embodiments, the predecessor in
// zombies) and must never collide.

// deathFlavor is how a body entered the zombie state — it selects what the
// escort must DO before it merely waits.
type deathFlavor int

const (
	// flavorNatural: the body's OWN goroutine judged itself dead (panic / a
	// DownReporter loud exit / ctx collapse) and is already unwinding. The
	// escort has nothing to signal — it only watches for a stuck exit defer
	// (e.g. a worker-joining Stop that never returns).
	flavorNatural deathFlavor = iota
	// flavorQuiet: an external QUIET teardown (replace / DespawnQuiet / StopAll)
	// — the escort fires the signal-only teardown (initiateStop) and watches.
	flavorQuiet
	// flavorDespawn: an external BY-NAME termination (Despawn / DespawnID /
	// DespawnChild) — the escort first drives the despawn-flavoured teardown (a
	// port emits a best-effort KindDespawn frame ending the remote's execution
	// arm) and watches.
	flavorDespawn
)

// zombie is one enrolled corpse: which body, when it was judged dead, how, and
// whether the escort has already declared it leaked. Its done channel is read
// off the body (doneCh) so this struct stays a pure record.
type zombie struct {
	id     actor.ActorID
	since  time.Time
	flavor deathFlavor
	body   embodiment
	reason string
	// leaked is set once by the escort when grace elapses before the body
	// exits. Guarded by r.mu (written under markLeaked, read under Zombies).
	leaked bool
}

// ZombieInfo is the read-only observation of one enrolled corpse (Zombies()):
// age = now - since; Leaked = grace elapsed without reap.
type ZombieInfo struct {
	ID     actor.ActorID
	Age    time.Duration
	Leaked bool
}

// enrollLocked records body as a zombie IN THE SAME CRITICAL SECTION as the
// death judgement (P0-1 time-order law: enrolment must precede any cancel/close/
// despawn signal, else the body can self-reap before it is on the account and
// break the account⇔residue invariant). MUST be called with r.mu held. Returns
// the entry and whether it was newly enrolled — idempotent: a second enrolment
// (an external entry then the body's own removeIf, or vice versa) returns the
// existing entry and NEVER downgrades its flavor. The caller launches the escort
// only for a NEW entry, and only AFTER releasing r.mu (the signal half is never
// under the lock).
func (r *Runtime) enrollLocked(body embodiment, id actor.ActorID, flavor deathFlavor, reason string) (*zombie, bool) {
	if z, ok := r.zombies[body]; ok {
		return z, false
	}
	z := &zombie{id: id, since: r.clock(), flavor: flavor, body: body, reason: reason}
	r.zombies[body] = z
	return z, true
}

// retireLocked enrolls body (P0-1, under r.mu) and returns a thunk the caller
// runs AFTER releasing r.mu (the signal half is never under the lock — red line
// ⑥). A body already enrolled returns a no-op thunk (its escort is already
// running). The thunk fires the NON-BLOCKING teardown signal SYNCHRONOUSLY — the
// quiet-teardown intent (a body's own racing self-death must arbitrate quiet, no
// down edge) cannot wait for the async escort, or Home.Close would leak a down
// edge in the window between judge-dead and the escort's first scheduling. Only
// the two genuinely-blocking / async parts are deferred to the escort: a port's
// KindDespawn frame write (blocking codec write) and the bounded exit join.
func (r *Runtime) retireLocked(body embodiment, id actor.ActorID, flavor deathFlavor, reason string) func() {
	z, isNew := r.enrollLocked(body, id, flavor, reason)
	if !isNew {
		return func() {}
	}
	return func() {
		switch flavor {
		case flavorQuiet:
			// Full non-blocking teardown, synchronously: set the quiet flag, cancel,
			// closeConn, self-evict + cascade children. All O(1)/non-blocking, so it
			// belongs on the entry path, not the escort.
			body.initiateStop()
		case flavorDespawn:
			// Mark the quiet intent synchronously (so a racing self-death is quiet),
			// but leave the actual teardown to the escort — the KindDespawn frame must
			// be written (blocking codec write) BEFORE closeConn, which cannot run
			// inline (P1-5).
			body.beginTeardown()
		case flavorNatural:
			// Self-terminating body: no external signal, and its down edge is loud
			// (a genuine abnormal death). The escort only watches for a stuck exit.
		}
		go r.escort(z)
	}
}

// escort is the bounded per-corpse goroutine — the ONLY join in the whole
// runtime, and it is bounded by grace so it can never itself leak (invariant ②:
// escort lifetime ≤ grace). For a despawn it first drives the async teardown (the
// KindDespawn frame + closeConn, bounded by dl); the quiet/natural flavours were
// already signalled synchronously (or are self-driven), so the escort only joins.
// It waits for the body's goroutine to exit OR for grace to elapse; on the grace
// edge it declares the corpse leaked (kept on the account + logged + counted); the
// body, whenever it finally exits, self-strikes via reapZombie (late-reap for a
// leaked one).
func (r *Runtime) escort(z *zombie) {
	dl, cancel := context.WithTimeout(context.Background(), r.grace)
	defer cancel()
	if z.flavor == flavorDespawn {
		z.body.signalDespawn(dl, z.reason)
	}
	select {
	case <-z.body.doneCh():
		// Struck by the body's own临终 defer (reapZombie); nothing to do.
	case <-dl.Done():
		r.markLeaked(z)
	}
}

// reapZombie strikes body off the ledger — invoked from the body's OWN临终
// defer the instant its goroutine truly exits (the account⇔residue invariant:
// an entry exists IFF a physical corpse remains). Idempotent (unknown body =
// no-op). A body that had already been declared leaked is a late-reap: it is
// struck all the same (the residue is gone), logged so the earlier leak is
// closed out.
func (r *Runtime) reapZombie(body embodiment) {
	r.mu.Lock()
	z, ok := r.zombies[body]
	if ok {
		delete(r.zombies, body)
	}
	r.mu.Unlock()
	if ok && z.leaked {
		r.logger.Warn("actorrt.zombie.late_reap", "actor", z.id, "age", r.clock().Sub(z.since))
	}
}

// markLeaked declares z leaked (grace elapsed before exit): kept on the account,
// logged, counted (red line ⑤: a leak is NEVER silent). Guarded so a body that
// reaped in the same instant (reapZombie won the race) is not re-counted — if the
// entry is gone or already leaked, this is a no-op.
func (r *Runtime) markLeaked(z *zombie) {
	r.mu.Lock()
	cur, ok := r.zombies[z.body]
	if !ok || cur != z || z.leaked {
		r.mu.Unlock()
		return
	}
	z.leaked = true
	r.mu.Unlock()
	r.leakedTotal.Add(1)
	r.logger.Error("actorrt.zombie.leaked", "actor", z.id, "grace", r.grace)
}

// DrainZombies is the all-kill scenario's SOLE legitimate wait position (channel
// / daemon teardown): after StopAll has judged every body dead, wait for the
// whole cohort to exit under ONE aggregate deadline (not per-corpse serial), and
// return the still-present (leaked) ids for the shutdown log. deadline ≤ 0 uses
// the configured grace.
func (r *Runtime) DrainZombies(deadline time.Duration) []actor.ActorID {
	if deadline <= 0 {
		deadline = r.grace
	}
	r.mu.RLock()
	dones := make([]<-chan struct{}, 0, len(r.zombies))
	for body := range r.zombies {
		dones = append(dones, body.doneCh())
	}
	r.mu.RUnlock()
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	for _, d := range dones {
		select {
		case <-d:
		case <-timer.C:
			return r.leakedList()
		}
	}
	return r.leakedList()
}

// leakedList snapshots the ids of every corpse still on the account (after an
// aggregate drain, these are exactly the ones that did not exit in time).
func (r *Runtime) leakedList() []actor.ActorID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]actor.ActorID, 0, len(r.zombies))
	for _, z := range r.zombies {
		out = append(out, z.id)
	}
	return out
}

// Zombies reads the ledger (id / age / leaked) — the observation face of the
// already-judged-dead-but-not-reaped state. Side-effect-free.
func (r *Runtime) Zombies() []ZombieInfo {
	now := r.clock()
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ZombieInfo, 0, len(r.zombies))
	for _, z := range r.zombies {
		out = append(out, ZombieInfo{ID: z.id, Age: now.Sub(z.since), Leaked: z.leaked})
	}
	return out
}

// LeakedTotal is the cumulative count of corpses ever declared leaked — the
// counter half of the leak-is-never-silent invariant (red line ⑤).
func (r *Runtime) LeakedTotal() int64 { return r.leakedTotal.Load() }
