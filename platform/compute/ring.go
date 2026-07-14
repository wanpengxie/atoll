package compute

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/internal/hostcommon"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// redialInitialBackoff / redialMaxBackoff bound the daemon's reconnect retry
// (§10.13 推导3: link death degrades the wire, never the hosted work — so the
// daemon just keeps trying to get the wire back, at no accelerating cost to
// the home). Exponential from the floor, capped at the ceiling; reset to the
// floor only after a connection PROVES itself (see redialBackoffAfterLink).
const (
	redialInitialBackoff = 1 * time.Second
	redialMaxBackoff     = 30 * time.Second
)

// nextRedialBackoff doubles cur, capped at redialMaxBackoff.
func nextRedialBackoff(cur time.Duration) time.Duration {
	next := cur * 2
	if next > redialMaxBackoff {
		return redialMaxBackoff
	}
	return next
}

// redialBackoffAfterLink decides the pre-sleep backoff after a non-graceful link
// drop, from how long the session `lived`. A session that reached redialMaxBackoff
// is a genuine connection that later dropped — reset the ladder to the floor. A
// shorter session is a Dial-succeeds-then-instantly-drops flap: keep the ladder
// where it is (the caller grows it once more before the next dial) so an
// "attach → 1s death" loop backs off toward the ceiling instead of hammering the
// home at 1/s forever. The reset is deliberately NOT keyed on Dial success: the
// WS+attach round trip says nothing about link lifespan.
func redialBackoffAfterLink(cur, lived time.Duration) time.Duration {
	if lived >= redialMaxBackoff {
		return redialInitialBackoff
	}
	return cur
}

// jitterBackoff randomizes d to the range [d/2, d] (AWS "equal jitter") so
// daemons that all lost the link to the same home outage don't redial in
// lockstep. It only randomizes the SLEEP; the stored backoff ladder
// (nextRedialBackoff) stays a clean unjittered doubling sequence.
func jitterBackoff(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

// ErrForwardersLeaked reports that Run's teardown could not join
// its background forwarder goroutines within the bounded timeout.
var ErrForwardersLeaked = errors.New("compute: forwarders leaked; storage root ownership transferred to process exit")

type computeLifecycleHooks struct {
	forwarderTimeout time.Duration
	// forwarderLeaked, when non-nil, replaces the invocation-local forwarder
	// leak account so a test can assert its delta directly.
	forwarderLeaked *atomic.Int64
	obsExited       func()
	storageExited   func()
	storagePump     func(context.Context, *storageHostForwarder)
}

// computeRing is the daemon's reconcile ring: it diffs its plan snapshot against the
// locally-live cells and drives the actor lifecycle (open/spawn/start, despawn/
// detach) to close the gap. It is the daemon-hosted counterpart of Home's
// reconcileActivation — same paradigm (§10.13 推导2), different host.
type computeRing struct {
	rt        *actorrt.Runtime
	del       actorrt.Deliverer
	watcher   *cellDownWatcher
	obsFwd    *cellObsForwarder
	cancelFwd *cellCancelForwarder
	source    PlanSource
	logger    *slog.Logger

	// prevCurrent is the AlwaysOn desired set the LAST reconcile pass declared —
	// mirrors Home's prevEagerDesired: the 削 diff is prevCurrent − current, never
	// live − current (LiveIDs has no other embodiment category on a daemon this
	// period, but the same never-actual-diff discipline holds so a future daemon-
	// local category never gets silently evicted). Touched only from the single
	// caller goroutine (Run's own loop), so it needs no lock.
	prevCurrent map[actor.ActorID]desiredIncarnation
	// builtEpoch is written only after a body has been built successfully. An
	// absent entry therefore remains actionable and is retried next tick.
	builtEpoch map[actor.ActorID]int64
	// arms is the per-id wire-flap membrane (§10.13 推导3): populated once at
	// buildOne, Rebind'd (never re-created) on every later reopen — the cell's
	// Caps were built from these facades, so a reconnect never touches the cell.
	arms map[actor.ActorID]*link.RebindableArms
}

type desiredIncarnation struct {
	Kind  actor.Kind
	Epoch int64
}

// runLink drives ONE connected session on d: an initial reconcile pass (which
// reopens every already-live actor's stream on this new link, F6, before it
// resolves anything freshly missing) followed by a poll loop. It returns true
// (graceful) only on ctx cancellation; Dialer.Close owns the complete detach,
// drain, and carrier-close protocol. It returns false if the link itself goes down, so the
// caller redials. It never touches rt's cells: a link death degrades the wire,
// the hosted work is untouched (§10.13 推导3).
func (r *computeRing) runLink(ctx context.Context, d *link.Dialer, poll, resync time.Duration) (graceful bool) {
	r.reconcile(ctx, d, false)

	t := time.NewTicker(poll)
	defer t.Stop()
	resyncT := time.NewTicker(resync)
	defer resyncT.Stop()
	for {
		select {
		case <-ctx.Done():
			return true
		case <-d.Done():
			return false
		case <-t.C:
			r.reconcile(ctx, d, false)
		case <-resyncT.C:
			// Periodic resync (#7 kubelet 两件套 半②): re-declare the full set
			// unconditionally so the home re-diffs and absorbs any漏网 drift —
			// level-triggered, independent of whether this tick has a diff.
			r.reconcile(ctx, d, true)
		}
	}
}

// reconcile runs one pass of the ring against link d: read desired, 削 what
// fell out of it (despawn + detach), then 补 what is desired-and-not-fully-
// hosted on THIS link — either never built, or live but missing a stream here
// (F6: a fresh reconnect where nothing has a stream yet, or a single stream
// that died while the link stayed up). Reattach the full declared set, then
// per missing id either buildOne (never live: resolve factory, OpenStream,
// SpawnIfAbsent, StartStream) or reopenOne (already live: OpenStream, Rebind, Start-
// Stream — never re-SpawnIfAbsent). A desired-read failure leaves the prior state
// untouched and retries next tick.
//
// The full-set Reattach fires on ANY of three conditions (#7 kubelet 两件套):
// 补边 (missing ids need their stream opened), 削边 (this tick shrank the set —
// a pure缩容 has no missing but the home must still be told, else the fallen-out
// host row滞留 indefinitely, account收敛 only on a later privileged扩容/reconnect),
// or resync (the caller's slow periodic re-declaration, level-triggered兜底 that
// absorbs any drift — a削章 dereg that failed home-side, a late migration).
func (r *computeRing) reconcile(ctx context.Context, d *link.Dialer, resync bool) {
	plan, err := d.PullPlan(ctx)
	if err != nil {
		r.logger.Warn("platform.compute.plan_pull_failed", "err", err)
	} else if err := r.source.ApplyPlan(plan); err != nil {
		r.logger.Warn("platform.compute.plan_apply_failed", "err", err)
	}
	members, err := r.source.Members(ctx)
	if err != nil {
		r.logger.Error("platform.compute.desired_failed", "err", err)
		return
	}

	current := make(map[actor.ActorID]desiredIncarnation, len(members))
	for _, m := range members {
		current[m.ID] = desiredIncarnation{Kind: m.Kind, Epoch: m.Epoch}
	}

	// 削: previously-declared ids no longer desired. Local despawn ends the cell's
	// execution arm; DetachStream tells the home this arm is gone QUIET (§10.5/S1).
	// DetachStream is attachment-only (membership ≠ attachment): the home host row
	// only falls out when a later full-set Reattach re-diffs, so a 削 forces that
	// Reattach this same tick (shrank) rather than waiting for a补边 that may never come.
	shrank := false
	for id := range r.prevCurrent {
		if _, ok := current[id]; ok {
			continue
		}
		r.rt.DespawnID(id)
		d.DetachStream(id)
		delete(r.arms, id)
		delete(r.builtEpoch, id)
		r.watcher.mu.Lock()
		delete(r.watcher.down, id)
		r.watcher.mu.Unlock()
		shrank = true
		r.logger.Info("platform.compute.deactivated", "actor", string(id))
	}

	// Epoch delta is an incarnation replacement, not an in-place refresh. Cut
	// the old local body and stream before declaring/opening the new generation.
	// Missing builtEpoch is equally actionable: only a successful build may
	// establish that account.
	for id, want := range current {
		got, recorded := r.builtEpoch[id]
		if recorded && got == want.Epoch {
			continue
		}
		if !recorded {
			if _, live := r.rt.Stat(id); !live {
				continue
			}
		}
		r.rt.DespawnID(id)
		d.DetachStream(id)
		delete(r.arms, id)
		delete(r.builtEpoch, id)
		r.watcher.mu.Lock()
		delete(r.watcher.down, id)
		r.watcher.mu.Unlock()
		r.logger.Info("platform.compute.epoch_replaced", "actor", string(id), "old_epoch", got, "new_epoch", want.Epoch)
	}

	// 补: desired-and-not-fully-hosted on THIS link — live ∪ stream-existence
	// (F6), never live alone: a cell can be live while its stream on d is gone.
	live := make(map[actor.ActorID]bool)
	for _, id := range r.rt.LiveIDs() {
		live[id] = true
	}
	var missing []actor.ActorID
	for id := range current {
		if !live[id] || !d.HasStream(id) {
			missing = append(missing, id)
		}
	}

	if len(missing) > 0 || shrank || resync {
		// Reattach the FULL current declared set (kubelet node-status idiom, never
		// an increment — §S-P8) so the home's allowed set covers every id about to
		// OpenStream, then build/reopen each missing actor. On a削-only or resync
		// tick missing is empty, so the loop below is a no-op — the Reattach itself
		// is the payload (re-diff the home's host rows).
		decls := make([]link.Declaration, 0, len(current))
		for id, desired := range current {
			decls = append(decls, link.Declaration{ActorID: id, Kind: desired.Kind, Binding: actor.BindingRuntimeInboundViaRelay, Epoch: desired.Epoch})
		}
		if err := d.Reattach(ctx, decls); err != nil {
			r.logger.Warn("platform.compute.reattach_failed", "err", err, "actors", len(decls))
			// Every missing id stays not-fully-hosted, so next tick's diff retries
			// them (the ids remain in `current`, untouched below). A失败的削 Reattach
			// is not re-fired by the poll loop (prevCurrent updates to current below,
			// so the 削 does not re-diff) — the periodic resync is its收敛 backstop.
		} else {
			for _, id := range missing {
				want := current[id]
				if live[id] {
					r.reopenOne(ctx, id, want.Epoch, d)
				} else {
					if r.buildOne(ctx, id, want.Kind, want.Epoch, d) {
						r.builtEpoch[id] = want.Epoch
					}
				}
			}
		}
	}

	r.prevCurrent = current
}

// buildOne opens the actor's link stream, resolves its factory via the builder,
// and spawns + starts it — the FIRST time this id is ever hosted. A failure at
// any step is logged; the id stays out of the live set, so next tick's diff
// retries it.
func (r *computeRing) buildOne(ctx context.Context, id actor.ActorID, kind actor.Kind, epoch int64, d *link.Dialer) bool {
	factory, ok := r.source.Lookup(id)
	if !ok {
		r.logger.Warn("platform.compute.no_builder", "actor", string(id))
		return false
	}
	// Open the actor's link stream first: the RemoteWriter (the cell's pen) must
	// exist before the cell is spawned. One stream == one actor, so the dispatch
	// handler routes every envelope on this stream into THIS actor's mailbox (the
	// stream IS the target — no audience demux on the daemon).
	arms, err := d.OpenStream(ctx, id, epoch, r.dispatchFor(id, d), r.cancelFor(id))
	if err != nil {
		r.logger.Warn("platform.compute.open_stream_failed", "actor", string(id), "err", err)
		return false
	}
	// The cell's Caps are built from the REBINDABLE facades, never the raw
	// per-stream proxies directly — this is the one-time construction the wire-
	// flap membrane needs: a later reconnect Rebinds this SAME *RebindableArms,
	// the cell (built once, right below) never rebuilds (§10.13 推导3).
	rb := link.NewRebindableArms(arms)
	r.arms[id] = rb
	// Two-phase construction, mirroring the home activation path (§10.13 推导7①/G12): the
	// build closure runs inside SpawnIfAbsent, BEFORE go-live, so link.NewLiveArms welds
	// the cell's caps to THIS incarnation and fences every call until it goes
	// live — a factory that writes during construction is refused here exactly
	// like a cell born at home, closing the daemon-side parity gap the raw
	// (ungated) facades used to leave open.
	inc, built, buildErr := r.rt.SpawnIfAbsent(id, kind, func(inc actorrt.Incarnation) actorrt.Actor {
		r.watcher.install(id, inc, arms.Down)
		// daemon Hooks.Canceller = the caller-side cancel-upstream forwarder (the
		// out-generation matrix's former known gap, now filled): a daemon-hosted
		// caller's Cancel commits its own unanswered_timeout terminal AND forwards a
		// KindCancelRequest frame up this caller's stream, so the home reverse-
		// resolves the target and reaches the receiver's in-station account — the
		// daemon-hosted parity of the cell-path Home.CancelRequest.
		return hostcommon.Build(link.NewLiveArms(rb, inc, r.rt), actorbase.Hooks{Canceller: r.cancelFwd.cancellerFor(id)}, factory)
	})
	if buildErr != nil || !built {
		if buildErr != nil {
			r.logger.Error("platform.compute.build_failed", "actor", string(id), "error", buildErr)
		} else {
			r.logger.Error("platform.compute.build_cas_lost", "actor", string(id))
		}
		d.DetachStream(id)
		delete(r.arms, id)
		r.watcher.removeIf(id, inc)
		return false
	}
	if !r.rt.IsLive(inc) {
		r.watcher.removeIf(id, inc)
		d.DetachStream(id)
		delete(r.arms, id)
		return false
	}
	d.StartStream(id)
	return true
}

// reopenOne re-establishes an ALREADY-LIVE actor's link stream on d — a fresh
// reconnect (every stream on the new Dialer starts nonexistent) or a single
// stream that died while the link stayed up (F6). It OpenStreams again and
// Rebinds the actor's membrane to the fresh arms, but never re-Spawns: the
// cell (identity + in-memory state) outlives the wire session, only the wire
// arm is replaced. A failure here is logged/retried exactly like buildOne's
// — the cell keeps running throughout, its wire arm just stays in the
// disconnect-window (fail-closed) state a moment longer.
func (r *computeRing) reopenOne(ctx context.Context, id actor.ActorID, epoch int64, d *link.Dialer) {
	rb := r.arms[id]
	if rb == nil {
		// Cannot happen in practice (live ⟹ buildOne already ran and populated
		// r.arms), but fail closed rather than lose the membrane silently.
		r.logger.Error("platform.compute.reopen_missing_arms", "actor", string(id))
		return
	}
	inc, live := r.rt.CurrentIncarnation(id)
	if !live {
		return
	}
	arms, err := d.OpenStream(ctx, id, epoch, r.dispatchFor(id, d), r.cancelFor(id))
	if err != nil {
		r.logger.Warn("platform.compute.reopen_stream_failed", "actor", string(id), "err", err)
		return
	}
	rb.Rebind(arms)
	r.watcher.install(id, inc, arms.Down)
	if !r.rt.IsLive(inc) {
		r.watcher.removeIf(id, inc)
		d.DetachStream(id)
		return
	}
	d.StartStream(id)
}

// dispatchFor builds one actor's inbound dispatch closure over link d: deliver
// into its mailbox and, for any non-Delivered outcome, report it UP d as a pure
// observation (KindDeliverResult) — the home logs it exactly as its own delivery
// tap does. Delivered = silence. The daemon holds no truth, so this is
// observation only; closure is materialised home-side from the log. d is bound
// at OpenStream time (never read off a mutable ring field), so a dispatch fired
// from a since-superseded connection's read loop can never race a reconnect.
func (r *computeRing) dispatchFor(id actor.ActorID, d *link.Dialer) func(env *message.Envelope) error {
	return func(env *message.Envelope) error {
		res, derr := r.del.Deliver([]actor.ActorID{id}, env)
		if derr != nil {
			return derr
		}
		if outcome, ok := res.Per[id]; ok && outcome != actorrt.Delivered {
			d.SendDeliverResult(id, env.ID, hostcommon.OutcomeString(outcome), "")
		}
		return nil
	}
}

// cancelFor builds one actor's cancel hook: fire the named request's reqCtx on
// this daemon's runtime. rt-scoped only, so it needs no per-connection binding.
func (r *computeRing) cancelFor(id actor.ActorID) func(requestID message.ID) {
	return func(requestID message.ID) { r.rt.CancelRequest(id, requestID) }
}
