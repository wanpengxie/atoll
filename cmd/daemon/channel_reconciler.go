package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/wanpengxie/ActOS/adapters/framework"
	proxyfacade "github.com/wanpengxie/ActOS/adapters/framework/proxy_facade"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	runtimestore "github.com/wanpengxie/ActOS/runtime/store"
)

// channelReconciler is the per-channel Reconciler from
// channel-lifecycle-reconcile-architecture.md §3 / §6 step3.
//
// It is the SINGLE serial mutation entry for this channel's adapter wiring
// (framework facade install + scheduler.Deliverer handler registration +
// device route map). All wiring changes flow through Reconcile so the dual
// "static factory at boot + update_members one-shot Register" assembly is
// replaced by one level-triggered derivation:
//
//	desired = static compiled-in modules ∪ ListDesiredProxyMembers(facts)
//	wiring  = reconcile(desired, current live wiring)
//
// Properties required by the spec:
//   - idempotent & replayable: Reconcile derives desired from facts (the
//     projection-of-record) and current live wiring, so re-running it (boot,
//     reclaim, update_members, or the cheap fallback resync) converges to the
//     same wiring. This satisfies §9 DoD #4 (rebuild from facts alone) and
//     #5 (a lost post-commit callback is repaired by the next reconcile).
//   - serial: a single mutex serialises every Reconcile call. The channel's
//     single-ownership + fencing/epoch arbitration (lock row) already keeps
//     mutation single-writer across the process; the mutex prevents the
//     in-process concurrency the spec calls out (boot / reclaim / update_members
//     / device-lifecycle all racing the manager+deliverer+route-map). No
//     global generation is introduced (§1 推论4 / §3).
//   - durable trigger: boot/reclaim/update_members trigger it explicitly; a
//     cheap per-channel low-frequency resync goroutine (§3 / §5) covers the
//     "commit succeeded but the follow-up reconcile was lost" window.
type channelReconciler struct {
	channelID channel.ID
	mgr       *framework.Manager
	cells     *actorrt.Runtime
	registry  *runtimestore.ActorRegistry
	logger    *zerolog.Logger

	// adapter actor factory — captured so dynamically reconciled facades
	// spawn the same Manager-dispatching cell the static modules use.
	actorFor func(actorID actor.ActorID) actorrt.Actor

	// deviceRoute lets proxy facades coexist with device-relay routing:
	// (actor_id → adapter name) is consulted by the inbound device→daemon
	// callback. Shared with the wiring closure; guarded by deviceRouteMu.
	deviceRouteMu *sync.RWMutex
	deviceRoute   map[actor.ActorID]string

	mu sync.Mutex
	// wired tracks every actor this reconciler has installed + registered —
	// both the static compiled-in modules (wired on the first pass) and the
	// fact-derived proxy facades.
	wired map[actor.ActorID]struct{}
	// staticModules are the channel-template compiled-in modules. They are
	// part of the reconciler's desired set on every pass (never collapsed),
	// installed + registered by the first Reconcile rather than by the
	// composition root. This is what collapses the previous dual assembly
	// (static factory Install at boot + fact-derived Register) into one
	// level-triggered derivation.
	staticModules []adapter.Module
	// static records the compiled-in modules' actor ids — never torn down by
	// reconcile (they are part of the channel template, not fact-derived).
	static map[actor.ActorID]struct{}
}

func newChannelReconciler(
	channelID channel.ID,
	mgr *framework.Manager,
	cells *actorrt.Runtime,
	registry *runtimestore.ActorRegistry,
	logger *zerolog.Logger,
	actorFor func(actorID actor.ActorID) actorrt.Actor,
	deviceRouteMu *sync.RWMutex,
	deviceRoute map[actor.ActorID]string,
	staticModules []adapter.Module,
) *channelReconciler {
	r := &channelReconciler{
		channelID:     channelID,
		mgr:           mgr,
		cells:         cells,
		registry:      registry,
		logger:        logger,
		actorFor:      actorFor,
		deviceRouteMu: deviceRouteMu,
		deviceRoute:   deviceRoute,
		wired:         make(map[actor.ActorID]struct{}),
		staticModules: staticModules,
		static:        make(map[actor.ActorID]struct{}),
	}
	for _, mod := range staticModules {
		r.static[mod.Declares().ActorID] = struct{}{}
	}
	return r
}

// Reconcile recomputes proxy facade wiring from the channel's facts and the
// current live wiring, installing newly-desired facades and collapsing
// reachability for surplus ones. Idempotent: a no-op when wiring already
// matches desired. Holds the serial mutation lock for the whole pass.
func (r *channelReconciler) Reconcile(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Static-wire pass: the channel-template compiled-in modules are part of
	// the desired set on every pass. The first Reconcile installs +
	// registers them (the composition root no longer calls mgr.Install /
	// Deliverer.Register directly); later passes short-circuit via r.wired.
	for _, mod := range r.staticModules {
		if err := r.wireStaticModule(ctx, mod); err != nil {
			return err
		}
	}

	desired, err := r.registry.ListDesiredProxyMembers(ctx)
	if err != nil {
		return fmt.Errorf("cmd/daemon: reconcile %s list desired: %w", r.channelID, err)
	}
	desiredSet := make(map[actor.ActorID]struct{}, len(desired))
	for _, dm := range desired {
		desiredSet[dm.ID] = struct{}{}
	}

	// Wire-up pass: install + register any desired facade not yet wired.
	for _, dm := range desired {
		if _, ok := r.wired[dm.ID]; ok {
			// Already wired by us (or a prior pass). Recover timers is
			// idempotent and cheap; skip — startReadiness/BootRecoverTimers
			// already covered install-time recovery.
			continue
		}
		if err := r.wireProxyFacade(ctx, dm); err != nil {
			// A desired proxy member whose registered fact lacks a usable
			// capability_set is an invariant violation: every proxy actor is
			// registered together with its complete capability_set fact in one
			// path (handleUpdateMembers → ApplyMemberTransitions), so a desired
			// member that cannot form a valid facade declaration means the log
			// is corrupt, not "incomplete pending repair". Fail the pass.
			return fmt.Errorf("cmd/daemon: reconcile %s proxy facade %s: incomplete capability_set on registered fact: %w",
				r.channelID, dm.ID, err)
		}
	}

	// Collapse pass: any proxy facade we wired but that is no longer in the
	// desired set has reachability removed (handler unregistered + route
	// dropped). The framework Manager has no module-teardown API, so this is
	// the available collapse: an inbound request resolves to handler-not-
	// found (retryable / terminal closure) rather than silently routing to a
	// stale facade. Static compiled-in modules are never collapsed.
	for id := range r.wired {
		if _, isStatic := r.static[id]; isStatic {
			continue
		}
		if _, stillDesired := desiredSet[id]; stillDesired {
			continue
		}
		r.collapseProxyFacade(id)
	}
	return nil
}

// wireStaticModule installs + registers one channel-template compiled-in
// module. Idempotent: short-circuits once the module's actor is in r.wired,
// so the resync goroutine and update_members-triggered passes never re-install
// it. The static modules are never collapsed (they are not fact-derived).
//
// Like wireProxyFacade, the install is gated on mgr.DeclarationForActor rather
// than on r.wired alone: if r.wired was cleared (a forced re-Reconcile to
// rebuild wiring) or a prior pass installed the module but failed before
// marking it wired (e.g. RecoverTimersForActor errored), the Manager already
// holds the module and a second mgr.Install would fail as a duplicate. Probing
// the Manager makes "clear wiring + re-Reconcile" idempotently rebuildable:
// already-installed → only recover timers + (re)register handler + (re)add
// route; never re-Install.
func (r *channelReconciler) wireStaticModule(ctx context.Context, mod adapter.Module) error {
	decl := mod.Declares()
	if _, ok := r.wired[decl.ActorID]; ok {
		return nil
	}
	if _, installed := r.mgr.DeclarationForActor(decl.ActorID); !installed {
		if err := r.mgr.Install(ctx, []adapter.Module{mod}); err != nil {
			return fmt.Errorf("cmd/daemon: reconcile %s static install %s: %w", r.channelID, decl.ActorID, err)
		}
	}
	if err := r.mgr.RecoverTimersForActor(ctx, decl.ActorID); err != nil {
		return fmt.Errorf("cmd/daemon: reconcile %s static recover timers %s: %w", r.channelID, decl.ActorID, err)
	}
	r.cells.Spawn(decl.ActorID, r.actorFor(decl.ActorID))
	if decl.Binding == actor.BindingRuntimeInboundViaRelay {
		r.deviceRouteMu.Lock()
		r.deviceRoute[decl.ActorID] = decl.Name
		r.deviceRouteMu.Unlock()
	}
	r.wired[decl.ActorID] = struct{}{}
	if r.logger != nil {
		r.logger.Info().
			Str("event", "daemon.reconcile_static_wired").
			Str("channel_id", string(r.channelID)).
			Str("actor_id", string(decl.ActorID)).
			Int("type_count", len(decl.Types)).
			Msg("reconciler wired static module")
	}
	return nil
}

func (r *channelReconciler) wireProxyFacade(ctx context.Context, dm runtimestore.DesiredProxyMember) error {
	decl, installed := r.mgr.DeclarationForActor(dm.ID)
	if !installed {
		var err error
		decl, err = proxyfacade.DeclarationFromCapability(dm.ID, dm.CapabilitySet)
		if err != nil {
			return fmt.Errorf("cmd/daemon: reconcile %s facade declaration %s: %w", r.channelID, dm.ID, err)
		}
		mod, err := proxyfacade.New(decl)
		if err != nil {
			return fmt.Errorf("cmd/daemon: reconcile %s facade module %s: %w", r.channelID, dm.ID, err)
		}
		if err := r.mgr.Install(ctx, []adapter.Module{mod}); err != nil {
			return fmt.Errorf("cmd/daemon: reconcile %s facade install %s: %w", r.channelID, dm.ID, err)
		}
	}
	if err := r.mgr.RecoverTimersForActor(ctx, decl.ActorID); err != nil {
		return fmt.Errorf("cmd/daemon: reconcile %s facade recover timers %s: %w", r.channelID, dm.ID, err)
	}
	r.cells.Spawn(decl.ActorID, r.actorFor(decl.ActorID))
	r.deviceRouteMu.Lock()
	r.deviceRoute[decl.ActorID] = decl.Name
	r.deviceRouteMu.Unlock()
	r.wired[decl.ActorID] = struct{}{}
	if r.logger != nil {
		r.logger.Info().
			Str("event", "daemon.reconcile_facade_wired").
			Str("channel_id", string(r.channelID)).
			Str("actor_id", string(decl.ActorID)).
			Int("type_count", len(decl.Types)).
			Msg("reconciler wired proxy facade actor")
	}
	return nil
}

func (r *channelReconciler) collapseProxyFacade(id actor.ActorID) {
	// Despawn stops + removes the facade's cell — inbound envelopes to id
	// are no longer routed to a stale facade. A request in flight to the
	// despawned actor is closed by the substrate death signal
	// (receiver_unavailable), not a global fallback (construction-spec §3.4).
	r.cells.Despawn(id)
	r.deviceRouteMu.Lock()
	delete(r.deviceRoute, id)
	r.deviceRouteMu.Unlock()
	delete(r.wired, id)
	if r.logger != nil {
		r.logger.Info().
			Str("event", "daemon.reconcile_facade_collapsed").
			Str("channel_id", string(r.channelID)).
			Str("actor_id", string(id)).
			Msg("reconciler collapsed surplus proxy facade reachability")
	}
}

// runResync drives the cheap per-channel durable fallback (§3 / §5): a
// low-frequency level-triggered resync that repairs any wiring whose
// follow-up reconcile was lost (commit succeeded, callback failed/crashed).
// It is NOT redundant complexity — it is the durability tail of the
// "fact appended ⇒ must reconcile" guarantee. Exits when ctx is cancelled
// (channel teardown).
func (r *channelReconciler) runResync(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Reconcile(ctx); err != nil {
				if r.logger != nil {
					r.logger.Warn().Err(err).
						Str("event", "daemon.reconcile_resync_failed").
						Str("channel_id", string(r.channelID)).
						Msg("per-channel reconcile resync pass failed")
				}
			}
		}
	}
}

// reconcileResyncInterval is the low-frequency durable fallback period. Kept
// cheap (a single fact-replay query + map diff per channel). Exposed as a var
// so tests can shorten it.
var reconcileResyncInterval = 60 * time.Second
