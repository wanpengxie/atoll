package home

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// actorSystem is a Platform workflow facade. It owns no runtime invariant:
// Controller, Host and SystemKernel remain independent fields on Home.
//
// The system kernel appears here only in its native roles — physical routing
// (its body lives in the kernel) and addressability (is the kernel running).
// It is never a member: it has no record, no admission and no lifecycle.
type actorSystem struct {
	home         *Home
	serverDomain actorhost.ExecutionDomain
	logger       *slog.Logger

	desiredCtx    context.Context
	desiredCancel context.CancelFunc
	desiredWake   chan struct{}
	desiredPoll   time.Duration
	desiredWG     sync.WaitGroup
}

func newActorSystem(h *Home, logger *slog.Logger) *actorSystem {
	ctx, cancel := context.WithCancel(context.Background())
	return &actorSystem{
		home:          h,
		serverDomain:  actorhost.ExecutionDomain("server"),
		logger:        logger,
		desiredCtx:    ctx,
		desiredCancel: cancel,
		desiredWake:   make(chan struct{}, 1),
		desiredPoll:   100 * time.Millisecond,
	}
}

func (a *actorSystem) start(ctx context.Context, systemUnit *actorrt.Unit) error {
	if err := a.home.controller.Start(ctx); err != nil {
		return err
	}
	if err := a.home.systemKernel.Start(systemUnit); err != nil {
		a.home.controller.Close()
		return err
	}
	if err := a.readServerDesired(); err != nil {
		a.home.controller.Close()
		return err
	}
	a.desiredWG.Add(1)
	go a.runServerDesired()
	go a.watchSystemFailure()
	return nil
}

func (a *actorSystem) watchSystemFailure() {
	select {
	case cause, ok := <-a.home.systemKernel.Failed():
		if ok && cause != nil && !a.home.closed.Load() {
			a.logger.Error("platform.home.system_kernel_failed", "err", cause)
			go func() { _ = a.home.closeInternal("system_kernel_failed") }()
		}
	case <-a.desiredCtx.Done():
	}
}

func (a *actorSystem) readServerDesired() error {
	desired, err := a.home.controller.DesiredFor(a.serverDomain, a.serverDomain)
	if err != nil {
		return err
	}
	return a.home.serverHost.AcceptFullDesired(desired)
}

func (a *actorSystem) runServerDesired() {
	defer a.desiredWG.Done()
	ticker := time.NewTicker(a.desiredPoll)
	defer ticker.Stop()
	for {
		select {
		case <-a.desiredCtx.Done():
			return
		case <-a.desiredWake:
		case <-ticker.C:
		}
		if err := a.readServerDesired(); err != nil &&
			a.desiredCtx.Err() == nil &&
			!errors.Is(err, actorctl.ErrClosed) &&
			!errors.Is(err, actorhost.ErrHostClosed) {
			a.logger.Warn("platform.server_desired_read_failed", "err", err)
		}
	}
}

func (a *actorSystem) pokeServerDesired() {
	select {
	case a.desiredWake <- struct{}{}:
	default:
	}
}

// finishTransition is the composition-root tail of one committed command. It
// consumes facts only: ended ids and reconcile hints.
func finishTransition[T any](
	a *actorSystem,
	transition actorctl.Transition[T],
	err error,
) (T, error) {
	if err != nil {
		return transition.Result, err
	}
	effects := homeActorEffects{home: a.home}
	if len(transition.Ended) != 0 {
		effects.ActorsEnded(transition.Ended)
	}
	if transition.Reconcile.Server {
		a.pokeServerDesired()
	}
	for _, peer := range transition.Reconcile.Peers {
		a.pokeServerDesired()
		effects.PlanPoke(peer)
	}
	if transition.Reconcile.Server || len(transition.Reconcile.Peers) != 0 {
		a.home.pokeReconcile()
	}
	return transition.Result, nil
}

func (a *actorSystem) Admit(ctx context.Context, request actorctl.AdmitRequest) (actorctl.AdmitResult, error) {
	t, err := a.home.controller.Admit(ctx, request)
	result, err := finishTransition(a, t, err)
	if err == nil && request.Principal != "" {
		a.home.notifyMembership(request.Principal)
	}
	return result, err
}

func (a *actorSystem) Introduce(ctx context.Context, request actorctl.IntroduceRequest) (channel.IntroduceResult, error) {
	t, err := a.home.controller.Introduce(ctx, request)
	return finishTransition(a, t, err)
}

func (a *actorSystem) Fork(ctx context.Context, request actorctl.ForkRequest) (actorctl.ForkResult, error) {
	t, err := a.home.controller.Fork(ctx, request)
	return finishTransition(a, t, err)
}

func (a *actorSystem) Restart(ctx context.Context, request actorctl.RestartRequest) error {
	t, err := a.home.controller.Restart(ctx, request)
	_, err = finishTransition(a, t, err)
	return err
}

func (a *actorSystem) ApplyDeclaration(ctx context.Context, change actorctl.DeclarationChange) error {
	t, err := a.home.controller.ApplyDeclaration(ctx, change)
	_, err = finishTransition(a, t, err)
	return err
}

func (a *actorSystem) End(ctx context.Context, request actorctl.EndRequest) (actorctl.EndResult, error) {
	principals := a.principalsOf([]actor.ActorID{request.Target})
	t, err := a.home.controller.End(ctx, request)
	result, err := finishTransition(a, t, err)
	if err == nil {
		a.home.announceEnded(ctx, t.Ended, request.Reason, endedBy(request.CallerActorID))
		a.home.notifyMembership(principals...)
	}
	return result, err
}

func (a *actorSystem) Remove(ctx context.Context, request actorctl.RemoveRequest) (channel.RemoveResult, error) {
	principals := a.principalsOf([]actor.ActorID{request.Target})
	t, err := a.home.controller.Remove(ctx, request)
	result, err := finishTransition(a, t, err)
	if err == nil {
		a.home.announceEnded(ctx, t.Ended, "system_remove", actor.SystemActorID)
		a.home.notifyMembership(principals...)
	}
	return result, err
}

func endedBy(caller actor.ActorID) actor.ActorID {
	if caller == "" {
		return actor.SystemActorID
	}
	return caller
}

// principalsOf reads the login principals of ids before they are ended, so the
// membership-change tail can name them afterwards.
func (a *actorSystem) principalsOf(ids []actor.ActorID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		facts, active, err := a.home.controller.ActorFacts(context.Background(), id)
		if err == nil && active && facts.Principal != "" {
			out = append(out, facts.Principal)
		}
	}
	return out
}

func (a *actorSystem) PlanFor(domain actorhost.ExecutionDomain) ([]actorhost.Desired, error) {
	return a.home.controller.DesiredFor(domain, a.serverDomain)
}

func (a *actorSystem) AuthorizeAttach(id actor.ActorID, key actorhost.AttemptKey, peer actorhost.ExecutionDomain) error {
	return a.home.controller.AuthorizeAttach(id, key, peer)
}

func (a *actorSystem) AttachBinding(id actor.ActorID, key actorhost.AttemptKey, peer actorhost.ExecutionDomain, binding actorhost.Binding) error {
	if err := a.home.controller.AuthorizeAttach(id, key, peer); err != nil {
		return err
	}
	return a.home.serverHost.Attach(id, key, binding)
}

func (a *actorSystem) BindingDown(id actor.ActorID, binding actorhost.Binding) {
	a.home.serverHost.BindingDown(id, binding)
}

// ---------------------------------------------------------------------------
// physical routing: the kernel's body lives in the kernel (§2.6 retained C)
// ---------------------------------------------------------------------------

func (a *actorSystem) Deliver(id actor.ActorID, env *message.Envelope) error {
	if id == actor.SystemActorID {
		return a.home.systemKernel.Deliver(env)
	}
	return a.home.serverHost.Deliver(id, env)
}

func (a *actorSystem) CancelRequest(id actor.ActorID, requestID message.ID) {
	if id == actor.SystemActorID {
		a.home.systemKernel.CancelRequest(requestID)
		return
	}
	a.home.serverHost.CancelRequest(id, requestID)
}

func (a *actorSystem) Stat(id actor.ActorID) (actorrt.UnitStat, bool) {
	if id == actor.SystemActorID {
		return a.home.systemKernel.Stat()
	}
	snapshot, ok := a.home.serverHost.Inspect(id)
	if !ok {
		return actorrt.UnitStat{}, false
	}
	if snapshot.Actual == actorhost.ActualBody && snapshot.Unit != nil && snapshot.Unit.IsAlive() {
		return snapshot.Unit.Stat(), true
	}
	if snapshot.Actual == actorhost.ActualRoute && snapshot.Binding.Valid() {
		if kind, found := a.home.controller.ActiveKind(id); found {
			return actorrt.UnitStat{StartedAt: snapshot.StartedAt, Kind: kind}, true
		}
	}
	return actorrt.UnitStat{}, false
}

func (a *actorSystem) Incarnation(id actor.ActorID) (actorrt.Incarnation, bool) {
	if id == actor.SystemActorID {
		return a.home.systemKernel.Incarnation()
	}
	snapshot, ok := a.home.serverHost.Inspect(id)
	if !ok || snapshot.Actual != actorhost.ActualBody || snapshot.Unit == nil || !snapshot.Unit.IsAlive() {
		return actorrt.Incarnation{}, false
	}
	return snapshot.Unit.Self(), true
}

func (a *actorSystem) Attempt(id actor.ActorID) (actorhost.AttemptKey, bool) {
	if id == actor.SystemActorID {
		return "", false
	}
	snapshot, ok := a.home.serverHost.Inspect(id)
	if !ok || snapshot.Actual != actorhost.ActualRoute || !snapshot.Binding.Valid() {
		return "", false
	}
	return snapshot.Attempt, true
}

func (a *actorSystem) HostedIDs() []actor.ActorID {
	identities, err := a.home.controller.ActiveIdentities()
	if err != nil {
		return nil
	}
	out := make([]actor.ActorID, 0, len(identities))
	for _, identity := range identities {
		if _, live := a.Stat(identity.ID); live {
			out = append(out, identity.ID)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// projections
// ---------------------------------------------------------------------------

func (a *actorSystem) ActorFacts(
	ctx context.Context,
	id actor.ActorID,
) (storespec.ActorFacts, bool, error) {
	return a.home.controller.ActorFacts(ctx, id)
}

func (a *actorSystem) ActiveIdentities() ([]storespec.ActiveIdentity, error) {
	return a.home.controller.ActiveIdentities()
}

func (a *actorSystem) ResolvePrincipal(principal string) (actor.ActorID, bool, error) {
	return a.home.controller.ResolvePrincipal(principal)
}

func (a *actorSystem) DeclaredInstances(declID string) ([]actor.ActorID, error) {
	return a.home.controller.DeclaredInstances(declID)
}

func (a *actorSystem) AdmitIdentity(
	ctx context.Context,
	id actor.ActorID,
) (storespec.IdentityAdmission, bool, error) {
	return a.home.controller.AdmitIdentity(ctx, id)
}

// IsActive is the addressability verdict of the routing organ, not a registry
// query: for the kernel it asks "is the kernel running", for a member it asks
// the Controller's roster.
func (a *actorSystem) IsActive(ctx context.Context, id actor.ActorID) (bool, error) {
	if id == actor.SystemActorID {
		return a.home.systemKernel.IsRunning(), nil
	}
	return a.home.controller.IsActive(ctx, id)
}

// ResourceActorFacts is the ONE translation point between the Controller's
// basis and the resource door's final facts. Owner is derived from the
// immutable genesis pointer (a human whose principal equals
// genesis.OwnerPrincipal) — the value ledger keeps no second owner account, and
// the derivation basis rides the SAME ledger snapshot as Active: one Controller
// read, no stitched projection. The basis stops here; accessdoor never sees it.
func (a *actorSystem) ResourceActorFacts(
	ctx context.Context,
	id actor.ActorID,
) (storespec.ResourceActorFacts, error) {
	basis, err := a.home.controller.ResourceActorBasis(ctx, id)
	if err != nil || !basis.Active {
		return storespec.ResourceActorFacts{}, err
	}
	return storespec.ResourceActorFacts{
		Active: true,
		Owner: a.home.isOwner(storespec.ActorFacts{
			Kind: basis.Kind, Principal: basis.Principal,
		}),
		PreferredStorageHost: basis.PreferredStorageHost,
	}, nil
}

func (a *actorSystem) Quiesce(ctx context.Context) error {
	return a.home.controller.Quiesce(ctx)
}

func (a *actorSystem) close(ctx context.Context) error {
	a.desiredCancel()
	a.desiredWG.Wait()
	var faults []error
	if err := a.home.serverHost.Close(ctx); err != nil {
		faults = append(faults, err)
	}
	a.home.controller.Close()
	if err := a.home.systemKernel.Close(ctx); err != nil {
		faults = append(faults, err)
	}
	return errors.Join(faults...)
}

var _ storespec.ChannelAuthority = (*actorSystem)(nil)
var _ storespec.CollaborationAuthority = (*actorSystem)(nil)
