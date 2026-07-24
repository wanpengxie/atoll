package home

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// actorSystem is a Platform workflow facade. It owns no runtime invariant:
// Controller, Host and SystemKernel remain independent fields on Home.
type actorSystem struct {
	home         *Home
	serverDomain actorhost.ExecutionDomain
	systemRow    storespec.ActorControlRow
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
	boot, err := a.home.controller.Start(ctx)
	if err != nil {
		return err
	}
	a.systemRow = cloneSystemRow(boot.System)
	if err := a.readServerDesired(); err != nil {
		a.home.controller.Close()
		return err
	}
	if err := a.home.systemKernel.Start(systemUnit); err != nil {
		a.home.controller.Close()
		return err
	}
	a.desiredWG.Add(1)
	go a.runServerDesired()
	go a.watchSystemFailure()
	return nil
}

func cloneSystemRow(row storespec.ActorControlRow) storespec.ActorControlRow {
	row.Config = append([]byte(nil), row.Config...)
	return row
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

func (a *actorSystem) wakeDefinition(def actorctl.ActorDefinition) {
	if def.Placement.Kind == storespec.PlacementDaemon {
		homeActorEffects{home: a.home}.PlanPoke(actorhost.ExecutionDomain(def.Placement.Host))
	}
}

func finishTransition[T any](
	a *actorSystem,
	transition actorctl.Transition[T],
	err error,
) (T, error) {
	if err != nil {
		return transition.Result, err
	}
	effects := homeActorEffects{home: a.home}
	for _, id := range transition.Born {
		if err := effects.RunActorBorn(id); err != nil {
			go func() { _ = a.home.closeInternal("actor_birth_projection_failed") }()
			return transition.Result, err
		}
	}
	if len(transition.Ended) != 0 {
		effects.RunActorsEnded(transition.Ended)
	}
	if len(transition.Wake) != 0 {
		a.pokeServerDesired()
		for _, def := range transition.Wake {
			a.wakeDefinition(def)
		}
	}
	effects.ApplyPostCommit(transition.Effects)
	return transition.Result, nil
}

func (a *actorSystem) Admit(ctx context.Context, request actorctl.AdmitRequest) (actorctl.AdmitResult, error) {
	t, err := a.home.controller.Admit(ctx, request)
	return finishTransition(a, t, err)
}

func (a *actorSystem) Introduce(ctx context.Context, request actorctl.IntroduceRequest) (actorctl.IntroduceResult, error) {
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

func (a *actorSystem) AttachDaemon(ctx context.Context, request actorctl.AttachDaemonRequest) (actorctl.AttachDaemonResult, error) {
	t, err := a.home.controller.AttachDaemon(ctx, request)
	result, err := finishTransition(a, t, err)
	if err == nil {
		homeActorEffects{home: a.home}.PlanPoke(actorhost.ExecutionDomain(request.DaemonID))
	}
	return result, err
}

func (a *actorSystem) End(ctx context.Context, request actorctl.EndRequest) (actorctl.EndResult, error) {
	t, err := a.home.controller.End(ctx, request)
	return finishTransition(a, t, err)
}

func (a *actorSystem) Remove(ctx context.Context, request actorctl.RemoveRequest) (actorctl.RemoveResult, error) {
	t, err := a.home.controller.Remove(ctx, request)
	return finishTransition(a, t, err)
}

func (a *actorSystem) DetachDaemon(ctx context.Context, request actorctl.DetachDaemonRequest) (actorctl.DetachDaemonResult, error) {
	t, err := a.home.controller.DetachDaemon(ctx, request)
	result, err := finishTransition(a, t, err)
	if err == nil {
		homeActorEffects{home: a.home}.PlanPoke(actorhost.ExecutionDomain(request.DaemonID))
	}
	return result, err
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

func (a *actorSystem) RemoteFork(ctx context.Context, id actor.ActorID, key actorhost.AttemptKey, requestID message.ID, spec actorcaps.ForkSpec) (actor.ActorID, error) {
	result, err := a.Fork(ctx, actorctl.ForkRequest{
		CallerActorID: id, CallerAttempt: key, RequestID: requestID, Spec: spec,
	})
	return result.ChildActorID, err
}

func (a *actorSystem) RemoteEndSelf(ctx context.Context, id actor.ActorID, key actorhost.AttemptKey, request actorcaps.EndSelfRequest) error {
	_, err := a.End(ctx, actorctl.EndRequest{
		CallerActorID: id, CallerAttempt: key, Target: id, Reason: request.Reason,
	})
	return err
}

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
		value, found, err := a.home.controller.Lookup(id)
		if err == nil && found {
			return actorrt.UnitStat{StartedAt: snapshot.StartedAt, Kind: value.Definition.Kind}, true
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
	rows, err := a.ListActive(context.Background())
	if err != nil {
		return nil
	}
	out := make([]actor.ActorID, 0, len(rows))
	for _, row := range rows {
		if _, live := a.Stat(row.ID); live {
			out = append(out, row.ID)
		}
	}
	return out
}

func (a *actorSystem) LookupActive(ctx context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	if id == actor.SystemActorID {
		if a.home.controller.Phase() != actorctl.Running {
			return storespec.ActorControlRow{}, false, actorctl.ErrClosed
		}
		return cloneSystemRow(a.systemRow), true, nil
	}
	return a.home.controller.LookupActive(ctx, id)
}

func (a *actorSystem) ListActive(ctx context.Context) ([]storespec.ActorControlRow, error) {
	rows, err := a.home.controller.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	rows = append(rows, cloneSystemRow(a.systemRow))
	slices.SortFunc(rows, func(left, right storespec.ActorControlRow) int {
		switch {
		case left.ID < right.ID:
			return -1
		case left.ID > right.ID:
			return 1
		default:
			return 0
		}
	})
	return rows, nil
}

func (a *actorSystem) WorldOf(ctx context.Context, id actor.ActorID) (storespec.ActorWorld, bool, error) {
	if id == actor.SystemActorID {
		return storespec.WorldDurable, true, nil
	}
	return a.home.controller.WorldOf(ctx, id)
}

func (a *actorSystem) CheckAuthor(ctx context.Context, stamp storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	_, ok, err := a.LookupActive(ctx, stamp.ID)
	if err != nil {
		return storespec.AuthorNotMember, err
	}
	if !ok {
		return storespec.AuthorNotMember, nil
	}
	return storespec.AuthorOK, nil
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

var _ actorctl.Commands = (*actorSystem)(nil)
var _ storespec.ActorAuthority = (*actorSystem)(nil)
