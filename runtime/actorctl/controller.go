package actorctl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type Controller struct {
	stateMu sync.RWMutex
	phase   ControllerPhase
	actors  map[actor.ActorID]ActiveActor
	system  storespec.ActorControlRow

	gates         controlGates
	placementGate sync.Mutex
}

func newController() *Controller {
	return &Controller{phase: Bootstrapping, actors: make(map[actor.ActorID]ActiveActor)}
}

func (c *Controller) phaseValue() ControllerPhase {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.phase
}

func (c *Controller) publishBoot(system storespec.ActorControlRow, actors map[actor.ActorID]ActiveActor) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.phase != Bootstrapping {
		return ErrAlreadyStarted
	}
	c.system = cloneControlRow(system)
	c.actors = maps.Clone(actors)
	c.phase = Running
	return nil
}

func (c *Controller) close() {
	c.stateMu.Lock()
	c.phase = Closed
	c.actors = make(map[actor.ActorID]ActiveActor)
	c.stateMu.Unlock()
}

func cloneControlRow(row storespec.ActorControlRow) storespec.ActorControlRow {
	row.Config = append([]byte(nil), row.Config...)
	return row
}

func cloneActive(value ActiveActor) ActiveActor {
	value.Definition.Execution.Config = append([]byte(nil), value.Definition.Execution.Config...)
	return value
}

func (c *Controller) lookup(id actor.ActorID) (ActiveActor, bool, error) {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	switch c.phase {
	case Bootstrapping:
		return ActiveActor{}, false, ErrBootstrapping
	case Closed:
		return ActiveActor{}, false, ErrClosed
	}
	value, ok := c.actors[id]
	return cloneActive(value), ok, nil
}

func (c *Controller) list() (map[actor.ActorID]ActiveActor, storespec.ActorControlRow, error) {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	switch c.phase {
	case Bootstrapping:
		return nil, storespec.ActorControlRow{}, ErrBootstrapping
	case Closed:
		return nil, storespec.ActorControlRow{}, ErrClosed
	}
	out := make(map[actor.ActorID]ActiveActor, len(c.actors))
	for id, value := range c.actors {
		out[id] = cloneActive(value)
	}
	return out, cloneControlRow(c.system), nil
}

func (c *Controller) isCurrent(id actor.ActorID, key actorhost.AttemptKey) error {
	if id == actor.SystemActorID {
		return ErrReservedSystem
	}
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	switch c.phase {
	case Bootstrapping:
		return ErrBootstrapping
	case Closed:
		return ErrClosed
	}
	value, ok := c.actors[id]
	if !ok {
		return ErrInactive
	}
	if value.Desired.AttemptKey != key {
		return ErrStaleAttempt
	}
	return nil
}

func (c *Controller) active(id actor.ActorID) error {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	if c.phase != Running {
		if c.phase == Bootstrapping {
			return ErrBootstrapping
		}
		return ErrClosed
	}
	if id == actor.SystemActorID {
		return nil
	}
	if _, ok := c.actors[id]; !ok {
		return ErrInactive
	}
	return nil
}

func mintAttempt() (actorhost.AttemptKey, error) { return actorhost.NewAttemptKey() }

func (c *Controller) upsert(stored StoredActor, desired DesiredState) error {
	definition, err := definitionFromStored(stored)
	if err != nil {
		return err
	}
	id := stored.Row.ID
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.phase != Running {
		if c.phase == Bootstrapping {
			return ErrBootstrapping
		}
		return ErrClosed
	}
	c.actors[id] = ActiveActor{Definition: definition, Desired: desired}
	return nil
}

func (c *Controller) delete(ids []actor.ActorID) {
	c.stateMu.Lock()
	for _, id := range ids {
		delete(c.actors, id)
	}
	c.stateMu.Unlock()
}

func (c *Controller) desiredFor(domain, server actorhost.ExecutionDomain) ([]actorhost.Desired, error) {
	actors, _, err := c.list()
	if err != nil {
		return nil, err
	}
	ids := make([]actor.ActorID, 0, len(actors))
	for id := range actors {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b actor.ActorID) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	out := make([]actorhost.Desired, 0, len(ids))
	for _, id := range ids {
		value := actors[id]
		def := value.Definition
		switch def.Placement.Kind {
		case storespec.PlacementServer:
			if domain == server {
				out = append(out, actorhost.BodyDesired{
					ActorID: id, AttemptKey: value.Desired.AttemptKey, ExecutionSpec: def.Execution,
				})
			}
		case storespec.PlacementDaemon:
			peer := actorhost.ExecutionDomain(def.Placement.Host)
			if domain == server {
				out = append(out, actorhost.CarrierDesired{
					ActorID: id, AttemptKey: value.Desired.AttemptKey, PeerDomain: peer,
				})
			} else if domain == peer {
				out = append(out, actorhost.BodyDesired{
					ActorID: id, AttemptKey: value.Desired.AttemptKey, ExecutionSpec: def.Execution,
				})
			}
		default:
			return nil, storespec.ErrInvalidPlacement
		}
	}
	return out, nil
}

type ChannelActors struct {
	store        Store
	effects      Effects
	serverDomain actorhost.ExecutionDomain
	host         *actorhost.HostSupervisor
	controller   *Controller
	owner        commandOwner
	kernel       systemKernel
	now          func() time.Time
	logger       *slog.Logger

	channelID           channel.ID
	penMinter           PenMinter
	accessMinter        AccessMinter
	stateResolver       StateResolver
	scheduleMinter      ScheduleMinter
	serverDesiredCtx    context.Context
	serverDesiredCancel context.CancelFunc
	serverDesiredWake   chan struct{}
	serverDesiredPoll   time.Duration
	serverDesiredWG     sync.WaitGroup

	startMu sync.Mutex
	started bool
}

func NewChannelActors(cfg Config) (*ChannelActors, error) {
	if cfg.Store == nil || cfg.ServerDomain == "" || cfg.BuildManagedBody == nil {
		return nil, ErrInvalidMutation
	}
	effects := cfg.Effects
	if effects == nil {
		effects = nopEffects{}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	hostCfg := cfg.ServerHost
	hostCfg.Domain = cfg.ServerDomain
	logger := hostCfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
		hostCfg.Logger = logger
	}
	var actorsFromBuilder atomic.Pointer[ChannelActors]
	hostCfg.BodyBuilder = func(input actorhost.BodyBuildInput) actorrt.Actor {
		// The owner is filled after Host construction and before any desired
		// snapshot can schedule this builder.
		self := actorsFromBuilder.Load()
		if self == nil {
			return nil
		}
		caps, err := self.buildManagedCaps(input)
		if err != nil {
			logger.Warn("actorctl.managed_caps_failed", "actor", input.ActorID, "err", err)
			return nil
		}
		return cfg.BuildManagedBody(ManagedBodyInput{
			ActorID:       input.ActorID,
			ExecutionSpec: input.ExecutionSpec,
		}, caps)
	}
	host, err := actorhost.New(hostCfg)
	if err != nil {
		return nil, err
	}
	serverDesiredParent := hostCfg.Parent
	if serverDesiredParent == nil {
		serverDesiredParent = context.Background()
	}
	serverDesiredCtx, serverDesiredCancel := context.WithCancel(serverDesiredParent)
	serverDesiredPoll := hostCfg.PollInterval
	if serverDesiredPoll <= 0 {
		serverDesiredPoll = 100 * time.Millisecond
	}
	actors := &ChannelActors{
		store:               cfg.Store,
		effects:             effects,
		serverDomain:        cfg.ServerDomain,
		host:                host,
		controller:          newController(),
		now:                 now,
		logger:              logger,
		channelID:           cfg.ChannelID,
		penMinter:           cfg.PenMinter,
		accessMinter:        cfg.AccessMinter,
		stateResolver:       cfg.StateResolver,
		scheduleMinter:      cfg.ScheduleMinter,
		serverDesiredCtx:    serverDesiredCtx,
		serverDesiredCancel: serverDesiredCancel,
		serverDesiredWake:   make(chan struct{}, 1),
		serverDesiredPoll:   serverDesiredPoll,
	}
	actors.kernel.owner = actors
	actorsFromBuilder.Store(actors)
	return actors, nil
}

func (a *ChannelActors) Start(ctx context.Context, unit *actorrt.Unit) error {
	a.startMu.Lock()
	defer a.startMu.Unlock()
	if a.started {
		return ErrAlreadyStarted
	}
	if unit == nil || unit.Self().ID() != actor.SystemActorID ||
		unit.Stat().Kind != actor.KindSystem || unit.State() != actorrt.UnitPrepared {
		return ErrInvalidKernel
	}
	rows, err := a.store.ListDeclaredActive(ctx)
	if err != nil {
		return err
	}
	var system storespec.ActorControlRow
	systemCount := 0
	managed := make(map[actor.ActorID]ActiveActor)
	for _, row := range rows {
		if row.ID == actor.SystemActorID {
			if row.Kind != actor.KindSystem {
				return ErrInvalidKernel
			}
			system = cloneControlRow(row)
			systemCount++
			continue
		}
		def, err := definitionFromStored(StoredActor{Row: row, Origin: OriginDurable})
		if err != nil {
			return err
		}
		key, err := mintAttempt()
		if err != nil {
			return err
		}
		managed[row.ID] = ActiveActor{
			Definition: def,
			Desired:    DesiredState{AttemptKey: key},
		}
	}
	if systemCount != 1 {
		return ErrInvalidKernel
	}
	if err := unit.InstallEventSink(&a.kernel); err != nil {
		return err
	}
	a.kernel.unit = unit
	if err := unit.Start(); err != nil {
		unit.Stop()
		<-unit.Done()
		return err
	}
	if !unit.IsAlive() {
		unit.Stop()
		<-unit.Done()
		return ErrInvalidKernel
	}
	if err := a.controller.publishBoot(system, managed); err != nil {
		unit.Stop()
		<-unit.Done()
		return err
	}
	if err := a.readServerDesired(); err != nil {
		a.controller.close()
		unit.Stop()
		<-unit.Done()
		return err
	}
	a.serverDesiredWG.Add(1)
	go a.runServerDesired()
	a.kernel.startWatch()
	a.started = true
	return nil
}

// readServerDesired is the Server-side counterpart of daemon PullPlan: one
// owner reads the current complete level from its source and offers it to the
// Host. Commands never manufacture or carry Host snapshots.
func (a *ChannelActors) readServerDesired() error {
	desired, err := a.controller.desiredFor(a.serverDomain, a.serverDomain)
	if err != nil {
		return err
	}
	return a.host.AcceptFullDesired(desired)
}

func (a *ChannelActors) runServerDesired() {
	defer a.serverDesiredWG.Done()
	ticker := time.NewTicker(a.serverDesiredPoll)
	defer ticker.Stop()
	for {
		select {
		case <-a.serverDesiredCtx.Done():
			return
		case <-a.serverDesiredWake:
		case <-ticker.C:
		}
		if err := a.readServerDesired(); err != nil &&
			a.serverDesiredCtx.Err() == nil &&
			!errors.Is(err, ErrClosed) &&
			!errors.Is(err, actorhost.ErrHostClosed) {
			a.logger.Warn("actorctl.server_desired_read_failed", "err", err)
		}
	}
}

func (a *ChannelActors) pokeServerDesired() {
	select {
	case a.serverDesiredWake <- struct{}{}:
	default:
	}
}

func (a *ChannelActors) wakeDefinition(def ActorDefinition) {
	if def.Placement.Kind == storespec.PlacementDaemon {
		domain := actorhost.ExecutionDomain(def.Placement.Host)
		a.effects.PlanPoke(domain)
	}
}

func (a *ChannelActors) PlanFor(domain actorhost.ExecutionDomain) ([]actorhost.Desired, error) {
	return a.controller.desiredFor(domain, a.serverDomain)
}

func (a *ChannelActors) AuthorActive(id actor.ActorID) error {
	return a.controller.active(id)
}

// Deliver routes SystemActorID to the one-shot kernel and every managed ActorID
// through the Server Host current endpoint.
func (a *ChannelActors) Deliver(id actor.ActorID, env *message.Envelope) error {
	if a == nil || env == nil {
		return actorhost.ErrNotHosted
	}
	if id == actor.SystemActorID {
		return a.kernel.deliver(env)
	}
	return a.host.Deliver(id, env)
}

func (a *ChannelActors) CancelRequest(id actor.ActorID, requestID message.ID) {
	if a == nil {
		return
	}
	if id == actor.SystemActorID {
		a.kernel.cancelRequest(requestID)
		return
	}
	a.host.CancelRequest(id, requestID)
}

// Stat is the narrow, read-only local execution observation surface.
func (a *ChannelActors) Stat(id actor.ActorID) (actorrt.UnitStat, bool) {
	if a == nil {
		return actorrt.UnitStat{}, false
	}
	if id == actor.SystemActorID {
		return a.kernel.stat()
	}
	snapshot, ok := a.host.Inspect(id)
	if !ok {
		return actorrt.UnitStat{}, false
	}
	if snapshot.Actual == actorhost.ActualBody && snapshot.Unit != nil && snapshot.Unit.IsAlive() {
		return snapshot.Unit.Stat(), true
	}
	if snapshot.Actual == actorhost.ActualRoute && snapshot.Binding != nil {
		value, found, err := a.controller.lookup(id)
		if err == nil && found {
			return actorrt.UnitStat{StartedAt: snapshot.StartedAt, Kind: value.Definition.Kind}, true
		}
	}
	return actorrt.UnitStat{}, false
}

func (a *ChannelActors) Incarnation(id actor.ActorID) (actorrt.Incarnation, bool) {
	if a == nil {
		return actorrt.Incarnation{}, false
	}
	if id == actor.SystemActorID {
		return a.kernel.incarnation()
	}
	snapshot, ok := a.host.Inspect(id)
	if !ok || snapshot.Actual != actorhost.ActualBody || snapshot.Unit == nil || !snapshot.Unit.IsAlive() {
		return actorrt.Incarnation{}, false
	}
	return snapshot.Unit.Self(), true
}

// Attempt returns the exact remote execution coordinate when id is currently
// represented by a route. Local bodies use Incarnation instead.
func (a *ChannelActors) Attempt(id actor.ActorID) (actorhost.AttemptKey, bool) {
	if a == nil || id == actor.SystemActorID {
		return "", false
	}
	snapshot, ok := a.host.Inspect(id)
	if !ok || snapshot.Actual != actorhost.ActualRoute || snapshot.Binding == nil {
		return "", false
	}
	return snapshot.Attempt, true
}

func (a *ChannelActors) HostedIDs() []actor.ActorID {
	rows, err := a.listActiveRows()
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

func (a *ChannelActors) Lookup(id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	if id == actor.SystemActorID {
		_, system, err := a.controller.list()
		if err != nil {
			return storespec.ActorControlRow{}, false, err
		}
		return system, true, nil
	}
	value, ok, err := a.controller.lookup(id)
	if err != nil || !ok {
		return storespec.ActorControlRow{}, ok, err
	}
	return rowFromActive(id, value), true, nil
}

func (a *ChannelActors) listActiveRows() ([]storespec.ActorControlRow, error) {
	values, system, err := a.controller.list()
	if err != nil {
		return nil, err
	}
	out := make([]storespec.ActorControlRow, 0, len(values)+1)
	out = append(out, system)
	for id, value := range values {
		out = append(out, rowFromActive(id, value))
	}
	slices.SortFunc(out, func(left, right storespec.ActorControlRow) int {
		if left.ID < right.ID {
			return -1
		}
		if left.ID > right.ID {
			return 1
		}
		return 0
	})
	return out, nil
}

func (a *ChannelActors) Quiesce(ctx context.Context) error {
	return a.owner.quiesce(ctx)
}

func (a *ChannelActors) Close(ctx context.Context) error {
	var faults []error
	if err := a.Quiesce(ctx); err != nil {
		return err
	}
	a.serverDesiredCancel()
	a.serverDesiredWG.Wait()
	if err := a.host.Close(ctx); err != nil {
		faults = append(faults, err)
	}
	a.controller.close()
	if err := a.kernel.close(ctx); err != nil {
		faults = append(faults, err)
	}
	return errors.Join(faults...)
}

func (a *ChannelActors) failStop(cause error) {
	a.serverDesiredCancel()
	a.controller.close()
	a.effects.Fatal(cause)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.host.Close(ctx)
	}()
}

func freshChildID(parent actor.ActorID, hint string) actor.ActorID {
	if hint == "" {
		hint = "child"
	}
	return actor.ActorID(fmt.Sprintf("%s/%s-%s", parent, hint, uuid.NewString()))
}

func normalizeFork(spec actorcaps.ForkSpec, parent ActorDefinition) (actorcaps.ForkSpec, storespec.Placement, error) {
	if _, ok := actor.ParseKind(string(spec.Kind)); !ok || spec.Kind == actor.KindSystem || spec.Class == "" {
		return actorcaps.ForkSpec{}, storespec.Placement{}, ErrForkInvalid
	}
	if spec.NameHint == "" {
		spec.NameHint = "child"
	}
	if len(spec.NameHint) > 64 {
		return actorcaps.ForkSpec{}, storespec.Placement{}, ErrForkInvalid
	}
	placement := parent.Placement
	if spec.Placement != nil {
		switch spec.Placement.Kind {
		case "server":
			placement = storespec.NewServerPlacement()
		case "daemon":
			var err error
			placement, err = storespec.NewDaemonPlacement(spec.Placement.DesiredHost)
			if err != nil {
				return actorcaps.ForkSpec{}, storespec.Placement{}, err
			}
		default:
			return actorcaps.ForkSpec{}, storespec.Placement{}, ErrForkInvalid
		}
	}
	return spec, placement, nil
}

var _ Commands = (*ChannelActors)(nil)
