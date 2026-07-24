package actorctl

import (
	"context"
	"maps"
	"slices"
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Controller is the sole owner of the managed actor value ledger.
//
// It deliberately owns no Host, System kernel, capability minter, actor
// factory, desired loop, routing surface, or Platform callback. Every mutation
// enters through a typed method below; the control gate, Store commit and
// in-memory publication stay inside this organ.
type Controller struct {
	store Store
	owner commandOwner

	stateMu sync.RWMutex
	phase   ControllerPhase
	actors  map[actor.ActorID]ActiveActor

	gates         controlGates
	placementGate sync.Mutex
}

// New constructs a bootstrapping managed actor Controller.
func New(store Store) (*Controller, error) {
	if store == nil {
		return nil, ErrInvalidMutation
	}
	return &Controller{
		store:  store,
		phase:  Bootstrapping,
		actors: make(map[actor.ActorID]ActiveActor),
	}, nil
}

// Bootstrap is the immutable non-managed value discovered while booting the
// Controller Store. The Controller validates it but does not retain it: the
// System kernel is a separate lifecycle root owned by the Platform.
type Bootstrap struct {
	System storespec.ActorControlRow
}

// Start publishes the complete managed value-ledger image exactly once.
func (c *Controller) Start(ctx context.Context) (Bootstrap, error) {
	if c == nil {
		return Bootstrap{}, ErrClosed
	}
	rows, err := c.store.RestoreActive(ctx)
	if err != nil {
		return Bootstrap{}, err
	}
	var system storespec.ActorControlRow
	systemCount := 0
	managed := make(map[actor.ActorID]ActiveActor)
	for _, row := range rows {
		if row.ID == actor.SystemActorID {
			if row.Kind != actor.KindSystem {
				return Bootstrap{}, ErrInvalidKernel
			}
			system = cloneControlRow(row)
			systemCount++
			continue
		}
		def, err := definitionFromStored(StoredActor{Row: row})
		if err != nil {
			return Bootstrap{}, err
		}
		key, err := mintAttempt()
		if err != nil {
			return Bootstrap{}, err
		}
		managed[row.ID] = ActiveActor{
			Definition: def,
			Desired:    DesiredState{AttemptKey: key},
		}
	}
	if systemCount != 1 {
		return Bootstrap{}, ErrInvalidKernel
	}

	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.phase != Bootstrapping {
		if c.phase == Closed {
			return Bootstrap{}, ErrClosed
		}
		return Bootstrap{}, ErrAlreadyStarted
	}
	c.actors = maps.Clone(managed)
	c.phase = Running
	return Bootstrap{System: system}, nil
}

// Quiesce seals command admission and joins all commands already admitted.
func (c *Controller) Quiesce(ctx context.Context) error {
	if c == nil {
		return ErrClosed
	}
	return c.owner.quiesce(ctx)
}

// Close discards the process-local managed projection. Callers must Quiesce
// before closing the Controller.
func (c *Controller) Close() {
	if c == nil {
		return
	}
	c.stateMu.Lock()
	c.phase = Closed
	c.actors = make(map[actor.ActorID]ActiveActor)
	c.stateMu.Unlock()
}

func (c *Controller) beginCommand() (func(), error) {
	if c == nil {
		return nil, ErrClosed
	}
	return c.owner.begin()
}

// Phase reports the current Controller lifecycle phase.
func (c *Controller) Phase() ControllerPhase {
	if c == nil {
		return Closed
	}
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.phase
}

func cloneControlRow(row storespec.ActorControlRow) storespec.ActorControlRow {
	row.Config = append([]byte(nil), row.Config...)
	return row
}

func cloneActive(value ActiveActor) ActiveActor {
	value.Definition.Execution.Config = append([]byte(nil), value.Definition.Execution.Config...)
	return value
}

// Lookup returns one coherent managed value-ledger entry.
func (c *Controller) Lookup(id actor.ActorID) (ActiveActor, bool, error) {
	if id == actor.SystemActorID {
		return ActiveActor{}, false, ErrReservedSystem
	}
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

// Snapshot returns the coherent managed value-ledger level.
func (c *Controller) Snapshot() (map[actor.ActorID]ActiveActor, error) {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	switch c.phase {
	case Bootstrapping:
		return nil, ErrBootstrapping
	case Closed:
		return nil, ErrClosed
	}
	out := make(map[actor.ActorID]ActiveActor, len(c.actors))
	for id, value := range c.actors {
		out[id] = cloneActive(value)
	}
	return out, nil
}

// checkCurrentSnapshot is a read-only sliding-window verdict. Lifecycle
// mutations never use it outside their Controller-owned control gate.
func (c *Controller) checkCurrentSnapshot(id actor.ActorID, key actorhost.AttemptKey) error {
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

// AuthorActive is the collaboration/identity verdict. It does not compare
// AttemptKey or declaration version.
func (c *Controller) AuthorActive(id actor.ActorID) error {
	if id == actor.SystemActorID {
		return ErrReservedSystem
	}
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	if c.phase != Running {
		if c.phase == Bootstrapping {
			return ErrBootstrapping
		}
		return ErrClosed
	}
	if _, ok := c.actors[id]; !ok {
		return ErrInactive
	}
	return nil
}

func mintAttempt() (actorhost.AttemptKey, error) { return actorhost.NewAttemptKey() }

func (c *Controller) delete(ids []actor.ActorID) {
	c.stateMu.Lock()
	for _, id := range ids {
		delete(c.actors, id)
	}
	c.stateMu.Unlock()
}

// DesiredFor projects a complete execution-domain level from managed truth.
func (c *Controller) DesiredFor(
	domain, server actorhost.ExecutionDomain,
) ([]actorhost.Desired, error) {
	actors, err := c.Snapshot()
	if err != nil {
		return nil, err
	}
	ids := make([]actor.ActorID, 0, len(actors))
	for id := range actors {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b actor.ActorID) int {
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
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

// ActiveRows returns the managed collaboration projection in canonical order.
func (c *Controller) ActiveRows() ([]storespec.ActorControlRow, error) {
	values, err := c.Snapshot()
	if err != nil {
		return nil, err
	}
	out := make([]storespec.ActorControlRow, 0, len(values))
	for id, value := range values {
		out = append(out, rowFromActive(id, value))
	}
	slices.SortFunc(out, func(left, right storespec.ActorControlRow) int {
		switch {
		case left.ID < right.ID:
			return -1
		case left.ID > right.ID:
			return 1
		default:
			return 0
		}
	})
	return out, nil
}

// AuthorizeAttach is the logical A/G + placement admission for one daemon
// route. Physical publication remains a Host operation owned by Platform.
func (c *Controller) AuthorizeAttach(
	id actor.ActorID,
	key actorhost.AttemptKey,
	peer actorhost.ExecutionDomain,
) error {
	if err := c.checkCurrentSnapshot(id, key); err != nil {
		return err
	}
	value, ok, err := c.Lookup(id)
	if err != nil {
		return err
	}
	if !ok {
		return ErrInactive
	}
	if value.Definition.Placement.Kind != storespec.PlacementDaemon ||
		actorhost.ExecutionDomain(value.Definition.Placement.Host) != peer {
		return ErrInvalidMutation
	}
	return nil
}
