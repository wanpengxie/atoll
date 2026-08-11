package actorctl

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Store is the record port the Controller drives. It is defined here purely so
// unit tests can substitute a fake; runtime assembly always hands in the real
// *actorstore.Store. It speaks record language only — no authorization
// coordinate ever crosses it, which is how "the actor store does not own
// AttemptKey" becomes a compiler guarantee.
type Store interface {
	RestoreActive(context.Context) ([]storespec.ActorRecord, error)
	Insert(context.Context, storespec.ActorDraft) (storespec.ActorRecord, error)
	UpdateDefinition(context.Context, actor.ActorID, storespec.ActorDefinition) (storespec.ActorRecord, error)
	Deregister(context.Context, []actor.ActorID) error
	InstallEntry(storespec.ActorRecord)
}

// Controller is the sole owner of the managed actor value ledger.
//
// Every lifecycle command serializes on ONE ledger lock and performs exactly
// one complete ledger change inside it: outside the lock only the before-state
// or the after-state is ever visible, never an intermediate. Fallible steps
// (validation, AttemptKey pre-mint, the durable transaction) all happen before
// the change settles; after it settles nothing can fail — publication is plain
// memory assignment, and the Controller never reads a record back.
type Controller struct {
	store Store
	nowMs func() int64
	owner commandOwner

	// ledger guards the entire ledger state: phase, the member ledger and the
	// fork replay table. Readers take the read end; nothing bypasses it.
	ledger sync.RWMutex
	phase  ControllerPhase
	actors map[actor.ActorID]managedActor
	forks  map[forkKey]forkEntry
}

// New constructs a bootstrapping managed actor Controller.
func New(store Store, nowMs func() int64) (*Controller, error) {
	if store == nil {
		return nil, ErrInvalidMutation
	}
	if nowMs == nil {
		nowMs = func() int64 { return time.Now().UnixMilli() }
	}
	return &Controller{
		store:  store,
		nowMs:  nowMs,
		phase:  Bootstrapping,
		actors: make(map[actor.ActorID]managedActor),
		forks:  make(map[forkKey]forkEntry),
	}, nil
}

// Start publishes the complete managed value-ledger image exactly once. Every
// restored record is a member: the kernel has no record, so there is nothing to
// sift out and no bootstrap value to hand back.
func (c *Controller) Start(ctx context.Context) error {
	if c == nil {
		return ErrClosed
	}
	records, err := c.store.RestoreActive(ctx)
	if err != nil {
		return err
	}
	managed := make(map[actor.ActorID]managedActor, len(records))
	for _, record := range records {
		key, err := mintAttempt()
		if err != nil {
			return err
		}
		managed[record.ID] = managedActor{Record: record.Clone(), Attempt: key}
	}

	c.ledger.Lock()
	defer c.ledger.Unlock()
	if c.phase != Bootstrapping {
		if c.phase == Closed {
			return ErrClosed
		}
		return ErrAlreadyStarted
	}
	c.actors = managed
	c.phase = Running
	return nil
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
	c.ledger.Lock()
	c.phase = Closed
	c.actors = make(map[actor.ActorID]managedActor)
	c.ledger.Unlock()
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
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	return c.phase
}

func mintAttempt() (actorhost.AttemptKey, error) { return actorhost.NewAttemptKey() }

// runnableLocked is the phase guard shared by every ledger read and write.
func (c *Controller) runnableLocked() error {
	switch c.phase {
	case Bootstrapping:
		return ErrBootstrapping
	case Closed:
		return ErrClosed
	}
	return nil
}

// checkCurrentSnapshot is the A/G verdict: acting as the CURRENT term.
func (c *Controller) checkCurrentSnapshot(id actor.ActorID, key actorhost.AttemptKey) error {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	return c.checkCurrentLocked(id, key)
}

func (c *Controller) checkCurrentLocked(id actor.ActorID, key actorhost.AttemptKey) error {
	if err := c.runnableLocked(); err != nil {
		return err
	}
	value, ok := c.actors[id]
	if !ok {
		return ErrInactive
	}
	if value.Attempt != key {
		return ErrStaleAttempt
	}
	return nil
}

// AuthorActive is the A verdict: the identity is a member right now. It spans
// terms and never compares AttemptKey.
func (c *Controller) AuthorActive(id actor.ActorID) error {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
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

// IsActive is the one "is this legal right now" boolean question.
func (c *Controller) IsActive(_ context.Context, id actor.ActorID) (bool, error) {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	if err := c.runnableLocked(); err != nil {
		return false, err
	}
	_, ok := c.actors[id]
	return ok, nil
}

// ActiveKind is the remote Stat narrow query. It never needs a whole record.
func (c *Controller) ActiveKind(id actor.ActorID) (actor.Kind, bool) {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	if c.phase != Running {
		return "", false
	}
	value, ok := c.actors[id]
	if !ok {
		return "", false
	}
	return value.Record.Kind, true
}

// AdmitIdentity returns one coherent ActorID-level collaboration snapshot.
func (c *Controller) AdmitIdentity(
	_ context.Context,
	id actor.ActorID,
) (storespec.IdentityAdmission, bool, error) {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	if err := c.runnableLocked(); err != nil {
		return storespec.IdentityAdmission{}, false, err
	}
	value, ok := c.actors[id]
	if !ok {
		return storespec.IdentityAdmission{}, false, nil
	}
	return storespec.IdentityAdmission{ID: id, Kind: value.Record.Kind}, true, nil
}

// ActorFacts is the narrow identity-fact question: who is behind this actor and
// what kind is it. Owner-ness is deliberately absent — that verdict is derived
// from the genesis pointer at the Platform door, never from the value ledger.
func (c *Controller) ActorFacts(
	_ context.Context,
	id actor.ActorID,
) (storespec.ActorFacts, bool, error) {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	if err := c.runnableLocked(); err != nil {
		return storespec.ActorFacts{}, false, err
	}
	value, ok := c.actors[id]
	if !ok {
		return storespec.ActorFacts{}, false, nil
	}
	return storespec.ActorFacts{
		Kind: value.Record.Kind, Principal: value.Record.Principal,
		SourceDeclID: value.Record.SourceDeclID,
	}, true, nil
}

// ResolvePrincipal turns a login principal into the member behind it. It is the
// inverse of ActorFacts' principal field, answered off the same in-memory value
// ledger and under the same lock, so it can never disagree with what the rest of
// the Controller is serving.
//
// An empty principal resolves to nothing. Every non-human member carries "" (the
// registry forbids them a login principal), so matching one would hand an
// arbitrary agent back as the answer to "who is logged in as nobody".
func (c *Controller) ResolvePrincipal(principal string) (actor.ActorID, bool, error) {
	if principal == "" {
		return "", false, nil
	}
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	if err := c.runnableLocked(); err != nil {
		return "", false, err
	}
	for id, value := range c.actors {
		if value.Record.Principal == principal {
			return id, true, nil
		}
	}
	return "", false, nil
}

// ActiveIdentities answers "who is here right now" for the presence and
// connection-slot sweeps. It carries no definition.
func (c *Controller) ActiveIdentities() ([]storespec.ActiveIdentity, error) {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	if err := c.runnableLocked(); err != nil {
		return nil, err
	}
	out := make([]storespec.ActiveIdentity, 0, len(c.actors))
	for id, value := range c.actors {
		out = append(out, storespec.ActiveIdentity{ID: id, Kind: value.Record.Kind})
	}
	sortByActorID(out, func(v storespec.ActiveIdentity) actor.ActorID { return v.ID })
	return out, nil
}

// DeclaredInstances answers "which actors did this declaration produce", in
// canonical id order. It returns ids alone — the business membrane asks for
// instances, never for rows.
func (c *Controller) DeclaredInstances(declID string) ([]actor.ActorID, error) {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	if err := c.runnableLocked(); err != nil {
		return nil, err
	}
	out := make([]actor.ActorID, 0, 1)
	for id, value := range c.actors {
		if declID != "" && value.Record.SourceDeclID == declID {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out, nil
}

// DeclaredReconcileList answers "what does declaration reconcile compare
// against". Its only consumer is the Platform declaration pull loop.
func (c *Controller) DeclaredReconcileList() ([]DeclaredInstance, error) {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	if err := c.runnableLocked(); err != nil {
		return nil, err
	}
	out := make([]DeclaredInstance, 0, len(c.actors))
	for id, value := range c.actors {
		if value.Record.SourceDeclID == "" {
			continue
		}
		out = append(out, DeclaredInstance{
			ID: id, Kind: value.Record.Kind,
			SourceDeclID: value.Record.SourceDeclID,
			Definition:   value.Record.Definition.Clone(),
		})
	}
	sortByActorID(out, func(v DeclaredInstance) actor.ActorID { return v.ID })
	return out, nil
}

// ResourceActorBasis is the Controller's native single-snapshot answer for the
// resource domain: liveness plus the owner-derivation basis (Kind, Principal),
// all read under one ledger lock. It is a basis, not a verdict — the Platform
// door derives storespec.ResourceActorFacts from it (owner lives on the genesis
// pointer, never in the value ledger), and the basis travels no further.
type ResourceActorBasis struct {
	Active               bool
	Kind                 actor.Kind
	Principal            string
	PreferredStorageHost string
}

func (c *Controller) ResourceActorBasis(
	_ context.Context,
	id actor.ActorID,
) (ResourceActorBasis, error) {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	if err := c.runnableLocked(); err != nil {
		return ResourceActorBasis{}, err
	}
	value, ok := c.actors[id]
	if !ok {
		return ResourceActorBasis{}, nil
	}
	basis := ResourceActorBasis{
		Active:    true,
		Kind:      value.Record.Kind,
		Principal: value.Record.Principal,
	}
	if value.Record.Placement.Kind == storespec.PlacementDaemon {
		basis.PreferredStorageHost = value.Record.Placement.Host
	}
	return basis, nil
}

// DesiredFor projects a complete execution-domain level from managed truth.
func (c *Controller) DesiredFor(
	domain, server actorhost.ExecutionDomain,
) ([]actorhost.Desired, error) {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	if err := c.runnableLocked(); err != nil {
		return nil, err
	}
	ids := make([]actor.ActorID, 0, len(c.actors))
	for id := range c.actors {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	out := make([]actorhost.Desired, 0, len(ids))
	for _, id := range ids {
		value := c.actors[id]
		spec := executionSpec(value.Record)
		switch value.Record.Placement.Kind {
		case storespec.PlacementServer:
			if domain == server {
				out = append(out, actorhost.BodyDesired{
					ActorID: id, AttemptKey: value.Attempt, ExecutionSpec: spec,
				})
			}
		case storespec.PlacementDaemon:
			peer := actorhost.ExecutionDomain(value.Record.Placement.Host)
			if domain == server {
				out = append(out, actorhost.CarrierDesired{
					ActorID: id, AttemptKey: value.Attempt, PeerDomain: peer,
				})
			} else if domain == peer {
				out = append(out, actorhost.BodyDesired{
					ActorID: id, AttemptKey: value.Attempt, ExecutionSpec: spec,
				})
			}
		default:
			return nil, storespec.ErrInvalidPlacement
		}
	}
	return out, nil
}

func executionSpec(record storespec.ActorRecord) actorhost.ExecutionSpec {
	return actorhost.ExecutionSpec{
		Kind:   record.Kind,
		Class:  record.Definition.Class,
		Config: cloneRaw(record.Definition.Config),
	}
}

// AuthorizeAttach is the logical A/G + placement admission for one daemon
// route. Physical publication remains a Host operation owned by Platform.
func (c *Controller) AuthorizeAttach(
	id actor.ActorID,
	key actorhost.AttemptKey,
	peer actorhost.ExecutionDomain,
) error {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	if err := c.checkCurrentLocked(id, key); err != nil {
		return err
	}
	value := c.actors[id]
	if value.Record.Placement.Kind != storespec.PlacementDaemon ||
		actorhost.ExecutionDomain(value.Record.Placement.Host) != peer {
		return ErrInvalidMutation
	}
	return nil
}

func sortByActorID[T any](values []T, key func(T) actor.ActorID) {
	slices.SortFunc(values, func(left, right T) int {
		switch a, b := key(left), key(right); {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	})
}

func cloneRaw(raw []byte) []byte { return append([]byte(nil), raw...) }

var _ storespec.ActorFactsAuthority = (*Controller)(nil)
var _ storespec.PrincipalIdentity = (*Controller)(nil)
var _ storespec.IdentityRoster = (*Controller)(nil)
var _ storespec.DeclaredInstanceReader = (*Controller)(nil)
var _ storespec.IdentityPresence = (*Controller)(nil)
var _ storespec.CollaborationAuthority = (*Controller)(nil)
