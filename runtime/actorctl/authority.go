package actorctl

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// IdentityAuthority is an opaque Controller-minted identity capability.
// Its coordinates cannot be chosen or rewritten by Platform.
type IdentityAuthority struct {
	controller *Controller
	id         actor.ActorID
}

func (a IdentityAuthority) Admit() error {
	if a.controller == nil {
		return ErrInactive
	}
	return a.controller.AuthorActive(a.id)
}

func (a IdentityAuthority) ActorID() actor.ActorID { return a.id }

// RunAuthority is an opaque Controller-minted logical execution capability.
// Admit performs one complete A/G verdict; callers must not precede or follow
// it with a separate ActorID/generation admission.
type RunAuthority struct {
	controller *Controller
	id         actor.ActorID
	attempt    actorhost.AttemptKey
}

func (a RunAuthority) Admit() error {
	if a.controller == nil {
		return ErrStaleAttempt
	}
	return a.controller.checkCurrentSnapshot(a.id, a.attempt)
}

func (a RunAuthority) ActorID() actor.ActorID           { return a.id }
func (a RunAuthority) AttemptKey() actorhost.AttemptKey { return a.attempt }

// PreparedRun is the coherent immutable input for exactly one managed Unit
// capability mint. Only Controller can construct one.
type PreparedRun struct {
	id         actor.ActorID
	attempt    actorhost.AttemptKey
	definition ActorDefinition
	identity   IdentityAuthority
	run        RunAuthority
}

func (p PreparedRun) ActorID() actor.ActorID           { return p.id }
func (p PreparedRun) AttemptKey() actorhost.AttemptKey { return p.attempt }
func (p PreparedRun) Definition() ActorDefinition      { return cloneDefinition(p.definition) }
func (p PreparedRun) Identity() IdentityAuthority      { return p.identity }
func (p PreparedRun) Run() RunAuthority                { return p.run }

func cloneDefinition(def ActorDefinition) ActorDefinition {
	def.Execution.Config = append([]byte(nil), def.Execution.Config...)
	return def
}

// PrepareRun returns one coherent A/G/Definition snapshot. It does not inspect
// Host physical current and does not mint individual capability arms.
func (c *Controller) PrepareRun(
	id actor.ActorID,
	key actorhost.AttemptKey,
	spec actorhost.ExecutionSpec,
) (PreparedRun, error) {
	if id == actor.SystemActorID {
		return PreparedRun{}, ErrReservedSystem
	}
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	switch c.phase {
	case Bootstrapping:
		return PreparedRun{}, ErrBootstrapping
	case Closed:
		return PreparedRun{}, ErrClosed
	}
	value, ok := c.actors[id]
	if !ok {
		return PreparedRun{}, ErrInactive
	}
	if value.Desired.AttemptKey != key || !value.Definition.Execution.Equal(spec) {
		return PreparedRun{}, ErrStaleAttempt
	}
	def := cloneActive(value).Definition
	return PreparedRun{
		id: id, attempt: key, definition: def,
		identity: IdentityAuthority{controller: c, id: id},
		run:      RunAuthority{controller: c, id: id, attempt: key},
	}, nil
}

func (c *Controller) LookupActive(
	_ context.Context,
	id actor.ActorID,
) (storespec.ActorControlRow, bool, error) {
	value, ok, err := c.Lookup(id)
	if err != nil || !ok {
		return storespec.ActorControlRow{}, ok, err
	}
	return rowFromActive(id, value), true, nil
}

func (c *Controller) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	return c.ActiveRows()
}

func (c *Controller) WorldOf(
	_ context.Context,
	id actor.ActorID,
) (storespec.ActorWorld, bool, error) {
	value, ok, err := c.Lookup(id)
	if err != nil || !ok {
		return 0, ok, err
	}
	if value.Definition.Origin == OriginRunWorld {
		return storespec.WorldRun, true, nil
	}
	return storespec.WorldDurable, true, nil
}

// CheckAuthor is collaboration authority. BirthVersion is intentionally
// ignored: declaration metadata is not managed execution permission.
func (c *Controller) CheckAuthor(
	ctx context.Context,
	stamp storespec.AuthorStamp,
) (storespec.AuthorVerdict, error) {
	_, active, err := c.LookupActive(ctx, stamp.ID)
	if err != nil {
		return storespec.AuthorNotMember, err
	}
	if !active {
		return storespec.AuthorNotMember, nil
	}
	return storespec.AuthorOK, nil
}

var _ storespec.ActorAuthority = (*Controller)(nil)
