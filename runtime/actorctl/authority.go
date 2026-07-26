package actorctl

import (
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
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
// capability mint. Only Controller can construct one. It carries no definition:
// the body builder already holds Host's exact desired input.
type PreparedRun struct {
	id       actor.ActorID
	attempt  actorhost.AttemptKey
	kind     actor.Kind
	identity IdentityAuthority
	run      RunAuthority
}

func (p PreparedRun) ActorID() actor.ActorID           { return p.id }
func (p PreparedRun) AttemptKey() actorhost.AttemptKey { return p.attempt }
func (p PreparedRun) Kind() actor.Kind                 { return p.kind }
func (p PreparedRun) Identity() IdentityAuthority      { return p.identity }
func (p PreparedRun) Run() RunAuthority                { return p.run }

// PrepareRun returns one coherent A/G snapshot for one body assembly. It does
// not inspect Host physical current and does not mint individual capability
// arms.
func (c *Controller) PrepareRun(
	id actor.ActorID,
	key actorhost.AttemptKey,
	spec actorhost.ExecutionSpec,
) (PreparedRun, error) {
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	if err := c.runnableLocked(); err != nil {
		return PreparedRun{}, err
	}
	value, ok := c.actors[id]
	if !ok {
		return PreparedRun{}, ErrInactive
	}
	if value.Attempt != key || !executionSpec(value.Record).Equal(spec) {
		return PreparedRun{}, ErrStaleAttempt
	}
	return PreparedRun{
		id: id, attempt: key, kind: value.Record.Kind,
		identity: IdentityAuthority{controller: c, id: id},
		run:      RunAuthority{controller: c, id: id, attempt: key},
	}, nil
}
