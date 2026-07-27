package actorctl

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// forkKey/forkEntry are the whole in-process idempotency mechanism. Fork is the
// only command that needs one: its child id is freshly minted, so the ledger
// itself cannot answer "did I already do this". Every other command's
// idempotency is carried by truth (semantic key, deregistration latch, equal
// value no-op).
//
// The table is NEVER pruned. Its upper bound is the number of forks this
// process performs, and a hit returns the first result for the whole process
// lifetime — including after the child has died, so one request can never
// produce a second child. A process restart empties it, which is correct: the
// crash killed every entry record, so a retry legitimately births a new child.
type forkKey struct {
	caller  actor.ActorID
	request message.ID
}

type forkEntry struct {
	child  actor.ActorID
	digest string
}

// Fork births one entry-table record. The whole command is a straight line
// inside the ledger lock with zero durable footprint.
func (c *Controller) Fork(
	ctx context.Context,
	request ForkRequest,
) (Transition[ForkResult], error) {
	done, err := c.beginCommand()
	if err != nil {
		return Transition[ForkResult]{}, err
	}
	defer done()
	_ = ctx

	if request.CallerActorID == "" || request.RequestID == "" {
		return Transition[ForkResult]{}, ErrForkInvalid
	}
	digest, err := channel.Digest(forkDigestInput{
		Caller:    request.CallerActorID,
		Kind:      request.Spec.Kind,
		Class:     request.Spec.Class,
		NameHint:  request.Spec.NameHint,
		Config:    request.Spec.Config,
		Placement: request.Spec.Placement,
	})
	if err != nil {
		return Transition[ForkResult]{}, ErrForkInvalid
	}

	c.ledger.Lock()
	defer c.ledger.Unlock()
	if err := c.runnableLocked(); err != nil {
		return Transition[ForkResult]{}, err
	}

	// The verdict is the door's first step — before the replay lookup, so a
	// stale or ended caller is refused instead of being answered from the
	// table. A current caller retrying the same RequestID still lands on the
	// replay row right after passing the gate.
	if err := c.checkCurrentLocked(request.CallerActorID, request.CallerAttempt); err != nil {
		return Transition[ForkResult]{}, err
	}
	key := forkKey{caller: request.CallerActorID, request: request.RequestID}
	if entry, found := c.forks[key]; found {
		if entry.digest != digest {
			return Transition[ForkResult]{}, ErrForkConflict
		}
		return Transition[ForkResult]{Result: ForkResult{ChildActorID: entry.child}}, nil
	}
	parent := c.actors[request.CallerActorID]
	spec, placement, err := normalizeFork(request.Spec, parent.Record)
	if err != nil {
		return Transition[ForkResult]{}, err
	}
	child := freshChildID(request.CallerActorID, spec.NameHint)
	// Reserved-name check at the mint point: a minted id may never collide with
	// the identity vocabulary's reserved constants.
	if child == actor.SystemActorID {
		return Transition[ForkResult]{}, ErrForkInvalid
	}
	attempt, err := mintAttempt()
	if err != nil {
		return Transition[ForkResult]{}, err
	}
	record := storespec.ActorRecord{
		ID:        child,
		Kind:      spec.Kind,
		CreatedAt: c.nowMs(),
		Definition: storespec.ActorDefinition{
			Class:  spec.Class,
			Config: cloneRaw(spec.Config),
		},
		Placement: placement,
	}

	// Settled: nothing below can fail.
	c.store.InstallEntry(record)
	c.actors[child] = managedActor{Record: record, Attempt: attempt}
	c.forks[key] = forkEntry{child: child, digest: digest}

	transition := Transition[ForkResult]{Result: ForkResult{ChildActorID: child}}
	transition.Reconcile.add(placement)
	return transition, nil
}

// forkDigestInput mirrors only the actor-facing Fork operation value. The child
// id and AttemptKey are physical choices and cannot affect the digest.
type forkDigestInput struct {
	Caller    actor.ActorID      `json:"caller"`
	Kind      actor.Kind         `json:"kind"`
	Class     string             `json:"class"`
	NameHint  string             `json:"name_hint,omitempty"`
	Config    []byte             `json:"config,omitempty"`
	Placement *channel.Placement `json:"placement,omitempty"`
}

func freshChildID(parent actor.ActorID, hint string) actor.ActorID {
	if hint == "" {
		hint = "child"
	}
	return actor.ActorID(fmt.Sprintf("%s/%s-%s", parent, hint, uuid.NewString()))
}

func normalizeFork(
	spec actorcaps.ForkSpec,
	parent storespec.ActorRecord,
) (actorcaps.ForkSpec, storespec.Placement, error) {
	// KindHuman is refused at the mint point for the same reason the durable
	// side's validateDraft welds "human ⇔ principal": a fork has no principal
	// source, so a human child would be a member the human roster cannot
	// recognize. Humans are born by admission alone.
	if _, ok := actor.ParseKind(string(spec.Kind)); !ok ||
		spec.Kind == actor.KindSystem ||
		spec.Kind == actor.KindHuman ||
		spec.Class == "" {
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
