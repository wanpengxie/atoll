package actorctl

import (
	"context"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// PenMinter, AccessMinter, ScheduleMinter and StateResolver are the narrow
// runtime-owned mint ports actorctl consumes to construct the final Server
// managed Caps. They are declared HERE, top-down: actorctl states the exact
// shape it needs to weld the value-ledger gate onto each arm, and the platform
// assembly root (the only legitimate holder of the concrete capability minters)
// adapts by passing its welded minters in. Naming the concrete minter types
// (harness.Minter / accessdoor.AccessMinter / schedule.Minter) inside actorctl
// would trip the minter-confinement walls — a runtime sibling has no business
// holding a mint-for-anyone capability type — so these ports reference only the
// opaque capability seams (harness.Pen / accessdoor handles / ScheduleHandle).
type PenMinter interface {
	Mint(actorID actor.ActorID, kind actor.Kind, chID channel.ID, birthVersion int64) harness.Pen
}

type AccessMinter interface {
	Mint(caller storespec.AuthorStamp) accessdoor.ResourceAccessHandle
}

type ScheduleMinter interface {
	MintCurrent(author storespec.AuthorStamp, current func() bool) schedule.ScheduleHandle
}

type StateResolver interface {
	Resolve(ctx context.Context, owner storespec.AuthorStamp) (accessdoor.AccessHandle, error)
}

// managedInvocation is the single value-ledger gate shared by all five arms of
// one exact Server managed body. Every new local capability invocation clears it
// exactly once before touching a raw arm.
//
// admit() judges the root authorisation on the linearised value ledger first
// (Controller current == Run(ActorID, AttemptKey)) and only then confirms the
// exact physical caller (Host actual == exact Body(ActorID, AttemptKey, Unit)).
//
// Lock discipline (hard): admit runs per-call on the arm hot path.
// controller.isCurrent is an stateMu.RLock snapshot read — it NEVER takes the
// controlGate and NEVER reaches into the Host. actual.IsCurrent is the Host's
// own exact-current primitive. The two reads are not atomic across each other:
// the ledger may turn over between them (sliding-window semantics), and that is
// intended — a pass followed by a G1→G2 turnover lets the in-flight call finish
// naturally while the next G1 call is refused at the ledger.
type managedInvocation struct {
	controller *Controller
	actorID    actor.ActorID
	attempt    actorhost.AttemptKey
	actual     actorhost.ActualCurrent
}

func (g *managedInvocation) admit() error {
	// Root authorisation on the linearised value ledger.
	if err := g.controller.isCurrent(g.actorID, g.attempt); err != nil {
		return err
	}
	// Exact physical caller.
	if !g.actual.IsCurrent() {
		return ErrStaleAttempt
	}
	return nil
}

type currentPen struct {
	raw  harness.Pen
	gate *managedInvocation
}

func (p currentPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	if err := p.gate.admit(); err != nil {
		return harness.WriteResult{}, err
	}
	return p.raw.Write(ctx, env)
}

type currentAccess struct {
	raw  accessdoor.AccessHandle
	gate *managedInvocation
}

func (a currentAccess) Invoke(
	ctx context.Context,
	op access.Operation,
	id resource.ResourceID,
	args []byte,
	grant *access.Grant,
) (accessdoor.Outcome, error) {
	if err := a.gate.admit(); err != nil {
		return accessdoor.Outcome{}, err
	}
	return a.raw.Invoke(ctx, op, id, args, grant)
}

type currentResourceAccess struct {
	raw  accessdoor.ResourceAccessHandle
	gate *managedInvocation
}

func (a currentResourceAccess) Invoke(
	ctx context.Context,
	op access.Operation,
	id resource.ResourceID,
	args []byte,
	grant *access.Grant,
) (accessdoor.Outcome, error) {
	if err := a.gate.admit(); err != nil {
		return accessdoor.Outcome{}, err
	}
	return a.raw.Invoke(ctx, op, id, args, grant)
}

func (a currentResourceAccess) Create(
	ctx context.Context,
	id resource.ResourceID,
	spec accessdoor.CreateSpec,
	initial []byte,
) (accessdoor.Outcome, error) {
	if err := a.gate.admit(); err != nil {
		return accessdoor.Outcome{}, err
	}
	return a.raw.Create(ctx, id, spec, initial)
}

func (a currentResourceAccess) Stat(
	ctx context.Context,
	id resource.ResourceID,
) (accessdoor.StatResult, error) {
	if err := a.gate.admit(); err != nil {
		return accessdoor.StatResult{}, err
	}
	return a.raw.Stat(ctx, id)
}

func (a currentResourceAccess) List(
	ctx context.Context,
	query accessdoor.ListQuery,
) (accessdoor.ListPage, error) {
	if err := a.gate.admit(); err != nil {
		return accessdoor.ListPage{}, err
	}
	return a.raw.List(ctx, query)
}

func (a currentResourceAccess) Open(
	ctx context.Context,
	id resource.ResourceID,
	op access.Operation,
) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	if err := a.gate.admit(); err != nil {
		return accessdoor.FileAccess{}, accessdoor.Outcome{}, err
	}
	return a.raw.Open(ctx, id, op)
}

func (a currentResourceAccess) Redeem(
	ctx context.Context,
	route accessdoor.FileRoute,
) (accessdoor.FileAccess, error) {
	if err := a.gate.admit(); err != nil {
		return accessdoor.FileAccess{}, err
	}
	return a.raw.Redeem(ctx, route)
}

type currentSchedule struct {
	raw  schedule.ScheduleHandle
	gate *managedInvocation
}

func (s currentSchedule) Schedule(ctx context.Context, request schedule.ScheduleReq) (schedule.TimerID, error) {
	if err := s.gate.admit(); err != nil {
		return "", err
	}
	return s.raw.Schedule(ctx, request)
}

func (s currentSchedule) Cancel(ctx context.Context, id schedule.TimerID) error {
	if err := s.gate.admit(); err != nil {
		return err
	}
	return s.raw.Cancel(ctx, id)
}

func (s currentSchedule) Ack(ctx context.Context, id schedule.TimerID) error {
	if err := s.gate.admit(); err != nil {
		return err
	}
	return s.raw.Ack(ctx, id)
}

// buildManagedCaps is the sole final construction of the Server managed Caps for
// one exact Body. It reads the Controller Definition, welds one AuthorStamp,
// mints one shared value-ledger gate, draws each raw arm from the injected
// runtime minters, and welds the SAME gate onto all five arms. The business
// builder downstream receives only the finished, gated actorcaps.Caps.
func (a *ChannelActors) buildManagedCaps(input actorhost.BodyBuildInput) (actorcaps.Caps, error) {
	value, ok, err := a.controller.lookup(input.ActorID)
	if err != nil {
		return actorcaps.Caps{}, err
	}
	if !ok {
		return actorcaps.Caps{}, ErrInactive
	}
	def := value.Definition
	author := storespec.AuthorStamp{ID: input.ActorID, BirthVersion: def.DefinitionVersion}
	gate := &managedInvocation{
		controller: a.controller,
		actorID:    input.ActorID,
		attempt:    input.AttemptKey,
		actual:     input.Current,
	}
	caps := actorcaps.Caps{
		Lifecycle: managedLifecycle{
			actors: a, id: input.ActorID, key: input.AttemptKey, gate: gate,
		},
	}
	if a.penMinter != nil {
		caps.Pen = currentPen{
			raw:  a.penMinter.Mint(input.ActorID, def.Kind, a.channelID, def.DefinitionVersion),
			gate: gate,
		}
	}
	if a.accessMinter != nil {
		caps.Access = currentResourceAccess{raw: a.accessMinter.Mint(author), gate: gate}
	}
	if a.stateResolver != nil {
		state, err := a.stateResolver.Resolve(context.Background(), author)
		if err != nil {
			return actorcaps.Caps{}, err
		}
		caps.State = currentAccess{raw: state, gate: gate}
	}
	if a.scheduleMinter != nil {
		// MintCurrent's predicate is the physical incarnation-local timer fence
		// (unchanged from the pre-gate wiring); the value-ledger gate rides the
		// arm entry points via currentSchedule, not the fire path.
		caps.Schedule = currentSchedule{
			raw:  a.scheduleMinter.MintCurrent(author, gate.actual.IsCurrent),
			gate: gate,
		}
	}
	return caps, nil
}
