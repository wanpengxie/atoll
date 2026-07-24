package home

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var ErrActorNotCurrent = errors.New("platform: actor is not current")

type currentPen struct {
	raw     harness.Pen
	current actorhost.ActualCurrent
}

func (p currentPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	if !p.current.IsCurrent() {
		return harness.WriteResult{}, ErrActorNotCurrent
	}
	return p.raw.Write(ctx, env)
}

type currentAccess struct {
	raw     accessdoor.AccessHandle
	current actorhost.ActualCurrent
}

func (a currentAccess) Invoke(
	ctx context.Context,
	op access.Operation,
	id resource.ResourceID,
	args []byte,
	grant *access.Grant,
) (accessdoor.Outcome, error) {
	if !a.current.IsCurrent() {
		return accessdoor.Outcome{}, ErrActorNotCurrent
	}
	return a.raw.Invoke(ctx, op, id, args, grant)
}

type currentResourceAccess struct {
	raw     accessdoor.ResourceAccessHandle
	current actorhost.ActualCurrent
}

func (a currentResourceAccess) Invoke(
	ctx context.Context,
	op access.Operation,
	id resource.ResourceID,
	args []byte,
	grant *access.Grant,
) (accessdoor.Outcome, error) {
	if !a.current.IsCurrent() {
		return accessdoor.Outcome{}, ErrActorNotCurrent
	}
	return a.raw.Invoke(ctx, op, id, args, grant)
}

func (a currentResourceAccess) Create(
	ctx context.Context,
	id resource.ResourceID,
	spec accessdoor.CreateSpec,
	initial []byte,
) (accessdoor.Outcome, error) {
	if !a.current.IsCurrent() {
		return accessdoor.Outcome{}, ErrActorNotCurrent
	}
	return a.raw.Create(ctx, id, spec, initial)
}

func (a currentResourceAccess) Stat(
	ctx context.Context,
	id resource.ResourceID,
) (accessdoor.StatResult, error) {
	if !a.current.IsCurrent() {
		return accessdoor.StatResult{}, ErrActorNotCurrent
	}
	return a.raw.Stat(ctx, id)
}

func (a currentResourceAccess) List(
	ctx context.Context,
	query accessdoor.ListQuery,
) (accessdoor.ListPage, error) {
	if !a.current.IsCurrent() {
		return accessdoor.ListPage{}, ErrActorNotCurrent
	}
	return a.raw.List(ctx, query)
}

func (a currentResourceAccess) Open(
	ctx context.Context,
	id resource.ResourceID,
	op access.Operation,
) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	if !a.current.IsCurrent() {
		return accessdoor.FileAccess{}, accessdoor.Outcome{}, ErrActorNotCurrent
	}
	return a.raw.Open(ctx, id, op)
}

func (a currentResourceAccess) Redeem(
	ctx context.Context,
	route accessdoor.FileRoute,
) (accessdoor.FileAccess, error) {
	if !a.current.IsCurrent() {
		return accessdoor.FileAccess{}, ErrActorNotCurrent
	}
	return a.raw.Redeem(ctx, route)
}

type currentSchedule struct {
	raw     schedule.ScheduleHandle
	current actorhost.ActualCurrent
}

func (s currentSchedule) Schedule(ctx context.Context, request schedule.ScheduleReq) (schedule.TimerID, error) {
	if !s.current.IsCurrent() {
		return "", ErrActorNotCurrent
	}
	return s.raw.Schedule(ctx, request)
}

func (s currentSchedule) Cancel(ctx context.Context, id schedule.TimerID) error {
	if !s.current.IsCurrent() {
		return ErrActorNotCurrent
	}
	return s.raw.Cancel(ctx, id)
}

func (s currentSchedule) Ack(ctx context.Context, id schedule.TimerID) error {
	if !s.current.IsCurrent() {
		return ErrActorNotCurrent
	}
	return s.raw.Ack(ctx, id)
}

// buildManagedCaps is the Server production assembly for one exact Body. The
// business actor receives only actor-facing capabilities; ActualCurrent stays
// inside these facades and the direct lifecycle handle.
func (h *Home) buildManagedCaps(
	input actorhost.BodyBuildInput,
	lifecycle actorcaps.LifecycleHandle,
) (actorcaps.Caps, error) {
	row, ok, err := h.actors.Lookup(input.ActorID)
	if err != nil {
		return actorcaps.Caps{}, err
	}
	if !ok {
		return actorcaps.Caps{}, ErrEndNotMember
	}
	author := storespec.AuthorStamp{
		ID: input.ActorID, BirthVersion: row.CurrentDeclVersion,
	}
	state, err := h.stateHandles.Resolve(context.Background(), author)
	if err != nil {
		return actorcaps.Caps{}, err
	}
	return actorcaps.Caps{
		Pen: currentPen{
			raw:     h.minter.Mint(input.ActorID, row.Kind, h.channelID, row.CurrentDeclVersion),
			current: input.Current,
		},
		Access: currentResourceAccess{
			raw: h.cs.Access.Mint(author), current: input.Current,
		},
		State: currentAccess{
			raw: state, current: input.Current,
		},
		Schedule: currentSchedule{
			raw:     h.schedMinter.MintCurrent(author, input.Current.IsCurrent),
			current: input.Current,
		},
		Lifecycle: lifecycle,
	}, nil
}
