package managedcaps

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var ErrInvalidInput = errors.New("managedcaps: invalid mint input")

// LifecycleOperations is the completed Platform command face used by the
// Lifecycle arm. It does not expose Controller gates or transition internals.
type LifecycleOperations interface {
	Fork(context.Context, actorctl.ForkRequest) (actorctl.ForkResult, error)
	End(context.Context, actorctl.EndRequest) (actorctl.EndResult, error)
}

// Minter mints all kernel handles for one PreparedRun in one call.
type Minter struct {
	channelID channel.ID
	pen       authorityPenMinter
	access    authorityAccessMinter
	state     authorityStateResolver
	schedule  authorityScheduleMinter
	lifecycle LifecycleOperations
}

type authorityPenMinter interface {
	MintAuthority(capauth.Authority, actor.Kind, channel.ID) harness.Pen
}

type authorityAccessMinter interface {
	MintAuthority(capauth.Authority) accessdoor.ResourceAccessHandle
}

type authorityStateResolver interface {
	ResolveAuthority(capauth.Authority, storespec.ActorWorld) (accessdoor.AccessHandle, error)
}

type authorityScheduleMinter interface {
	MintAuthority(capauth.Authority) schedule.ScheduleHandle
}

func New(
	channelID channel.ID,
	pen harness.Minter,
	access accessdoor.AccessMinter,
	state accessdoor.StateHandleResolver,
	scheduleMinter schedule.Minter,
	lifecycle LifecycleOperations,
) (*Minter, error) {
	if channelID == "" || pen == nil || access == nil || state == nil ||
		scheduleMinter == nil || lifecycle == nil {
		return nil, ErrInvalidInput
	}
	authorityPen, penOK := pen.(authorityPenMinter)
	authorityAccess, accessOK := access.(authorityAccessMinter)
	authorityState, stateOK := state.(authorityStateResolver)
	authoritySchedule, scheduleOK := scheduleMinter.(authorityScheduleMinter)
	if !penOK || !accessOK || !stateOK || !scheduleOK {
		return nil, ErrInvalidInput
	}
	return &Minter{
		channelID: channelID,
		pen:       authorityPen,
		access:    authorityAccess,
		state:     authorityState,
		schedule:  authoritySchedule,
		lifecycle: lifecycle,
	}, nil
}

// Mint is the only outward managed bundle-mint operation.
func (m *Minter) Mint(
	_ context.Context,
	prepared actorctl.PreparedRun,
) (actorcaps.Caps, error) {
	if m == nil || prepared.ActorID() == "" || prepared.AttemptKey() == "" {
		return actorcaps.Caps{}, ErrInvalidInput
	}
	def := prepared.Definition()
	state, err := m.state.ResolveAuthority(prepared.Identity(), prepared.World())
	if err != nil {
		return actorcaps.Caps{}, err
	}
	return actorcaps.Caps{
		Pen:      m.pen.MintAuthority(prepared.Run(), def.Kind, m.channelID),
		Access:   m.access.MintAuthority(prepared.Run()),
		State:    state,
		Schedule: m.schedule.MintAuthority(prepared.Identity()),
		Lifecycle: lifecycleHandle{
			operations: m.lifecycle,
			id:         prepared.ActorID(),
			attempt:    prepared.AttemptKey(),
		},
	}, nil
}

type lifecycleHandle struct {
	operations LifecycleOperations
	id         actor.ActorID
	attempt    actorhost.AttemptKey
}

func (h lifecycleHandle) Fork(
	ctx context.Context,
	requestID message.ID,
	spec actorcaps.ForkSpec,
) (actor.ActorID, error) {
	result, err := h.operations.Fork(ctx, actorctl.ForkRequest{
		CallerActorID: h.id,
		CallerAttempt: h.attempt,
		RequestID:     requestID,
		Spec:          spec,
	})
	return result.ChildActorID, err
}

func (h lifecycleHandle) EndSelf(
	ctx context.Context,
	request actorcaps.EndSelfRequest,
) error {
	_, err := h.operations.End(ctx, actorctl.EndRequest{
		CallerActorID: h.id,
		CallerAttempt: h.attempt,
		Target:        h.id,
		Reason:        request.Reason,
	})
	return err
}

var _ actorcaps.LifecycleHandle = lifecycleHandle{}
