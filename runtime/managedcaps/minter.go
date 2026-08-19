package managedcaps

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

var ErrInvalidInput = errors.New("managedcaps: invalid mint input")

// LifecycleOperations is the completed Platform command face used by the
// Lifecycle arm. It does not expose Controller gates or transition internals.
type LifecycleOperations interface {
	End(context.Context, actorctl.EndRequest) (actorctl.EndResult, error)
}

// Minter mints all kernel handles for one PreparedRun in one call. It holds no
// channel id: the channel is the harness's own binding constant, stamped where
// it is known, never carried around the assembly.
type Minter struct {
	pen       harness.Minter
	access    accessdoor.AccessMinter
	state     accessdoor.StateHandleResolver
	schedule  schedule.Minter
	lifecycle LifecycleOperations
}

func New(
	pen harness.Minter,
	access accessdoor.AccessMinter,
	state accessdoor.StateHandleResolver,
	scheduleMinter schedule.Minter,
	lifecycle LifecycleOperations,
) (*Minter, error) {
	if pen == nil || access == nil || state == nil ||
		scheduleMinter == nil || lifecycle == nil {
		return nil, ErrInvalidInput
	}
	return &Minter{
		pen:       pen,
		access:    access,
		state:     state,
		schedule:  scheduleMinter,
		lifecycle: lifecycle,
	}, nil
}

// Mint is the only outward managed bundle-mint operation.
func (m *Minter) Mint(
	ctx context.Context,
	prepared actorctl.PreparedRun,
) (actorcaps.Caps, error) {
	if m == nil || prepared.ActorID() == "" || prepared.AttemptKey() == "" {
		return actorcaps.Caps{}, ErrInvalidInput
	}
	state, err := m.state.ResolveAuthority(ctx, prepared.Identity())
	if err != nil {
		return actorcaps.Caps{}, err
	}
	return actorcaps.Caps{
		Pen:      m.pen.MintAuthority(prepared.Run(), prepared.Kind()),
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
