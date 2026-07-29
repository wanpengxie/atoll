package systemcaps

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

var ErrInvalidInput = errors.New("systemcaps: invalid mint input")

type rootAuthority struct{}

func (rootAuthority) ActorID() actor.ActorID { return actor.SystemActorID }
func (rootAuthority) Admit() error           { return nil }

// unsupportedState is the kernel's State arm. The kernel has never consumed a
// State handle (it is a constant, not a member: it has no record and therefore
// no backing to route to), so the arm is an empty implementation that refuses
// every call rather than a second, kernel-shaped state authority. The Caps
// shape is unchanged; a real implementation lands the day a real use case
// demands it.
type unsupportedState struct{}

var ErrStateUnsupported = errors.New("systemcaps: the kernel has no state backing")

func (unsupportedState) Invoke(
	context.Context, access.Operation, resource.ResourceID, []byte, *access.Grant,
) (accessdoor.Outcome, error) {
	return accessdoor.Outcome{}, ErrStateUnsupported
}

// Minter owns the construction capabilities needed for one root bundle. Like
// the managed minter it holds no channel id — the harness stamps its own.
type Minter struct {
	pen      harness.Minter
	access   accessdoor.AccessMinter
	schedule schedule.Minter
}

func New(
	pen harness.Minter,
	access accessdoor.AccessMinter,
	scheduler schedule.Minter,
) (*Minter, error) {
	if pen == nil || access == nil || scheduler == nil {
		return nil, ErrInvalidInput
	}
	return &Minter{pen: pen, access: access, schedule: scheduler}, nil
}

// Mint mints the SystemActor's whole kernel bundle once.
func (m *Minter) Mint(context.Context) (actorcaps.Caps, error) {
	if m == nil {
		return actorcaps.Caps{}, ErrInvalidInput
	}
	authority := rootAuthority{}
	return actorcaps.Caps{
		Pen:       m.pen.MintAuthority(authority, actor.KindSystem),
		Access:    m.access.MintAuthority(authority),
		State:     unsupportedState{},
		Schedule:  m.schedule.MintAuthority(authority),
		Lifecycle: nil,
	}, nil
}
