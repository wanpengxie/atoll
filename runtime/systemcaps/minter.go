package systemcaps

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

var ErrInvalidInput = errors.New("systemcaps: invalid mint input")

type rootAuthority struct{}

func (rootAuthority) ActorID() actor.ActorID { return actor.SystemActorID }
func (rootAuthority) Admit() error           { return nil }

type penMinter interface {
	MintAuthority(capauth.Authority, actor.Kind, channel.ID) harness.Pen
}

type accessMinter interface {
	MintAuthority(capauth.Authority) accessdoor.ResourceAccessHandle
}

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

type scheduleMinter interface {
	MintAuthority(capauth.Authority) schedule.ScheduleHandle
}

// Minter owns the construction capabilities needed for one root bundle.
type Minter struct {
	channelID channel.ID
	pen       penMinter
	access    accessMinter
	schedule  scheduleMinter
}

func New(
	channelID channel.ID,
	pen harness.Minter,
	access accessdoor.AccessMinter,
	scheduler schedule.Minter,
) (*Minter, error) {
	p, pok := pen.(penMinter)
	a, aok := access.(accessMinter)
	t, tok := scheduler.(scheduleMinter)
	if channelID == "" || !pok || !aok || !tok {
		return nil, ErrInvalidInput
	}
	return &Minter{channelID: channelID, pen: p, access: a, schedule: t}, nil
}

// Mint mints the SystemActor's whole kernel bundle once.
func (m *Minter) Mint(context.Context) (actorcaps.Caps, error) {
	if m == nil {
		return actorcaps.Caps{}, ErrInvalidInput
	}
	authority := rootAuthority{}
	return actorcaps.Caps{
		Pen:       m.pen.MintAuthority(authority, actor.KindSystem, m.channelID),
		Access:    m.access.MintAuthority(authority),
		State:     unsupportedState{},
		Schedule:  m.schedule.MintAuthority(authority),
		Lifecycle: nil,
	}, nil
}
