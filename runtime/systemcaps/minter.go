package systemcaps

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
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

type stateResolver interface {
	ResolveAuthority(context.Context, capauth.Authority) (accessdoor.AccessHandle, error)
}

type scheduleMinter interface {
	MintAuthority(capauth.Authority) schedule.ScheduleHandle
}

// Minter owns the construction capabilities needed for one root bundle.
type Minter struct {
	channelID channel.ID
	pen       penMinter
	access    accessMinter
	state     stateResolver
	schedule  scheduleMinter
}

func New(
	channelID channel.ID,
	pen harness.Minter,
	access accessdoor.AccessMinter,
	state accessdoor.StateHandleResolver,
	scheduler schedule.Minter,
) (*Minter, error) {
	p, pok := pen.(penMinter)
	a, aok := access.(accessMinter)
	s, sok := state.(stateResolver)
	t, tok := scheduler.(scheduleMinter)
	if channelID == "" || !pok || !aok || !sok || !tok {
		return nil, ErrInvalidInput
	}
	return &Minter{channelID: channelID, pen: p, access: a, state: s, schedule: t}, nil
}

// Mint mints the SystemActor's whole kernel bundle once.
func (m *Minter) Mint(ctx context.Context) (actorcaps.Caps, error) {
	if m == nil {
		return actorcaps.Caps{}, ErrInvalidInput
	}
	authority := rootAuthority{}
	state, err := m.state.ResolveAuthority(ctx, authority)
	if err != nil {
		return actorcaps.Caps{}, err
	}
	return actorcaps.Caps{
		Pen:       m.pen.MintAuthority(authority, actor.KindSystem, m.channelID),
		Access:    m.access.MintAuthority(authority),
		State:     state,
		Schedule:  m.schedule.MintAuthority(authority),
		Lifecycle: nil,
	}, nil
}
