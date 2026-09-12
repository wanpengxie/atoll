package systemcaps

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/ledgerview"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

var ErrInvalidInput = errors.New("systemcaps: invalid mint input")

type rootAuthority struct{}

func (rootAuthority) ActorID() actor.ActorID { return actor.SystemActorID }
func (rootAuthority) Admit() error           { return nil }

// Minter owns the construction capabilities needed for one root bundle. Like
// the managed minter it holds no channel id — the harness stamps its own.
type Minter struct {
	pen      harness.Minter
	access   accessdoor.AccessMinter
	schedule schedule.Minter
	view     ledgerview.Minter
}

func New(
	pen harness.Minter,
	access accessdoor.AccessMinter,
	scheduler schedule.Minter,
	view ledgerview.Minter,
) (*Minter, error) {
	if pen == nil || access == nil || scheduler == nil || view == nil {
		return nil, ErrInvalidInput
	}
	return &Minter{pen: pen, access: access, schedule: scheduler, view: view}, nil
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
		State:     nil,
		Schedule:  m.schedule.MintAuthority(authority),
		Lifecycle: nil,
		View:      m.view.MintAuthority(authority),
	}, nil
}
