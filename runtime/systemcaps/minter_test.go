package systemcaps

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/ledgerview"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

type recordingPenMinter struct {
	authority capauth.Authority
}

func (m *recordingPenMinter) MintAuthority(authority capauth.Authority, _ actor.Kind) harness.Pen {
	m.authority = authority
	return nil
}

type noopAccessMinter struct{}

func (noopAccessMinter) MintAuthority(capauth.Authority) accessdoor.ResourceAccessHandle {
	return nil
}

func (noopAccessMinter) MintStateAuthority(capauth.Authority) accessdoor.AccessHandle {
	return nil
}

type noopScheduleMinter struct{}

func (noopScheduleMinter) MintAuthority(capauth.Authority) schedule.ScheduleHandle {
	return nil
}

type recordingViewMinter struct {
	authority capauth.Authority
}

func (m *recordingViewMinter) MintAuthority(authority capauth.Authority) actorcaps.LedgerView {
	m.authority = authority
	return nil
}

func TestNewRequiresLedgerViewMinter(t *testing.T) {
	_, err := New(&recordingPenMinter{}, noopAccessMinter{}, noopScheduleMinter{}, nil)
	if err != ErrInvalidInput {
		t.Fatalf("New error = %v, want %v", err, ErrInvalidInput)
	}
}

func TestMintUsesOneRootAuthorityForPenAndView(t *testing.T) {
	pen := &recordingPenMinter{}
	view := &recordingViewMinter{}
	minter, err := New(pen, noopAccessMinter{}, noopScheduleMinter{}, view)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := minter.Mint(context.Background()); err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if pen.authority == nil || view.authority == nil {
		t.Fatal("Pen and View must both receive an authority")
	}
	if pen.authority != view.authority {
		t.Fatalf("Pen authority %#v differs from View authority %#v", pen.authority, view.authority)
	}
	if pen.authority.ActorID() != actor.SystemActorID {
		t.Fatalf("root authority actor = %q, want %q", pen.authority.ActorID(), actor.SystemActorID)
	}
}

var _ harness.Minter = (*recordingPenMinter)(nil)
var _ accessdoor.AccessMinter = noopAccessMinter{}
var _ schedule.Minter = noopScheduleMinter{}
var _ ledgerview.Minter = (*recordingViewMinter)(nil)
