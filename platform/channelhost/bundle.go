package channelhost

import (
	"context"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type Bundle interface {
	Generation() uint64
	Gateway() GatewayHitch
	View() View
}

type GatewayHitch interface {
	SubjectSlotFor(actor.ActorID) (*subjectgate.Slot, bool)
	Subscribe() (<-chan struct{}, func())
}

// View is the business membrane's read face. Every actor-truth method is
// question-shaped: callers never receive a runtime record and never import a
// runtime storage or control DTO.
type View interface {
	HumanRoster(context.Context) ([]channelspec.HumanRosterEntry, error)
	ResolvePrincipal(context.Context, string) (actor.ActorID, bool, error)
	OwnerPrincipal(context.Context) (string, bool, error)
	ReadVisibleAfterSeq(context.Context, int64, int) ([]storespec.StoredRow, int64, error)
	ReadVisibleBeforeSeq(context.Context, int64, int) ([]storespec.StoredRow, int64, bool, error)
	ReadVisibleTurnWindowBeforeSeq(context.Context, channelspec.HistoryWindowQuery) (channelspec.HistoryWindow, error)
	IsActive(context.Context, actor.ActorID) (bool, error)
	ActorFacts(context.Context, actor.ActorID) (channelspec.ActorFacts, bool, error)
	IsBound(context.Context, string) (bool, error)
	Roster(context.Context) ([]channelspec.ObsRosterRow, error)
}

type bundle struct {
	home       *home.Home
	generation uint64
}

func (b *bundle) Generation() uint64    { return b.generation }
func (b *bundle) Gateway() GatewayHitch { return gatewayAdapter{b.home} }
func (b *bundle) View() View            { return viewAdapter{b.home} }

type gatewayAdapter struct{ home *home.Home }

func (a gatewayAdapter) SubjectSlotFor(id actor.ActorID) (*subjectgate.Slot, bool) {
	return home.GatewaySlot(a.home, id)
}
func (a gatewayAdapter) Subscribe() (<-chan struct{}, func()) { return home.GatewaySubscribe(a.home) }

type viewAdapter struct{ home *home.Home }

func (a viewAdapter) HumanRoster(ctx context.Context) ([]channelspec.HumanRosterEntry, error) {
	return a.home.View().HumanRoster(ctx)
}
func (a viewAdapter) Roster(ctx context.Context) ([]channelspec.ObsRosterRow, error) {
	return a.home.View().Roster(ctx)
}
func (a viewAdapter) ResolvePrincipal(ctx context.Context, principal string) (actor.ActorID, bool, error) {
	return a.home.View().ResolvePrincipal(ctx, principal)
}
func (a viewAdapter) OwnerPrincipal(ctx context.Context) (string, bool, error) {
	return a.home.View().OwnerPrincipal(ctx)
}
func (a viewAdapter) ReadVisibleAfterSeq(ctx context.Context, seq int64, limit int) ([]storespec.StoredRow, int64, error) {
	return a.home.View().ReadVisibleAfterSeq(ctx, seq, limit)
}
func (a viewAdapter) ReadVisibleBeforeSeq(ctx context.Context, seq int64, limit int) ([]storespec.StoredRow, int64, bool, error) {
	return a.home.View().ReadVisibleBeforeSeq(ctx, seq, limit)
}
func (a viewAdapter) ReadVisibleTurnWindowBeforeSeq(ctx context.Context, query channelspec.HistoryWindowQuery) (channelspec.HistoryWindow, error) {
	return a.home.View().ReadVisibleTurnWindowBeforeSeq(ctx, query)
}
func (a viewAdapter) IsActive(ctx context.Context, id actor.ActorID) (bool, error) {
	return a.home.View().IsActive(ctx, id)
}
func (a viewAdapter) ActorFacts(ctx context.Context, id actor.ActorID) (channelspec.ActorFacts, bool, error) {
	return a.home.View().ActorFacts(ctx, id)
}

func (a viewAdapter) IsBound(ctx context.Context, id string) (bool, error) {
	return a.home.View().IsBound(ctx, id)
}
func (a viewAdapter) ActorsPlacedOn(ctx context.Context, deviceID string) ([]actor.ActorID, error) {
	return a.home.View().ActorsPlacedOn(ctx, deviceID)
}
