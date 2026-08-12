package channelhost

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type Bundle interface {
	Generation() uint64
	Gateway() GatewayHitch
	Call() Caller
	View() View
}

type Caller interface {
	Call(context.Context, actor.ActorID, string, any) (json.RawMessage, error)
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
	DeclaredInstances(context.Context, string) ([]actor.ActorID, error)
	HasDeclaredInstance(context.Context, string) (bool, error)
	ResolvePrincipal(context.Context, string) (actor.ActorID, bool, error)
	OwnerPrincipal(context.Context) (string, bool, error)
	ReadVisibleAfterSeq(context.Context, channel.Reader, int64, int) ([]storespec.StoredRow, int64, error)
	ActorFacts(context.Context, actor.ActorID) (channelspec.ActorFacts, bool, error)
	IsBound(context.Context, string) (bool, error)
}

type bundle struct {
	home       *home.Home
	generation uint64
}

func (b *bundle) Generation() uint64    { return b.generation }
func (b *bundle) Gateway() GatewayHitch { return gatewayAdapter{b.home} }
func (b *bundle) Call() Caller          { return callAdapter{b.home} }
func (b *bundle) View() View            { return viewAdapter{b.home} }

type callAdapter struct{ home *home.Home }

func (a callAdapter) Call(ctx context.Context, target actor.ActorID, word string, payload any) (json.RawMessage, error) {
	return home.Call(a.home, ctx, target, word, payload)
}

type gatewayAdapter struct{ home *home.Home }

func (a gatewayAdapter) SubjectSlotFor(id actor.ActorID) (*subjectgate.Slot, bool) {
	return home.GatewaySlot(a.home, id)
}
func (a gatewayAdapter) Subscribe() (<-chan struct{}, func()) { return home.GatewaySubscribe(a.home) }

type viewAdapter struct{ home *home.Home }

func (a viewAdapter) HumanRoster(ctx context.Context) ([]channelspec.HumanRosterEntry, error) {
	return a.home.View().HumanRoster(ctx)
}
func (a viewAdapter) DeclaredInstances(ctx context.Context, d string) ([]actor.ActorID, error) {
	return a.home.View().DeclaredInstances(ctx, d)
}
func (a viewAdapter) HasDeclaredInstance(ctx context.Context, d string) (bool, error) {
	return a.home.View().HasDeclaredInstance(ctx, d)
}
func (a viewAdapter) ResolvePrincipal(ctx context.Context, principal string) (actor.ActorID, bool, error) {
	return a.home.View().ResolvePrincipal(ctx, principal)
}
func (a viewAdapter) OwnerPrincipal(ctx context.Context) (string, bool, error) {
	return a.home.View().OwnerPrincipal(ctx)
}
func (a viewAdapter) ReadVisibleAfterSeq(ctx context.Context, reader channel.Reader, seq int64, limit int) ([]storespec.StoredRow, int64, error) {
	return a.home.View().ReadVisibleAfterSeq(ctx, reader, seq, limit)
}
func (a viewAdapter) ActorFacts(ctx context.Context, id actor.ActorID) (channelspec.ActorFacts, bool, error) {
	return a.home.View().ActorFacts(ctx, id)
}

func (a viewAdapter) IsBound(ctx context.Context, id string) (bool, error) {
	return a.home.View().IsBound(ctx, id)
}
