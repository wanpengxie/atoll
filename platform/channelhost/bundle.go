package channelhost

import (
	"context"
	"net/http"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type Bundle interface {
	Generation() uint64
	Gateway() GatewayHitch
	Daemon() DaemonLink
	SysOp() SysOp
	View() View
}

type GatewayHitch interface {
	SubjectSlotFor(actor.ActorID) (*subjectgate.Slot, bool)
	Subscribe() (<-chan struct{}, func())
}

type DaemonLink interface {
	ServeAttach(http.ResponseWriter, *http.Request, string)
}

type SysOp interface {
	Admit(context.Context, channel.AdmitRequest) (channel.AdmitResult, error)
	Introduce(context.Context, channel.IntroduceRequest) (channel.IntroduceResult, error)
	Remove(context.Context, channel.RemoveRequest) (channel.RemoveResult, error)
	AttachDaemon(context.Context, channel.DaemonRequest) (channel.BindingResult, error)
	DetachDaemon(context.Context, channel.DaemonRequest) (channel.BindingResult, error)
}

type View interface {
	DefaultAgent(context.Context) (actor.ActorID, bool, error)
	DeclaredBySource(context.Context, string) ([]storespec.ActorControlRow, error)
	ActiveActors(context.Context) ([]storespec.ActorControlRow, error)
	ResolvePrincipal(context.Context, actor.Kind, string) (actor.ActorID, bool, error)
	OwnerPrincipal(context.Context) (string, bool, error)
	ReadVisibleAfterSeq(context.Context, channel.Reader, int64, int) ([]storespec.StoredRow, int64, error)
	ActorFacts(context.Context, actor.ActorID) (channel.ActorFacts, bool, error)
	IsAttached(string) bool
	IsBound(context.Context, string) (bool, error)
	Resources() ResourceReadView
}

type ResourceReadView interface {
	List(context.Context, channel.Reader, channel.ResourceListQuery) (channel.ResourcePage, error)
	Stat(context.Context, channel.Reader, resource.ResourceID) (channel.ResourceMeta, error)
	Fetch(context.Context, channel.Reader, resource.ResourceID) (channel.ResourceFetch, error)
}

type bundle struct {
	home       *home.Home
	sysOp      SysOp
	generation uint64
}

func (b *bundle) Generation() uint64    { return b.generation }
func (b *bundle) Gateway() GatewayHitch { return gatewayAdapter{b.home} }
func (b *bundle) Daemon() DaemonLink    { return daemonAdapter{b.home} }
func (b *bundle) SysOp() SysOp          { return b.sysOp }
func (b *bundle) View() View            { return viewAdapter{b.home} }

type gatewayAdapter struct{ home *home.Home }

func (a gatewayAdapter) SubjectSlotFor(id actor.ActorID) (*subjectgate.Slot, bool) {
	return home.GatewaySlot(a.home, id)
}
func (a gatewayAdapter) Subscribe() (<-chan struct{}, func()) { return home.GatewaySubscribe(a.home) }

type daemonAdapter struct{ home *home.Home }

func (a daemonAdapter) ServeAttach(w http.ResponseWriter, r *http.Request, daemon string) {
	home.LinkServe(a.home, w, r, daemon)
}

type viewAdapter struct{ home *home.Home }

func (a viewAdapter) DefaultAgent(ctx context.Context) (actor.ActorID, bool, error) {
	return a.home.View().DefaultAgent(ctx)
}
func (a viewAdapter) DeclaredBySource(ctx context.Context, d string) ([]storespec.ActorControlRow, error) {
	return a.home.View().DeclaredBySource(ctx, d)
}
func (a viewAdapter) ActiveActors(ctx context.Context) ([]storespec.ActorControlRow, error) {
	return a.home.View().ActiveActors(ctx)
}
func (a viewAdapter) ResolvePrincipal(ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, bool, error) {
	return a.home.View().ResolvePrincipal(ctx, kind, principal)
}
func (a viewAdapter) OwnerPrincipal(ctx context.Context) (string, bool, error) {
	return a.home.View().OwnerPrincipal(ctx)
}
func (a viewAdapter) ReadVisibleAfterSeq(ctx context.Context, reader channel.Reader, seq int64, limit int) ([]storespec.StoredRow, int64, error) {
	return a.home.View().ReadVisibleAfterSeq(ctx, reader, seq, limit)
}
func (a viewAdapter) ActorFacts(ctx context.Context, id actor.ActorID) (channel.ActorFacts, bool, error) {
	return a.home.View().ActorFacts(ctx, id)
}

func (a viewAdapter) IsAttached(id string) bool { return a.home.View().IsAttached(id) }
func (a viewAdapter) IsBound(ctx context.Context, id string) (bool, error) {
	return a.home.View().IsBound(ctx, id)
}
func (a viewAdapter) Resources() ResourceReadView {
	return resourceViewAdapter{a.home.View().Resources()}
}

type resourceViewAdapter struct{ view home.ResourceView }

func (a resourceViewAdapter) List(ctx context.Context, as channel.Reader, q channel.ResourceListQuery) (channel.ResourcePage, error) {
	return a.view.List(ctx, as, q)
}
func (a resourceViewAdapter) Stat(ctx context.Context, as channel.Reader, id resource.ResourceID) (channel.ResourceMeta, error) {
	return a.view.Stat(ctx, as, id)
}
func (a resourceViewAdapter) Fetch(ctx context.Context, as channel.Reader, id resource.ResourceID) (channel.ResourceFetch, error) {
	return a.view.Fetch(ctx, as, id)
}
