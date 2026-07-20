package channelhost

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
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
	PlanForDaemon(context.Context, string) ([]platform.PlanActor, error)
	KickDaemon(string) int
}

type SysOp interface {
	Admit(context.Context, channel.AdmitRequest) (channel.AdmitResult, error)
	Introduce(context.Context, channel.IntroduceRequest) (channel.IntroduceResult, error)
	AttachDaemon(context.Context, channel.DaemonRequest) (channel.BindingResult, error)
	DetachDaemon(context.Context, channel.DaemonRequest) (channel.BindingResult, error)
	ApplyDeclVersion(context.Context, channel.ApplyDeclVersionRequest) (channel.ApplyDeclVersionResult, error)
	RevokeDeclTargets(context.Context, channel.RevokeDeclRequest) (channel.RevokeResult, error)
	RevokeDaemon(context.Context, channel.DaemonRequest) (channel.RevokeResult, error)
}

type View interface {
	DefaultAgent(context.Context) (actor.ActorID, bool, error)
	DeclaredByPrincipal(context.Context, string) (storespec.ActorControlRow, bool, error)
	DeclaredBySource(context.Context, string) ([]storespec.ActorControlRow, error)
	DeclarationVersions(context.Context, actor.ActorID) (storespec.ActorControlRow, storespec.ActorControlRow, error)
	ActiveActors(context.Context) ([]storespec.ActorControlRow, error)
	ResolvePrincipal(context.Context, actor.Kind, string) (actor.ActorID, bool, error)
	DaemonObligationCounts(context.Context, string) (int, int, int, error)
	OwnerPrincipal(context.Context) (string, bool, error)
	ReadAfterSeq(context.Context, int64, int) ([]storespec.StoredRow, error)
	MaxSeq(context.Context) (int64, error)
	ListActors(context.Context) ([]storespec.Record, error)
	Stat(actor.ActorID) (time.Time, bool)
	IsAttached(string) bool
}

type bundle struct {
	home       *home.Home
	generation uint64
}

func (b *bundle) Generation() uint64    { return b.generation }
func (b *bundle) Gateway() GatewayHitch { return gatewayAdapter{b.home} }
func (b *bundle) Daemon() DaemonLink    { return daemonAdapter{b.home} }
func (b *bundle) SysOp() SysOp          { return unavailableSysOp{} }
func (b *bundle) View() View            { return viewAdapter{b.home} }

type gatewayAdapter struct{ home *home.Home }

func (a gatewayAdapter) SubjectSlotFor(id actor.ActorID) (*subjectgate.Slot, bool) {
	return a.home.SubjectSlotFor(id)
}
func (a gatewayAdapter) Subscribe() (<-chan struct{}, func()) { return a.home.Subscribe() }

type daemonAdapter struct{ home *home.Home }

func (a daemonAdapter) ServeAttach(w http.ResponseWriter, r *http.Request, daemon string) {
	a.home.ServeAttach(w, r, daemon)
}
func (a daemonAdapter) PlanForDaemon(ctx context.Context, daemon string) ([]platform.PlanActor, error) {
	return a.home.PlanForDaemon(ctx, daemon)
}
func (a daemonAdapter) KickDaemon(daemon string) int { return a.home.KickDaemon(daemon) }

type viewAdapter struct{ home *home.Home }

func (a viewAdapter) DefaultAgent(ctx context.Context) (actor.ActorID, bool, error) {
	return a.home.DefaultAgent(ctx)
}
func (a viewAdapter) DeclaredByPrincipal(ctx context.Context, p string) (storespec.ActorControlRow, bool, error) {
	return a.home.DeclaredByPrincipal(ctx, p)
}
func (a viewAdapter) DeclaredBySource(ctx context.Context, d string) ([]storespec.ActorControlRow, error) {
	return a.home.DeclaredBySource(ctx, d)
}
func (a viewAdapter) DeclarationVersions(ctx context.Context, id actor.ActorID) (storespec.ActorControlRow, storespec.ActorControlRow, error) {
	return a.home.DeclarationVersions(ctx, id)
}
func (a viewAdapter) ActiveActors(ctx context.Context) ([]storespec.ActorControlRow, error) {
	return a.home.ActiveActors(ctx)
}
func (a viewAdapter) ResolvePrincipal(ctx context.Context, kind actor.Kind, principal string) (actor.ActorID, bool, error) {
	return a.home.ResolvePrincipal(ctx, kind, principal)
}
func (a viewAdapter) DaemonObligationCounts(ctx context.Context, daemon string) (int, int, int, error) {
	return a.home.DaemonObligationCounts(ctx, daemon)
}
func (a viewAdapter) OwnerPrincipal(ctx context.Context) (string, bool, error) {
	return a.home.View().OwnerPrincipal(ctx)
}
func (a viewAdapter) ReadAfterSeq(ctx context.Context, seq int64, limit int) ([]storespec.StoredRow, error) {
	return a.home.View().ReadAfterSeq(ctx, seq, limit)
}
func (a viewAdapter) MaxSeq(ctx context.Context) (int64, error) { return a.home.View().MaxSeq(ctx) }
func (a viewAdapter) ListActors(ctx context.Context) ([]storespec.Record, error) {
	return a.home.View().ListActors(ctx)
}
func (a viewAdapter) Stat(id actor.ActorID) (time.Time, bool) { return a.home.View().Stat(id) }
func (a viewAdapter) IsAttached(id string) bool               { return a.home.View().IsAttached(id) }

var errSysOpUnavailable = errors.New("channelhost: sysop not assembled")

type unavailableSysOp struct{}

func (unavailableSysOp) Admit(context.Context, channel.AdmitRequest) (channel.AdmitResult, error) {
	return channel.AdmitResult{}, errSysOpUnavailable
}
func (unavailableSysOp) Introduce(context.Context, channel.IntroduceRequest) (channel.IntroduceResult, error) {
	return channel.IntroduceResult{}, errSysOpUnavailable
}
func (unavailableSysOp) AttachDaemon(context.Context, channel.DaemonRequest) (channel.BindingResult, error) {
	return channel.BindingResult{}, errSysOpUnavailable
}
func (unavailableSysOp) DetachDaemon(context.Context, channel.DaemonRequest) (channel.BindingResult, error) {
	return channel.BindingResult{}, errSysOpUnavailable
}
func (unavailableSysOp) ApplyDeclVersion(context.Context, channel.ApplyDeclVersionRequest) (channel.ApplyDeclVersionResult, error) {
	return channel.ApplyDeclVersionResult{}, errSysOpUnavailable
}
func (unavailableSysOp) RevokeDeclTargets(context.Context, channel.RevokeDeclRequest) (channel.RevokeResult, error) {
	return channel.RevokeResult{}, errSysOpUnavailable
}
func (unavailableSysOp) RevokeDaemon(context.Context, channel.DaemonRequest) (channel.RevokeResult, error) {
	return channel.RevokeResult{}, errSysOpUnavailable
}
