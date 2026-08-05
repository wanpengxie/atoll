package gateway

import (
	"context"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// Route is one channel a principal is currently entitled to (spec §3.2 解析面
// 输出定形): the channel id, the Home handle that serves its log/signal, and the
// member's per-channel subject id. The gateway anchors leases when resolution
// completes on its own clock.
//
// A route is a MEMBERSHIP route, and that is the whole of it — there is no access
// class to read. Connection-local observation uses ObserverRoute instead of this
// membership set: it has no subject id, holds no presence slot, and cannot drive
// business frames.
type Route struct {
	Channel   channel.ID
	Bundle    channelhost.Bundle
	SubjectID actor.ActorID
}

// EntitlementResolver is the app-domain seam (injected by the assembly root, spec
// §3.2 EntitlementResolver 注入缝): given a principal it returns the full set of
// channels that principal holds MEMBERSHIP in. Temporary observations resolve
// independently through ObserverResolver. This method also returns per-channel
// failures and an err for a whole-snapshot failure. The interface is
// defined HERE (drivers/gateway) and implemented app-side, bridged through
// cmd/server — so drivers never imports app (archtest 围栏), mirroring the WSGateway/
// Routing seam shape.
//
// Contract (spec §3.2):
//   - routes = every entitled channel; a channel absent from routes AND from failed
//     = confirmed no eligibility → retire immediately (no lease).
//   - failed = per-channel query failure → serve from last good within T_stale, then
//     pause.
//   - err != nil = whole-snapshot failure → the entire prior snapshot rides its lease.
type EntitlementResolver interface {
	Snapshot(ctx context.Context, principal string) (routes []Route, failed []channel.ID, err error)
}

// ResolverFunc adapts a bare function to EntitlementResolver (like http.HandlerFunc).
type ResolverFunc func(ctx context.Context, principal string) ([]Route, []channel.ID, error)

// Snapshot implements EntitlementResolver.
func (f ResolverFunc) Snapshot(ctx context.Context, principal string) ([]Route, []channel.ID, error) {
	return f(ctx, principal)
}

// ObserverRoute is a connection-local read entitlement. Unlike Route it has
// no subject slot and can never drive upstream business frames.
type ObserverRoute struct {
	Channel channel.ID
	Bundle  channelhost.Bundle
	Reader  channel.Reader
}

// ObserverResolver evaluates app-owned public-observation policy without
// making gateway import app. reason is a stable wire reason when denied.
type ObserverResolver interface {
	ResolveObservation(ctx context.Context, principal string, channelID channel.ID) (route ObserverRoute, reason string, err error)
}

type ObserverResolverFunc func(context.Context, string, channel.ID) (ObserverRoute, string, error)

func (f ObserverResolverFunc) ResolveObservation(ctx context.Context, principal string, channelID channel.ID) (ObserverRoute, string, error) {
	return f(ctx, principal, channelID)
}
