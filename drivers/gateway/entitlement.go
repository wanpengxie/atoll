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
// class to read. Observer traffic rides the per-channel SSE/HTTP read plane and
// never enters this route set, which is what the resolver's own contract says
// (app.EntitlementRoute). A gateway-level observer would be a different thing
// than a member with a narrower field: it has no subject id, so it can hold no
// presence slot and drive no business frame, and every consumer here would have
// to be told about it. Give it its own shape when something actually needs one.
type Route struct {
	Channel   channel.ID
	Bundle    channelhost.Bundle
	SubjectID actor.ActorID
}

// EntitlementResolver is the app-domain seam (injected by the assembly root, spec
// §3.2 EntitlementResolver 注入缝): given a principal it returns the full set of
// channels that principal is currently entitled to (member ∪ observer), the
// per-channel failures, and an err for a whole-snapshot failure. The interface is
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
