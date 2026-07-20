package gateway

import (
	"context"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// AccessClass is a principal's access to one channel (连接模型勘误期 §3.2 解析面):
// a member writes + reads + emits presence; an observer has a revocable, per-channel
// realm read capability and never inherits write or presence rights.
type AccessClass string

const (
	AccessMember   AccessClass = "member"
	AccessObserver AccessClass = "observer"
)

// Route is one channel a principal is currently entitled to (spec §3.2 解析面
// 输出定形): the channel id, the Home handle that serves its log/signal, the
// access class and the member's per-channel subject id (member only — empty for an
// observer). The gateway anchors leases when resolution completes on its own clock.
type Route struct {
	Channel   channel.ID
	Bundle    channelhost.Bundle
	Access    AccessClass
	SubjectID actor.ActorID // member only; zero value for observer
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
