package gateway

import (
	"context"
	"time"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// AccessClass is a principal's access to one channel (连接模型勘误期 §3.2 解析面):
// a member writes + reads + emits presence; an observer (workspace 观众) only reads.
type AccessClass string

const (
	AccessMember   AccessClass = "member"
	AccessObserver AccessClass = "observer"
)

// Route is one channel a principal is currently entitled to (spec §3.2 解析面
// 输出定形): the channel id, the Home handle that serves its log/signal, the
// access class, the member's per-channel subject id (member only — empty for an
// observer), and the wall-clock at which this fact was checked (lease anchor).
type Route struct {
	Channel   channel.ID
	Home      *home.Home
	Access    AccessClass
	SubjectID actor.ActorID // member only; zero value for observer
	CheckedAt time.Time
}

// ChannelFailure carries a per-channel resolution failure (spec §3.2 部分失败
// 语义): "查得坏消息" for exactly this channel. The session keeps serving it from
// the last good snapshot within the T_stale lease, then pauses it — distinct from
// a channel simply absent from routes (= confirmed no eligibility, retire now).
type ChannelFailure struct {
	Channel channel.ID
	Err     error
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
	Snapshot(ctx context.Context, principal string) (routes []Route, failed []ChannelFailure, err error)
}

// ResolverFunc adapts a bare function to EntitlementResolver (like http.HandlerFunc).
type ResolverFunc func(ctx context.Context, principal string) ([]Route, []ChannelFailure, error)

// Snapshot implements EntitlementResolver.
func (f ResolverFunc) Snapshot(ctx context.Context, principal string) ([]Route, []ChannelFailure, error) {
	return f(ctx, principal)
}
