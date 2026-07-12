package gateway

import (
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// RevocationSource is the gateway's read-side revocation feed (build spec §S3 /
// design §5.5 臂死亡触发②③). The gateway subscribes; the assembly root (cmd/server)
// implements it and feeds two emit points into it:
//   ① platform membership撤销 = Home.Remove dereg cascade (HomeConfig.OnRevoke);
//   ② app ACL撤销 = workspace member-change write point (workspace_members delete).
// A revoked (channel, subject) seals that subject's频道臂 — the read pump stops,
// the slot testimony is Forgotten, and未推 feed frames are purged (真相 never
// touched). Missed events degrade to the read-side每批 reader-resource recheck
// backstop (design §5.5).
//
// It is CONSUMED by drivers/gateway; the assembly root provides the implementation
// so drivers → app stays forbidden (the two emit points live in platform and app).
type RevocationSource interface {
	SubscribeRevoked(fn func(ch channel.ID, subject actor.ActorID))
}

// RevocationHub is the assembly-root-owned fan-in the two emit points feed and the
// gateway subscribes to. It lives in this package (the natural owner of the
// contract) so cmd/server constructs it, hands Emit to platform (via
// HomeConfig.OnRevoke) and app (the workspace write point), and hands the hub
// itself to the gateway as its RevocationSource — bridging both emitters without
// drivers importing app.
type RevocationHub struct {
	mu   sync.Mutex
	subs []func(ch channel.ID, subject actor.ActorID)
}

// NewRevocationHub builds an empty hub.
func NewRevocationHub() *RevocationHub { return &RevocationHub{} }

// SubscribeRevoked registers a revocation handler (the gateway's arm-seal).
func (h *RevocationHub) SubscribeRevoked(fn func(ch channel.ID, subject actor.ActorID)) {
	h.mu.Lock()
	h.subs = append(h.subs, fn)
	h.mu.Unlock()
}

// Emit fans one revocation out to every subscriber. Both emit points call this:
// platform's Home.Remove (membership) and the app's workspace member-change point
// (ACL).
func (h *RevocationHub) Emit(ch channel.ID, subject actor.ActorID) {
	h.mu.Lock()
	subs := make([]func(channel.ID, actor.ActorID), len(h.subs))
	copy(subs, h.subs)
	h.mu.Unlock()
	for _, fn := range subs {
		fn(ch, subject)
	}
}
