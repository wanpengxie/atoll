// Package gateway is the human ingress driver's home (design
// humancell-gateway-design-v2.md §5.3): the one thick component that swallows the
// external world's dirt — auth sessions, multi-tab arbitration, reconnect storms,
// cross-connector session aggregation, binding management — and standardises it
// into the channel frame protocol. It has ZERO channel write/action capability
// (reads are a controlled流句柄, writes go through the subject's own cell via the
// per-identity slot); the pen never leaves the wall.
//
// The十字 (design §5.5): gateway = Σ_user( Σ_channel 频道臂 × Σ_device lane ). A
// user件 (userEntry) is the十字路口 — its south face is the频道臂 (channelArm: the
// binding write-half + read流句柄 lifecycle), its north face the瞬时 lane集 (活连接
// 的 out-queues, 断开即蒸发). connector = 树外方言车间 (drivers/gateway/connector),
// holds no state, no游标.
//
// Fence (archtest drivers_confinement_test.go): drivers/* may import only the
// lib/protocol/runtime + platform export faces + registry; nobody imports
// drivers/* except the assembly root cmd/*. The gateway reaches app-side policy
// (routing) and the two revocation emit points through injected seams the assembly
// root wires (Routing / RevocationSource), never by importing app.
package gateway

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Routing is the app-domain routing-resolution面 the assembly root injects (design
// §5.3: routing政策留 app). Given a raw submit intent with a (possibly empty)
// audience, it resolves the concrete audience + kind (default_agent / boost floor /
// group broadcast). A per-request routing condition (no reachable brain) comes back
// as a non-empty retryable detail → the gateway maps it to an unavailable error
// frame (never written as truth). err is a genuine internal failure.
type Routing func(ctx context.Context, chID channel.ID, audienceIn []actor.ActorID, kindIn message.Kind) (audience []actor.ActorID, kind message.Kind, retryable string, err error)

// Config configures the Gateway (assembly-root injected).
type Config struct {
	// Epoch is the gateway epoch stamped on every presence level (design §5.4).
	// A fresh process = a fresh epoch (new epoch先撤销旧证词再快照 at the slot). 0 →
	// a process-lifetime constant is chosen by New.
	Epoch int64
	// Routing is the app-domain routing resolver (see Routing). nil → submit
	// frames with an empty audience are refused (unavailable) rather than写黑洞.
	Routing Routing
	// Revocation is the read-side revocation feed (design §5.5 臂死亡触发②③). nil →
	// no live revocation (reconnect re-auth is the only gate); the read-side每批
	// reader recheck remains the membership backstop.
	Revocation RevocationSource
	Logger     *slog.Logger
}

// Gateway is the human ingress component (one per process). It owns the session
// cross (user entries keyed by subject id) + the injected routing/revocation seams.
type Gateway struct {
	epoch      int64
	routing    Routing
	revocation RevocationSource
	logger     *slog.Logger

	mu      sync.Mutex
	entries map[actor.ActorID]*userEntry

	// edgeSeq is the gateway-global presence edge cursor. It MUST outlive an
	// entry's recreation: a reconnect mints a fresh userEntry, but the slot still
	// remembers the last edgeSeq at this epoch — so a per-entry counter reset would
	// make the re-online edge (a lower edgeSeq) get dedup-dropped and presence never
	// recover. A global monotonic counter keeps every publish strictly increasing;
	// the current-entry straddle guard (removeDevice) already ensures only the live
	// entry ever publishes, so time-ordered edgeSeq is correct.
	edgeSeq atomic.Int64

	leakMu sync.Mutex
	leaked int64
}

// userEntry is the十字路口 for one subject (§5.6 user件 column): its device set (the
// north-face lanes), its presence edge cursor, and its single频道臂 (today one
// connection = one channel, so the arm is 1:1 with the entry). Born on the first
// device's认证成功 attach (两相: a failed auth leaves NO entry — 零残账); dies on the
// last device out ∨ gateway Close.
type userEntry struct {
	subjectID actor.ActorID
	chID      channel.ID
	slot      *platform.SubjectSlot
	arm       *channelArm
	devices   map[*Session]struct{}
}

// New constructs the gateway. Dependencies arrive via Config (assembly root).
func New(cfg Config) *Gateway {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	epoch := cfg.Epoch
	if epoch == 0 {
		epoch = defaultEpoch()
	}
	return &Gateway{
		epoch:      epoch,
		routing:    cfg.Routing,
		revocation: cfg.Revocation,
		logger:     logger,
		entries:    map[actor.ActorID]*userEntry{},
	}
}

// Start brings the gateway up: it subscribes to the revocation feed so a
// membership/ACL revocation seals the subject's频道臂.
func (g *Gateway) Start() error {
	if g.revocation != nil {
		g.revocation.SubscribeRevoked(g.onRevoked)
	}
	return nil
}

// Close tears the gateway down. 关站序 (design §5.5 / DoD-9): the gateway goes
// silent BEFORE Home — every arm is sealed (read pumps stop, slot testimony
// Forgotten, sessions torn down) so no still-live session can touch a closing
// Home. Idempotent.
func (g *Gateway) Close() error {
	g.mu.Lock()
	arms := make([]*channelArm, 0, len(g.entries))
	for _, e := range g.entries {
		arms = append(arms, e.arm)
	}
	g.mu.Unlock()
	for _, a := range arms {
		a.seal()
	}
	return nil
}

// LeakedPumps reports how many read pumps were abandoned响亮 (未 join before the
// seal budget ArmSealJoinTimeout) over this gateway's lifetime (DoD-5 泄漏计数).
func (g *Gateway) LeakedPumps() int64 {
	g.leakMu.Lock()
	defer g.leakMu.Unlock()
	return g.leaked
}

// onRevoked seals the subject's频道臂 (membership/ACL revocation, §5.5 臂死亡触发②③).
// A subject with no live entry is a no-op (nothing to seal).
func (g *Gateway) onRevoked(ch channel.ID, subject actor.ActorID) {
	g.mu.Lock()
	e := g.entries[subject]
	g.mu.Unlock()
	if e != nil {
		e.arm.seal()
	}
}

// ensureEntry gets-or-creates the subject's user件 under the gateway lock. The
// entry pre-exists in the map so a later attach for the same subject (multi-tab)
// reuses it (device aggregation) rather than minting a rival件.
func (g *Gateway) ensureEntry(home *platform.Home, chID channel.ID, subjectID actor.ActorID, slot *platform.SubjectSlot) *userEntry {
	g.mu.Lock()
	defer g.mu.Unlock()
	e := g.entries[subjectID]
	if e == nil {
		arm := newChannelArm(home, chID, subjectID, slot)
		arm.leaked = &g.leaked
		arm.leakMu = &g.leakMu
		e = &userEntry{
			subjectID: subjectID,
			chID:      chID,
			slot:      slot,
			arm:       arm,
			devices:   map[*Session]struct{}{},
		}
		g.entries[subjectID] = e
	}
	return e
}

// addDevice registers one session on the entry's device set and, on the FIRST
// device (首入), writes the online level into the slot (presence session
// accounting — the refcount reborn in the会话账). edgeSeq strictly increases per
// entry so the slot dedups replays.
func (g *Gateway) addDevice(e *userEntry, s *Session) {
	g.mu.Lock()
	defer g.mu.Unlock()
	first := len(e.devices) == 0
	e.devices[s] = struct{}{}
	if first && e.slot != nil {
		e.slot.PublishLevel(g.epoch, g.edgeSeq.Add(1), platform.LevelOnline)
	}
}

// removeDevice drops one session from the entry and, on the LAST device out
// (末出), writes the explicit offline level and retires the entry. 旧件晚删摘不掉
// 新件 (DoD-11): a teardown whose entry is no longer the map's current one (a
// superseded件 / double-close) only drops its own bookkeeping — it never touches
// the live entry's slot or presence.
func (g *Gateway) removeDevice(e *userEntry, s *Session) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.entries[e.subjectID] != e {
		delete(e.devices, s) // superseded entry — never touch the current one
		return
	}
	if _, ok := e.devices[s]; !ok {
		return
	}
	delete(e.devices, s)
	if len(e.devices) == 0 {
		if e.slot != nil {
			e.slot.PublishLevel(g.epoch, g.edgeSeq.Add(1), platform.LevelOffline)
		}
		delete(g.entries, e.subjectID)
	}
}

// entryFor returns the subject's live entry (test/assertion helper).
func (g *Gateway) entryFor(subjectID actor.ActorID) (*userEntry, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	e, ok := g.entries[subjectID]
	return e, ok
}
