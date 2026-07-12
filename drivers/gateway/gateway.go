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
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// ErrGatewayClosed refuses an Attach after the gateway has begun关站 (Close): a
// still-arriving connection gets an unavailable-class refusal, never a session that
// could touch a closing Home (关站序: gateway 先静默 before Home).
var ErrGatewayClosed = errors.New("gateway: closed")

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
	closed  bool // set by Close; a later Attach is refused (关站序 straddle, P0-4)
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
// north-face lanes, presence aggregated per-identity across channels), its
// per-identity slot, and its频道臂集 arms — one arm per (subject × channel) (design
// §5.6: user 件死=全臂同死；membership 撤销=仅此一臂死, so the entry holds Σ_channel
// arms, not a single one). Born on the first device's认证成功 attach (两相: a failed
// auth leaves NO entry — 零残账); dies on the last device out ∨ gateway Close.
// Presence首入末出 is per-identity (the device set aggregates across all channel
// arms — 多频道臂 does not change it).
type userEntry struct {
	subjectID actor.ActorID
	slot      *platform.SubjectSlot
	arms      map[channel.ID]*channelArm
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
	g.closed = true
	var arms []*channelArm
	for _, e := range g.entries {
		for _, a := range e.arms {
			arms = append(arms, a)
		}
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

// onRevoked seals PRECISELY the subject's (channel) 频道臂 (membership/ACL revocation,
// §5.5 臂死亡触发②③: 仅此一臂死, never误杀 the subject's other channels' arms), then
// drops it so a fresh attach rebinds a live arm. A subject with no live entry / no arm
// for that channel is a no-op.
func (g *Gateway) onRevoked(ch channel.ID, subject actor.ActorID) {
	g.mu.Lock()
	var arm *channelArm
	if e := g.entries[subject]; e != nil {
		arm = e.arms[ch]
	}
	g.mu.Unlock()
	if arm != nil {
		arm.seal()
		g.dropArm(subject, ch, arm)
	}
}

// ensureArm gets-or-creates the subject's user件 AND its (channel) 频道臂 under the
// gateway lock. The entry pre-exists in the map so a later attach for the same
// subject (multi-tab / another channel) reuses it (device aggregation) rather than
// minting a rival件; the arm is keyed by channel so distinct channels never share a
// binding世代 (跨频道 arm 串线 fix, P0-1). Refuses after Close (P0-4 straddle: taken
// under the same lock Close sets closed).
func (g *Gateway) ensureArm(home *platform.Home, chID channel.ID, subjectID actor.ActorID, slot *platform.SubjectSlot) (*userEntry, *channelArm, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, nil, ErrGatewayClosed
	}
	e := g.entries[subjectID]
	if e == nil {
		e = &userEntry{
			subjectID: subjectID,
			slot:      slot,
			arms:      map[channel.ID]*channelArm{},
			devices:   map[*Session]struct{}{},
		}
		g.entries[subjectID] = e
	}
	arm := e.arms[chID]
	if arm == nil {
		arm = newChannelArm(home, chID, subjectID, slot)
		arm.leaked = &g.leaked
		arm.leakMu = &g.leakMu
		e.arms[chID] = arm
	}
	return e, arm, nil
}

// dropArm removes a sealed (channel) arm from the subject's entry IFF it is still the
// map's current one — a fresh attach that already rebound a live arm is never touched
// (旧臂晚删摘不掉新臂). The entry itself is retired by removeDevice (末出); a dropped arm
// with no more devices leaves the entry to be retired there.
func (g *Gateway) dropArm(subjectID actor.ActorID, chID channel.ID, arm *channelArm) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e := g.entries[subjectID]; e != nil && e.arms[chID] == arm {
		delete(e.arms, chID)
	}
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
