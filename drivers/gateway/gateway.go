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
	"time"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/platform/subjectgate"
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

	// closeOnce runs the teardown (seal every arm + join all pumps) EXACTLY once.
	// sync.Once.Do blocks every concurrent caller until the one teardown returns, so
	// a second Close never observes closed and races ahead while the first is still
	// mid-seal/join — all Close callers return only after the SAME teardown completes
	// (修复批四轮 P1: 并发 Close 统一等 teardown).
	closeOnce sync.Once

	// closeEntered is a TEST-ONLY seam (nil in production): invoked at the top of every
	// Close BEFORE closeOnce.Do, so a test can count/handshake how many goroutines have
	// entered Close and prove a second concurrent caller is parked on the single teardown
	// (修复批六轮 P2: 并发 Close 屏障握手, not a sleep竞猜).
	closeEntered func()

	// ctx is the gateway-lifetime context; Close cancels it so tail-only sessions
	// (which own no arm to seal) still go silent BEFORE Home — their read pumps
	// derive from it (关站序 tail closure, P1). Member sessions are cancelled via
	// their arm's seal; tail-only sessions have no arm, so this ctx is their edge.
	ctx    context.Context
	cancel context.CancelFunc

	// tailPumps joins the tail-only read pumps at Close (symmetric with the member
	// path's per-arm join). A tail pump's wg.Add is taken in beginFeed under the SAME
	// closed re-check as a member's arm.track, so Close either observes the pump (and
	// waits it out) or has already set closed (and the late pump is refused).
	tailPumps sync.WaitGroup

	// bindingSeq is the gateway-global绑定世代 source shared into every arm. It MUST
	// outlive an arm's rebuild (ABA fix, P0-1): drawing每 fresh binding's gen from one
	// monotonic counter makes the绑定世代 strictly increase per (subject,channel)
	// across seal→rebind, so a stale in-flight frame is distinguishable from the new
	// binding at the slot's delivery linearization point (see arm.nextGen / slot.Deliver).
	bindingSeq atomic.Int64

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
	slot      *subjectgate.Slot
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
	ctx, cancel := context.WithCancel(context.Background())
	return &Gateway{
		epoch:      epoch,
		routing:    cfg.Routing,
		revocation: cfg.Revocation,
		logger:     logger,
		entries:    map[actor.ActorID]*userEntry{},
		ctx:        ctx,
		cancel:     cancel,
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
	if g.closeEntered != nil {
		g.closeEntered() // test seam: a caller has entered Close (before the Once gate)
	}
	// sync.Once.Do blocks concurrent callers until the one teardown returns, so EVERY
	// Close (first or Nth, concurrent or serial) returns only after the arms are all
	// sealed and every pump joined — a second Close can never return while the first is
	// still mid-seal/join (修复批四轮 P1).
	started := time.Now()
	g.closeOnce.Do(func() {
		g.mu.Lock()
		g.closed = true
		g.cancel() // tail-only sessions' ctx cascade → their read pumps unblock
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
		// Join the tail-only pumps too (the member pumps were joined per-arm by seal).
		// They own no arm/Home resource beyond the read流句柄 and unblock on the cancel
		// above, so a plain join suffices — 设计 §5.5 defers only裸观众撤权, never关站静默.
		g.tailPumps.Wait()
		g.logger.Info("platform.gateway.closed", "leaked_pumps", g.LeakedPumps(),
			"duration", time.Since(started))
	})
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

// ensureArmLocked gets-or-creates the subject's user件 AND its (channel) 频道臂; the
// caller holds g.mu and has verified !g.closed. The entry pre-exists in the map so a
// later attach for the same subject (multi-tab / another channel) reuses it (device
// aggregation) rather than minting a rival件; the arm is keyed by channel so distinct
// channels never share a binding世代 (跨频道 arm 串线 fix, P0-1). Returns the GRANTED
// binding gen: a freshly-built arm advances its gen once (a new binding); an
// already-live arm hands back its current gen UNCHANGED — a second device joining a
// live arm shares the binding, never推进 it, so it can never stale a co-arm tab
// (多设备同时合法, §5.6 恢复差异).
func (g *Gateway) ensureArmLocked(home *home.Home, chID channel.ID, subjectID actor.ActorID, slot *subjectgate.Slot) (*userEntry, *channelArm, int64) {
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
		arm = newChannelArm(home, chID, subjectID, slot, &g.bindingSeq)
		arm.leaked = &g.leaked
		arm.leakMu = &g.leakMu
		e.arms[chID] = arm
		return e, arm, arm.nextGen() // fresh binding: advance once
	}
	return e, arm, arm.currentGen() // join a live binding: share the gen
}

// ensureArm is the locked test seam over ensureArmLocked (refuses after Close, P0-4
// straddle). Production attach goes through seatMember (which fuses the closed gate
// with device seating).
func (g *Gateway) ensureArm(home *home.Home, chID channel.ID, subjectID actor.ActorID, slot *subjectgate.Slot) (*userEntry, *channelArm, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, nil, ErrGatewayClosed
	}
	e, arm, _ := g.ensureArmLocked(home, chID, subjectID, slot)
	return e, arm, nil
}

// seatMember is the member attach's single critical section (P0-4 straddle closure):
// the closed gate, arm ensure + gen grant, AND device seating (末入 online publish)
// all land under ONE g.mu hold, so a concurrent Close can never straddle between the
// closed check and the device landing — it either wins the lock (closed → refuse, zero
// residual) or seats fully and Close then seals the arm it can see. Returns the granted
// binding gen. On refusal nothing is written (no half-open entry, no orphan device).
func (g *Gateway) seatMember(home *home.Home, chID channel.ID, subjectID actor.ActorID, slot *subjectgate.Slot, s *Session) (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return 0, ErrGatewayClosed
	}
	e, arm, gen := g.ensureArmLocked(home, chID, subjectID, slot)
	s.entry = e
	s.arm = arm
	g.addDeviceLocked(e, s)
	return gen, nil
}

// seatTailOnly is the tail-only (non-member observer) attach's closed-gated critical
// section (P1 straddle closure, symmetric with seatMember): the closed gate AND the
// ctx derivation land under ONE g.mu hold, so a concurrent Close either refuses (closed
// → ErrGatewayClosed, zero residual) or the tail session's ctx derives from the live
// gateway ctx and Close's cancel then reaches it. A tail-only session owns no arm/slot/
// device (看得见≠在里面) — its only substrate coupling is the read流句柄, gated here.
func (g *Gateway) seatTailOnly() (context.Context, context.CancelFunc, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, nil, ErrGatewayClosed
	}
	ctx, cancel := context.WithCancel(g.ctx)
	return ctx, cancel, nil
}

// beginFeed admits the read pump under the gateway lock's closed re-check (P0-4
// straddle other half): the pump's join-track (wg.Add) is taken HERE, in the SAME
// critical section that reads closed, so Close either observes the track (and its
// join waits the pump out) or has already set closed (and the pump is refused). A
// member pump tracks on its arm (seal joins it); a tail-only pump has no arm, so it
// tracks on the gateway's tailPumps set (Close joins it) — both refused after closed
// so neither reads a closing Home. Returns false → the caller tears the session down.
func (g *Gateway) beginFeed(s *Session) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	if s.arm != nil {
		s.arm.track()
	} else {
		g.tailPumps.Add(1)
	}
	return true
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
	g.addDeviceLocked(e, s)
}

// addDeviceLocked seats one device (caller holds g.mu). Fused into seatMember's
// critical section so device landing is atomic with the closed gate.
func (g *Gateway) addDeviceLocked(e *userEntry, s *Session) {
	first := len(e.devices) == 0
	e.devices[s] = struct{}{}
	if first && e.slot != nil {
		e.slot.PublishLevel(g.epoch, g.edgeSeq.Add(1), subjectgate.LevelOnline)
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
			e.slot.PublishLevel(g.epoch, g.edgeSeq.Add(1), subjectgate.LevelOffline)
		}
		delete(g.entries, e.subjectID)
	}
	// Summarize this device's own lane drops at teardown — not per-push (P3):
	// a lane that never filled logs nothing.
	if s.lane != nil {
		if dropped := s.lane.DroppedCount(); dropped > 0 {
			g.logger.Info("platform.gateway.lane_dropped", "subject", string(e.subjectID),
				"dropped", dropped)
		}
	}
}

// entryFor returns the subject's live entry (test/assertion helper).
func (g *Gateway) entryFor(subjectID actor.ActorID) (*userEntry, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	e, ok := g.entries[subjectID]
	return e, ok
}
