// Package gateway is the human ingress driver's home (design
// humancell-gateway-design-v2.md §5.3, 连接模型勘误期 spec): the one thick component
// that swallows the external world's dirt — auth sessions, multi-tab arbitration,
// reconnect storms, cross-connector session aggregation — and standardises it into
// the channel frame protocol. It has ZERO channel write/action capability (reads
// are a controlled流句柄, writes go through each channel's subject cell via that
// channel's per-identity slot); the pen never leaves the wall.
//
// 连接即人 (spec §0/§1): a connection = an authenticated person + one pipe. The
// server records for one connection only three things — who you are (principal),
// where each channel's cursor sits (device-carried), and whether the pipe is alive.
// A connection subscribes to the实时动态 of ALL the person's合法频道 (户籍 ∪ 读资格,
// resolved live by the injected EntitlementResolver); "which window is open" is
// client内政 — the server has no such concept. There is NO client-controllable
// channel binding/subscription state (the臂/binding-generation axis was proven a
// false axis and整删).
//
// Two reconcile drivers (spec §3.2), each converging one object off the SAME truth
// (this person's channel set changed):
//   - 资格对账 = the per-session read pump's loop phase (subscriptions ← resolver);
//   - 在场对账 = one gateway-wide goroutine (presence coverage ← devices × member set).
//
// Fence (archtest drivers_confinement_test.go): drivers/* may import only the
// lib/protocol/runtime + platform export faces + registry; the gateway reaches
// app-side policy (Routing, EntitlementResolver) and the membership poke through
// injected seams the assembly root wires, never by importing app.
package gateway

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// ErrGatewayClosed refuses an Attach/StartFeed after Close has begun (关站序:
// gateway 先静默 before Home) — a still-arriving connection gets an
// unavailable-class refusal, never a session that could touch a closing Home.
var ErrGatewayClosed = errors.New("gateway: closed")

// Reconcile clocks (spec §3.2). Default values; each is INJECTABLE per-Gateway via
// Config (六轮 P1-5: a fast deterministic test needs to shrink T_sweep/T_presence
// without waiting the real 30s/5s backstop — a package const could never be shortened
// per-test-instance).
const (
	// defaultStale is the per-channel eligibility lease: after the last SUCCESSFUL
	// check, a query failure is tolerated this long before the channel is paused.
	defaultStale = 30 * time.Second
	// defaultSweep is the entitlement reconcile's periodic backstop (poke 为主, sweep
	// 兜底).
	defaultSweep = 30 * time.Second
	// defaultRead bounds every resolver / ReadAfterSeq call (no unbounded lock waits).
	defaultRead = 5 * time.Second
	// defaultPresence is the presence reconcile loop's periodic backstop.
	defaultPresence = 5 * time.Second
	// defaultPumpJoinTimeout bounds Gateway.Close's wait for every read pump to
	// self-report (§2.1 #11/#12, 五轮 P1-3 平移: the old ArmSealJoinTimeout's有界 join
	// budget, migrated to the unified session pump). A pump that misses this deadline
	// (e.g. a resolver that ignores its ctx and blocks forever) is counted as LEAKED —
	// Close logs it and proceeds rather than hanging indefinitely.
	defaultPumpJoinTimeout = 10 * time.Second
)

// Routing is the app-domain routing-resolution面 the assembly root injects (design
// §5.3: routing政策留 app). Given a raw submit intent with a (possibly empty)
// audience it resolves the concrete audience + kind. A per-request routing
// condition (no reachable brain) comes back as a non-empty retryable detail → the
// gateway maps it to an unavailable error frame (never written as truth). err is a
// genuine internal failure.
type Routing func(ctx context.Context, chID channel.ID, audienceIn []actor.ActorID, kindIn message.Kind) (audience []actor.ActorID, kind message.Kind, retryable string, err error)

// Clock is the gateway's single injected time source. Now anchors leases and
// telemetry; NewTimer arms the actual reconcile loops at an ABSOLUTE deadline. The
// absolute form is load-bearing: a scheduler pause between computing a remaining
// lease and arming a relative timer must not move the alarm past lastOK+T_stale.
type Clock interface {
	Now() time.Time
	NewTimer(deadline time.Time) Timer
}

// Timer is the one-shot alarm shape used by the entitlement and presence loops.
// Production wraps time.Timer; tests advance an injected clock and still drive the
// real loop/select path rather than calling reconcile by hand.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTimer(deadline time.Time) Timer {
	return systemTimer{timer: time.NewTimer(time.Until(deadline))}
}

type systemTimer struct{ timer *time.Timer }

func (t systemTimer) C() <-chan time.Time { return t.timer.C }
func (t systemTimer) Stop() bool          { return t.timer.Stop() }

// Config configures the Gateway (assembly-root injected).
type Config struct {
	// Epoch is the gateway epoch stamped on every presence level (design §5.4). A
	// fresh process = a fresh epoch. 0 → a process-lifetime constant chosen by New.
	Epoch int64
	// Routing is the app-domain routing resolver (see Routing). nil → submit frames
	// with an empty audience are refused (unavailable) rather than写黑洞.
	Routing Routing
	// Resolver is the app-domain entitlement面 (principal → 合法频道集). nil → the
	// presence loop does not run and every business frame is forbidden (no channel is
	// ever resolved eligible).
	Resolver EntitlementResolver
	// Clock drives both lease anchoring and the real sweep/presence alarms; nil →
	// the system wall clock.
	Clock  Clock
	Logger *slog.Logger

	// The four reconcile clocks (spec §3.2), each 0 → its documented default (see the
	// default* consts). A test shrinks these to observe the sweep/presence backstops
	// converge without a real 30s/5s wait (六轮 P1-5).
	StaleLease      time.Duration // per-channel eligibility lease (defaultStale)
	SweepInterval   time.Duration // entitlement reconcile periodic backstop (defaultSweep)
	ReadTimeout     time.Duration // resolver / ReadAfterSeq bound (defaultRead)
	PresenceTick    time.Duration // presence reconcile periodic backstop (defaultPresence)
	PumpJoinTimeout time.Duration // Close's bounded pump-join budget (defaultPumpJoinTimeout)
}

// covKey identifies one presence coverage cell: this principal, this channel.
type covKey struct {
	principal string
	channel   channel.ID
}

// covEntry is the presence对账圈's private current-state per covKey (spec §3.2
// 收敛对象乙): the slot the online testimony was published into, and the wall-clock
// of the last successful reconcile touch.
type covEntry struct {
	slot   *subjectgate.Slot
	lastOK time.Time
}

// Gateway is the human ingress component (one per process). It owns the device
// accounts (keyed by principal) + the injected routing/resolver/poke seams + the
// presence reconcile loop.
type Gateway struct {
	epoch    int64
	routing  Routing
	resolver EntitlementResolver
	clock    Clock
	logger   *slog.Logger

	// Reconcile clocks (spec §3.2), resolved from Config with defaults in New.
	tStale          time.Duration
	tSweep          time.Duration
	tRead           time.Duration
	tPresence       time.Duration
	pumpJoinTimeout time.Duration

	mu      sync.Mutex
	closed  bool // set by Close; a later Attach/beginFeed is refused (关站序 straddle)
	entries map[string]*userEntry

	// pumps joins every session read pump at Close (泵联结组合一, §2.1 #12): its
	// Add is taken in beginFeed under the SAME closed re-check as Close, so Close
	// either observes the pump (and joins it) or has already set closed (late pump
	// refused). Close's join on this WaitGroup is BOUNDED by pumpJoinTimeout (§2.1
	// #11/#12, 五轮 P1-3): a pump that never reaches its Done (e.g. registered via
	// beginFeed but its goroutine never got to launch because the synchronous first
	// reconcile is stuck in a resolver that ignores ctx) must not hang Close forever —
	// it is counted into leakedPumps and logged instead.
	pumps sync.WaitGroup
	// registeredPumps is the exact number of beginFeed registrations not yet retired.
	// It gives bounded Close an honest N-pump timeout snapshot (WaitGroup exposes no
	// count). leakedPumps records that snapshot at the timeout boundary.
	registeredPumps atomic.Int64
	leakedPumps     atomic.Int64

	// delivering is the递交许可 counter half of the统一会话闸 (spec §3.2, 四轮 P0-3):
	// a business-frame delivery Adds under the session's closed gate and Dones when
	// it returns; Close waits it to zero AFTER every session is closed (no Add can
	// race the Wait — a closed session admits no new permit). Only "已获准" deliveries
	// (past the session gate) are counted; the responsibility边界 (spec §3.2 v0.8) is
	// "no new delivery + gateway自有 goroutine 归零 + 调用方解阻" — in-flight interpreter
	// work is Home 段's teardown, not the gateway's.
	delivering sync.WaitGroup

	// Presence reconcile loop (收敛对象乙, one goroutine, Close joins). coverage is
	// LOOP-PRIVATE — touched only by the loop goroutine (and by Close's清账 AFTER the
	// join), so no lock.
	presenceCtx    context.Context
	presenceCancel context.CancelFunc
	presenceWG     sync.WaitGroup
	presencePoke   chan struct{} // buffered(1) 踢圈 edge (device/资格 poke)
	presenceOn     bool          // whether the loop was launched (resolver != nil)
	coverage       map[covKey]*covEntry

	// closeOnce runs the teardown EXACTLY once; sync.Once.Do blocks concurrent
	// callers until the one teardown returns (并发 Close 统一等 teardown).
	closeOnce sync.Once
	// closeEntered is a TEST-ONLY seam (nil in production): invoked at the top of
	// every Close BEFORE closeOnce.Do, so a test can prove a second concurrent caller
	// is parked on the single teardown.
	closeEntered func()
	// beforePresenceWait is a TEST-ONLY failpoint (nil in production). It parks the
	// real loop immediately before its poke/timer select, allowing tests to merge
	// concurrent poke edges deterministically without hand-running a reconcile.
	beforePresenceWait func()
}

// userEntry is the device account for one principal (spec §3.2 终形 {devices}): the
// set of that person's live connections. Presence首入末出 is derived from it by the
// reconcile loop. Born on the first device's attach; dies on the last device out ∨
// Close.
type userEntry struct {
	devices map[*Session]struct{}
}

// New constructs the gateway and, when a resolver is injected, launches the
// presence reconcile loop. Dependencies arrive via Config (assembly root).
func New(cfg Config) *Gateway {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	clock := cfg.Clock
	if clock == nil {
		clock = systemClock{}
	}
	epoch := cfg.Epoch
	if epoch == 0 {
		epoch = defaultEpoch()
	}
	pctx, pcancel := context.WithCancel(context.Background())
	durOr := func(d, def time.Duration) time.Duration {
		if d <= 0 {
			return def
		}
		return d
	}
	g := &Gateway{
		epoch:           epoch,
		routing:         cfg.Routing,
		resolver:        cfg.Resolver,
		clock:           clock,
		logger:          logger,
		tStale:          durOr(cfg.StaleLease, defaultStale),
		tSweep:          durOr(cfg.SweepInterval, defaultSweep),
		tRead:           durOr(cfg.ReadTimeout, defaultRead),
		tPresence:       durOr(cfg.PresenceTick, defaultPresence),
		pumpJoinTimeout: durOr(cfg.PumpJoinTimeout, defaultPumpJoinTimeout),
		entries:         map[string]*userEntry{},
		presenceCtx:     pctx,
		presenceCancel:  pcancel,
		presencePoke:    make(chan struct{}, 1),
		coverage:        map[covKey]*covEntry{},
	}
	return g
}

// LeakedPumps reports how many read pumps Close gave up joining within
// pumpJoinTimeout (账目诚实, §2.1 #11).
func (g *Gateway) LeakedPumps() int64 { return g.leakedPumps.Load() }

// Start brings the gateway up: it launches the presence reconcile loop (once a
// resolver is present). Idempotent-safe when called once by the assembly root.
func (g *Gateway) Start() error {
	if g.resolver != nil && !g.presenceOn {
		g.presenceOn = true
		g.presenceWG.Add(1)
		go g.presenceLoop()
	}
	return nil
}

// Poke wakes every session of principal (dirty + wake, so the read pump re-resolves
// its subscriptions) and kicks the presence loop (spec §3.2 poke 缝). Pure及时性 —
// a lost poke only delays convergence; the resolver/sweep is the正门.
func (g *Gateway) Poke(principal string) {
	g.mu.Lock()
	var sessions []*Session
	if e := g.entries[principal]; e != nil {
		for s := range e.devices {
			sessions = append(sessions, s)
		}
	}
	g.mu.Unlock()
	for _, s := range sessions {
		s.markDirty()
	}
	g.kickPresence()
}

// kickPresence pokes the presence reconcile loop (non-blocking edge).
func (g *Gateway) kickPresence() {
	select {
	case g.presencePoke <- struct{}{}:
	default:
	}
}

// addDevice seats one session on its principal's device account (device edge → 踢圈).
// Refuses after Close (关站序 straddle). The FIRST device does NOT publish presence
// here — presence is the reconcile loop's job (it will see the new device on its
// next tick / this poke).
func (g *Gateway) addDevice(principal string, s *Session) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrGatewayClosed
	}
	e := g.entries[principal]
	if e == nil {
		e = &userEntry{devices: map[*Session]struct{}{}}
		g.entries[principal] = e
	}
	e.devices[s] = struct{}{}
	g.kickPresence()
	return nil
}

// removeDevice drops one session from its principal's device account (末出 → 踢圈,
// the loop publishes offline). Retires the entry on the last device out. Summarizes
// the lane's own drops at teardown (not per-push).
func (g *Gateway) removeDevice(principal string, s *Session) {
	g.mu.Lock()
	if e := g.entries[principal]; e != nil {
		if _, ok := e.devices[s]; ok {
			delete(e.devices, s)
			if len(e.devices) == 0 {
				delete(g.entries, principal)
			}
		}
	}
	g.mu.Unlock()
	g.kickPresence()
	if s.lane != nil {
		if dropped := s.lane.DroppedCount(); dropped > 0 {
			g.logger.Info("platform.gateway.lane_dropped", "principal", principal, "dropped", dropped)
		}
	}
}

// tryRegisterPump is the GATEWAY half of泵登记 (统一会话闸, 五轮 P1-3): under g.mu it
// refuses after the全站闩 (Close already set g.closed) and otherwise joins the pump
// into g.pumps. It does NOT by itself decide "is THIS session closed" — that is
// s.beginFeed's job, gated under s.mu, the session's own half of the same统一会话闸
// (spec §3.2: "closed、泵登记、递交许可 三者同一把锁" means both halves must be
// consulted, not just the gateway's — a session that called s.Close() directly, with
// the gateway still open, must still refuse a late StartFeed).
func (g *Gateway) tryRegisterPump() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.pumps.Add(1)
	g.registeredPumps.Add(1)
	return true
}

// unregisterPump retires exactly one successful beginFeed registration, whether the
// async pump ran or a post-reconcile close re-check prevented that late start.
func (g *Gateway) unregisterPump() {
	g.registeredPumps.Add(-1)
	g.pumps.Done()
}

// Close tears the gateway down (关站全序). 停圈 → join 圈 → 清账 (spec §3.2 六轮
// P1-1) fused with the统一会话闸: set the全站闩, stop+join the presence loop, clean
// its coverage, close every session (cancel session ctx), join the read pumps, then
// wait已获准递交归零. This固定顺序 lets app关 Home only AFTER (关站序 两段全序). Idempotent;
// concurrent callers all return only after the ONE teardown completes.
func (g *Gateway) Close() error {
	if g.closeEntered != nil {
		g.closeEntered()
	}
	started := g.clock.Now()
	g.closeOnce.Do(func() {
		g.mu.Lock()
		g.closed = true
		var sessions []*Session
		for _, e := range g.entries {
			for s := range e.devices {
				sessions = append(sessions, s)
			}
		}
		g.mu.Unlock()

		// 停圈 → join 圈 → 清账 (reverse order would leave a "圈复活补发 online" window).
		g.presenceCancel()
		g.presenceWG.Wait()
		for _, ce := range g.coverage {
			// 条件清账: only online coverage slots are ForgetEpoch'd (offline残值不清 —
			// it is a生前目击 of离场, content恒真, advisory语义内合法, spec §3.2 收窄).
			ce.slot.ForgetEpoch(g.epoch)
		}
		g.coverage = map[covKey]*covEntry{}

		// 关全部 Session (cancel session ctx) → join 读泵 (有界, 五轮 P1-3) → 等已获准递交归零.
		for _, s := range sessions {
			s.Close()
		}
		g.joinPumpsBounded()
		g.delivering.Wait()
		g.logger.Info("platform.gateway.closed", "duration", g.clock.Now().Sub(started))
	})
	return nil
}

// joinPumpsBounded waits g.pumps to zero, bounded by pumpJoinTimeout (§2.1 #11/#12,
// 五轮 P1-3 平移 of the old ArmSealJoinTimeout budget to the unified会话闸's single pump
// WaitGroup). Every session was already Close()d by the caller, which cancels its ctx
// and so unblocks any pump loop that is actually running — the only way this join can
// miss its deadline is a pump that was REGISTERED (tryRegisterPump's Add) but whose
// goroutine never got to run (e.g. parked in StartFeed's synchronous first reconcile,
// itself stuck in a resolver call that ignores ctx). That pump is leaked: Close counts
// it, logs it, and proceeds rather than hanging forever (账目诚实, never a silent hang).
func (g *Gateway) joinPumpsBounded() {
	done := make(chan struct{})
	go func() {
		g.pumps.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(g.pumpJoinTimeout):
		leaked := g.registeredPumps.Load()
		g.leakedPumps.Add(leaked)
		g.logger.Error("platform.gateway.pump_join_timeout", "leaked", leaked)
	}
}

// presenceLoop is the在场对账圈 (spec §3.2 收敛对象乙): one goroutine that每圈 from
// the CURRENT truth整算 desired (devices × member频道集) and converges coverage to
// it (缺→补 online / 多→撤 offline / 换值→换账). Zero并发协议: it consumes no events
// (乱序/迟到/重复 have no meaning) — an旧离线压新在线 is undone next圈. Node节拍 =
// device/资格 poke (毫秒级) + T_presence周期兜底.
func (g *Gateway) presenceLoop() {
	defer g.presenceWG.Done()
	var timer Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case <-g.presenceCtx.Done():
			return
		default:
		}
		g.presenceReconcile()
		if timer != nil {
			timer.Stop()
		}
		timer = g.clock.NewTimer(g.clock.Now().Add(g.tPresence))
		if g.beforePresenceWait != nil {
			g.beforePresenceWait()
		}
		select {
		case <-g.presenceCtx.Done():
			return
		case <-g.presencePoke:
		case <-timer.C():
		}
	}
}

// presenceReconcile runs one圈: recompute desired online (人×member频道) and converge
// the coverage account.
func (g *Gateway) presenceReconcile() {
	// desired inputs: principals with ≥1 device (设备账 snapshot under g.mu).
	g.mu.Lock()
	principals := make([]string, 0, len(g.entries))
	for p, e := range g.entries {
		if len(e.devices) > 0 {
			principals = append(principals, p)
		}
	}
	g.mu.Unlock()

	desired := map[covKey]*subjectgate.Slot{}
	for _, p := range principals {
		select {
		case <-g.presenceCtx.Done():
			return
		default:
		}
		rctx, cancel := context.WithTimeout(g.presenceCtx, g.tRead)
		routes, failed, err := g.resolver.Snapshot(rctx, p)
		cancel()
		if err != nil {
			// whole-snapshot failure → preserve this principal's current coverage
			// (advisory presence rides the failure; sweep retries). Never offline on a
			// transient blip.
			for k, ce := range g.coverage {
				if k.principal == p {
					desired[k] = ce.slot
				}
			}
			continue
		}
		for _, r := range routes {
			if r.Access != AccessMember {
				continue // observers get no presence testimony (no槽)
			}
			slot, ok := r.Home.SubjectSlotFor(r.SubjectID)
			if !ok {
				continue // embodiment lag — no slot yet, next圈
			}
			desired[covKey{principal: p, channel: r.Channel}] = slot
		}
		// per-channel failures (P2-9, 六轮终审): a channel reported as a per-channel
		// ChannelFailure this round (whole-snapshot SUCCEEDED, only this one channel's
		// query failed) must NOT be treated as "gone" — that would force an immediate
		// false-offline on a transient blip, the exact thing N2's whole-snapshot
		// preservation already guards against, just not extended to the per-channel
		// case. Preserve this principal's EXISTING coverage for any such channel.
		for _, cf := range failed {
			k := covKey{principal: p, channel: cf.Channel}
			if _, already := desired[k]; already {
				continue
			}
			if ce, ok := g.coverage[k]; ok {
				desired[k] = ce.slot
			}
		}
	}

	now := g.clock.Now()
	// 缺 → 补 online (idempotent); 换值 → 撤旧补新.
	for k, slot := range desired {
		cur := g.coverage[k]
		switch {
		case cur == nil:
			slot.PublishCurrent(g.epoch, subjectgate.LevelOnline)
			g.coverage[k] = &covEntry{slot: slot, lastOK: now}
		case cur.slot != slot:
			// Remove→秒 re-Admit: key unchanged, slotRef换代 → 撤旧 online, 向新补.
			cur.slot.PublishCurrent(g.epoch, subjectgate.LevelOffline)
			slot.PublishCurrent(g.epoch, subjectgate.LevelOnline)
			cur.slot = slot
			cur.lastOK = now
		default:
			slot.PublishCurrent(g.epoch, subjectgate.LevelOnline) // 幂等补发 (no-op if unchanged)
			cur.lastOK = now
		}
	}
	// 多 → 发 offline 撤证 + 销账 (末出/出籍两因同臂).
	for k, cur := range g.coverage {
		if _, ok := desired[k]; ok {
			continue
		}
		cur.slot.PublishCurrent(g.epoch, subjectgate.LevelOffline)
		delete(g.coverage, k)
	}
}

// entryFor returns the principal's live device account (test/assertion helper).
func (g *Gateway) entryFor(principal string) (*userEntry, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	e, ok := g.entries[principal]
	return e, ok
}
