package gateway

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// ArmSealJoinTimeout bounds the detach seal's join (design §5.5 / DoD-5, 法条 F
// 推导: > LaneWriteTimeoutMs so a slow lane always dies BEFORE the join, never
// dragging seal past this budget). A pump that has not stopped by then is
// abandoned响亮 (leak counted) rather than blocking the seal forever.
const ArmSealJoinTimeout = 15 * time.Second

// channelArm is the十字's per (subject × channel) south face (§5.6 频道臂 column):
// the binding write-half + the read流句柄 lifecycle, gated by a shared世代
// (臂世代 = 绑定世代). owner = the user件 (法条 G single-axis); membership/ACL are
// the授权/撤销 axis (RevocationSource → seal), NOT a second owner.
//
// Seal (detach / revocation) is the共享世代 admission gate's teardown: mark sealed,
// cancel the arm ctx (every read pump + reader for this subject unblocks), Forget
// the slot testimony (证词账清洁边), and bounded-join the pumps. After seal no
// direction produces an observable success (upstream → stale_binding; downstream →
// no push). 已提交进真相的 upstream writes are never retracted — purge only touches
//未推 lane frames.
type channelArm struct {
	home      *platform.Home
	chID      channel.ID
	subjectID actor.ActorID
	slot      *platform.SubjectSlot // nil for a tail-only (non-member) arm

	mu     sync.Mutex
	gen    int64
	sealed bool
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // the arm's read pumps

	// bindingSeq is the绑定世代 source. It MUST outlive an arm's rebuild (ABA fix,
	// P0-1): a seal drops the arm and the next attach mints a NEW arm, so a per-arm
	// counter would reset to 1 and re-issue the SAME gen the sealed arm used —
	// making a stale in-flight frame (that passed初验 on the old arm) indistinguish-
	// able from the new binding at the delivery linearization point (ABA). Drawing
	// from a shared (gateway-level) monotonic counter makes the绑定世代 strictly
	// increase per (subject,channel) across every rebuild, so the slot's SetBinding
	// value differs across rebind and Deliver's gen gate can reject the stale frame.
	// A bare-arm unit test passes nil → a fresh local counter (single-arm scope).
	bindingSeq *atomic.Int64

	leaked      *int64 // gateway-level abandoned-pump tally (set by owner)
	leakMu      *sync.Mutex
	joinTimeout time.Duration // ArmSealJoinTimeout in production; test-tunable
}

func newChannelArm(home *platform.Home, chID channel.ID, subjectID actor.ActorID, slot *platform.SubjectSlot, bindingSeq *atomic.Int64) *channelArm {
	ctx, cancel := context.WithCancel(context.Background())
	if bindingSeq == nil {
		bindingSeq = &atomic.Int64{} // bare-arm unit test: local monotonic counter
	}
	return &channelArm{
		home:        home,
		chID:        chID,
		subjectID:   subjectID,
		slot:        slot,
		ctx:         ctx,
		cancel:      cancel,
		bindingSeq:  bindingSeq,
		joinTimeout: ArmSealJoinTimeout,
	}
}

// nextGen advances the绑定世代 (from the shared monotonic source) and, for a member
// arm, writes it into the slot's layer-2 register. Returns the new gen. 世代属于
// "绑定"(臂的一次生命)非"设备" (§5.6 恢复差异): it advances ONCE, when the arm is
// FIRST built (a fresh binding) — a later device joining an already-live arm reuses
// the current gen (ensureArmLocked hands it currentGen, never a fresh nextGen), so a
// second tab never staled the first (多设备同时合法 fix). A seal drops the arm; the
// next attach mints a NEW arm whose nextGen draws the NEXT value from the shared
// counter (strictly greater than any the sealed arm used, ABA fix P0-1) — so a stale
// frame's gen is distinguishable from the new binding at the delivery linearization
// point, and the old (sealed) arm ALSO refuses via its sealed flag (belt + braces).
func (a *channelArm) nextGen() int64 {
	gen := a.bindingSeq.Add(1)
	a.mu.Lock()
	a.gen = gen
	a.mu.Unlock()
	if a.slot != nil {
		a.slot.SetBinding(gen)
	}
	return gen
}

// admit is the共享世代 admission gate read: an upstream frame or a downstream push
// is admitted only while the arm is unsealed. Returns false after seal (both
// directions → no observable success).
func (a *channelArm) admit() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return !a.sealed
}

func (a *channelArm) isSealed() bool { return !a.admit() }

// currentGen reads the绑定世代 under the arm lock (P0-9: a.gen is never read raw —
// nextGen writes it under a.mu, so every read takes the same lock).
func (a *channelArm) currentGen() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.gen
}

// admitUpstream is the上行 half of the双向共享 admission gate (P0-2): an upstream
// business frame is admitted only while the arm is unsealed AND its binding_gen
// matches the current绑定世代. Both checks under ONE lock so a concurrent seal /
// rebind can never straddle them. sealed distinguishes the stale cause in the caller
// (both map to stale_binding — the边界 vs unavailable is 世代不符/已封 vs cell 暂缺).
func (a *channelArm) admitUpstream(bindingGen int64) (ok, sealed bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sealed {
		return false, true
	}
	if bindingGen != a.gen {
		return false, false
	}
	return true, false
}

// context returns the arm's cancellation context (every pump/reader derives from
// it, so seal unblocks them all at once).
func (a *channelArm) context() context.Context { return a.ctx }

// track registers one read pump on the arm's join set.
//
// Seal-join 账的边界 (§5.5 join 上界, ledger A×L): the join set covers ONLY the
// feed pump (runFeed) — the single goroutine that references arm/Home resources
// (arm ctx, home.Subscribe, View().ReadAfterSeq, the lane cursor). The connector's
// ws writer goroutine (connector/web writerPump) is deliberately NOT tracked. The
// isolation argument is NOT "the writer holds no resources" — it does hold a strong
// *Session reference (and through it home/entry/arm). The argument is a LIVENESS one:
// the writer is guaranteed to exit regardless — its per-write deadline (wsWriteWait /
// LaneWriteTimeoutMs = 10s) fires on a stuck peer and errors the write, and runFeed's
// deferred lane.close() unblocks a writer parked on the drain — so its strong ref
// never keeps anything alive past its own bounded clock. Because that deadline is
// pinned STRICTLY < ArmSealJoinTimeout (法条 F), the writer cannot drag out a seal:
// seal joins ONLY the feed pump (which stops the moment the lane fills or the arm ctx
// cancels) and returns within budget, while the doomed writer unwinds independently.
func (a *channelArm) track()   { a.wg.Add(1) }
func (a *channelArm) untrack() { a.wg.Done() }

// seal tears the arm down (detach / revocation). Idempotent. Cancels the ctx
// (unblocks pumps), Forgets the slot testimony, then bounded-joins the pumps
// (ArmSealJoinTimeout): a pump still alive at the deadline is abandoned响亮 and
// counted, never blocking seal forever (§5.5 join 上界).
func (a *channelArm) seal() {
	a.mu.Lock()
	if a.sealed {
		a.mu.Unlock()
		return
	}
	a.sealed = true
	cancel := a.cancel
	slot := a.slot
	a.mu.Unlock()

	cancel()
	if slot != nil {
		slot.Forget()
	}

	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(a.joinTimeout):
		if a.leaked != nil && a.leakMu != nil {
			a.leakMu.Lock()
			*a.leaked++
			a.leakMu.Unlock()
		}
	}
}
