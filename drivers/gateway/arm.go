package gateway

import (
	"context"
	"sync"
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

	leaked      *int64 // gateway-level abandoned-pump tally (set by owner)
	leakMu      *sync.Mutex
	joinTimeout time.Duration // ArmSealJoinTimeout in production; test-tunable
}

func newChannelArm(home *platform.Home, chID channel.ID, subjectID actor.ActorID, slot *platform.SubjectSlot) *channelArm {
	ctx, cancel := context.WithCancel(context.Background())
	return &channelArm{
		home:        home,
		chID:        chID,
		subjectID:   subjectID,
		slot:        slot,
		ctx:         ctx,
		cancel:      cancel,
		joinTimeout: ArmSealJoinTimeout,
	}
}

// nextGen advances the绑定世代 (a fresh attach establishes a new binding) and, for
// a member arm, writes it into the slot's layer-2 register. Returns the new gen.
func (a *channelArm) nextGen() int64 {
	a.mu.Lock()
	a.gen++
	gen := a.gen
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

// context returns the arm's cancellation context (every pump/reader derives from
// it, so seal unblocks them all at once).
func (a *channelArm) context() context.Context { return a.ctx }

// track registers one read pump on the arm's join set.
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
