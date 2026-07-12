package gateway

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// These are white-box integration tests over the REAL Gateway.Attach path (a real
// platform.Home + a real admitted human slot), not the bare-session十字 helpers — the
// two regressions they cover (多设备互杀, Close×Attach straddle) only manifest through
// the full attach seat + gen grant + feed launch, so they must exercise it end to end.

const attachTestChannelID = channel.ID("gw-attach")

// openAttachHome opens a Home and admits one human ("alice"), whose per-identity slot
// Admit ensures synchronously — so a real Attach finds a member slot (no live cell is
// needed: Attach only LOOKS the slot up, and an empty channel feeds nothing). The long
// reconcile interval keeps the background ticker from racing the test.
func openAttachHome(t *testing.T) (*platform.Home, actor.ActorID) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "gw-attach.sqlite")
	h, err := platform.Open(platform.HomeConfig{
		ChannelID:         attachTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	id, err := h.Admit(context.Background(), actor.KindHuman, "alice")
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	return h, id
}

func mustSubmit(t *testing.T, gen int64) platform.Frame {
	t.Helper()
	f, err := platform.NewFrame(platform.FrameSubmit, gen, "ref", platform.SubmitPayload{
		MsgType: "human.message", Audience: []string{"tool:kimi"}, Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("NewFrame: %v", err)
	}
	return f
}

// codeOf returns the error code of an error frame, or "" for a non-error frame.
func codeOf(t *testing.T, f platform.Frame) string {
	t.Helper()
	if f.Type != platform.FrameError {
		return ""
	}
	var p platform.ErrorPayload
	if err := f.DecodePayload(&p); err != nil {
		t.Fatalf("decode error payload: %v", err)
	}
	return p.Code
}

// TestAttachMultiTabSharesBinding (REGRESSION 多设备互杀): a second tab attaching to the
// same (subject, channel) must NOT advance the binding gen — both tabs share the one
// live arm's gen, so tab 1's business frames stay admitted after tab 2 joins (多设备
// 同时合法, §5.6). Only a seal-then-rebind mints a fresh binding on a NEW arm.
func TestAttachMultiTabSharesBinding(t *testing.T) {
	h, _ := openAttachHome(t)
	g := New(Config{})
	ctx := context.Background()
	ch := attachTestChannelID

	s1, gen1, err := g.Attach(ctx, h, ch, "alice", nil)
	if err != nil {
		t.Fatalf("attach tab1: %v", err)
	}
	defer s1.Close()
	s2, gen2, err := g.Attach(ctx, h, ch, "alice", nil)
	if err != nil {
		t.Fatalf("attach tab2: %v", err)
	}
	defer s2.Close()

	if gen1 != gen2 {
		t.Fatalf("multi-tab must SHARE the binding gen (tab2 must not推进 it): tab1=%d tab2=%d", gen1, gen2)
	}
	if s1.arm != s2.arm {
		t.Fatal("two tabs on the same (subject,channel) must share ONE arm")
	}

	// Both tabs' business frames pass the gen gate after both attached — the regression
	// was tab1 getting stale_binding once tab2 attached. No live cell → unavailable is
	// the honest downstream verdict; stale_binding here would be the bug.
	if got := codeOf(t, s1.Upstream(ctx, mustSubmit(t, gen1))); got == platform.CodeStaleBinding {
		t.Fatal("tab1 frame staled by tab2's attach (多设备互杀 regression)")
	}
	if got := codeOf(t, s2.Upstream(ctx, mustSubmit(t, gen2))); got == platform.CodeStaleBinding {
		t.Fatalf("tab2 frame unexpectedly stale: %q", got)
	}

	// Seal-then-rebind: tab1 detaches → seals the shared arm → drops it.
	detach, _ := platform.NewFrame(platform.FrameDetach, gen1, "d", platform.DetachPayload{ChannelID: string(ch)})
	s1.Upstream(ctx, detach)
	if !s2.arm.isSealed() {
		t.Fatal("detach must seal the shared arm")
	}
	// tab2 (an old device on the now-sealed binding) is refused stale_binding.
	if got := codeOf(t, s2.Upstream(ctx, mustSubmit(t, gen2))); got != platform.CodeStaleBinding {
		t.Fatalf("old device after seal must get stale_binding, got %q", got)
	}
	// A fresh attach rebinds a NEW arm (fresh binding); its frames are not stale.
	s3, gen3, err := g.Attach(ctx, h, ch, "alice", nil)
	if err != nil {
		t.Fatalf("attach tab3 (rebind): %v", err)
	}
	defer s3.Close()
	if s3.arm == s2.arm {
		t.Fatal("rebind after seal must mint a NEW arm")
	}
	if got := codeOf(t, s3.Upstream(ctx, mustSubmit(t, gen3))); got == platform.CodeStaleBinding {
		t.Fatal("rebound session frame must not be stale")
	}
}

// TestDeliverStaleAfterSealRebindRealPath (P0-1 real interleave): the ABA guard end
// to end over the real Attach + slot path. A frame passed the arm's admitUpstream初验
// on binding A; before it reaches the slot's Deliver, A is sealed (detach) and a fresh
// attach rebinds binding B into the SAME slot. Two properties must hold together: (1)
// the绑定世代 is monotonic across rebuild (genB strictly > genA — no per-arm reset
// re-issuing genA, ABA), and (2) the stale gen-A frame reaching Deliver AFTER the
// rebind is refused ErrStaleBinding at the linearization point, never written into
// binding B — while a gen-B frame delivers. Either fix alone is insufficient: without
// (1) the gens collide and (2)'s check passes the stale frame; without (2) the TOCTOU
// window stays open. Together they close it.
func TestDeliverStaleAfterSealRebindRealPath(t *testing.T) {
	h, subjectID := openAttachHome(t)
	g := New(Config{})
	ctx := context.Background()
	ch := attachTestChannelID

	s1, genA, err := g.Attach(ctx, h, ch, "alice", nil)
	if err != nil {
		t.Fatalf("attach A: %v", err)
	}
	defer s1.Close()

	// Make the member slot live with a trivial interpreter (no reconcile ring here),
	// so Deliver reaches the gen gate rather than short-circuiting on no-occupant.
	slot, ok := h.SubjectSlotFor(subjectID)
	if !ok {
		t.Fatal("member slot must exist after attach")
	}
	frames, _, release := slot.AttachInterpreter()
	defer release()
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case job := <-frames:
				r, _ := platform.NewFrame(platform.FrameReceipt, 0, job.Frame.Ref, platform.SubmitReceipt{MessageID: "m", Seq: 1})
				job.Reply(platform.FrameResult{Frame: r})
			case <-stop:
				return
			}
		}
	}()
	defer close(stop)

	// The in-flight frame that passed admitUpstream on binding A.
	inflight := mustSubmit(t, genA)

	// Seal A (detach) + rebind B into the same slot.
	detach, _ := platform.NewFrame(platform.FrameDetach, genA, "d", platform.DetachPayload{ChannelID: string(ch)})
	s1.Upstream(ctx, detach)
	s2, genB, err := g.Attach(ctx, h, ch, "alice", nil)
	if err != nil {
		t.Fatalf("attach B (rebind): %v", err)
	}
	defer s2.Close()

	// (1) Monotonic across rebuild — the ABA precondition.
	if genB <= genA {
		t.Fatalf("rebind must mint a strictly greater binding gen (ABA fix): genA=%d genB=%d", genA, genB)
	}
	// (2) The stale gen-A frame reaching Deliver after the rebind is refused at the
	// linearization point (not silently written into binding B).
	if _, derr := slot.Deliver(inflight, genA); derr != platform.ErrStaleBinding {
		t.Fatalf("stale gen-A frame at Deliver must be refused ErrStaleBinding, got %v", derr)
	}
	// A current gen-B frame delivers.
	if _, derr := slot.Deliver(mustSubmit(t, genB), genB); derr != nil {
		t.Fatalf("current gen-B frame must deliver, got %v", derr)
	}
}

// TestGatewayCloseAttachStraddleRealPath (P0-4, real Attach): concurrent full
// Gateway.Attach (seat + gen grant + StartFeed) racing Close. Every attach either
// seats fully (its pump then joined by Close's seal, zero leak) or is refused
// ErrGatewayClosed / has its late feed refused — never a half-open session, an
// uncounted pump, or a panic. After Close every subsequent attach is refused.
func TestGatewayCloseAttachStraddleRealPath(t *testing.T) {
	h, _ := openAttachHome(t)
	ctx := context.Background()
	ch := attachTestChannelID

	for iter := 0; iter < 25; iter++ {
		g := New(Config{})
		var wg sync.WaitGroup
		var mu sync.Mutex
		var sessions []*Session
		const N = 6
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s, _, err := g.Attach(ctx, h, ch, "alice", nil)
				if err == ErrGatewayClosed {
					return // refused cleanly (Close won the lock)
				}
				if err != nil {
					t.Errorf("attach: %v", err)
					return
				}
				s.StartFeed() // refuses (self-closes) if Close landed after the seat
				mu.Lock()
				sessions = append(sessions, s)
				mu.Unlock()
			}()
		}
		wg.Add(1)
		go func() { defer wg.Done(); _ = g.Close() }()
		wg.Wait()

		// Every tracked pump was joined by the seal — nothing abandoned uncounted.
		if leaked := g.LeakedPumps(); leaked != 0 {
			t.Fatalf("iter %d: straddle leaked an uncounted pump: %d", iter, leaked)
		}
		// Close has returned: every later attach is refused.
		if _, _, err := g.Attach(ctx, h, ch, "alice", nil); err != ErrGatewayClosed {
			t.Fatalf("iter %d: post-Close attach want ErrGatewayClosed, got %v", iter, err)
		}
		for _, s := range sessions {
			s.Close()
		}
	}
}

// TestSealJoinIsolatesStuckWriter (A×L, REAL writer over a REAL connection): the seal
// join set covers ONLY the feed pump. A genuine stuck writer — a goroutine that drains
// the REAL lane (sess.Down()) and writes each frame to a REAL net.Pipe whose far end is
// never read, so a synchronous write blocks exactly like connector/web's writerPump
// parked on a dead peer — stays parked straight through the seal. The writer holds a
// strong *Session ref (the isolation argument is LIVENESS, not resource-freedom): (1)
// the REAL runFeed pump fills the non-draining lane to capacity and tears itself down
// (满→断连); (2) seal joins that pump within the REAL 15s budget WITHOUT waiting for the
// parked writer — proven by seal returning far below both the writer's write deadline
// and the join budget; (3) the writer's own write deadline fires and unwinds it (退出链
// reachable), independent of the seal. Folding the writer into the join set (or failing
// to tear the pump down on a full lane) would blow the budget assertion.
func TestSealJoinIsolatesStuckWriter(t *testing.T) {
	h, _ := openAttachHome(t)
	ctx := context.Background()
	ch := attachTestChannelID

	// Generate > LaneCapacity committed feed rows (each Admit writes one registration
	// event) so the pump's backfill overflows the (barely-drained) lane.
	for i := 0; i < LaneCapacity+10; i++ {
		if _, err := h.Admit(ctx, actor.KindHuman, fmt.Sprintf("filler-%d", i)); err != nil {
			t.Fatalf("admit filler %d: %v", i, err)
		}
	}

	g := New(Config{})
	s, _, err := g.Attach(ctx, h, ch, "alice", nil)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	defer s.Close()

	// production wsWriteWait is LaneWriteTimeoutMs (10s); shrunk here so the deadline-
	// driven exit-chain assertion runs on a short wall clock. It stays comfortably >
	// the expected seal time (ms) so seal-fast vs writer-deadline is an unambiguous
	// discriminator: a seal that joined the writer would take ~writerDeadline.
	const writerDeadline = 2 * time.Second
	p1, p2 := net.Pipe()
	defer p1.Close()
	defer p2.Close() // p2 is NEVER read → p1.Write blocks (synchronous pipe)

	writerBlocking := make(chan struct{})
	writerGone := make(chan struct{})
	go func() {
		defer close(writerGone)
		// Take the first frame unconditionally (backfill guarantees ≥1) and park on the
		// write — the stuck writer we are isolating. Parked in Write it can no longer
		// observe lane closure, exactly like writerPump stuck in ws.WriteMessage.
		b, ok := <-s.Down()
		if !ok {
			return
		}
		_ = p1.SetWriteDeadline(time.Now().Add(writerDeadline))
		close(writerBlocking)
		_, _ = p1.Write(b) // blocks until the write deadline (p2 never read)
		s.Close()          // deadline fired → the writer unwinds (holds a *Session ref, but exits)
	}()

	// Launch the real feed pump; the parked writer drains only one frame, so the lane
	// fills to capacity and the next push tears the pump down (满→断连).
	s.StartFeed()
	<-writerBlocking // the writer is now genuinely parked in a blocking write
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("feed pump never tore down the full lane (满→断连 broken)")
	}

	// Seal uses the REAL 15s join budget (法条 F). It joins ONLY the feed pump (already
	// torn down), never the parked writer — so it returns in milliseconds, far below the
	// writer's still-pending write deadline. A seal that had joined the writer would
	// instead take ~writerDeadline.
	start := time.Now()
	s.arm.seal()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("seal took %v — a stuck writer dragged it out (writer must be isolated, not joined)", elapsed)
	}
	if elapsed := time.Since(start); elapsed >= ArmSealJoinTimeout {
		t.Fatalf("seal waited the full join budget (%v)", elapsed)
	}
	if leaked := g.LeakedPumps(); leaked != 0 {
		t.Fatalf("feed pump not joined cleanly: leaked=%d", leaked)
	}
	// The writer's exit chain IS reachable: its write deadline fires and unwinds it,
	// independent of the seal (which already returned).
	select {
	case <-writerGone:
	case <-time.After(writerDeadline + 3*time.Second):
		t.Fatal("stuck writer never exited via its write deadline (退出链 unreachable)")
	}
}

// TestTailOnlyClosedGateAndCascade (P1): a tail-only observer session passes the same
// closed admission gate as a member and derives its ctx from the gateway ctx — so Close
// cancels its read pump and JOINS it before returning (关站静默, symmetric with member
// arm seal), and a post-Close tail attach is refused ErrGatewayClosed.
func TestTailOnlyClosedGateAndCascade(t *testing.T) {
	h, _ := openAttachHome(t)
	ctx := context.Background()
	ch := attachTestChannelID

	g := New(Config{})
	// "bob" is not an admitted member → a tail-only (workspace observer) session.
	s, gen, err := g.Attach(ctx, h, ch, "bob", nil)
	if err != nil {
		t.Fatalf("tail attach: %v", err)
	}
	if s.IsMember() {
		t.Fatal("bob must be a tail-only observer (not a channel member)")
	}
	if gen != 0 {
		t.Fatalf("a tail-only attach grants no binding gen, got %d", gen)
	}
	s.StartFeed()

	done := make(chan struct{})
	go func() { _ = g.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return — a tail pump was not joined (关站静默 broken)")
	}
	select {
	case <-s.ctx.Done():
	default:
		t.Fatal("Close must cancel the tail session ctx (关站序 tail closure)")
	}
	if _, _, err := g.Attach(ctx, h, ch, "bob", nil); err != ErrGatewayClosed {
		t.Fatalf("post-Close tail attach want ErrGatewayClosed, got %v", err)
	}
	s.Close()
}

// TestGatewayCloseTailOnlyStraddleRealPath (P1, straddle tail格): concurrent tail-only
// Attach + StartFeed racing Close. Every attach either refuses cleanly (ErrGatewayClosed)
// or seats and has its pump joined by Close's tailPumps.Wait; never a hang or a pump left
// reading a closing Home. After Close every later tail attach is refused.
func TestGatewayCloseTailOnlyStraddleRealPath(t *testing.T) {
	h, _ := openAttachHome(t)
	ctx := context.Background()
	ch := attachTestChannelID

	for iter := 0; iter < 25; iter++ {
		g := New(Config{})
		var wg sync.WaitGroup
		var mu sync.Mutex
		var sessions []*Session
		const N = 6
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s, _, err := g.Attach(ctx, h, ch, "bob", nil)
				if err == ErrGatewayClosed {
					return
				}
				if err != nil {
					t.Errorf("tail attach: %v", err)
					return
				}
				s.StartFeed() // self-closes if Close landed after the seat
				mu.Lock()
				sessions = append(sessions, s)
				mu.Unlock()
			}()
		}
		wg.Add(1)
		go func() { defer wg.Done(); _ = g.Close() }() // returns only after tailPumps join
		wg.Wait()

		if _, _, err := g.Attach(ctx, h, ch, "bob", nil); err != ErrGatewayClosed {
			t.Fatalf("iter %d: post-Close tail attach want ErrGatewayClosed, got %v", iter, err)
		}
		for _, s := range sessions {
			s.Close()
		}
	}
}
