package gateway

import (
	"context"
	"fmt"
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

// TestSealJoinIsolatesStuckWriter (Gap 4 A×L, real feed pump not a模型替身): the seal
// join set covers ONLY the feed pump. A genuinely stuck consumer (a goroutine that never
// drains the lane, modelling a ws writer blocked on a dead peer) is present throughout,
// yet the REAL runFeed pump fills the non-draining lane to capacity, tears itself down
// (满→断连), and seal joins it in budget — never waiting for the stuck writer (which is
// NOT tracked: lane closure isolates it). A regression that either failed to tear the
// pump down on a full lane, or folded the writer into the join set, would hang here.
func TestSealJoinIsolatesStuckWriter(t *testing.T) {
	h, _ := openAttachHome(t)
	ctx := context.Background()
	ch := attachTestChannelID

	// Generate > LaneCapacity committed feed rows (each Admit writes one registration
	// event) so the pump's backfill overflows a non-draining lane.
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
	s.arm.joinTimeout = time.Second // shrink so a regression that waited on the writer fails fast

	// The stuck writer: a real goroutine that NEVER drains the lane and stays blocked
	// straight through the seal (it is not tracked by the arm — the isolation under test).
	writerStuck := make(chan struct{})
	writerGone := make(chan struct{})
	go func() {
		defer close(writerGone)
		<-writerStuck
	}()

	// Launch the real feed pump; with nothing draining the lane it fills to capacity and
	// the next push tears it down (满→断连). Because seal has NOT run yet, the only
	// possible teardown cause is the full lane.
	s.StartFeed()
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("feed pump never tore down the full non-draining lane (满→断连 broken)")
	}

	// Seal joins the (already-terminated) feed pump promptly, without waiting for the
	// stuck writer or blowing the join budget.
	start := time.Now()
	s.arm.seal()
	if elapsed := time.Since(start); elapsed >= s.arm.joinTimeout {
		t.Fatalf("seal waited the join budget (%v) — a stuck writer dragged it out", elapsed)
	}
	if leaked := g.LeakedPumps(); leaked != 0 {
		t.Fatalf("feed pump not joined cleanly: leaked=%d", leaked)
	}
	// The writer is STILL stuck — proof the seal never joined/waited on it.
	select {
	case <-writerGone:
		t.Fatal("stuck writer exited — it must be isolated from the seal, not part of it")
	default:
	}
	close(writerStuck)
	<-writerGone
}
