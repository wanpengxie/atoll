package home

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// DoD 14 反面测试（v1.4 §2.6 组装纪律）：pen 把手写允许滑窗容忍（一笔在途写
// 放行、下一笔才被拒——见 runtime/harness/sliding_window_test.go）；动词类
// （apply/End）没有这种容忍窗口——它们的读→判→提交全段都在 actorGates 临界
// 区内，door 释放给下一个竞争者时，那个竞争者看到的必须是刚提交的新鲜状态，
// 不是它自己排队前捕获的旧快照。两个测试都用真实并发 goroutine 竞争同一把
// gate 锁（而非顺序调用两次），用 -race 验证没有偷跑窗口。

// TestApplyDeclarationConcurrentRacersRecheckFreshStateNotSlidingSnapshot：
// 两个 goroutine 并发对同一 actor 申请同一个目标版本。gate 把它们串行化；
// 无论调度顺序，赢家必唯一、输家必被 ErrApplyVersionRegress 拒——且输家的
// 拒绝依据是它进入临界区后重新 LookupActive 到的新值（current==2），不是
// 它排队前看到的 current==1。若动词像 pen 写一样滑窗，两个竞争者都可能基于
// current==1 判定"版本推进合法"而双双成功，破坏版本单调不变量。
func TestApplyDeclarationConcurrentRacersRecheckFreshStateNotSlidingSnapshot(t *testing.T) {
	for n := 0; n < 20; n++ {
		h := openWhiteboxHome(t)
		ctx := context.Background()
		id, err := h.Admit(ctx, actor.KindHuman, "apply-race")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.EditDeclaration(ctx, storespec.DeclEditBundle{
			ActorID: id, Class: "human.v2", Placement: storespec.NewServerPlacement(), CreatedAt: 2,
		}); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		results := make([]error, 2)
		start := make(chan struct{})
		for i := 0; i < 2; i++ {
			i := i
			go func() {
				defer wg.Done()
				<-start
				_, results[i] = h.ApplyDeclaration(ctx, id, 2)
			}()
		}
		close(start)
		wg.Wait()

		wins, regressions := 0, 0
		for _, rerr := range results {
			switch {
			case rerr == nil:
				wins++
			case errors.Is(rerr, ErrApplyVersionRegress):
				regressions++
			default:
				t.Fatalf("iteration %d unexpected racer error: %v", n, rerr)
			}
		}
		if wins != 1 || regressions != 1 {
			t.Fatalf("iteration %d wins=%d regressions=%d results=%v (verb slid on a stale pre-gate snapshot instead of rechecking the critical section)", n, wins, regressions, results)
		}
		row, ok, err := h.controlIndex.LookupActive(ctx, id)
		if err != nil || !ok || row.CurrentDeclVersion != 2 {
			t.Fatalf("iteration %d final row=%+v ok=%v err=%v", n, row, ok, err)
		}
		_ = h.Close()
	}
}

// TestEndConcurrentWithAuthorVersionAdvanceNeverSlidesOnStaleAuthority：一个
// goroutine 用旧 birth-version 铸造的生死把手对 child 发 End，另一个 goroutine
// 并发对 parent（= author）本体 ApplyDeclaration 换代——两者在 actorGates 上
// 竞争同一把 parent 锁（End 先锁 author.ID 再锁 target，与 Apply 锁 id 是同一
// 把锁）。无论谁先拿到锁，结果必须自洽：
//   - Apply 先提交 ⇒ End 进临界区时 CheckAuthor 用新鲜 CurrentDeclVersion 复
//     查，旧票被拒（ErrEndVersionStale），child 原样存活——不允许"已经通过
//     gate 排队"就被当成授权仍然有效的滑窗放行。
//   - End 先提交 ⇒ 那一刻 author 版本确实还没变，End 合法执行，child 必须
//     真被终结（不是半途而废的滑窗态）；随后 Apply 独立成功。
//
// 两种路径都用同一份新鲜读面数据核验，禁止"两者都成功"或"End 报错但版本没
// 变过"这类自相矛盾的结果——那正是滑窗会产生的坏结果。
func TestEndConcurrentWithAuthorVersionAdvanceNeverSlidesOnStaleAuthority(t *testing.T) {
	for n := 0; n < 20; n++ {
		h := openWhiteboxHome(t)
		ctx := context.Background()
		parent, err := h.Admit(ctx, actor.KindHuman, "end-vs-apply-race")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.EditDeclaration(ctx, storespec.DeclEditBundle{
			ActorID: parent, Class: "human.v2", Placement: storespec.NewServerPlacement(), CreatedAt: 2,
		}); err != nil {
			t.Fatal(err)
		}
		child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "worker"}, "end-vs-apply-child")
		if err != nil {
			t.Fatal(err)
		}
		staleEnd := lifecycleEndHandle{home: h, author: storespec.AuthorStamp{ID: parent, BirthVersion: 1}}

		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		var applyErr, endErr error
		go func() {
			defer wg.Done()
			<-start
			_, applyErr = h.ApplyDeclaration(ctx, parent, 2)
		}()
		go func() {
			defer wg.Done()
			<-start
			endErr = staleEnd.End(ctx, child, "race")
		}()
		close(start)
		wg.Wait()

		if applyErr != nil {
			t.Fatalf("iteration %d apply half of the race failed: %v", n, applyErr)
		}
		parentRow, ok, err := h.controlIndex.LookupActive(ctx, parent)
		if err != nil || !ok || parentRow.CurrentDeclVersion != 2 {
			t.Fatalf("iteration %d parent row after race=%+v ok=%v err=%v", n, parentRow, ok, err)
		}
		_, childActive, err := h.controlIndex.LookupActive(ctx, child)
		if err != nil {
			t.Fatalf("iteration %d child lookup: %v", n, err)
		}

		switch {
		case endErr == nil:
			if childActive {
				t.Fatalf("iteration %d End reported success but child still active — half-slid outcome", n)
			}
		case errors.Is(endErr, ErrEndVersionStale):
			if !childActive {
				t.Fatalf("iteration %d End was rejected as stale yet child got ended anyway — critical section did not gate the effect", n)
			}
		default:
			t.Fatalf("iteration %d unexpected end error: %v", n, endErr)
		}
		_ = h.Close()
	}
}
