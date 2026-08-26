package gateway

// Read pump tests (spec §3.2 收敛对象甲 pump phase): static backlog full delivery
// (DoD-7⑤), round-robin fairness (DoD-9), Admit poke → ≤下一泵轮入流 (DoD-7③), and
// busy-loop sweep observation under sustained backlog with NO poke (P0-1, 六轮终审).
// Most of these exercise the REAL runFeed goroutine over a REAL Home, waking on
// poke/Home-signal immediately so the default 30s backstop timers never fire in-test;
// TestBusyLoopObservesSweepUnderSustainedBacklog is the one exception — it injects a
// short SweepInterval via Config specifically to drive convergence off the timer
// backstop alone (no poke at all), the scenario this file previously had zero coverage
// for.

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type blockingHistoryBundle struct {
	channelhost.Bundle
	started chan struct{}
	release chan struct{}
}

func (b blockingHistoryBundle) View() channelhost.View {
	return blockingHistoryView{View: b.Bundle.View(), started: b.started, release: b.release}
}

type blockingHistoryView struct {
	channelhost.View
	started chan struct{}
	release chan struct{}
}

type attachSeamBundle struct {
	channelhost.Bundle
	beforeRead bool
	reached    chan struct{}
	release    chan struct{}
}

func (b attachSeamBundle) View() channelhost.View {
	return attachSeamView{View: b.Bundle.View(), beforeRead: b.beforeRead, reached: b.reached, release: b.release}
}

type attachSeamView struct {
	channelhost.View
	beforeRead bool
	reached    chan struct{}
	release    chan struct{}
}

func (v attachSeamView) ReadVisibleBeforeSeq(ctx context.Context, beforeSeq int64, limit int) ([]storespec.StoredRow, int64, bool, error) {
	if beforeSeq != 0 || limit != 1 {
		return v.View.ReadVisibleBeforeSeq(ctx, beforeSeq, limit)
	}
	wait := func() error {
		select {
		case v.reached <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-v.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if v.beforeRead {
		if err := wait(); err != nil {
			return nil, 0, false, err
		}
	}
	rows, head, older, err := v.View.ReadVisibleBeforeSeq(ctx, beforeSeq, limit)
	if err == nil && !v.beforeRead {
		if waitErr := wait(); waitErr != nil {
			return nil, 0, false, waitErr
		}
	}
	return rows, head, older, err
}

type failingHistoryBundle struct {
	channelhost.Bundle
	windowErr error
	pageErr   error
}

func (b failingHistoryBundle) View() channelhost.View {
	return failingHistoryView{View: b.Bundle.View(), windowErr: b.windowErr, pageErr: b.pageErr}
}

type failingHistoryView struct {
	channelhost.View
	windowErr error
	pageErr   error
}

func (v failingHistoryView) ReadVisibleBeforeSeq(ctx context.Context, beforeSeq int64, limit int) ([]storespec.StoredRow, int64, bool, error) {
	if beforeSeq == 0 && limit == 1 && v.windowErr != nil {
		return nil, 0, false, v.windowErr
	}
	return v.View.ReadVisibleBeforeSeq(ctx, beforeSeq, limit)
}

func (v failingHistoryView) ReadVisibleTurnWindowBeforeSeq(ctx context.Context, query channelspec.HistoryWindowQuery) (channelspec.HistoryWindow, error) {
	if query.MinimumCompleteRoots == 1 && v.pageErr != nil {
		return channelspec.HistoryWindow{}, v.pageErr
	}
	if v.windowErr != nil {
		return channelspec.HistoryWindow{}, v.windowErr
	}
	return v.View.ReadVisibleTurnWindowBeforeSeq(ctx, query)
}

func (v blockingHistoryView) ReadVisibleTurnWindowBeforeSeq(ctx context.Context, query channelspec.HistoryWindowQuery) (channelspec.HistoryWindow, error) {
	select {
	case v.started <- struct{}{}:
	default:
	}
	select {
	case <-v.release:
		return v.View.ReadVisibleTurnWindowBeforeSeq(ctx, query)
	case <-ctx.Done():
		return channelspec.HistoryWindow{}, ctx.Err()
	}
}

func TestAttachCarriesMetadataAndAnchorsLiveAtSnapshotHead(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "bounded-tail"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, defaultHistoryLimit+35)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	wantAll := sourceSeqs(t, h)
	head := wantAll[len(wantAll)-1]

	s, _ := g.Attach(principal, map[channel.ID]int64{"c": 0})
	if !s.PrimeFeed() {
		t.Fatal("PrimeFeed refused")
	}
	meta := s.PrepareHistoryMetadata("c")
	if len(meta) != 1 || meta[0].HeadSeq != head || !meta[0].HasRows || meta[0].LastActivity == 0 {
		t.Fatalf("unexpected attach history metadata: %+v", meta)
	}
	if got := s.lane.cursor.at("c"); got != head {
		t.Fatalf("live cursor not anchored at metadata head: got=%d want=%d", got, head)
	}
	if len(s.BackfillDown()) != 0 {
		t.Fatal("attach metadata must not enqueue message bodies")
	}
	s.LaunchFeed()
	s.Close()
}

func TestAttachMetadataAndLiveSubscriptionHaveNoCommitSeam(t *testing.T) {
	for _, tc := range []struct {
		name       string
		beforeRead bool
		wantLive   bool
	}{
		{name: "commit before snapshot belongs to history", beforeRead: true},
		{name: "commit after snapshot remains live", beforeRead: false, wantLive: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := newClock()
			res := newResolver()
			g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
			principal := "attach-seam-" + tc.name
			h, id := openHome(t, channel.ID("c"), principal)
			admitRows(t, h, 1)
			oldHead := sourceSeqs(t, h)[0]
			reached := make(chan struct{}, 1)
			release := make(chan struct{})
			bundle := attachSeamBundle{Bundle: h, beforeRead: tc.beforeRead, reached: reached, release: release}
			res.set(principal, []Route{{Channel: "c", SubjectID: id, Bundle: bundle}}, nil, nil)

			s, _ := g.Attach(principal, nil)
			if !s.PrimeFeed() {
				t.Fatal("PrimeFeed refused")
			}
			metaDone := make(chan []subjectgate.HistoryMetaEntry, 1)
			go func() { metaDone <- s.PrepareHistoryMetadata("c") }()
			select {
			case <-reached:
			case <-time.After(2 * time.Second):
				t.Fatal("metadata read did not reach seam")
			}
			admitRows(t, h, 1)
			newHead := sourceSeqs(t, h)[1]
			close(release)
			meta := <-metaDone
			wantMetaHead := oldHead
			if tc.beforeRead {
				wantMetaHead = newHead
			}
			if len(meta) != 1 || meta[0].HeadSeq != wantMetaHead {
				t.Fatalf("metadata seam head: got=%+v want=%d", meta, wantMetaHead)
			}

			feed, stop := observeFeed(s)
			s.LaunchFeed()
			if tc.wantLive {
				waitFor(t, func() bool { return feed.lastSeq("c") == newHead }, "post-snapshot commit was lost")
			} else {
				time.Sleep(30 * time.Millisecond)
				if got := feed.delivered("c"); got != 0 {
					t.Fatalf("snapshot-owned commit was duplicated on live feed: %d", got)
				}
			}
			s.Close()
			stop()
		})
	}
}

func TestAttachHistoryFailureIsExplicitAndLogged(t *testing.T) {
	clk := newClock()
	res := newResolver()
	logs := &logCapture{}
	g := newTestGateway(t, Config{Resolver: res, Logger: logs.logger()}, settings{clock: clk})
	const principal = "attach-history-error"
	h, id := openHome(t, channel.ID("c"), principal)
	broken := failingHistoryBundle{Bundle: h, windowErr: errors.New("read failed")}
	res.set(principal, []Route{{Channel: "c", SubjectID: id, Bundle: broken}}, nil, nil)

	s, _ := g.Attach(principal, nil)
	if !s.PrimeFeed() {
		t.Fatal("PrimeFeed refused")
	}
	grants := s.PrepareHistoryMetadata("c")
	if len(grants) != 1 || grants[0].ChannelID != "c" || grants[0].ErrorCode != subjectgate.CodeUnavailable {
		t.Fatalf("attach history failure was silent: %+v", grants)
	}
	if !logs.has("gateway.history.attach_failed") {
		t.Fatal("attach history failure was not logged")
	}
	s.Close()
}

func TestAttachMetadataDoesNotFillBackfillLane(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "attach-reader-free"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, defaultHistoryLimit+10)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(principal, nil)
	if !s.PrimeFeed() {
		t.Fatal("PrimeFeed refused")
	}
	meta := s.PrepareHistoryMetadata("c")
	if len(meta) != 1 || meta[0].HeadSeq == 0 {
		t.Fatalf("missing metadata: %+v", meta)
	}
	if got := s.lane.cursor.at("c"); got == 0 {
		t.Fatal("live cursor was not advanced to metadata snapshot")
	}
	if len(s.BackfillDown()) != 0 {
		t.Fatal("metadata-only attach unexpectedly enqueued backfill")
	}
	s.LaunchFeed()
	s.Close()
	s.historyWG.Wait()
}

func TestHistoryBeforePagesWithoutMovingLiveCursor(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "history-page"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 12)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	all := sourceSeqs(t, h)
	head := all[len(all)-1]

	s, _ := g.Attach(principal, map[channel.ID]int64{"c": head}, 1)
	feed, stop := observeFeed(s)
	s.StartFeed()
	defer func() {
		s.Close()
		stop()
	}()
	request, err := subjectgate.NewFrame(subjectgate.FrameHistoryBefore, "history-1", subjectgate.HistoryBeforePayload{
		ChannelID: "c", BeforeSeq: 0, Limit: 3, Generation: 1, Purpose: "user-demand", Priority: "foreground",
	})
	if err != nil {
		t.Fatal(err)
	}
	response := s.Upstream(request)
	if response.Type != subjectgate.FrameReceipt {
		t.Fatalf("history request failed: %+v", response)
	}
	var accepted subjectgate.HistoryAcceptedReceipt
	if err := json.Unmarshal(response.Payload, &accepted); err != nil || !accepted.Accepted {
		t.Fatal(err)
	}
	want := all[len(all)-3:]
	waitFor(t, func() bool {
		_, ended := feed.pageEnd("history-1")
		return len(feed.sequences("c")) == len(want) && ended
	}, "history feed rows")
	got := feed.sequences("c")
	page, _ := feed.pageEnd("history-1")
	if !slices.Equal(got, want) || !page.HasOlder {
		t.Fatalf("unexpected history page: got=%v want=%v page=%+v", got, want, page)
	}
	if cursor := s.lane.cursor.at("c"); cursor != head {
		t.Fatalf("history read moved live cursor: got=%d want=%d", cursor, head)
	}
}

func TestAcceptedHistoryFailureAlwaysEndsWithCorrelatedPageEnd(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "history-page-error"
	h, id := openHome(t, channel.ID("c"), principal)
	broken := failingHistoryBundle{Bundle: h, pageErr: errors.New("read failed")}
	res.set(principal, []Route{{Channel: "c", SubjectID: id, Bundle: broken}}, nil, nil)
	s, _ := g.Attach(principal, nil, 1)
	feed, stop := observeFeed(s)
	s.StartFeed()
	defer func() {
		s.Close()
		stop()
	}()

	request, _ := subjectgate.NewFrame(subjectgate.FrameHistoryBefore, "history-error", subjectgate.HistoryBeforePayload{
		ChannelID: "c", BeforeSeq: 10, Limit: 10, Generation: 1, Purpose: "hydrate", Priority: "background",
	})
	response := s.Upstream(request)
	var accepted subjectgate.HistoryAcceptedReceipt
	if response.Type != subjectgate.FrameReceipt || response.DecodePayload(&accepted) != nil || !accepted.Accepted {
		t.Fatalf("history was not accepted: %+v", response)
	}
	waitFor(t, func() bool {
		end, ok := feed.pageEnd("history-error")
		return ok && end.ErrorCode == subjectgate.CodeUnavailable
	}, "accepted history failure did not terminate")
}

func TestDispatchQueuesHistoryAcceptanceBeforeStartingBackfill(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "history-publication-barrier"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 3)
	all := sourceSeqs(t, h)
	head := all[len(all)-1]
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	res.set(principal, []Route{{
		Channel: "c", SubjectID: id,
		Bundle: blockingHistoryBundle{Bundle: h, started: started, release: release},
	}}, nil, nil)
	s, _ := g.Attach(principal, map[channel.ID]int64{"c": head}, 1)
	s.StartFeed()
	defer s.Close()

	request, _ := subjectgate.NewFrame(subjectgate.FrameHistoryBefore, "history-barrier", subjectgate.HistoryBeforePayload{
		ChannelID: "c", BeforeSeq: 0, Limit: 3, Generation: 1, Purpose: "hydrate", Priority: "background",
	})
	dispatched := make(chan struct{})
	go func() {
		s.Dispatch(request)
		close(dispatched)
	}()
	select {
	case raw := <-s.LiveDown():
		frame, err := subjectgate.ParseEnvelope(raw)
		if err != nil || frame.Type != subjectgate.FrameReceipt || frame.Ref != "history-barrier" {
			t.Fatalf("first published frame was not acceptance: frame=%+v err=%v", frame, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("history acceptance was not published")
	}
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not return after publishing acceptance")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("history read was not released after acceptance publication")
	}
	close(release)
}

func TestHistoryReadNeverBlocksRealtimeFeed(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "history-does-not-block-live"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 3)
	all := sourceSeqs(t, h)
	head := all[len(all)-1]
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	res.set(principal, []Route{{
		Channel: "c", SubjectID: id,
		Bundle: blockingHistoryBundle{Bundle: h, started: started, release: release},
	}}, nil, nil)

	s, _ := g.Attach(principal, map[channel.ID]int64{"c": head}, 1)
	feed, stop := observeFeed(s)
	s.StartFeed()
	request, err := subjectgate.NewFrame(subjectgate.FrameHistoryBefore, "slow-history", subjectgate.HistoryBeforePayload{
		ChannelID: "c", BeforeSeq: 0, Limit: 20, Generation: 1, Purpose: "user-demand", Priority: "foreground",
	})
	if err != nil {
		t.Fatal(err)
	}
	response := make(chan subjectgate.Frame, 1)
	go func() { response <- s.Upstream(request) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("history read did not start")
	}

	// A commit arriving while history is deliberately blocked must cross the
	// realtime feed boundary before that history read is released.
	admitRows(t, h, 1)
	newAll := sourceSeqs(t, h)
	newHead := newAll[len(newAll)-1]
	waitFor(t, func() bool { return feed.lastSeq("c") >= newHead }, "live feed stalled behind history read")
	select {
	case frame := <-response:
		var accepted subjectgate.HistoryAcceptedReceipt
		if frame.Type != subjectgate.FrameReceipt || frame.DecodePayload(&accepted) != nil || !accepted.Accepted {
			t.Fatalf("history was not accepted immediately: %+v", frame)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("history acceptance blocked on the historical read")
	}

	releaseOnce.Do(func() { close(release) })
	waitFor(t, func() bool { _, ok := feed.pageEnd("slow-history"); return ok }, "history did not complete after release")
	s.Close()
	stop()
}

func TestHistoryCancelStopsReadAndReleasesAdmission(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "history-cancel"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 3)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	res.set(principal, []Route{{
		Channel: "c", SubjectID: id,
		Bundle: blockingHistoryBundle{Bundle: h, started: started, release: release},
	}}, nil, nil)
	s, _ := g.Attach(principal, nil, 3)
	s.StartFeed()
	defer s.Close()

	request, _ := subjectgate.NewFrame(subjectgate.FrameHistoryBefore, "history-to-cancel", subjectgate.HistoryBeforePayload{
		ChannelID: "c", Limit: 20, Generation: 3, Purpose: "user-demand", Priority: "foreground",
	})
	response := s.Upstream(request)
	if response.Type != subjectgate.FrameReceipt {
		t.Fatalf("history was not accepted: %+v", response)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("history read did not start")
	}
	cancelFrame, _ := subjectgate.NewFrame(subjectgate.FrameHistoryCancel, "cancel-1", subjectgate.HistoryCancelPayload{
		ChannelID: "c", TargetRef: "history-to-cancel", Generation: 3,
	})
	cancelled := s.Upstream(cancelFrame)
	if cancelled.Type != subjectgate.FrameReceipt {
		t.Fatalf("history cancel was not accepted: %+v", cancelled)
	}
	waitFor(t, func() bool {
		s.historyMu.Lock()
		defer s.historyMu.Unlock()
		return s.historyCount == 0 && len(s.historyInflight) == 0
	}, "cancelled history did not release admission")
}

func TestLivePumpPublishesExactScannedCheckpoint(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "checkpoint"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 4)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(principal, nil, 7)
	feed, stop := observeFeed(s)
	s.StartFeed()
	defer func() { s.Close(); stop() }()
	waitFor(t, func() bool {
		checkpoint, ok := feed.latestCheckpoint("c")
		return ok && checkpoint.ScanLowSeq == 1 && checkpoint.ScannedSeq >= feed.lastSeq("c")
	}, "live scan checkpoint was not published")
	checkpoint, _ := feed.latestCheckpoint("c")
	if checkpoint.Generation != 7 || checkpoint.ScannedSeq < checkpoint.ScanLowSeq {
		t.Fatalf("invalid checkpoint: %+v", checkpoint)
	}
}

// TestStaticBacklogFullDelivery (DoD-7⑤): a channel with a static backlog >2×feedBatch
// and zero new writes is fully delivered — the pump's 积压续跑 keeps a channel runnable
// while it reads a full batch, so it drains the whole tail rather than one batch per
// edge. Asserted on the real downstream feed reaching the channel head.
func TestStaticBacklogFullDelivery(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "rob"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 2*feedBatch+5) // >2×feedBatch rows beyond the member's own admit row
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)

	want := sourceSeqs(t, h)
	head := want[len(want)-1]
	if head <= 2*feedBatch {
		t.Fatalf("test needs a backlog >2×feedBatch, got head=%d", head)
	}

	s, _ := g.Attach(principal, nil)
	feed, stop := observeFeed(s)
	s.StartFeed()

	waitFor(t, func() bool { return len(feed.sequences("c")) >= len(want) },
		"static backlog was not fully delivered (积压续跑 broken)")
	s.Close()
	stop()
	if got := feed.sequences("c"); !slices.Equal(got, want) {
		t.Fatalf("downstream feed sequence mismatch: got %v want %v", got, want)
	}
}

// TestPumpFairness (DoD-9): a hot channel with a large backlog must not starve a cold
// channel with a tiny one — round-robin one batch per channel per轮 delivers the cold
// channel's rows within a couple of rotations, long before the hot channel finishes
// draining. Asserted: the cold channel reaches its head while the hot channel may still
// be draining.
func TestPumpFairness(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "sam"
	hot, hotID := openHome(t, channel.ID("hot"), principal)
	cold, coldID := openHome(t, channel.ID("cold"), principal)
	admitRows(t, hot, 2*feedBatch+5) // multi-batch backlog (round-robin one batch/轮)
	admitRows(t, cold, 3)            // tiny backlog

	res.set(principal, []Route{
		memberRoute("hot", hot, hotID, clk.now()),
		memberRoute("cold", cold, coldID, clk.now()),
	}, nil, nil)

	coldSeqs := sourceSeqs(t, cold)
	hotSeqs := sourceSeqs(t, hot)
	coldHead := coldSeqs[len(coldSeqs)-1]
	hotHead := hotSeqs[len(hotSeqs)-1]
	s, _ := g.Attach(principal, nil)
	var feed *feedObserver
	var stop func()
	coldReached := make(chan int64, 1)
	var coldOnce sync.Once
	feed, stop = observeFeed(s, func(ch channel.ID, _ int) {
		if ch == "cold" && feed.lastSeq("cold") >= coldHead {
			coldOnce.Do(func() { coldReached <- int64(feed.delivered("hot")) })
		}
	})
	s.StartFeed()

	var hotAtCold int64
	select {
	case hotAtCold = <-coldReached:
	case <-time.After(2 * time.Second):
		t.Fatal("cold channel starved by the hot channel (fairness broken)")
	}
	// At the exact wire boundary where cold reaches its head, hot may have emitted at
	// most its one batch for that round. An implementation that drains hot completely
	// before visiting cold therefore fails instead of passing on eventual delivery.
	if hotAtCold > feedBatch || hotAtCold >= int64(len(sourceSeqs(t, hot))) {
		t.Fatalf("cold arrived only after hot overran its one-batch turn: hot=%d head=%d batch=%d", hotAtCold, hotHead, feedBatch)
	}
	s.Close()
	stop()
}

func sourceSeqs(t *testing.T, h *testChannel) []int64 {
	t.Helper()
	var seqs []int64
	after := int64(0)
	for {
		rows, scanned, err := h.View().ReadVisibleAfterSeq(context.Background(), after, feedBatch)
		if err != nil {
			t.Fatalf("ReadAfterSeq(%d): %v", after, err)
		}
		for _, row := range rows {
			seqs = append(seqs, row.Seq)
		}
		after = scanned
		if len(rows) < feedBatch {
			break
		}
	}
	if len(seqs) == 0 {
		t.Fatal("source log unexpectedly empty")
	}
	return seqs
}

// TestBusyLoopObservesSweepUnderSustainedBacklog (P0-1 终审锚): a channel with a deep
// sustained backlog (every batch read is a FULL feedBatch) keeps the pump on the
// busy→continue path indefinitely — before the fix, that path never called wait(), so
// the fired periodic sweep timer was never drained and dirty never got re-armed. A
// caller who revokes eligibility with NO poke (the exact "poke lost/never sent"
// scenario codex's terminal review named) would then stream past the revocation with NO
// upper bound — a permission data leak, not an in-一圈 advisory偏差. This test injects a
// short SweepInterval and asserts the channel retires within a bounded real-time window
// WHILE the backlog is still far from fully drained (proving the busy loop, not merely
// the eventual drain-to-completion, is what caught the revocation).
func TestBusyLoopObservesSweepUnderSustainedBacklog(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk, sweepInterval: 10 * time.Millisecond})
	const principal = "revoked-busy"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 3*feedBatch) // deep backlog: every batch read is a full feedBatch.
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)

	seqs := sourceSeqs(t, h)
	head := seqs[len(seqs)-1]
	revoked := make(chan struct{})
	var revokeOnce sync.Once
	s, _ := g.Attach(principal, nil)
	feed, stop := observeFeed(s, func(ch channel.ID, count int) {
		if ch != "c" || count != 1 {
			return
		}
		revokeOnce.Do(func() {
			// Revoke while the first full batch is visibly crossing the downstream
			// boundary. runFeed has armed its sweep before it can emit this frame.
			res.set(principal, nil, nil, nil)
			clk.advance(10 * time.Millisecond)
			close(revoked)
		})
	})
	s.StartFeed()

	select {
	case <-revoked:
	case <-time.After(2 * time.Second):
		t.Fatal("pump never began crossing the first full batch")
	}

	waitFor(t, func() bool {
		_, ok := eligRoutes(s)["c"]
		return !ok
	}, "revoked channel must retire within a bounded time even under a sustained busy backlog (P0-1: busy→continue must still observe sweep/poke)")

	// The backlog must still be far from fully drained at the moment of retirement —
	// otherwise this test would pass even on the pre-fix code merely because the busy
	// loop happened to finish the whole backlog before anyone looked (proving nothing
	// about bounded revocation response).
	stoppedAt := feed.lastSeq("c")
	if stoppedAt >= head {
		t.Fatalf("backlog (%d rows) fully drained before the sweep-bound revocation could be observed; deepen the backlog or shrink SweepInterval — stoppedAt=%d head=%d", 3*feedBatch, stoppedAt, head)
	}

	s.Close()
	stop()
}

func TestBusyLoopDrainsObserveControlsBeforeNextFeedBatch(t *testing.T) {
	clk := newClock()
	res := newResolver()
	const principal = "observe-during-busy-feed"
	hot, memberID := openHome(t, channel.ID("hot"), principal)
	admitRows(t, hot, 4*feedBatch)
	observed, _ := openHome(t, channel.ID("observed"), "different-member")
	res.set(principal, []Route{memberRoute("hot", hot, memberID, clk.now())}, nil, nil)
	g := newTestGateway(t, Config{
		Resolver: res,
		Observer: ObserverResolverFunc(func(context.Context, string, channel.ID) (ObserverRoute, string, error) {
			return ObserverRoute{
				Channel: "observed", Bundle: observed,
				Reader: Reader{Principal: principal, Mode: ReaderObserver},
			}, "", nil
		}),
	}, settings{clock: clk})

	firstBatch := make(chan struct{})
	var once sync.Once
	s, _ := g.Attach(principal, nil)
	_, stop := observeFeed(s, func(ch channel.ID, count int) {
		if ch == "hot" && count == 1 {
			once.Do(func() { close(firstBatch) })
		}
	})
	s.StartFeed()
	select {
	case <-firstBatch:
	case <-time.After(2 * time.Second):
		t.Fatal("hot backlog never entered the busy pump path")
	}

	frame, err := subjectgate.NewFrame(subjectgate.FrameObserve, "observe-hot-loop", subjectgate.ObservePayload{ChannelID: "observed"})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan subjectgate.Frame, 1)
	go func() { result <- s.Upstream(frame) }()
	select {
	case got := <-result:
		if got.Type != subjectgate.FrameReceipt || got.Ref != "observe-hot-loop" {
			t.Fatalf("observe result=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("observe control starved behind the sustained full-batch feed")
	}

	s.Close()
	stop()
}

func TestObservationReasonsAreNormalizedAtGatewayBoundary(t *testing.T) {
	tests := []struct {
		code string
		want subjectgate.ObserveEndedReason
	}{
		{subjectgate.CodeNowMember, subjectgate.ObserveEndedNowMember},
		{subjectgate.CodeChannelNotFound, subjectgate.ObserveEndedChannelRetired},
		{subjectgate.CodeChannelUnavailable, subjectgate.ObserveEndedChannelUnavailable},
		{subjectgate.CodeCapabilityUnavailable, subjectgate.ObserveEndedCapabilityUnavailable},
		{"future_policy_reason", subjectgate.ObserveEndedCapabilityUnavailable},
	}
	for _, test := range tests {
		if got := observeEndedReason(test.code); got != test.want {
			t.Errorf("observeEndedReason(%q)=%q want %q", test.code, got, test.want)
		}
	}
	if got := normalizeObservationCode("future_policy_reason"); got != subjectgate.CodeCapabilityUnavailable {
		t.Fatalf("unknown resolver reason escaped the gateway boundary: %q", got)
	}
}

// TestAdmitWithoutPokeConvergesOnSweep (DoD-6/7③ timer backstop): the connection is
// already running with no channels. A real Home.Admit then appears in resolver truth,
// but there is deliberately no projection-event wire and no hand Poke. Advancing the
// injected clock fires runFeed's real sweep timer and the new channel enters the stream.
func TestAdmitWithoutPokeConvergesOnSweep(t *testing.T) {
	clk := newClock()
	res := newResolver()
	const sweep = 10 * time.Second
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk, sweepInterval: sweep})
	const principal = "no-poke-admit"
	res.set(principal, nil, nil, nil)

	s, _ := g.Attach(principal, nil)
	feed, stop := observeFeed(s)
	s.StartFeed()
	waitFor(t, func() bool { return res.callCount() >= 2 && clk.armCount() >= 2 },
		"running pump did not arm its sweep timer")

	// Admit after the pump is waiting. openHome has no membership callback, so this
	// truth change cannot reach the gateway except through the periodic sweep.
	h, id := openHome(t, channel.ID("late"), principal)
	admitRows(t, h, 1)
	res.set(principal, []Route{memberRoute("late", h, id, clk.now())}, nil, nil)
	want := sourceSeqs(t, h)
	head := want[len(want)-1]
	clk.advance(sweep)
	waitFor(t, func() bool { return feed.lastSeq("late") >= head },
		"no-poke Admit did not converge through the real sweep timer")

	s.Close()
	stop()
}

// TestAdmitPokeEntersStream (DoD-7③): a running pump with NO eligibility yet; when the
// resolver gains a channel and a poke fires (the Admit membership-change poke), the
// channel enters the stream within a bounded time (≤下一泵轮) — the dirty/wake edge
// re-resolves, subscribes, and pumps the backlog. Asserted on the real feed advancing.
func TestAdmitPokeEntersStream(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "tom"
	h, id := openHome(t, channel.ID("c"), principal)
	admitRows(t, h, 1)
	// Start with NO eligibility for tom.
	res.set(principal, nil, nil, nil)

	s, _ := g.Attach(principal, nil)
	feed, stop := observeFeed(s)
	s.StartFeed()
	// Initially no subscription (nothing eligible).
	waitFor(t, func() bool { return len(eligRoutes(s)) == 0 }, "expected no eligibility initially")

	// Admit lands: eligibility appears + a poke踹 the pump.
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	g.Poke(principal)

	want := sourceSeqs(t, h)
	head := want[len(want)-1]
	waitFor(t, func() bool { return feed.lastSeq("c") >= head },
		"Admit poke did not bring the channel into the stream (≤下一泵轮 broken)")
	s.Close()
	stop()
}
