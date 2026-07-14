package link

import (
	"context"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/runtime/actorrt"
)

func newLifecycleAcceptor() *Acceptor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Acceptor{
		ctx:       ctx,
		cancel:    cancel,
		closeDone: make(chan struct{}),
		logger:    slog.New(slog.DiscardHandler),
	}
}

func TestAcceptorServeCloseAdmissionStraddle(t *testing.T) {
	a := newLifecycleAcceptor()
	// Linearize one admitted Serve while holding the same barrier beginServe and
	// Close use. This exercises the actual wait-group/close ordering without a
	// production hook.
	a.admissionMu.Lock()
	if a.closed {
		t.Fatal("fresh acceptor is closed")
	}
	a.wg.Add(1)
	closed := make(chan struct{})
	go func() { _ = a.closeWithin(time.Second); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close crossed an in-flight admission")
	case <-time.After(20 * time.Millisecond):
	}
	a.admissionMu.Unlock()
	select {
	case <-closed:
		t.Fatal("Close returned before admitted Serve completed")
	case <-time.After(20 * time.Millisecond):
	}
	a.endServe()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not join admitted Serve")
	}
	if a.beginServe() {
		t.Fatal("Serve admitted after Close")
	}
	if a.Leaked() != 0 {
		t.Fatalf("Leaked = %d", a.Leaked())
	}
}

// TestAcceptorCloseJoinsOwnedDelayedCallback proves reject-drain timers share
// Acceptor's admission barrier: a callback admitted before Close is joined, so
// it cannot run against link state after Close returns.
func TestAcceptorCloseJoinsOwnedDelayedCallback(t *testing.T) {
	a := newLifecycleAcceptor()
	entered, release := make(chan struct{}), make(chan struct{})
	a.afterOwned(0, func() {
		close(entered)
		<-release
	})
	<-entered
	closed := make(chan struct{})
	go func() {
		_ = a.closeWithin(time.Second)
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while an owned delayed callback was still running")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not join the released delayed callback")
	}
	if got := a.Leaked(); got != 0 {
		t.Fatalf("Leaked = %d, want 0", got)
	}
}

// TestControlWorkerTimeoutAccountedThroughJoin drives the REAL teardown join
// (joinLinkWorkers — the exact function runLink calls) against a real
// linkSession whose control worker is wedged in its handler: the join must
// abandon within the control budget, count worker+waiter as ONE incident, and
// converge once the handler is released.
func TestControlWorkerTimeoutAccountedThroughJoin(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	ls, writer, _ := newControlKillRig(t, func([]byte) { close(entered); <-release })
	if _, err := writer.Write([]byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	<-entered
	ls.kill("test-close", nil)
	a := newLifecycleAcceptor()
	joined := make(chan struct{})
	go func() {
		a.joinLinkWorkers(ls, &actorGate{}, "daemon-x", 20*time.Millisecond, 20*time.Millisecond)
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("join did not abandon the wedged control worker within its budget")
	}
	if a.Leaked() != 1 {
		t.Fatalf("Leaked = %d, want one worker+waiter incident", a.Leaked())
	}
	close(release)
	_ = writer.Close()
	if !ls.waitControlWorkers(time.Second) {
		t.Fatal("released control worker/waiter did not converge")
	}
}

// TestActorGateSealFencesLateAdmission locks the Add×Wait race repair: a
// worker admitted before the seal is joined (or abandoned on the bound and
// accounted), and an admission attempt AFTER waitWithin has begun must be
// refused — no Add can ever chase an in-progress Wait.
func TestActorGateSealFencesLateAdmission(t *testing.T) {
	ls, writer, _ := newControlKillRig(t, func([]byte) {})
	ls.kill("test-close", nil)
	_ = writer.Close() // control worker exits cleanly; this test is about the actor half
	if !ls.waitControlWorkers(time.Second) {
		t.Fatal("control worker did not exit after writer close")
	}
	a := newLifecycleAcceptor()
	gate := &actorGate{}
	if !gate.admit() {
		t.Fatal("pre-seal admission refused")
	}
	release := make(chan struct{})
	go func() { <-release; gate.done() }()
	joined := make(chan struct{})
	go func() {
		a.joinLinkWorkers(ls, gate, "daemon-x", time.Second, 20*time.Millisecond)
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("join did not abandon the stuck actor worker within its budget")
	}
	if a.Leaked() != 1 {
		t.Fatalf("Leaked = %d, want one actor-join incident", a.Leaked())
	}
	if gate.admit() {
		t.Fatal("admission after seal was accepted — late Add can chase the Wait")
	}
	close(release)
}

// TestActorGateAdmitRaceWithSeal stresses concurrent admit×waitWithin under
// -race: every admit that returns true is matched by done(), every admit that
// loses to the seal must not Add — the Wait converges without panic.
func TestActorGateAdmitRaceWithSeal(t *testing.T) {
	for range 200 {
		gate := &actorGate{}
		var admitted sync.WaitGroup
		for range 4 {
			admitted.Add(1)
			go func() {
				defer admitted.Done()
				if gate.admit() {
					gate.done()
				}
			}()
		}
		if joined, _ := gate.waitWithin(time.Second); !joined {
			t.Fatal("gate did not converge")
		}
		admitted.Wait()
	}
}

func TestActorGateTimeoutObserversShareOneTerminal(t *testing.T) {
	gate := &actorGate{}
	if !gate.admit() {
		t.Fatal("initial admission refused")
	}
	const observers = 8
	results := make(chan bool, observers)
	for range observers {
		go func() {
			joined, _ := gate.waitWithin(10 * time.Millisecond)
			results <- joined
		}()
	}
	for range observers {
		if <-results {
			t.Fatal("observer joined before worker completion")
		}
	}
	gate.done()
	gate.mu.Lock()
	done := gate.waitDone
	gate.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shared terminal channel was not closed after worker completion")
	}
	if joined, _ := gate.waitWithin(time.Second); !joined {
		t.Fatal("terminal observer did not see the settled completion edge")
	}
}

func TestAcceptorConcurrentCloseCountsOneLeak(t *testing.T) {
	a := newLifecycleAcceptor()
	if !a.beginServe() {
		t.Fatal("initial Serve rejected")
	}
	const callers = 8
	done := make(chan struct{}, callers)
	for range callers {
		go func() { _ = a.closeWithin(20 * time.Millisecond); done <- struct{}{} }()
	}
	for range callers {
		<-done
	}
	if a.Leaked() != 1 {
		t.Fatalf("Leaked = %d, want one close incident", a.Leaked())
	}
	a.endServe()
}

func TestAcceptorCloseBudgetCoversOrderedInnerBudgets(t *testing.T) {
	want := 3*streamWriteBudget + attachHandshakeTimeout + 5*time.Second
	if got := acceptorCloseBudget(); got != want {
		t.Fatalf("acceptor close budget=%v want %v", got, want)
	}
}

func TestAcceptedAttachPromotesBeforeReplyBecomesObservable(t *testing.T) {
	a := newLifecycleAcceptor()
	a.slots = map[string]*incumbentSlot{}
	lc := &linkSession{}
	a.slots["daemon-x"] = &incumbentSlot{link: lc, state: incumbentCandidate}

	replyObserved := false
	ok := a.publishAcceptedAttach("daemon-x", lc, func() error {
		replyObserved = true
		pin, err := a.enterPortWriter("daemon-x", lc)
		if err != nil {
			return err
		}
		pin.finish(true)
		return nil
	})
	if !replyObserved || !ok {
		t.Fatalf("replyObserved=%v accepted=%v", replyObserved, ok)
	}
}

func TestLinkHandleConcurrentGracefulCloseSharesOneOrderedPipeline(t *testing.T) {
	gate := &actorGate{}
	entered, release := make(chan struct{}), make(chan struct{})
	var stagesMu sync.Mutex
	var stages []string
	record := func(stage string) {
		stagesMu.Lock()
		stages = append(stages, stage)
		stagesMu.Unlock()
	}
	var inc actorrt.Incarnation
	h := &linkHandle{
		gate: gate,
		invalidate: func() {
			if gate.admit() {
				gate.done()
				t.Error("invalidate ran before actor admission was sealed")
			}
			record("invalidate")
		},
		waitWorkers: func() {
			record("wait")
			close(entered)
			<-release
		},
		takePorts: func() []actorrt.Incarnation {
			record("snapshot")
			return []actorrt.Incarnation{inc}
		},
		quietPort: func(actorrt.Incarnation) { record("quiet") },
		closeCarrier: func() {
			record("carrier")
		},
	}

	done := make(chan struct{}, 2)
	go func() { h.closeQuietly(); done <- struct{}{} }()
	<-entered
	go func() { h.closeQuietly(); done <- struct{}{} }()
	select {
	case <-done:
		t.Fatal("a concurrent close returned before the shared pipeline completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-done
	<-done

	stagesMu.Lock()
	got := append([]string(nil), stages...)
	stagesMu.Unlock()
	want := []string{"invalidate", "wait", "snapshot", "quiet", "carrier"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("graceful stages=%v want %v", got, want)
	}
}

func TestHardKillDoesNotWaitForConcurrentGracefulClose(t *testing.T) {
	ls, controlPeer, _ := newControlKillRig(t, func([]byte) {})
	defer controlPeer.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	h := &linkHandle{
		gate:       &actorGate{},
		invalidate: func() {},
		waitWorkers: func() {
			close(entered)
			<-release
		},
		takePorts:    func() []actorrt.Incarnation { return nil },
		quietPort:    func(actorrt.Incarnation) {},
		closeCarrier: func() { _ = ls.Close() },
	}
	gracefulDone := make(chan struct{})
	go func() { h.closeQuietly(); close(gracefulDone) }()
	<-entered

	hardDone := make(chan struct{})
	go func() { ls.kill("hard-test", nil); close(hardDone) }()
	select {
	case <-hardDone:
	case <-time.After(time.Second):
		t.Fatal("hard kill waited for graceful worker drain")
	}
	select {
	case <-ls.closed():
	case <-time.After(time.Second):
		t.Fatal("hard kill did not close carrier")
	}
	close(release)
	select {
	case <-gracefulDone:
	case <-time.After(time.Second):
		t.Fatal("graceful close did not converge after hard kill")
	}
}
