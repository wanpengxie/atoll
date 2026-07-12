package link

import (
	"sync"
	"testing"
	"time"
)

func TestAcceptorServeCloseAdmissionStraddle(t *testing.T) {
	a := NewAcceptor(Config{})
	parked, release := make(chan struct{}), make(chan struct{})
	a.admissionHook = func() { close(parked); <-release }
	admitted := make(chan bool, 1)
	go func() { admitted <- a.beginServe() }()
	<-parked
	closed := make(chan struct{})
	go func() { _ = a.closeWithin(time.Second); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close crossed an in-flight admission")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if !<-admitted {
		t.Fatal("admission that linearized before Close was rejected")
	}
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
	a := NewAcceptor(Config{})
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
// accounted), and an admission attempt AFTER sealAndWait has begun must be
// refused — no Add can ever chase an in-progress Wait.
func TestActorGateSealFencesLateAdmission(t *testing.T) {
	ls, writer, _ := newControlKillRig(t, func([]byte) {})
	ls.kill("test-close", nil)
	_ = writer.Close() // control worker exits cleanly; this test is about the actor half
	if !ls.waitControlWorkers(time.Second) {
		t.Fatal("control worker did not exit after writer close")
	}
	a := NewAcceptor(Config{})
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

// TestActorGateAdmitRaceWithSeal stresses concurrent admit×sealAndWait under
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
		if !gate.sealAndWait(time.Second) {
			t.Fatal("gate did not converge")
		}
		admitted.Wait()
	}
}

func TestAcceptorConcurrentCloseCountsOneLeak(t *testing.T) {
	a := NewAcceptor(Config{})
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
