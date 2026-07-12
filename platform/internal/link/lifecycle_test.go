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

func TestControlWorkerTimeoutAccountsWorkerAndWaiterOnce(t *testing.T) {
	a := NewAcceptor(Config{})
	var workers sync.WaitGroup
	workers.Add(1)
	if waitGroupWithin(&workers, 20*time.Millisecond) {
		t.Fatal("stuck worker joined")
	}
	a.recordControlWorkerLeak()
	if a.Leaked() != 1 {
		t.Fatalf("Leaked = %d, want one incident", a.Leaked())
	}
	workers.Done() // releases both the worker account and its timeout waiter
}

func TestControlWorkerRealSessionTimeoutAndRecovery(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	ls, writer, _ := newControlKillRig(t, func([]byte) { close(entered); <-release })
	if _, err := writer.Write([]byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	<-entered
	ls.kill("test-close", nil)
	if ls.waitControlWorkers(20 * time.Millisecond) {
		t.Fatal("blocked control worker joined")
	}
	a := NewAcceptor(Config{})
	a.recordControlWorkerLeak()
	if a.Leaked() != 1 {
		t.Fatalf("Leaked = %d, want one worker+waiter incident", a.Leaked())
	}
	close(release)
	_ = writer.Close()
	if !ls.waitControlWorkers(time.Second) {
		t.Fatal("released control worker/waiter did not converge")
	}
}

func TestActorWorkerGroupIsBoundedAndAccounted(t *testing.T) {
	a := NewAcceptor(Config{})
	var actors sync.WaitGroup
	actors.Add(1)
	if waitGroupWithin(&actors, 20*time.Millisecond) {
		t.Fatal("stuck actor handshake joined")
	}
	a.leaked.Add(1)
	if a.Leaked() != 1 {
		t.Fatalf("Leaked = %d, want 1", a.Leaked())
	}
	actors.Done()
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
