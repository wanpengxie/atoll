package device

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

func decodeString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var out string
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return out
}

// waitUntilAccepted blocks until the run loop has drained the mailbox, i.e.
// every pushed delivery has been taken and handed to a goroutine. It is how a
// test says "the work is in flight" without reaching into the actor.
func waitUntilAccepted(t *testing.T, f *fakeSys) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(f.recvCh) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("the run loop never drained the mailbox")
		}
		time.Sleep(time.Millisecond)
	}
	// Draining is the loop reading; give the spawned goroutine its start.
	time.Sleep(20 * time.Millisecond)
}

// A device is a whole machine, and its words are as slow as whatever they run:
// device.exec caps at ten minutes. Answering deliveries one at a time therefore
// means one command owns the machine — a build, a scan, an install — and every
// other caller queues behind it, including a one-syscall file read. That is not
// a slow path, it is an unavailable one, and it showed up the first hour this
// actor was used for real.
//
// The proof is overlap, not speed: the fast word must reach its terminal while
// the slow one is demonstrably still running. Asserting on elapsed time instead
// would pass on a machine that merely got lucky with scheduling.
func TestASlowWordDoesNotOwnTheDevice(t *testing.T) {
	sys, _ := startActor(t)

	sys.push(requestID("slow", TypeExec, ExecPayload{Command: "sleep 2"}))
	sys.push(requestID("fast", TypeFileWrite, FileWritePayload{Path: "quick.txt", Content: "hi"}))

	if status, _, _ := waitTerminal(t, sys, "fast"); status != "completed" {
		t.Fatalf("fast word status = %s, want completed", status)
	}
	if terminalRecorded(sys, "slow") {
		t.Fatal("the slow word had already finished, so this run proves no overlap; make it slower")
	}
}

// Concurrency has to hold for the slow word against ITSELF, not just against a
// cheap one: several execs at once is the ordinary shape of an agent driving a
// machine. Each must get its own answer, and the run must not deadlock — which
// is the failure a shared lock or a shared buffer would produce here.
func TestManyExecsRunAtOnceAndEachGetsItsOwnAnswer(t *testing.T) {
	sys, _ := startActor(t)

	const n = 8
	for i := range n {
		sys.push(requestID(
			message.ID(fmt.Sprintf("exec-%d", i)),
			TypeExec,
			ExecPayload{Command: fmt.Sprintf("sleep 0.3; printf 'answer-%d'", i)},
		))
	}

	var wg sync.WaitGroup
	answers := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			status, _, raw := waitTerminal(t, sys, message.ID(fmt.Sprintf("exec-%d", i)))
			if status != "completed" {
				return
			}
			answers[i] = decodeString(t, raw["stdout"])
		}(i)
	}
	wg.Wait()

	for i := range n {
		want := fmt.Sprintf("answer-%d", i)
		if answers[i] != want {
			// A crossed answer here would mean the words share a buffer.
			t.Fatalf("exec-%d answered %q, want %q", i, answers[i], want)
		}
	}
}

// Death waits for the answers already accepted rather than stranding their
// callers: a caller whose request was taken and then dropped without a terminal
// waits out the framework timer for nothing.
func TestDeathWaitsForAcceptedWork(t *testing.T) {
	root := t.TempDir()
	a := NewActor(root, nil)
	sys := newFakeSys(testActorID)
	done := make(chan error, 1)
	go func() { done <- a.run(sys) }()

	sys.push(requestID("lingering", TypeExec, ExecPayload{Command: "sleep 0.5; printf done"}))
	// Give the loop a moment to accept the delivery, then kill the cell while
	// the command is still running.
	waitUntilAccepted(t, sys)
	sys.stop()
	<-done

	if !terminalRecorded(sys, "lingering") {
		t.Fatal("run() returned before the work it had accepted was answered")
	}
}
