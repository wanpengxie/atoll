package link

// linkSession mechanism unit tests: the control task pool (local busy, zombie
// accounting, panic recovery), inline probe table row, the three-stage open model
// (non-blocking capacity, seal unblocking, late-close settlement), per-write
// budgets, and carrier-error classification.

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

func TestControlTaskPoolBusyIsLocalAndSeatsFollowRealGoroutines(t *testing.T) {
	var evidence bool
	pool := newControlTaskPool(nil, func(SessionEndReason, string, error) { evidence = true })
	release := make(chan struct{})
	for i := 0; i < controlTaskCapacity; i++ {
		if !pool.submit(func() { <-release }, nil) {
			t.Fatalf("task %d unexpectedly rejected", i)
		}
	}
	busy := make(chan struct{}, 1)
	if pool.submit(func() {}, func() { busy <- struct{}{} }) {
		t.Fatal("full task pool admitted excess work")
	}
	select {
	case <-busy:
	default:
		t.Fatal("full task pool did not return busy immediately")
	}
	joined, abandoned := pool.drain(20 * time.Millisecond)
	if joined || abandoned != controlTaskCapacity {
		t.Fatalf("drain joined=%v abandoned=%d", joined, abandoned)
	}
	if evidence {
		t.Fatal("task-pool saturation escalated to session death")
	}
	if got := pool.active.Load(); got != controlTaskCapacity {
		t.Fatalf("active seats=%d want %d before real goroutines exit", got, controlTaskCapacity)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for pool.active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := pool.active.Load(); got != 0 {
		t.Fatalf("active seats remained after real exit: %d", got)
	}
}

// A zombie control task is accounted exactly once across its abandon timer
// and a later drain timeout.
func TestZombieControlTaskIsCountedOnce(t *testing.T) {
	pool := newControlTaskPool(nil, nil)
	pool.abandonAfter = 50 * time.Millisecond
	release := make(chan struct{})
	defer close(release)
	pool.submit(func() { <-release }, nil)
	time.Sleep(150 * time.Millisecond)
	if _, abandoned := pool.drain(50 * time.Millisecond); abandoned != 1 {
		t.Fatalf("one zombie accounted %d times", abandoned)
	}
}

func TestProbeTableRowIsInlineDespiteSaturatedControlTaskPool(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	ls := &linkSession{ctrl: left}
	ls.controlTasks = newControlTaskPool(nil, nil)
	ls.openSeats = make(chan struct{}, openAttemptCapacity)
	router, err := newControlRouter(
		[]controlKind{ctrlProbe},
		map[controlKind]controlRoute{ctrlProbe: probeRoute(parseProbe, false)},
		nil, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	ls.onControl = func(raw []byte) {
		router.dispatch(controlDispatchInput{link: ls}, raw)
	}
	release := make(chan struct{})
	for i := 0; i < controlTaskCapacity; i++ {
		ls.controlTasks.submit(func() { <-release }, nil)
	}
	done := make(chan struct{})
	go func() {
		ls.readControl(left)
		close(done)
	}()
	probe, _ := encodeControl(controlFrame{Kind: ctrlProbe, Probe: &Probe{Nonce: "n-1"}})
	go func() { _, _ = right.Write(append(probe, '\n')) }()
	_ = right.SetReadDeadline(time.Now().Add(time.Second))
	var reply controlFrame
	if err := json.NewDecoder(right).Decode(&reply); err != nil {
		t.Fatalf("read direct probe reply: %v", err)
	}
	if reply.Kind != ctrlProbeReply || reply.ProbeReply == nil || reply.ProbeReply.Nonce != "n-1" {
		t.Fatalf("probe reply=%+v", reply)
	}
	close(release)
	_ = right.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("control reader did not exit")
	}
}

func TestMalformedControlReportsEvidenceWithoutMechanismKill(t *testing.T) {
	carrierA, carrierB := net.Pipe()
	client, err := yamux.Client(carrierA, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	server, err := yamux.Server(carrierB, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer server.Close()
	evidence := make(chan SessionEndReason, 1)
	ls := newLinkSession(client, nil, nil, nil, nil,
		func(reason SessionEndReason, _ string, _ error) { evidence <- reason }, nil)
	reader, writer := net.Pipe()
	defer reader.Close()
	go ls.readControl(reader)
	if _, err := writer.Write([]byte("not-json\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case reason := <-evidence:
		if reason != SessionProtocolViolation {
			t.Fatalf("reason=%s", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("decode evidence was not reported")
	}
	select {
	case <-client.CloseChan():
		t.Fatal("link mechanism killed the carrier before an owner decision")
	default:
	}
	_ = writer.Close()
}

// A panicking control handler is a local fault, never an unrecovered crash.
func TestControlHandlerPanicIsLocalFault(t *testing.T) {
	carrierA, carrierB := net.Pipe()
	client, err := yamux.Client(carrierA, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	server, err := yamux.Server(carrierB, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer server.Close()
	evidence := make(chan SessionEndReason, 1)
	ls := newLinkSession(client,
		func([]byte) { panic("test-boom") }, nil, nil, nil,
		func(reason SessionEndReason, _ string, _ error) { evidence <- reason }, nil)
	reader, writer := net.Pipe()
	defer reader.Close()
	go ls.readControl(reader)
	if _, err := writer.Write(append(encodePlanPoke(), '\n')); err != nil {
		t.Fatal(err)
	}
	select {
	case reason := <-evidence:
		if reason != SessionLocalFault {
			t.Fatalf("reason=%s want local_fault", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("panic was not reported as local fault")
	}
}

func TestOpenCapacityBusyDoesNotReportSessionDeath(t *testing.T) {
	ls := &linkSession{
		openSeats: make(chan struct{}, openAttemptCapacity),
		evidence: func(SessionEndReason, string, error) {
			t.Fatal("local open pressure reported session death")
		},
	}
	for i := 0; i < openAttemptCapacity; i++ {
		ls.openSeats <- struct{}{}
	}
	if _, err := ls.openTagged(context.Background(), streamLane); !errors.Is(err, ErrOpenBusy) {
		t.Fatalf("open error=%v want busy", err)
	}
}

// Seal unblocking: an open worker stuck inside the library's blocking Open is
// popped out by closing the carrier, and the mechanism joins afterwards.
func TestSealUnblocksStuckOpenWorker(t *testing.T) {
	carrierA, carrierB := net.Pipe()
	clientCfg := linkYamuxConfig()
	clientCfg.AcceptBacklog = 1
	client, err := yamux.Client(carrierA, clientCfg)
	if err != nil {
		t.Fatal(err)
	}
	server, err := yamux.Server(carrierB, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close() // never Accepts: unacked SYNs hold the client backlog
	ls := newLinkSession(client, nil, nil, nil, nil,
		func(SessionEndReason, string, error) {}, nil)

	first, err := ls.openTagged(context.Background(), streamActor)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	defer first.Close()

	second := make(chan error, 1)
	go func() {
		_, err := ls.openTagged(context.Background(), streamActor)
		second <- err
	}()
	select {
	case err := <-second:
		t.Fatalf("second open did not block on the SYN backlog: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	_ = ls.closeCarrier()
	select {
	case err := <-second:
		if err == nil {
			t.Fatal("stuck open returned a live stream after seal")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("carrier close did not unblock the stuck open worker")
	}
	if !ls.waitWorkers(2 * time.Second) {
		t.Fatal("open worker did not join after carrier close")
	}
}

// Stage-3 settlement of an abandoned open: the worker that completes after
// its caller left closes the stream itself and accounts a late close.
func TestLateOpenIsReapedAndAccounted(t *testing.T) {
	carrierA, carrierB := net.Pipe()
	clientCfg := linkYamuxConfig()
	clientCfg.AcceptBacklog = 1
	client, err := yamux.Client(carrierA, clientCfg)
	if err != nil {
		t.Fatal(err)
	}
	server, err := yamux.Server(carrierB, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer server.Close()
	ls := newLinkSession(client, nil, nil, nil, nil,
		func(SessionEndReason, string, error) {}, nil)

	first, err := ls.openTagged(context.Background(), streamActor)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	defer first.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := ls.openTagged(ctx, streamActor); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second open err=%v want deadline", err)
	}
	// Now let the backlog drain: accepting the first stream ACKs it and
	// unblocks the abandoned worker, which must reap its own late stream.
	go func() {
		for {
			if _, acceptErr := server.AcceptStream(); acceptErr != nil {
				return
			}
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, late := ls.openCounts(); late == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	inFlight, late := ls.openCounts()
	t.Fatalf("late close not accounted: in_flight=%d late=%d", inFlight, late)
}

// The transport-contract write budget: a stream whose peer stops reading is
// judged dead water within the budget and only that stream is closed.
func TestWriteBudgetJudgesDeadWaterStreamLocally(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	bounded := &boundedConn{Conn: left, budget: 50 * time.Millisecond}
	if _, err := bounded.Write(make([]byte, 1024)); err == nil {
		t.Fatal("write into a dead-water pipe did not hit the budget")
	}
	if _, err := left.Write([]byte("x")); err == nil {
		t.Fatal("dead-water stream was not closed after the budget verdict")
	}
}

func TestYamuxConnectionWriteTimeoutIsCarrierEvidence(t *testing.T) {
	if !isConnectionWriteTimeout(yamux.ErrConnectionWriteTimeout) {
		t.Fatal("yamux connection write timeout was not classified as carrier loss")
	}
}
