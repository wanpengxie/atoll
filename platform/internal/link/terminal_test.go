package link

import (
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

func terminalTestDialer() *Dialer {
	return &Dialer{lc: &linkSession{}, streams: map[actor.ActorID]*actorStream{}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestTerminalGate_DownAndDetachWriteExactlyOneFrame(t *testing.T) {
	d := terminalTestDialer()
	local, peer := net.Pipe()
	defer peer.Close()
	as := &actorStream{id: "tool:a", stream: local, codec: ipc.NewCodec(local, local)}
	d.streams[as.id] = as
	start := make(chan struct{})
	var calls sync.WaitGroup
	calls.Add(2)
	go func() { defer calls.Done(); <-start; d.claimTerminal(as, terminalDown, "dead") }()
	go func() { defer calls.Done(); <-start; d.DetachStream(as.id) }()
	close(start)
	calls.Wait()
	frame, err := ipc.NewCodec(peer, peer).Read()
	if err != nil {
		t.Fatal(err)
	}
	if frame.Kind != ipc.KindDown && frame.Kind != ipc.KindDetach {
		t.Fatalf("terminal kind=%s", frame.Kind)
	}
	if !waitGroupWithin(&d.terminalTasks, time.Second) {
		t.Fatal("terminal task did not drain")
	}
	if got := d.terminalLost[terminalDown].Load() + d.terminalLost[terminalDetach].Load(); got != 1 {
		t.Fatalf("lost CAS count=%d want 1", got)
	}
	_ = peer.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := ipc.NewCodec(peer, peer).Read(); err == nil {
		t.Fatal("second terminal frame was written")
	}
}

func TestTerminalGate_KindDespawnClaimsDetachBeforeLocalDespawn(t *testing.T) {
	d := terminalTestDialer()
	local, peer := net.Pipe()
	defer peer.Close()
	streamCodec := ipc.NewCodec(local, local)
	as := &actorStream{id: "tool:a", stream: local, codec: streamCodec}
	as.writer = NewRemoteWriter(streamCodec)
	as.access = newRelayClient(streamCodec, ipc.KindAccess)
	as.sched = newRelayClient(streamCodec, ipc.KindSchedule)
	d.streams[as.id] = as
	claimed := make(chan bool, 1)
	d.despawnLocal = func(actor.ActorID) { claimed <- terminalVerdict(as.terminal.Load()) == terminalDetach }
	done := make(chan struct{})
	go func() { d.streamReadLoop(as, nil); close(done) }()
	codec := ipc.NewCodec(peer, peer)
	if err := codec.Write(ipc.Frame{Kind: ipc.KindDespawn}); err != nil {
		t.Fatal(err)
	}
	select {
	case ok := <-claimed:
		if !ok {
			t.Fatal("despawnLocal ran before detach verdict was claimed")
		}
	case <-time.After(time.Second):
		t.Fatal("despawnLocal not called")
	}
	frame, err := codec.Read()
	if err != nil || frame.Kind != ipc.KindDetach {
		t.Fatalf("detach frame=%+v err=%v", frame, err)
	}
	<-done
}

type blockingTerminalRWC struct {
	closed chan struct{}
	once   sync.Once
}

func newBlockingTerminalRWC() *blockingTerminalRWC {
	return &blockingTerminalRWC{closed: make(chan struct{})}
}
func (b *blockingTerminalRWC) Read([]byte) (int, error)  { <-b.closed; return 0, io.EOF }
func (b *blockingTerminalRWC) Write([]byte) (int, error) { <-b.closed; return 0, io.ErrClosedPipe }
func (b *blockingTerminalRWC) Close() error              { b.once.Do(func() { close(b.closed) }); return nil }

func TestTerminalGate_IndependentTimerClosesBlockedWriteAndDrainsTask(t *testing.T) {
	old := terminalWriteGrace
	terminalWriteGrace = 20 * time.Millisecond
	defer func() { terminalWriteGrace = old }()
	d := terminalTestDialer()
	wire := newBlockingTerminalRWC()
	as := &actorStream{id: "tool:a", stream: wire, codec: ipc.NewCodec(wire, wire)}
	if !d.claimTerminal(as, terminalDown, "blocked") {
		t.Fatal("terminal claim lost unexpectedly")
	}
	if !waitGroupWithin(&d.terminalTasks, time.Second) {
		t.Fatal("timer did not close blocked write")
	}
	if d.terminalTimeout.Load() != 1 {
		t.Fatalf("timeout metric=%d want 1", d.terminalTimeout.Load())
	}
}

func TestTerminalBudgetsKeepOuterJoinAboveInnerWrite(t *testing.T) {
	if terminalJoinGrace <= terminalWriteGrace {
		t.Fatalf("terminal join grace=%v must exceed write grace=%v", terminalJoinGrace, terminalWriteGrace)
	}
}

func TestTerminalGate_CloseSealsAdmissionAndLateWinnerAddsNoTask(t *testing.T) {
	d := terminalTestDialer()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	wire := newBlockingTerminalRWC()
	as := &actorStream{id: "tool:late", stream: wire, codec: ipc.NewCodec(wire, wire)}
	if !d.claimTerminal(as, terminalDown, "late") {
		t.Fatal("late stream did not win its local terminal account")
	}
	select {
	case <-wire.closed:
	default:
		t.Fatal("late winner was not closed inline")
	}
	if !waitGroupWithin(&d.terminalTasks, 50*time.Millisecond) {
		t.Fatal("late winner enrolled a task after Close")
	}
}

func TestDialerCloseDetachesSnapshotBeforeCarrierClose(t *testing.T) {
	ls, controlPeer, _ := newControlKillRig(t, func([]byte) {})
	defer controlPeer.Close()
	d := terminalTestDialer()
	d.lc = ls

	peers := make([]net.Conn, 0, 2)
	for _, id := range []actor.ActorID{"tool:a", "tool:b"} {
		local, peer := net.Pipe()
		peers = append(peers, peer)
		d.streams[id] = &actorStream{id: id, stream: local, codec: ipc.NewCodec(local, local)}
	}
	defer func() {
		for _, peer := range peers {
			_ = peer.Close()
		}
	}()

	closed := make(chan struct{})
	go func() { _ = d.Close(); close(closed) }()
	select {
	case <-ls.closed():
		t.Fatal("carrier closed while terminal detach writes were still blocked")
	case <-time.After(20 * time.Millisecond):
	}
	for i, peer := range peers {
		frame, err := ipc.NewCodec(peer, peer).Read()
		if err != nil || frame.Kind != ipc.KindDetach {
			t.Fatalf("stream %d terminal frame=%+v err=%v", i, frame, err)
		}
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not drain terminal writes and close the carrier")
	}
	select {
	case <-ls.closed():
	default:
		t.Fatal("Close returned before carrier close")
	}
}

func TestTerminalGate_WinnerPausedAfterCASCannotEnrollAfterCloseSeal(t *testing.T) {
	d := terminalTestDialer()
	wire := newBlockingTerminalRWC()
	as := &actorStream{id: "tool:paused", stream: wire, codec: ipc.NewCodec(wire, wire)}
	d.streams[as.id] = as
	if !as.terminal.CompareAndSwap(uint32(terminalUnclaimed), uint32(terminalDown)) {
		t.Fatal("failed to establish post-CAS seam")
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if !d.enrollTerminalWinner(as, terminalDown, "paused") {
		t.Fatal("post-CAS winner did not complete")
	}
	select {
	case <-wire.closed:
	default:
		t.Fatal("winner resumed after seal without closing inline")
	}
	if !waitGroupWithin(&d.terminalTasks, 50*time.Millisecond) {
		t.Fatal("winner enrolled a terminal task after Close sealed admission")
	}
}

func TestDialerConcurrentCloseSharesOneDetachAndDrain(t *testing.T) {
	ls, controlPeer, _ := newControlKillRig(t, func([]byte) {})
	defer controlPeer.Close()
	d := terminalTestDialer()
	d.lc = ls
	local, peer := net.Pipe()
	defer peer.Close()
	as := &actorStream{id: "tool:once", stream: local, codec: ipc.NewCodec(local, local)}
	d.streams[as.id] = as

	done := make(chan struct{}, 2)
	for range 2 {
		go func() { _ = d.Close(); done <- struct{}{} }()
	}
	select {
	case <-done:
		t.Fatal("Close returned before the shared detach write drained")
	case <-time.After(20 * time.Millisecond):
	}
	frame, err := ipc.NewCodec(peer, peer).Read()
	if err != nil || frame.Kind != ipc.KindDetach {
		t.Fatalf("terminal frame=%+v err=%v", frame, err)
	}
	<-done
	<-done
	if got := terminalVerdict(as.terminal.Load()); got != terminalDetach {
		t.Fatalf("terminal verdict=%s want detach", got)
	}
	_ = peer.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := ipc.NewCodec(peer, peer).Read(); err == nil {
		t.Fatal("concurrent Close emitted a second terminal frame")
	}
}

func TestDialerCloseAfterHardDeadSessionIsFastAndDoesNotReclaimTerminal(t *testing.T) {
	ls, controlPeer, _ := newControlKillRig(t, func([]byte) {})
	defer controlPeer.Close()
	d := terminalTestDialer()
	d.lc = ls
	local, peer := net.Pipe()
	as := &actorStream{id: "tool:dead", stream: local, codec: ipc.NewCodec(local, local)}
	as.terminal.Store(uint32(terminalSilent))
	d.streams[as.id] = as
	_ = local.Close()
	_ = peer.Close()
	ls.kill("hard-test", nil)

	started := time.Now()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("hard-dead cleanup took %v", elapsed)
	}
	if got := terminalVerdict(as.terminal.Load()); got != terminalSilent {
		t.Fatalf("terminal verdict changed to %s", got)
	}
	if got := d.terminalLost[terminalDetach].Load(); got != 1 {
		t.Fatalf("detach loser accounting=%d want 1", got)
	}
}

func TestTerminalGate_EOFClaimsSilent(t *testing.T) {
	d := testDialer()
	pr, pw := io.Pipe()
	_ = pr.Close()
	_ = pw.Close()
	as := testReadLoopStream("tool:eof", pr, io.Discard)
	d.streams[as.id] = as
	d.streamReadLoop(as, nil)
	if got := terminalVerdict(as.terminal.Load()); got != terminalSilent {
		t.Fatalf("EOF verdict=%s want silent", got)
	}
}
