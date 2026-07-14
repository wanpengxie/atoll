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
