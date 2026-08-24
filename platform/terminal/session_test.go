package terminal

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// fakeDevice is the device leg: it answers PTYOpen with PTYReady and then
// echoes whatever the test pushes into it.
type fakeDevice struct {
	mu     sync.Mutex
	toDoor chan []byte
	closed chan struct{}
	once   sync.Once
	wrote  [][]byte
	rest   []byte
}

func newFakeDevice() *fakeDevice {
	d := &fakeDevice{toDoor: make(chan []byte, 32), closed: make(chan struct{})}
	ready, _ := encodeControl(link.PTYReady{OK: true, Pid: 4242})
	d.toDoor <- ready
	return d
}

func encodeControl(v any) ([]byte, error) {
	var buf sliceWriter
	if err := link.WritePTYControl(&buf, v); err != nil {
		return nil, err
	}
	return buf.b, nil
}

type sliceWriter struct{ b []byte }

func (w *sliceWriter) Write(p []byte) (int, error) { w.b = append(w.b, p...); return len(p), nil }

func (d *fakeDevice) push(kind uint8, payload []byte) {
	var w sliceWriter
	_ = link.WritePTYFrame(&w, kind, payload)
	select {
	case d.toDoor <- w.b:
	case <-d.closed:
	}
}

func (d *fakeDevice) Read(p []byte) (int, error) {
	for len(d.rest) == 0 {
		select {
		case b, ok := <-d.toDoor:
			if !ok {
				return 0, io.EOF
			}
			d.rest = b
		case <-d.closed:
			return 0, io.EOF
		}
	}
	n := copy(p, d.rest)
	d.rest = d.rest[n:]
	return n, nil
}

func (d *fakeDevice) Write(p []byte) (int, error) {
	d.mu.Lock()
	d.wrote = append(d.wrote, append([]byte(nil), p...))
	d.mu.Unlock()
	return len(p), nil
}

func (d *fakeDevice) Close() error { d.once.Do(func() { close(d.closed) }); return nil }
func (d *fakeDevice) isClosed() bool {
	select {
	case <-d.closed:
		return true
	default:
		return false
	}
}

type fakeOpener struct{ dev *fakeDevice }

func (o *fakeOpener) OpenPTY(context.Context, channel.ID, string, uint16, uint16, bool) (io.ReadWriteCloser, error) {
	return o.dev, nil
}

func newTestManager(t *testing.T, grace time.Duration) (*Manager, *fakeDevice, func() []Record) {
	t.Helper()
	dev := newFakeDevice()
	var mu sync.Mutex
	var recs []Record
	m := NewManager(&fakeOpener{dev: dev}, func(_ context.Context, _ channel.ID, _ actor.ActorID, r Record) {
		mu.Lock()
		recs = append(recs, r)
		mu.Unlock()
	})
	m.grace = grace
	t.Cleanup(m.CloseAll)
	return m, dev, func() []Record {
		mu.Lock()
		defer mu.Unlock()
		return append([]Record(nil), recs...)
	}
}

func TestDetachKeepsTheShellAlive(t *testing.T) {
	// 保住进程，恒不保住输出 (§4.4). Losing the viewer must NOT kill the pty.
	m, dev, _ := newTestManager(t, time.Hour)
	s, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Attach("s1", "human:root"); err != nil {
		t.Fatal(err)
	}
	m.Detach(s)
	if dev.isClosed() {
		t.Fatal("detach killed the device leg — the shell must survive its viewer")
	}
	if m.Get("s1") == nil {
		t.Fatal("session dropped on detach")
	}
}

func TestGraceExpiryKillsTheSession(t *testing.T) {
	// 宽限期超时未回 → 杀掉，恒不留孤儿 (§4.4).
	m, dev, _ := newTestManager(t, 40*time.Millisecond)
	s, _ := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24)
	if _, _, err := m.Attach("s1", "human:root"); err != nil {
		t.Fatal(err)
	}
	m.Detach(s)
	deadline := time.After(2 * time.Second)
	for m.Get("s1") != nil {
		select {
		case <-deadline:
			t.Fatal("session outlived its grace period")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if !dev.isClosed() {
		t.Fatal("grace expiry left the device leg open — orphan shell")
	}
}

func TestReattachWithinGraceCancelsTheClock(t *testing.T) {
	m, dev, _ := newTestManager(t, 300*time.Millisecond)
	s, _ := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24)
	if _, _, err := m.Attach("s1", "human:root"); err != nil {
		t.Fatal(err)
	}
	m.Detach(s)
	time.Sleep(60 * time.Millisecond)
	if _, _, err := m.Attach("s1", "human:root"); err != nil {
		t.Fatalf("reattach within grace failed: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	if m.Get("s1") == nil || dev.isClosed() {
		t.Fatal("reattach did not cancel the grace clock")
	}
}

func TestOpenWithoutAViewerIsAlreadyOnTheClock(t *testing.T) {
	// An Open whose browser never arrives must not leak a shell.
	m, dev, _ := newTestManager(t, 40*time.Millisecond)
	if _, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for m.Get("s1") != nil {
		select {
		case <-deadline:
			t.Fatal("never-viewed session leaked")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if !dev.isClosed() {
		t.Fatal("never-viewed session left the device leg open")
	}
}

func TestAnotherCallerCannotAttach(t *testing.T) {
	// The口子 is per-caller: a session belongs to the human who opened it.
	m, _, _ := newTestManager(t, time.Hour)
	if _, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Attach("s1", "human:mallory"); err != ErrNotOwner {
		t.Fatalf("err = %v, want ErrNotOwner", err)
	}
}

func TestCommandsLandOnTheLedgerAndBytesDoNot(t *testing.T) {
	// The whole point of the line (§4): the viewer sees every byte, the
	// ledger sees one row per command and no bytes of its own.
	m, dev, records := newTestManager(t, time.Hour)
	if _, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24); err != nil {
		t.Fatal(err)
	}
	_, viewer, err := m.Attach("s1", "human:root")
	if err != nil {
		t.Fatal(err)
	}
	var seen []byte
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for b := range viewer {
			seen = append(seen, b...)
		}
	}()

	payload := append([]byte(nil), osc("1337;AtollCmd='make test'")...)
	payload = append(payload, osc("133;C")...)
	payload = append(payload, []byte("FAILED: 3 tests")...)
	payload = append(payload, osc("133;D;1")...)
	dev.push(link.PTYFrameData, payload)

	deadline := time.After(2 * time.Second)
	for len(records()) == 0 {
		select {
		case <-deadline:
			t.Fatal("command never reached the ledger")
		case <-time.After(5 * time.Millisecond):
		}
	}
	m.Close("s1")
	wg.Wait()

	recs := records()
	if len(recs) != 1 {
		t.Fatalf("want exactly one row, got %d: %+v", len(recs), recs)
	}
	got := recs[0]
	if got.Cmd != "make test" || got.ExitCode != 1 || !got.HasExit {
		t.Errorf("row = %+v, want {make test, exit 1}", got)
	}
	if got.OutputTail == "" {
		t.Error("row carries no output tail — an agent could not debug from it")
	}
	if !containsAll(seen, "FAILED: 3 tests") {
		t.Errorf("viewer did not receive the raw output; got %q", seen)
	}
}

func containsAll(hay []byte, needle string) bool {
	return len(hay) > 0 && string(hay) != "" && indexOf(string(hay), needle) >= 0
}

func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}

func TestASlowLedgerDoesNotFreezeTheTerminal(t *testing.T) {
	// The device pump must keep reading even when the ledger is wedged.
	// Recording synchronously on that path is how a terminal freezes, and a
	// comment promising otherwise is not a mechanism — this is.
	dev := newFakeDevice()
	release := make(chan struct{})
	var recorded sync.WaitGroup
	recorded.Add(1)
	var once sync.Once
	m := NewManager(&fakeOpener{dev: dev}, func(context.Context, channel.ID, actor.ActorID, Record) {
		once.Do(func() { recorded.Done() })
		<-release // wedge the ledger for the whole test
	})
	t.Cleanup(func() { close(release); m.CloseAll() })

	if _, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24); err != nil {
		t.Fatal(err)
	}
	_, viewer, err := m.Attach("s1", "human:root")
	if err != nil {
		t.Fatal(err)
	}
	var seen int64
	go func() {
		for b := range viewer {
			atomic.AddInt64(&seen, int64(len(b)))
		}
	}()

	// One command, so the (wedged) recorder is entered.
	first := append(append(append([]byte(nil), osc("1337;AtollCmd='slow'")...), osc("133;C")...), osc("133;D;0")...)
	dev.push(link.PTYFrameData, first)
	recorded.Wait() // the recorder is now blocked

	// While it is blocked, the terminal must still carry output.
	dev.push(link.PTYFrameData, []byte("STILL-ALIVE"))
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt64(&seen) < int64(len(first)+len("STILL-ALIVE")) {
		select {
		case <-deadline:
			t.Fatal("device output stopped while the ledger was wedged — the terminal would appear frozen")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestRecordQueueDropsRatherThanBlocking(t *testing.T) {
	// A ledger that cannot keep up costs rows, never keystrokes.
	dev := newFakeDevice()
	release := make(chan struct{})
	m := NewManager(&fakeOpener{dev: dev}, func(context.Context, channel.ID, actor.ActorID, Record) {
		<-release
	})
	t.Cleanup(func() { close(release); m.CloseAll() })
	if _, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Attach("s1", "human:root"); err != nil {
		t.Fatal(err)
	}
	one := append(append(append([]byte(nil), osc("1337;AtollCmd='x'")...), osc("133;C")...), osc("133;D;0")...)
	for i := 0; i < recordQueueDepth+64; i++ {
		dev.push(link.PTYFrameData, one)
	}
	deadline := time.After(3 * time.Second)
	for m.DroppedRecords() == 0 {
		select {
		case <-deadline:
			t.Fatal("queue never reported a drop — it is either unbounded or it blocked")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
