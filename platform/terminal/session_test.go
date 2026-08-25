package terminal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
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
	seq := 0
	m := NewManager(&fakeOpener{dev: dev}, func(_ context.Context, _ channel.ID, _ actor.ActorID, r Record) string {
		mu.Lock()
		defer mu.Unlock()
		recs = append(recs, r)
		seq++
		return fmt.Sprintf("row-%d", seq)
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
	if _, _, err := m.Attach("s1", "c0", "human:root"); err != nil {
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
	if _, _, err := m.Attach("s1", "c0", "human:root"); err != nil {
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
	if _, _, err := m.Attach("s1", "c0", "human:root"); err != nil {
		t.Fatal(err)
	}
	m.Detach(s)
	time.Sleep(60 * time.Millisecond)
	if _, _, err := m.Attach("s1", "c0", "human:root"); err != nil {
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

func TestReattachSupersedesTheOldViewer(t *testing.T) {
	// A tab switch races: the old socket may not have finished closing. The
	// owner must be able to take over rather than be told "busy" and pushed
	// into opening a second shell.
	m, _, _ := newTestManager(t, time.Hour)
	if _, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24); err != nil {
		t.Fatal(err)
	}
	_, firstAtt, err := m.Attach("s1", "c0", "human:root")
	if err != nil {
		t.Fatal(err)
	}
	_, secondAtt, err := m.Attach("s1", "c0", "human:root")
	if err != nil {
		t.Fatalf("owner could not take over its own session: %v", err)
	}
	select {
	case _, open := <-firstAtt.Bytes:
		if open {
			t.Fatal("superseded viewer still receiving")
		}
	case <-time.After(time.Second):
		t.Fatal("superseded viewer was not closed")
	}
	if secondAtt == nil {
		t.Fatal("takeover returned no stream")
	}
}

func TestAnotherCallerCannotAttach(t *testing.T) {
	// The口子 is per-caller: a session belongs to the human who opened it.
	m, _, _ := newTestManager(t, time.Hour)
	if _, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Attach("s1", "c0", "human:mallory"); err != ErrNotOwner {
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
	_, viewerAtt, err := m.Attach("s1", "c0", "human:root")
	if err != nil {
		t.Fatal(err)
	}
	var seen []byte
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for b := range viewerAtt.Bytes {
			seen = append(seen, b...)
		}
	}()

	payload := append([]byte(nil), osc("1337;AtollCmd='make test'")...)
	payload = append(payload, osc("133;C")...)
	payload = append(payload, []byte("FAILED: 3 tests")...)
	payload = append(payload, osc("133;D;1")...)
	dev.push(link.PTYFrameData, payload)

	// 等的是**命令行**：会话开启行会先到，恒不可拿它当命令来数。
	commandRows := func() []Record {
		var out []Record
		for _, r := range records() {
			if r.Event == "" {
				out = append(out, r)
			}
		}
		return out
	}
	deadline := time.After(2 * time.Second)
	for len(commandRows()) == 0 {
		select {
		case <-deadline:
			t.Fatal("command never reached the ledger")
		case <-time.After(5 * time.Millisecond):
		}
	}
	m.Close("s1")
	wg.Wait()

	cmds := commandRows()
	if len(cmds) != 1 {
		t.Fatalf("want exactly one command row, got %d: %+v", len(cmds), records())
	}
	got := cmds[0]
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
	m := NewManager(&fakeOpener{dev: dev}, func(context.Context, channel.ID, actor.ActorID, Record) string {
		once.Do(func() { recorded.Done() })
		<-release // wedge the ledger for the whole test
		return ""
	})
	t.Cleanup(func() { close(release); m.CloseAll() })

	if _, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24); err != nil {
		t.Fatal(err)
	}
	_, viewerAtt, err := m.Attach("s1", "c0", "human:root")
	if err != nil {
		t.Fatal(err)
	}
	var seen int64
	go func() {
		for b := range viewerAtt.Bytes {
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
	m := NewManager(&fakeOpener{dev: dev}, func(context.Context, channel.ID, actor.ActorID, Record) string {
		<-release
		return ""
	})
	t.Cleanup(func() { close(release); m.CloseAll() })
	if _, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Attach("s1", "c0", "human:root"); err != nil {
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

func TestSessionIsBoundToItsChannel(t *testing.T) {
	// 门判的是「你在这个频道里是成员吗」。若接回时不校验频道，同时属于 A 和 B
	// 的调用方可以拿 A 的 session id 声称自己在 B——不构成越权（会话仍是他自己
	// 的），但门判了一个频道却服务了另一个，且命令记录会落进 A 而调用方自称在 B。
	m, _, _ := newTestManager(t, time.Hour)
	if _, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Attach("s1", "c0.other", "human:root"); err != ErrNotOwner {
		t.Fatalf("跨频道接回未被拒：err = %v", err)
	}
	if _, _, err := m.Attach("s1", "c0", "human:root"); err != nil {
		t.Fatalf("本频道接回被误拒：%v", err)
	}
}

func TestSessionOpensWithARowAndCommandsHangFromIt(t *testing.T) {
	// 开会话是这条线上唯一一次真判权动作，故它本身必须在账本上——
	// 账本恒不该只记被授权之后的行为而漏掉授权本身。命令再挂到它下面，
	// 于是一次会话读起来是一件事，恒不是一堆互不相干的根。
	m, dev, records := newTestManager(t, time.Hour)
	s, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Attach("s1", "c0", "human:root"); err != nil {
		t.Fatal(err)
	}
	// 开启行走的是同一条有界队列（恒不同步写——账本一卡开会话就卡死），
	// 故要等它落定。
	wait := time.After(2 * time.Second)
	for len(records()) == 0 {
		select {
		case <-wait:
			t.Fatal("会话开启没有落账")
		case <-time.After(5 * time.Millisecond):
		}
	}
	opened := records()[0]
	if opened.Event != "opened" {
		t.Fatalf("第一条不是会话开启行：%+v", opened)
	}
	if opened.Parent != "" {
		t.Errorf("开启行不该有父，它就是根：%q", opened.Parent)
	}

	// 行落账 ≠ 会话已经知道自己的根：openRow 是记录回调里回填的，比 records()
	// 看到那一行更晚。机器一忙这个窗口就会拉开，命令行的 parent 会是空——那
	// 恒是真实存在的产品行为，但这条测试要验的是"接上了"，所以必须等到接上。
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.openRow != ""
	})

	payload := append(append(append([]byte(nil),
		osc("1337;AtollCmd='make test'")...), osc("133;C")...), osc("133;D;0")...)
	dev.push(link.PTYFrameData, payload)

	deadline := time.After(2 * time.Second)
	for len(records()) < 2 {
		select {
		case <-deadline:
			t.Fatal("命令没落账")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cmd := records()[1]
	if cmd.Cmd != "make test" {
		t.Fatalf("第二条不是命令行：%+v", cmd)
	}
	if cmd.Parent != "row-1" {
		t.Errorf("命令没挂到会话开启行上：parent=%q，想要 row-1", cmd.Parent)
	}

	// 关闭同样落账，且同挂一棵树。
	m.Close("s1")
	for len(records()) < 3 {
		select {
		case <-deadline:
			t.Fatal("会话关闭没落账")
		case <-time.After(5 * time.Millisecond):
		}
	}
	last := records()[2]
	if last.Event != "closed" || last.Parent != "row-1" {
		t.Errorf("关闭行不对：%+v", last)
	}
}

// —— 屏幕回放 ——
//
// 这一组存在的理由，如实记账：原来的形把终端的真相放在浏览器的 DOM 里，于是
// 切频道、切主视图、刷新页面都会把它弄丢，回来是黑屏。真相恒该在会话这一侧。

func TestAttachReplaysTheScreen(t *testing.T) {
	m, dev, _ := newTestManager(t, time.Hour)
	s, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	// 没有 viewer 的时候产生的输出，正是"回来要看到的那一屏"。
	_, att, err := m.Attach("s1", "c0", "human:root")
	if err != nil {
		t.Fatal(err)
	}
	if len(att.Replay) != 0 {
		t.Fatalf("全新会话恒不该有回放，拿到 %q", att.Replay)
	}
	dev.push(link.PTYFrameData, []byte("before you left\n"))
	waitFor(t, func() bool { return len(readAll(att.Bytes)) >= 0 })
	drain(att.Bytes)
	m.Detach(s)
	dev.push(link.PTYFrameData, []byte("while you were away\n"))

	var back *Attachment
	waitFor(t, func() bool {
		_, a, err := m.Attach("s1", "c0", "human:root")
		if err != nil {
			return false
		}
		back = a
		return strings.Contains(string(a.Replay), "while you were away")
	})
	if !strings.Contains(string(back.Replay), "before you left") {
		t.Fatalf("回放丢了离开之前那一屏: %q", back.Replay)
	}
	if !bytes.HasPrefix(back.Replay, []byte{0x1b, 'c'}) {
		t.Fatal("回放恒该以 RIS 开头——新 viewer 必须从一个确定的状态开始")
	}
}

func TestReplayIsBounded(t *testing.T) {
	m, dev, _ := newTestManager(t, time.Hour)
	if _, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24); err != nil {
		t.Fatal(err)
	}
	chunk := bytes.Repeat([]byte("x"), 64<<10)
	for i := 0; i < 8; i++ { // 512KB，两倍于上限
		dev.push(link.PTYFrameData, chunk)
	}
	waitFor(t, func() bool {
		_, att, err := m.Attach("s1", "c0", "human:root")
		return err == nil && len(att.Replay) > MaxReplay/2
	})
	_, att, err := m.Attach("s1", "c0", "human:root")
	if err != nil {
		t.Fatal(err)
	}
	// RIS 两字节之外，恒不超过上限。
	if len(att.Replay) > MaxReplay+2 {
		t.Fatalf("回放环恒该有界 %d，实际 %d", MaxReplay, len(att.Replay))
	}
}

// 快照与订阅必须在同一把锁内产生。分两次拿的话，两次之间到达的字节既不在快照里
// 也不在订阅里——凭空丢一段，而且恒难复现。这条在设备持续输出时反复 attach，
// 用字节的连号来验"无丢无重"。
func TestReplayAndLiveStreamDoNotLoseBytesAtTheSeam(t *testing.T) {
	m, dev, _ := newTestManager(t, time.Hour)
	s, err := m.Open(context.Background(), "s1", "c0", "human:root", "", 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	const total = 400
	writing := make(chan struct{})
	go func() {
		defer close(writing)
		for i := 0; i < total; i++ {
			dev.push(link.PTYFrameData, []byte(fmt.Sprintf("<%d>", i)))
		}
	}()

	seen := ""
	_, att, err := m.Attach("s1", "c0", "human:root")
	if err != nil {
		t.Fatal(err)
	}
	seen += string(att.Replay)
	deadline := time.After(10 * time.Second)
	for {
		done := false
		select {
		case b, ok := <-att.Bytes:
			if !ok {
				done = true
				break
			}
			seen += string(b)
		case <-time.After(120 * time.Millisecond):
			done = true
		case <-deadline:
			t.Fatal("timeout")
		}
		if !done {
			continue
		}
		if strings.Contains(seen, fmt.Sprintf("<%d>", total-1)) {
			break
		}
		// 中途重新 attach：接缝正是在这里发生的。
		m.Detach(s)
		_, att, err = m.Attach("s1", "c0", "human:root")
		if err != nil {
			t.Fatal(err)
		}
		seen += string(att.Replay)
	}
	<-writing
	// 每个标记恒该出现，且恒该按序。回放会与之前看过的内容重叠——那是允许的
	// （RIS 之后重画同一屏），这里验的是**恒不缺号**。
	last := -1
	for i := 0; i < total; i++ {
		idx := strings.LastIndex(seen, fmt.Sprintf("<%d>", i))
		if idx < 0 {
			t.Fatalf("接缝处丢了第 %d 个标记", i)
		}
		if idx < last {
			t.Fatalf("第 %d 个标记乱序", i)
		}
		last = idx
	}
}

func drain(ch <-chan []byte) {
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-time.After(80 * time.Millisecond):
			return
		}
	}
}

func readAll(ch <-chan []byte) []byte {
	var out []byte
	for {
		select {
		case b, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, b...)
		case <-time.After(50 * time.Millisecond):
			return out
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("条件在 5s 内没有成立")
}
