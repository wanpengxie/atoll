package terminal

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

var (
	ErrNoSession   = errors.New("terminal: no such session")
	ErrNotOwner    = errors.New("terminal: session belongs to another caller")
	ErrBusy        = errors.New("terminal: session already attached")
	ErrHostOffline = errors.New("terminal: device offline")
)

// GracePeriod is how long a session outlives its viewer.
//
// Fixed, not configurable (design §4.4, 零预留). Its whole job is to survive a
// wifi blip or a closed tab without turning the session into a durable object:
// the process is kept, the output produced meanwhile is dropped, and nothing
// is buffered. See §4.3 — the stream is for experience and需要不精准.
const GracePeriod = 60 * time.Second

// Opener opens the device leg of a session. Implemented by daemonhost.Host.
type Opener interface {
	OpenPTY(ctx context.Context, chID channel.ID, device string, cols, rows uint16, integration bool) (io.ReadWriteCloser, error)
}

// Recorder writes one row to the channel ledger, authored by the caller, and
// answers with the id of what it wrote (empty when the write did not land).
//
// This is the entire point of the line (design §4.2): the sender is stamped by
// the runtime, never filled in here. The id comes back because the session's
// own opening row is the root every later row hangs from — without it each
// command would be its own tree and a session would not be a thing on the
// ledger at all.
type Recorder func(ctx context.Context, chID channel.ID, caller actor.ActorID, rec Record) string

// Record is what lands on the ledger for one command.
type Record struct {
	SessionID string `json:"session_id"`
	// Event marks a session lifecycle row: "opened" / "closed". Empty means a
	// command row. 会话开启恒是这条线上唯一一次真判权动作（design §7），
	// 故它本身必须在账本上——账本恒不该只记被授权之后的行为，而漏掉授权本身。
	Event  string `json:"event,omitempty"`
	Device string `json:"device,omitempty"`
	Reason string `json:"reason,omitempty"`
	Cmd    string `json:"cmd,omitempty"`
	// Cwd is where the command ran. The same command means different things
	// in different trees, so the record is incomplete without it.
	Cwd       string `json:"cwd,omitempty"`
	ExitCode  int    `json:"exit_code"`
	HasExit   bool   `json:"-"`
	DurationMs int64 `json:"duration_ms,omitempty"`
	// Parent is the session's opening row. Commands hang from it so a session
	// reads as one errand rather than a scatter of unrelated roots.
	Parent string `json:"-"`
	// OutputTail is the bounded tail of what the command printed. Bounded by
	// MaxOutputTail, which is device.MaxStreamBytes — the same answer this
	// system already gave for "how much device output goes on the ledger"
	// (design §9-4). A rising bound is a signal, see §8-3.
	OutputTail string `json:"output_tail,omitempty"`
}

// MaxOutputTail mirrors drivers/tools/device.MaxStreamBytes. It is restated
// rather than imported to keep platform/ free of a drivers/ dependency; the
// two are asserted equal by a test in the device package's neighbourhood.
const MaxOutputTail = 64_000

// Session is one live terminal. It outlives any particular viewer.
type Session struct {
	ID      string
	Channel channel.ID
	Caller  actor.ActorID

	mu       sync.Mutex
	dev      io.ReadWriteCloser
	viewer   chan []byte // non-nil while a viewer is attached
	closed   bool
	graceAt  *time.Timer
	scanner  Scanner
	openCmd  time.Time
	tail     []byte
	inCmd    bool
	// ended is closed when the session is over for good, and endReason says
	// why. A viewer that cannot tell "the shell exited" from "the connection
	// dropped" will reconnect into a brand-new shell, which is never what the
	// person meant.
	ended     chan struct{}
	endOnce   sync.Once
	endReason string
	// openRow is the ledger id of this session's "opened" event.
	openRow string
}

// Ended reports the session's terminal state. Reason is empty while live.
func (s *Session) Ended() <-chan struct{} { return s.ended }

func (s *Session) EndReason() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endReason
}

func (s *Session) finish(reason string) {
	s.endOnce.Do(func() {
		s.mu.Lock()
		if s.endReason == "" {
			s.endReason = reason
		}
		s.mu.Unlock()
		close(s.ended)
	})
}

// Manager owns every live session on this node.
type Manager struct {
	opener   Opener
	record   Recorder
	now      func() time.Time
	grace    time.Duration

	mu       sync.Mutex
	sessions map[string]*Session
	closed   bool

	// records is the bounded hand-off from the device pumps to the ledger.
	// See relay.go: recording恒不得阻塞设备输出泵.
	// records is never closed: device pumps outlive CloseAll by however long
	// their reads take to unblock, and a closed channel would panic under
	// them. Shutdown is signalled by stop instead.
	records  chan recordJob
	stop     chan struct{}
	stopOnce sync.Once
	dropped  atomic.Uint64
	recorder sync.WaitGroup
}

type recordJob struct {
	ch     channel.ID
	caller actor.ActorID
	rec    Record
	// onWritten receives the id the row landed under (empty if it did not).
	onWritten func(string)
}

// recordQueueDepth is how many rows may wait. Deep enough that a brief ledger
// stall costs nothing; shallow enough that a long one is visibly lossy rather
// than silently unbounded.
const recordQueueDepth = 512

func NewManager(opener Opener, record Recorder) *Manager {
	m := &Manager{
		opener:   opener,
		record:   record,
		now:      time.Now,
		grace:    GracePeriod,
		sessions: make(map[string]*Session),
		records:  make(chan recordJob, recordQueueDepth),
		stop:     make(chan struct{}),
	}
	if record != nil {
		m.recorder.Add(1)
		go func() {
			defer m.recorder.Done()
			m.recordLoop()
		}()
	}
	return m
}

// DroppedRecords counts rows the ledger could not keep up with. A non-zero and
// rising value means the record is incomplete — worth surfacing, never worth
// fixing by blocking the terminal.
func (m *Manager) DroppedRecords() uint64 { return m.dropped.Load() }

// Open mints a session and starts its device leg. The session is live from
// this moment whether or not anyone is watching.
func (m *Manager) Open(ctx context.Context, id string, chID channel.ID, caller actor.ActorID, device string, cols, rows uint16) (*Session, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, errors.New("terminal: manager closed")
	}
	if _, exists := m.sessions[id]; exists {
		m.mu.Unlock()
		return nil, errors.New("terminal: duplicate session id")
	}
	m.mu.Unlock()

	dev, err := m.opener.OpenPTY(ctx, chID, device, cols, rows, true)
	if err != nil {
		return nil, err
	}
	s := &Session{ID: id, Channel: chID, Caller: caller, dev: dev, ended: make(chan struct{})}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = dev.Close()
		return nil, errors.New("terminal: manager closed")
	}
	m.sessions[id] = s
	m.mu.Unlock()

	// 开启行走同一条有界队列，恒不在这里同步写。
	//
	// 我先写成了同步——理由是"它是树根，后面每条命令都要引它"。那是错的：
	// 账本一卡，开会话就卡死，正是本包那条"账本恒不得阻塞人的手"被自己破掉
	// （TestASlowLedgerDoesNotFreezeTheTerminal 当场抓到）。**没有哪条记录
	// 值得用一个卡住的终端去换。**
	//
	// 代价是根 id 要过一会儿才回来：在它落定之前发生的命令挂不上树，成为各自
	// 的根。那是诚实降级——记录不完整恒好过终端不能用，而且窗口只有开头一瞬。
	if m.record != nil {
		m.enqueueOpen(s, Record{SessionID: id, Event: "opened", Device: device})
	}
	go m.pumpDevice(s)
	// A session with no viewer yet is already on the clock: an Open whose
	// browser never arrives must not leak a shell.
	s.startGrace(m)
	return s, nil
}

// Attach binds a viewer to an existing session. chID is the channel the
// caller's membership was just checked against; a session恒只在自己的频道里
// 被接回。Without that bind, a caller who belongs to both A and B could hand
// in A's session id while claiming B — no privilege is gained (the session is
// still their own), but the door would have judged one channel and served
// another, and每条命令记录会落进 A 而调用方自称在 B。恒不留这种错位。
func (m *Manager) Attach(id string, chID channel.ID, caller actor.ActorID) (*Session, <-chan []byte, error) {
	m.mu.Lock()
	s := m.sessions[id]
	m.mu.Unlock()
	if s == nil {
		return nil, nil, ErrNoSession
	}
	if s.Caller != caller {
		return nil, nil, ErrNotOwner
	}
	if s.Channel != chID {
		return nil, nil, ErrNotOwner
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, ErrNoSession
	}
	if s.viewer != nil {
		// Take over rather than refuse. A tab switch or a refresh routinely
		// races: the old socket has not finished closing when the new one
		// arrives. Refusing would push the person into opening a SECOND
		// shell, which is the thing this whole grace mechanism exists to
		// avoid. Ownership was already checked above, so the only party who
		// can supersede a viewer is the one who owns the session.
		close(s.viewer)
		s.viewer = nil
	}
	if s.graceAt != nil {
		s.graceAt.Stop()
		s.graceAt = nil
	}
	// Buffered so a slow viewer does not stall the device pump; overflow is
	// dropped, never queued (§4.3-2: 断线期间的输出恒丢).
	ch := make(chan []byte, 256)
	s.viewer = ch
	return s, ch, nil
}

// Detach unbinds the viewer and starts the grace clock. The shell keeps
// running: this is the whole of "保住进程，恒不保住输出".
func (m *Manager) Detach(s *Session) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.viewer != nil {
		close(s.viewer)
		s.viewer = nil
	}
	closed := s.closed
	s.mu.Unlock()
	if !closed {
		s.startGrace(m)
	}
}

func (s *Session) startGrace(m *Manager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.viewer != nil {
		return
	}
	if s.graceAt != nil {
		s.graceAt.Stop()
	}
	s.graceAt = time.AfterFunc(m.grace, func() { m.Close(s.ID) })
}

// Write sends viewer input to the device.
func (s *Session) Write(p []byte) error {
	s.mu.Lock()
	dev := s.dev
	closed := s.closed
	s.mu.Unlock()
	if closed || dev == nil {
		return ErrNoSession
	}
	return writeFrame(dev, p)
}

func (s *Session) Resize(cols, rows uint16) error {
	s.mu.Lock()
	dev := s.dev
	closed := s.closed
	s.mu.Unlock()
	if closed || dev == nil {
		return ErrNoSession
	}
	return resizeFrame(dev, cols, rows)
}

// Close ends a session for good: the shell dies, the viewer is dropped.
func (m *Manager) Close(id string) {
	m.mu.Lock()
	s := m.sessions[id]
	delete(m.sessions, id)
	m.mu.Unlock()
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if s.endReason == "" {
		s.endReason = "session closed"
	}
	if s.graceAt != nil {
		s.graceAt.Stop()
		s.graceAt = nil
	}
	if s.viewer != nil {
		close(s.viewer)
		s.viewer = nil
	}
	dev := s.dev
	s.dev = nil
	s.mu.Unlock()
	if dev != nil {
		_ = dev.Close()
	}
	s.finish("session closed")
	if m.record != nil {
		// 结束行走同一条有界队列：关闭恒不得因为账本忙而卡住。
		s.mu.Lock()
		root := s.openRow
		s.mu.Unlock()
		m.enqueue(s.Channel, s.Caller, Record{
			SessionID: s.ID, Event: "closed",
			Reason: s.EndReason(), Parent: root,
		})
	}
}

// CloseAll ends every session. Called when the node shuts down; lane
// retirement kills the device legs independently (design §6-4).
func (m *Manager) CloseAll() {
	m.mu.Lock()
	m.closed = true
	ids := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	for _, id := range ids {
		m.Close(id)
	}
	if m.record != nil {
		m.stopOnce.Do(func() { close(m.stop) })
		m.recorder.Wait()
	}
}

func (m *Manager) Get(id string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[id]
}
