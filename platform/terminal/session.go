package terminal

import (
	"context"
	"errors"
	"io"
	"sync"
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
	OpenPTY(ctx context.Context, chID channel.ID, cols, rows uint16, integration bool) (io.ReadWriteCloser, error)
}

// Recorder writes one command row to the channel ledger, authored by the
// caller. This is the entire point of the line (design §4.2): the sender is
// stamped by the runtime, never filled in here.
type Recorder func(ctx context.Context, chID channel.ID, caller actor.ActorID, rec Record)

// Record is what lands on the ledger for one command.
type Record struct {
	SessionID string `json:"session_id"`
	Cmd       string `json:"cmd,omitempty"`
	ExitCode  int    `json:"exit_code"`
	HasExit   bool   `json:"-"`
	DurationMs int64 `json:"duration_ms,omitempty"`
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
}

func NewManager(opener Opener, record Recorder) *Manager {
	return &Manager{
		opener:   opener,
		record:   record,
		now:      time.Now,
		grace:    GracePeriod,
		sessions: make(map[string]*Session),
	}
}

// Open mints a session and starts its device leg. The session is live from
// this moment whether or not anyone is watching.
func (m *Manager) Open(ctx context.Context, id string, chID channel.ID, caller actor.ActorID, cols, rows uint16) (*Session, error) {
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

	dev, err := m.opener.OpenPTY(ctx, chID, cols, rows, true)
	if err != nil {
		return nil, err
	}
	s := &Session{ID: id, Channel: chID, Caller: caller, dev: dev}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = dev.Close()
		return nil, errors.New("terminal: manager closed")
	}
	m.sessions[id] = s
	m.mu.Unlock()

	go m.pumpDevice(s)
	// A session with no viewer yet is already on the clock: an Open whose
	// browser never arrives must not leak a shell.
	s.startGrace(m)
	return s, nil
}

// Attach binds a viewer. Returns the downstream channel the caller must drain.
func (m *Manager) Attach(id string, caller actor.ActorID) (*Session, <-chan []byte, error) {
	m.mu.Lock()
	s := m.sessions[id]
	m.mu.Unlock()
	if s == nil {
		return nil, nil, ErrNoSession
	}
	if s.Caller != caller {
		return nil, nil, ErrNotOwner
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, ErrNoSession
	}
	if s.viewer != nil {
		return nil, nil, ErrBusy
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
}

func (m *Manager) Get(id string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[id]
}
