package link

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// atomicTime is a tiny mutex-guarded time holder — the lease's last-seen cell,
// written by the demux loop and read by the lease watchdog.
type atomicTime struct {
	mu sync.Mutex
	t  time.Time
}

func (a *atomicTime) set(t time.Time) { a.mu.Lock(); a.t = t; a.mu.Unlock() }
func (a *atomicTime) get() time.Time  { a.mu.Lock(); defer a.mu.Unlock(); return a.t }

// Op is the closed set of mux frame opcodes — exactly three. A stream's life
// is open → data* → close; nothing reserved-but-unwired. The wire spellings are
// pinned by a closed-set test (the two endpoints must agree on the exact byte).
type Op uint8

const (
	// OpOpen (either side): a new logical stream begins. The daemon opens one
	// stream per attached actor; the payload is empty (the ipc handshake that
	// follows on the stream carries the actor identity).
	OpOpen Op = 1
	// OpData (either side): one chunk of a stream's byte flow. The payload is
	// OPAQUE to the mux — native ipc frames (or stream-0 control JSON) ride here
	// untouched (zero-translation invariant).
	OpData Op = 2
	// OpClose (either side): a stream ends. The peer's stream Read returns EOF —
	// for an actor stream that EOF is the presence-down edge.
	OpClose Op = 3
)

// ControlStream is the reserved stream id for the link control plane (attach /
// attach_reply JSON). Actor streams use ids ≥ 1.
const ControlStream uint32 = 0

// frameHeaderBytes is the fixed prefix: stream uint32 BE + op uint8.
const frameHeaderBytes = 5

// encodeFrame builds one mux frame: stream (4 BE) | op (1) | payload.
func encodeFrame(stream uint32, op Op, payload []byte) []byte {
	buf := make([]byte, frameHeaderBytes+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], stream)
	buf[4] = byte(op)
	copy(buf[frameHeaderBytes:], payload)
	return buf
}

// decodeFrame parses one mux frame. The payload is a sub-slice of b (no copy);
// callers that retain it past the read must copy.
func decodeFrame(b []byte) (stream uint32, op Op, payload []byte, err error) {
	if len(b) < frameHeaderBytes {
		return 0, 0, nil, fmt.Errorf("link: short mux frame: %d bytes", len(b))
	}
	stream = binary.BigEndian.Uint32(b[0:4])
	op = Op(b[4])
	payload = b[frameHeaderBytes:]
	return stream, op, payload, nil
}

// ---------------------------------------------------------------------------
// stream — one logical byte-pipe carved out of the link
// ---------------------------------------------------------------------------

// frameSink writes one mux frame to the underlying link (WS). It is the single
// guarded write path both the demux loop and every stream share.
type frameSink func(stream uint32, op Op, payload []byte) error

// stream is one logical io.ReadWriteCloser carved out of a link. Inbound OpData
// chunks are appended to an internal buffer that Read drains (blocking until
// bytes arrive or the stream closes); Write wraps bytes in an OpData frame on
// the shared link write path. This makes a stream indistinguishable from a
// local pipe to ipc.Codec — the zero-translation seam.
type stream struct {
	id   uint32
	sink frameSink

	mu     sync.Mutex
	cond   *sync.Cond
	buf    []byte
	closed bool // local close (Close called or OpClose received)
	werr   error

	closeOnce sync.Once
	// onClose is invoked exactly once when the stream closes (either side),
	// so the owner can send OpClose / drop bookkeeping. May be nil.
	onClose func()
}

func newStream(id uint32, sink frameSink, onClose func()) *stream {
	s := &stream{id: id, sink: sink, onClose: onClose}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// push feeds one inbound OpData payload into the stream's read buffer. Called by
// the demux loop. The payload is copied (the caller's slice is reused).
func (s *stream) push(payload []byte) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.buf = append(s.buf, payload...)
	s.cond.Broadcast()
	s.mu.Unlock()
}

// Read drains buffered bytes, blocking until some arrive or the stream closes.
// Returns io.EOF once the stream is closed and the buffer is drained — that EOF
// is what an actor port reads as the presence-down edge.
func (s *stream) Read(p []byte) (int, error) {
	s.mu.Lock()
	for len(s.buf) == 0 && !s.closed {
		s.cond.Wait()
	}
	if len(s.buf) > 0 {
		n := copy(p, s.buf)
		s.buf = s.buf[n:]
		s.mu.Unlock()
		return n, nil
	}
	s.mu.Unlock()
	return 0, io.EOF
}

// Write wraps p in an OpData frame on the shared link write path. A failed link
// write is sticky (werr) so subsequent Writes fail fast.
func (s *stream) Write(p []byte) (int, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if s.werr != nil {
		err := s.werr
		s.mu.Unlock()
		return 0, err
	}
	s.mu.Unlock()
	if err := s.sink(s.id, OpData, p); err != nil {
		s.mu.Lock()
		s.werr = err
		s.mu.Unlock()
		return 0, err
	}
	return len(p), nil
}

// Close ends the stream locally and signals the peer (OpClose). Idempotent.
func (s *stream) Close() error {
	s.markClosed()
	// Best-effort OpClose to the peer; a dead link makes this a no-op.
	_ = s.sink(s.id, OpClose, nil)
	return nil
}

// markClosed flips the stream to closed and wakes any blocked Read, exactly
// once. Used both by Close (local) and by the demux loop on an inbound OpClose
// or link teardown.
func (s *stream) markClosed() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cond.Broadcast()
		s.mu.Unlock()
		if s.onClose != nil {
			s.onClose()
		}
	})
}

// errLinkClosed is returned by link writes/opens once the underlying link is
// torn down.
var errLinkClosed = errors.New("link: closed")

// Verify stream is a full ReadWriteCloser (the shape ipc.Codec consumes).
var _ io.ReadWriteCloser = (*stream)(nil)

// ---------------------------------------------------------------------------
// linkConn — the mux driver over one WS link
// ---------------------------------------------------------------------------

// wireConn is the minimal duplex byte-message transport a link runs over. WS
// (*websocket.Conn) satisfies it; the interface keeps frame.go free of the WS
// dependency and lets tests drive the mux over an in-memory pair.
type wireConn interface {
	// ReadMessage returns the next inbound binary message (one mux frame).
	ReadMessage() ([]byte, error)
	// WriteMessage sends one binary message (one mux frame). Implementations
	// must be safe for one concurrent writer; linkConn serialises writes.
	WriteMessage([]byte) error
	// Close tears the transport down; pending Reads/Writes error out.
	Close() error
}

// linkConn drives the mux over one wireConn: it serialises all outbound frames
// and demuxes inbound frames into per-stream pipes. It is host-agnostic — both
// the home acceptor and the daemon dialer build one. Stream 0 control frames are
// surfaced via onControl; OpOpen of a fresh actor stream via onOpen (home side
// receives peer-opened streams; daemon side opens its own).
type linkConn struct {
	conn wireConn

	writeMu sync.Mutex

	mu      sync.Mutex
	streams map[uint32]*stream
	closed  bool

	// onControl handles an inbound stream-0 OpData payload (control-plane JSON).
	onControl func(payload []byte)
	// onOpen handles a peer-initiated OpOpen of an actor stream, handing the
	// new stream to the owner (home side wires it to runtime.Attach). nil on the
	// daemon side, which only opens streams itself.
	onOpen func(s *stream)

	closeOnce sync.Once
}

// newLinkConn builds a mux driver. onControl/onOpen may be nil.
func newLinkConn(conn wireConn, onControl func([]byte), onOpen func(*stream)) *linkConn {
	return &linkConn{
		conn:      conn,
		streams:   make(map[uint32]*stream),
		onControl: onControl,
		onOpen:    onOpen,
	}
}

// writeFrame is the single guarded outbound path (the frameSink every stream
// shares). It serialises WS writes — gorilla forbids concurrent writers.
func (lc *linkConn) writeFrame(streamID uint32, op Op, payload []byte) error {
	lc.mu.Lock()
	if lc.closed {
		lc.mu.Unlock()
		return errLinkClosed
	}
	lc.mu.Unlock()
	lc.writeMu.Lock()
	defer lc.writeMu.Unlock()
	return lc.conn.WriteMessage(encodeFrame(streamID, op, payload))
}

// sendControl writes one control-plane payload on stream 0.
func (lc *linkConn) sendControl(payload []byte) error {
	return lc.writeFrame(ControlStream, OpData, payload)
}

// openStream opens a new actor stream (daemon side): registers it, sends OpOpen,
// and returns the io.ReadWriteCloser the caller runs ipc over. The stream's
// Close sends OpClose and deregisters.
func (lc *linkConn) openStream(id uint32) (*stream, error) {
	s := newStream(id, lc.writeFrame, func() { lc.dropStream(id) })
	lc.mu.Lock()
	if lc.closed {
		lc.mu.Unlock()
		return nil, errLinkClosed
	}
	lc.streams[id] = s
	lc.mu.Unlock()
	if err := lc.writeFrame(id, OpOpen, nil); err != nil {
		lc.dropStream(id)
		return nil, err
	}
	return s, nil
}

func (lc *linkConn) dropStream(id uint32) {
	lc.mu.Lock()
	delete(lc.streams, id)
	lc.mu.Unlock()
}

// run is the demux loop: read mux frames, route OpOpen/OpData/OpClose to the
// stream table or the control hooks. Exits when the link reads an error (peer
// gone / link torn down), tearing every stream down (all actor streams EOF =
// all that party's presence falls on the same edge). onFrame, if non-nil, is
// invoked for every inbound frame BEFORE routing — the lease's last-seen
// refresh hooks here (any inbound traffic is liveness).
func (lc *linkConn) run(onFrame func()) {
	defer lc.teardown()
	for {
		raw, err := lc.conn.ReadMessage()
		if err != nil {
			return
		}
		if onFrame != nil {
			onFrame()
		}
		streamID, op, payload, derr := decodeFrame(raw)
		if derr != nil {
			// A malformed mux frame is a protocol violation: fail the whole link
			// (closed-set discipline — never silently skip).
			return
		}
		if streamID == ControlStream {
			if op == OpData && lc.onControl != nil {
				lc.onControl(payload)
			}
			continue
		}
		switch op {
		case OpOpen:
			lc.mu.Lock()
			if lc.closed {
				lc.mu.Unlock()
				return
			}
			_, exists := lc.streams[streamID]
			if exists {
				lc.mu.Unlock()
				continue // duplicate open; ignore
			}
			s := newStream(streamID, lc.writeFrame, func() { lc.dropStream(streamID) })
			lc.streams[streamID] = s
			lc.mu.Unlock()
			if lc.onOpen != nil {
				lc.onOpen(s)
			}
		case OpData:
			lc.mu.Lock()
			s := lc.streams[streamID]
			lc.mu.Unlock()
			if s != nil {
				s.push(payload)
			}
		case OpClose:
			lc.mu.Lock()
			s := lc.streams[streamID]
			lc.mu.Unlock()
			if s != nil {
				s.markClosed()
			}
		default:
			return // unknown opcode = protocol violation
		}
	}
}

// teardown closes every live stream (each Read sees EOF) and the underlying
// transport. Idempotent. This is the single death funnel: link gone → every
// presence on it falls on the same edge.
func (lc *linkConn) teardown() {
	lc.closeOnce.Do(func() {
		lc.mu.Lock()
		lc.closed = true
		streams := make([]*stream, 0, len(lc.streams))
		for _, s := range lc.streams {
			streams = append(streams, s)
		}
		lc.streams = map[uint32]*stream{}
		lc.mu.Unlock()
		for _, s := range streams {
			s.markClosed()
		}
		_ = lc.conn.Close()
	})
}

// Close tears the link down from the owner side (lease expiry, server shutdown).
func (lc *linkConn) Close() error {
	lc.teardown()
	return nil
}
