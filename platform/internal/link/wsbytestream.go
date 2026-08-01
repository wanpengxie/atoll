package link

import (
	"io"
	"sync"

	"github.com/gorilla/websocket"
)

// wsByteStream adapts a gorilla *websocket.Conn into a plain io.ReadWriteCloser
// byte stream — the carrier shape yamux.Client/yamux.Server want. This is the
// 期11 spec §5.2 "换底" seam: the link runs a single top-level yamux SESSION
// directly over the raw WS connection, and yamux wants a byte
// stream, not WS's per-call binary-MESSAGE transport (ReadMessage/WriteMessage)
// — so this type exists to bridge WS's message framing down to bytes.
//
// wsByteStream is the only adapter that touches the websocket connection.
// Gorilla permits one reader and one writer, so this adapter serializes each
// direction and no other layer accesses websocket message framing.
//
// This is the one byte carrier the device's top-level yamux session rides on.
type wsByteStream struct {
	ws *websocket.Conn

	// rmu serialises the read side. gorilla tolerates one concurrent reader
	// and one concurrent writer on the SAME conn, but not two concurrent
	// readers (NextReader itself is not reentrant) — Read may be called
	// concurrently by multiple goroutines in general io.ReadWriteCloser usage
	// (yamux reads on its own receive loop only, in practice one goroutine,
	// but this adapter does not assume that of every future caller).
	rmu sync.Mutex
	// rr is the io.Reader for the WS message CURRENTLY being drained, or nil
	// when the previous message ran out and the next Read must call
	// NextReader to fetch a fresh one. A message's own EOF (io.Reader.Read
	// returning io.EOF once that message's bytes are exhausted) is NOT the
	// connection's EOF — gorilla signals connection-level closure/error via
	// NextReader's own returned error, never by NextReader's Reader ever
	// legitimately returning something other than EOF as its OWN
	// end-of-message marker — so rr==nil is exactly "go get the next
	// message", never treated as a stream-level close.
	rr io.Reader

	// wmu serialises the write side. gorilla explicitly disallows concurrent
	// writers on one *websocket.Conn (NextWriter is not reentrant either);
	// yamux itself already serialises its own writes through one send loop,
	// but — same reasoning as rmu — this adapter does not lean on that.
	wmu sync.Mutex
}

// newWSByteStream wraps ws as a byte-stream io.ReadWriteCloser. This is the
// RAW carrier yamux itself reads/writes — it carries yamux's own internal
// keepalive ping/pong alongside every substream's bytes, indistinguishably.
// It intentionally carries no session-liveness hook: spine traffic refreshes
// the carrier lease.
func newWSByteStream(ws *websocket.Conn) *wsByteStream {
	return &wsByteStream{ws: ws}
}

// Read fills p from the WS connection's message stream, transparently
// crossing message boundaries: once the current message is drained it calls
// NextReader for the next one and keeps going — a caller (yamux) sees one
// continuous byte stream, never a per-message chunk boundary.
func (s *wsByteStream) Read(p []byte) (int, error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	for {
		if s.rr == nil {
			_, r, err := s.ws.NextReader()
			if err != nil {
				// Connection-level: closed, reset, or a control-frame-only
				// read loop giving up. Propagate as-is (yamux treats any
				// Read error as carrier-dead, the same contract gorilla's
				// ReadMessage error semantics give).
				return 0, err
			}
			s.rr = r
		}
		n, err := s.rr.Read(p)
		if err != nil {
			// The CURRENT message is exhausted (or errored) — either way this
			// reader is spent, so drop it; the next loop iteration (this call
			// if n==0, a future call if n>0) fetches a fresh one.
			s.rr = nil
			if err == io.EOF {
				if n > 0 {
					// io.Reader contract allows returning (n>0, io.EOF)
					// together, but that EOF is this MESSAGE's end, not the
					// connection's — swallow it so the caller does not read
					// it as stream-EOF; the next Read call will transparently
					// move on to the next WS message.
					return n, nil
				}
				// Nothing to hand back this round — loop immediately into
				// NextReader for the next message rather than returning a
				// bogus (0, nil) or a wrong-meaning (0, io.EOF).
				continue
			}
			return n, err
		}
		return n, nil
	}
}

// Write sends p as exactly one WS binary message — NextWriter/Close is
// gorilla's own recommended idiom for writing a message in one shot; wmu
// keeps two concurrent Write calls from interleaving onto the same NextWriter
// (gorilla forbids concurrent writers).
func (s *wsByteStream) Write(p []byte) (int, error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	w, err := s.ws.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return 0, err
	}
	n, werr := w.Write(p)
	if werr != nil {
		_ = w.Close()
		return n, werr
	}
	if err := w.Close(); err != nil {
		return n, err
	}
	return n, nil
}

// Close closes the underlying WS connection.
func (s *wsByteStream) Close() error { return s.ws.Close() }

var _ io.ReadWriteCloser = (*wsByteStream)(nil)
