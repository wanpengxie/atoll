package link

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/wanpengxie/atoll/protocol/access"
)

const MaxExchangeChunk = 1 << 20

// ExchangeTicketHeader opens the redeeming leg of a transfer. Caller rides with
// the ticket because a ticket's scope is (channel, actor): the lane says which
// channel and which device, and this says which of that device's actors is
// redeeming — without it the actor half of the scope has nobody to check
// against.
type ExchangeTicketHeader struct {
	Ticket string `json:"ticket"`
	Caller string `json:"caller"`
}

type ExchangeHostHeader struct {
	Path string           `json:"path"`
	Mode access.Operation `json:"mode"`
}

type ExchangeStatus struct {
	OK     bool   `json:"ok"`
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func WriteExchangeControl(w io.Writer, value any) error { return writeStreamJSON(w, value) }
func ReadExchangeControl(r io.Reader, value any) error  { return readStreamJSON(r, value) }

func WriteExchangeChunk(w io.Writer, payload []byte) error {
	if len(payload) > MaxExchangeChunk {
		return fmt.Errorf("link: exchange chunk too large: %d > %d", len(payload), MaxExchangeChunk)
	}
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(payload)))
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

func WriteExchangeBytes(w io.Writer, src io.Reader) error {
	return WriteExchangeBytesNotifyEnd(w, src, nil)
}

// WriteExchangeBytesNotifyEnd is WriteExchangeBytes with a synchronous hook
// after the zero-length terminator has been written.
func WriteExchangeBytesNotifyEnd(w io.Writer, src io.Reader, onEnd func()) error {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if werr := WriteExchangeChunk(w, buf[:n]); werr != nil {
				return werr
			}
		}
		if errors.Is(err, io.EOF) {
			if err := WriteExchangeChunk(w, nil); err != nil {
				return err
			}
			if onEnd != nil {
				onEnd()
			}
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// RelayExchangeBytes relays one framed byte segment without retaining a whole
// chunk. It validates the 1 MiB protocol bound before forwarding its header.
func RelayExchangeBytes(dst io.Writer, src io.Reader) error {
	return RelayExchangeBytesNotifyEnd(dst, src, nil)
}

// RelayExchangeBytesNotifyEnd is RelayExchangeBytes with a synchronous hook
// after the zero-length terminator has been written. It lets a duplex pump
// distinguish a legitimate terminal status from a premature success without
// buffering or inventing another frame parser.
func RelayExchangeBytesNotifyEnd(dst io.Writer, src io.Reader, onEnd func()) error {
	for {
		n, status, err := readExchangeChunkHeader(src)
		if err != nil {
			return err
		}
		if status != nil {
			return &ExchangeTerminalError{Code: status.Code, Detail: status.Detail}
		}
		var head [4]byte
		binary.BigEndian.PutUint32(head[:], n)
		if _, err := dst.Write(head[:]); err != nil {
			return err
		}
		if n == 0 {
			if onEnd != nil {
				onEnd()
			}
			return nil
		}
		if _, err := io.CopyN(dst, src, int64(n)); err != nil {
			return err
		}
	}
}

// ReadExchangeBytes decodes one framed byte segment into an ordinary writer.
func ReadExchangeBytes(dst io.Writer, src io.Reader) error {
	for {
		n, status, err := readExchangeChunkHeader(src)
		if err != nil {
			return err
		}
		if status != nil {
			return &ExchangeTerminalError{Code: status.Code, Detail: status.Detail}
		}
		if n == 0 {
			return nil
		}
		if _, err := io.CopyN(dst, src, int64(n)); err != nil {
			return err
		}
	}
}

func readExchangeChunkHeader(src io.Reader) (uint32, *ExchangeStatus, error) {
	var first [1]byte
	if _, err := io.ReadFull(src, first[:]); err != nil {
		return 0, nil, err
	}
	if first[0] == '{' {
		var status ExchangeStatus
		if err := ReadExchangeControl(io.MultiReader(bytes.NewReader(first[:]), src), &status); err != nil {
			return 0, nil, err
		}
		if status.OK {
			return 0, nil, errors.New("link: premature successful exchange terminal")
		}
		return 0, &status, nil
	}
	var rest [3]byte
	if _, err := io.ReadFull(src, rest[:]); err != nil {
		return 0, nil, err
	}
	var head [4]byte
	head[0] = first[0]
	copy(head[1:], rest[:])
	n := binary.BigEndian.Uint32(head[:])
	if n > MaxExchangeChunk {
		return 0, nil, fmt.Errorf("link: exchange chunk too large: %d > %d", n, MaxExchangeChunk)
	}
	return n, nil, nil
}

type ExchangeTerminalError struct {
	Code   string
	Detail string
}

func (e *ExchangeTerminalError) Error() string {
	if e.Detail == "" {
		return "link: exchange failed: " + e.Code
	}
	return "link: exchange failed: " + e.Code + ": " + e.Detail
}

// ExchangeReader presents a framed READ response as an ordinary stream. EOF
// is returned only after the mandatory successful terminal frame.
type ExchangeReader struct {
	conn      io.ReadWriteCloser
	remaining uint32
	done      bool
	mu        sync.Mutex
}

func NewExchangeReader(conn io.ReadWriteCloser) *ExchangeReader { return &ExchangeReader{conn: conn} }

func (r *ExchangeReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return 0, io.EOF
	}
	for r.remaining == 0 {
		n, status, err := readExchangeChunkHeader(r.conn)
		if err != nil {
			return 0, exchangeReadError(err)
		}
		if status != nil {
			r.done = true
			_ = r.conn.Close()
			return 0, &ExchangeTerminalError{Code: status.Code, Detail: status.Detail}
		}
		r.remaining = n
		if r.remaining == 0 {
			var status ExchangeStatus
			if err := ReadExchangeControl(r.conn, &status); err != nil {
				return 0, exchangeReadError(err)
			}
			r.done = true
			_ = r.conn.Close()
			if !status.OK {
				return 0, &ExchangeTerminalError{Code: status.Code, Detail: status.Detail}
			}
			return 0, io.EOF
		}
	}
	if uint32(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.conn.Read(p)
	r.remaining -= uint32(n)
	return n, exchangeReadError(err)
}

// A transport EOF is never a successful READ terminal. The only ordinary EOF
// ExchangeReader exposes is synthesized above after both the zero-length byte
// terminator and an ok=true terminal frame have been consumed.
func exchangeReadError(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func (r *ExchangeReader) Close() error {
	r.mu.Lock()
	r.done = true
	r.mu.Unlock()
	return r.conn.Close()
}

// ExchangeWriteHandle is the remote file-write handle.
type ExchangeWriteHandle struct {
	conn io.ReadWriteCloser
	mu   sync.Mutex
	done bool
}

func NewExchangeWriteHandle(conn io.ReadWriteCloser) *ExchangeWriteHandle {
	return &ExchangeWriteHandle{conn: conn}
}

func (h *ExchangeWriteHandle) Write(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.done {
		return 0, errors.New("link: exchange write is closed")
	}
	written := 0
	for len(p) > 0 {
		n := min(len(p), MaxExchangeChunk)
		if err := WriteExchangeChunk(h.conn, p[:n]); err != nil {
			return written, err
		}
		written += n
		p = p[n:]
	}
	return written, nil
}

func (h *ExchangeWriteHandle) Commit() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.done {
		return errors.New("link: exchange write is closed")
	}
	h.done = true
	if err := WriteExchangeChunk(h.conn, nil); err != nil {
		_ = h.conn.Close()
		return err
	}
	var status ExchangeStatus
	err := ReadExchangeControl(h.conn, &status)
	_ = h.conn.Close()
	if err != nil {
		return err
	}
	if !status.OK {
		return &ExchangeTerminalError{Code: status.Code, Detail: status.Detail}
	}
	return nil
}

func (h *ExchangeWriteHandle) Abort() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.done {
		return nil
	}
	h.done = true
	return h.conn.Close()
}
