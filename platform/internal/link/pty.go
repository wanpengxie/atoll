package link

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// PTY session wire.
//
// This is deliberately NOT the exchange wire. Exchange is three segments
// (header → length-prefixed bytes → terminal) and judges its boundary by a
// zero-length chunk; a terminal never produces that zero until the shell is
// gone, and both directions are live at once. Reusing exchange framing here
// would have meant widening a contract whose whole shape says "one transfer,
// then done". See .dalek/pm/terminal-line-design.md §1.
//
// Shape: one PTYOpen header, then an unbounded interleaving of PTYFrame in
// both directions. There is no terminal segment — the session ends when the
// stream closes, and WHY it ended is carried by a PTYExit frame when the
// shell exited on its own. A stream that simply dies means the carrier died,
// which is exactly the "恒即死" case in §4.4.
const (
	// MaxPTYChunk bounds one data frame. Terminal output arrives in small
	// bursts; this is a sanity ceiling, not a flow-control window (yamux
	// already gives per-stream backpressure).
	MaxPTYChunk = 1 << 18

	PTYFrameData   uint8 = 1 // either direction: raw bytes to/from the pty
	PTYFrameResize uint8 = 2 // door → device: window size changed
	PTYFrameExit   uint8 = 3 // device → door: the shell exited, payload = code
)

// PTYOpen is the host-leg header, written by the server after it has verified
// the session ticket. Nothing here comes from the caller unverified: the door
// resolves Shell/Cwd from the device's own workspace, exactly as the exchange
// host header is server-generated (coord 恒不出门 same discipline).
type PTYOpen struct {
	// Shell is the program to run. Empty = the device's login shell.
	Shell string `json:"shell,omitempty"`
	// Cwd is where it starts. Empty = the channel workspace root.
	Cwd string `json:"cwd,omitempty"`
	// Cols/Rows are the initial window. Zero = 80x24.
	Cols uint16 `json:"cols,omitempty"`
	Rows uint16 `json:"rows,omitempty"`
	// Term is the TERM value. Empty = xterm-256color.
	Term string `json:"term,omitempty"`
	// Integration asks the device to inject the OSC 133 shell-integration
	// hooks so command boundaries land on the ledger. False = a bare terminal
	// whose commands are NOT recorded — the honest default is true.
	Integration bool `json:"integration,omitempty"`
}

// PTYReady is the device's answer to PTYOpen. A failed open says why here and
// then closes; a successful one is followed by frames.
type PTYReady struct {
	OK     bool   `json:"ok"`
	Code   string `json:"code,omitempty"`
	Detail string `json:"detail,omitempty"`
	Pid    int    `json:"pid,omitempty"`
}

type PTYResize struct {
	Cols uint16
	Rows uint16
}

func WritePTYControl(w io.Writer, value any) error { return writeStreamJSON(w, value) }
func ReadPTYControl(r io.Reader, value any) error  { return readStreamJSON(r, value) }

// WritePTYFrame writes one framed message: [kind:1][len:4][payload].
func WritePTYFrame(w io.Writer, kind uint8, payload []byte) error {
	if len(payload) > MaxPTYChunk {
		return fmt.Errorf("link: pty frame too large: %d > %d", len(payload), MaxPTYChunk)
	}
	var head [5]byte
	head[0] = kind
	binary.BigEndian.PutUint32(head[1:], uint32(len(payload)))
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

// ReadPTYFrame reads one frame. io.EOF means the peer closed, which is a
// legitimate end of session, not a protocol error.
func ReadPTYFrame(r io.Reader) (uint8, []byte, error) {
	var head [5]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(head[1:])
	if n > MaxPTYChunk {
		return 0, nil, fmt.Errorf("link: pty frame too large: %d > %d", n, MaxPTYChunk)
	}
	if n == 0 {
		return head[0], nil, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	return head[0], buf, nil
}

// EncodeResize / DecodeResize keep the window size on the wire as four bytes
// rather than JSON: it is the one control that rides the same frame stream as
// data, and a fixed shape keeps that stream trivially parseable.
func EncodeResize(cols, rows uint16) []byte {
	var b [4]byte
	binary.BigEndian.PutUint16(b[0:], cols)
	binary.BigEndian.PutUint16(b[2:], rows)
	return b[:]
}

func DecodeResize(p []byte) (PTYResize, error) {
	if len(p) != 4 {
		return PTYResize{}, errors.New("link: malformed pty resize payload")
	}
	return PTYResize{
		Cols: binary.BigEndian.Uint16(p[0:]),
		Rows: binary.BigEndian.Uint16(p[2:]),
	}, nil
}
