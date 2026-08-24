package terminal

import (
	"context"
	"io"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
)

func writeFrame(w io.Writer, p []byte) error {
	return link.WritePTYFrame(w, link.PTYFrameData, p)
}

func resizeFrame(w io.Writer, cols, rows uint16) error {
	return link.WritePTYFrame(w, link.PTYFrameResize, link.EncodeResize(cols, rows))
}

// pumpDevice is the one place where the two halves of this line meet.
//
// Every byte the device produces goes to the viewer verbatim (or is dropped if
// nobody is watching — §4.3-2). The SAME bytes are fed to the OSC scanner,
// which recognises command boundaries and yields ledger rows. Neither half can
// starve the other: the scan is synchronous and allocation-free per byte, and
// the viewer send is non-blocking.
func (m *Manager) pumpDevice(s *Session) {
	defer m.Close(s.ID)

	s.mu.Lock()
	dev := s.dev
	s.mu.Unlock()
	if dev == nil {
		return
	}
	// The device leg answers PTYOpen with PTYReady before any frame.
	var ready link.PTYReady
	if err := link.ReadPTYControl(dev, &ready); err != nil || !ready.OK {
		return
	}
	for {
		kind, payload, err := link.ReadPTYFrame(dev)
		if err != nil {
			return
		}
		switch kind {
		case link.PTYFrameData:
			m.onOutput(s, payload)
		case link.PTYFrameExit:
			// The shell exited on its own — a real terminal state, unlike a
			// dropped connection. Nothing to record beyond ending the session:
			// the last command's row was already closed by its own D mark.
			return
		}
	}
}

// onOutput passes bytes to the viewer and taps them for command marks.
//
// Output is attributed to commands by EVENT OFFSET, never by end-of-chunk
// state: one read routinely carries both the C and the D of a fast command,
// and inferring from the scanner's final state would mis-file every one of
// them.
func (m *Manager) onOutput(s *Session, payload []byte) {
	s.mu.Lock()
	passed, events := s.scanner.Feed(payload)

	var emit []Record
	prev := 0
	for _, ev := range events {
		if s.inCmd {
			s.tail = appendTail(s.tail, passed[prev:ev.Offset])
		}
		prev = ev.Offset
		switch ev.Kind {
		case EventStart:
			s.inCmd = true
			s.openCmd = time.Now()
			s.tail = s.tail[:0]
		case EventEnd:
			rec := Record{
				SessionID:  s.ID,
				Cmd:        ev.Text,
				ExitCode:   ev.ExitCode,
				HasExit:    ev.HasExit,
				OutputTail: string(s.tail),
			}
			if s.inCmd && !s.openCmd.IsZero() {
				rec.DurationMs = time.Since(s.openCmd).Milliseconds()
			}
			emit = append(emit, rec)
			s.inCmd = false
			s.tail = s.tail[:0]
		}
	}
	if s.inCmd && prev < len(passed) {
		s.tail = appendTail(s.tail, passed[prev:])
	}

	viewer := s.viewer
	// Non-blocking: a viewer that cannot keep up loses bytes rather than
	// stalling the device. §4.3 says the stream恒不需精准.
	if viewer != nil {
		select {
		case viewer <- append([]byte(nil), passed...):
		default:
		}
	}
	chID, caller := s.Channel, s.Caller
	s.mu.Unlock()

	if m.record == nil {
		return
	}
	for _, rec := range emit {
		m.record(context.Background(), chID, caller, rec)
	}
}

// appendTail keeps at most MaxOutputTail bytes, discarding from the front.
func appendTail(buf, add []byte) []byte {
	buf = append(buf, add...)
	if len(buf) > MaxOutputTail {
		buf = buf[len(buf)-MaxOutputTail:]
	}
	return buf
}
