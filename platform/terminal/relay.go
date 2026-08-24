package terminal

import (
	"context"
	"io"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// recordTimeout bounds one ledger write. It is generous — a row is worth
// waiting for — but finite, because an unbounded wait is how a queue turns
// into a leak.
const recordTimeout = 10 * time.Second

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
			// dropped connection. Say so: a viewer that cannot tell the two
			// apart reconnects and silently gets a SECOND shell.
			reason := "shell exited"
			if code := string(payload); code != "" && code != "0" {
				reason = "shell exited (" + code + ")"
			}
			s.finish(reason)
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
				SessionID: s.ID,
				Cmd:       ev.Text,
				Cwd:       ev.Cwd,
				ExitCode:  ev.ExitCode,
				HasExit:   ev.HasExit,
				// The live stream kept every byte; this copy is cleaned so a
				// reader gets text, not colour codes — and so a recorded OSC
				// mark can never be mistaken for a live one.
				OutputTail: StripControl(string(s.tail)),
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

	if m.record == nil || len(emit) == 0 {
		return
	}
	// The ledger write恒不得在设备输出泵的路径上. Recording synchronously here
	// would mean a slow subject cell stops this loop from reading, the pty's
	// buffer fills, and the terminal freezes — the exact opposite of what
	// pty_record.go's comment promises. Hand the rows to a bounded queue and
	// go back to reading.
	for _, rec := range emit {
		m.enqueue(chID, caller, rec)
	}
}

// enqueue hands one row to the recorder goroutine. The queue is bounded and
// LOSSY BY DESIGN: a ledger that cannot keep up costs us rows, never the
// person's keystrokes. A dropped row is a gap in the record; a blocked pump is
// a dead terminal, and only one of those is acceptable.
func (m *Manager) enqueue(chID channel.ID, caller actor.ActorID, rec Record) {
	select {
	case m.records <- recordJob{ch: chID, caller: caller, rec: rec}:
	default:
		m.dropped.Add(1)
	}
}

// recordLoop drains the queue. Each write gets its own deadline so one stuck
// delivery cannot wedge every row behind it.
func (m *Manager) recordLoop() {
	write := func(job recordJob) {
		ctx, cancel := context.WithTimeout(context.Background(), recordTimeout)
		defer cancel()
		m.record(ctx, job.ch, job.caller, job.rec)
	}
	for {
		select {
		case job := <-m.records:
			write(job)
		case <-m.stop:
			// Drain what is already queued, then go. Rows still in flight
			// from a pump that has not noticed shutdown yet are lost, which
			// is the same trade the queue makes everywhere else.
			for {
				select {
				case job := <-m.records:
					write(job)
				default:
					return
				}
			}
		}
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
