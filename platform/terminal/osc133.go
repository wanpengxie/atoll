// Package terminal owns the server half of the terminal line: it relays PTY
// bytes between a browser and a device, and it taps that relay for OSC 133
// shell-integration marks so that every command a human runs lands on the
// channel ledger.
//
// The split this package exists to enforce (terminal-line-design.md §4):
// bytes go over the lane and are NEVER written to the ledger; commands and
// their results are ledger rows and never ride the byte stream.
package terminal

import (
	"strconv"
	"strings"
)

// Scanner is a pass-through tap over one direction of a PTY stream.
//
// It NEVER rewrites or withholds bytes: Feed returns the input untouched and
// reports separately what it recognised. That is a hard requirement, not an
// implementation choice — the marks are in-band precisely because xterm.js
// consumes them too (design §4.1), and a tap that swallowed them would break
// the terminal's own prompt navigation.
//
// It is deliberately tolerant. OSC 133 is cooperative (design §7): a shell
// without the rc fragment simply produces no marks, and this scanner then
// reports nothing rather than guessing. Guessing — parsing prompts out of the
// byte stream — is the approach the design rules out.
type Scanner struct {
	// pending accumulates an OSC sequence that has not terminated yet. An OSC
	// can straddle any number of reads, so partial state must survive Feed.
	pending []byte
	inOSC   bool

	// cmd is the command text captured by the most recent AtollCmd mark, held
	// until the matching D mark closes the command.
	cmd     string
	cwd     string
	started bool
	// at is the offset just past the byte currently being consumed, so a
	// recognised sequence can report where it ended within the fed chunk.
	at int
}

// EventKind distinguishes the two boundaries we care about.
type EventKind int

const (
	// EventStart is the C mark: output begins after it.
	EventStart EventKind = iota
	// EventEnd is the D mark: it closes the bracket and carries the code.
	EventEnd
)

// Event is one recognised boundary, with the offset in the fed chunk just
// past the sequence that produced it. The offset is what lets a caller
// attribute output to the right command when one chunk contains several
// boundaries — inferring from end-of-chunk state is wrong whenever a C and a
// D arrive together, which is exactly what a fast command does.
type Event struct {
	Kind     EventKind
	Offset   int
	Text     string
	Cwd      string
	ExitCode int
	HasExit  bool
}

const (
	esc = 0x1b
	bel = 0x07
	// maxOSC bounds one accumulated sequence. A sequence longer than this is
	// not a mark we know; dropping the accumulation keeps a hostile or broken
	// producer from growing this buffer without bound. The bytes themselves
	// have already been passed through, so nothing is lost downstream.
	maxOSC = 8192
)

// Feed passes chunk through and returns the boundaries recognised within it.
// The returned byte slice is always exactly the input.
func (s *Scanner) Feed(chunk []byte) ([]byte, []Event) {
	var done []Event
	for i, b := range chunk {
		s.at = i + 1
		if !s.inOSC {
			// An OSC opens with ESC ]. We only need to notice the ']' when the
			// previous byte was ESC, so track that minimally.
			if b == esc {
				s.pending = s.pending[:0]
				s.pending = append(s.pending, b)
				continue
			}
			if len(s.pending) == 1 && s.pending[0] == esc && b == ']' {
				s.inOSC = true
				s.pending = s.pending[:0]
				continue
			}
			s.pending = s.pending[:0]
			continue
		}
		// Inside an OSC: terminated by BEL, or by ST (ESC \).
		if b == bel {
			s.finish(string(s.pending), &done)
			continue
		}
		if b == esc {
			// Possible ST; the next byte decides. Mark it by parking the ESC
			// at the end of pending and letting the next iteration see it.
			s.pending = append(s.pending, esc)
			continue
		}
		if n := len(s.pending); n > 0 && s.pending[n-1] == esc {
			if b == '\\' {
				s.finish(string(s.pending[:n-1]), &done)
				continue
			}
			// Not ST — the ESC was literal payload; keep both.
			s.pending = append(s.pending, b)
			continue
		}
		if len(s.pending) >= maxOSC {
			// Overlong: abandon accumulation, stay out of OSC state.
			s.pending = s.pending[:0]
			s.inOSC = false
			continue
		}
		s.pending = append(s.pending, b)
	}
	return chunk, done
}

func (s *Scanner) finish(body string, done *[]Event) {
	s.inOSC = false
	s.pending = s.pending[:0]
	s.interpret(body, done)
}

// interpret decodes the marks this line cares about and ignores every other
// OSC — a terminal carries many (title sets, colour queries, hyperlinks) and
// none of them are our business.
func (s *Scanner) interpret(body string, done *[]Event) {
	switch {
	case strings.HasPrefix(body, "133;"):
		rest := body[len("133;"):]
		switch {
		case strings.HasPrefix(rest, "C"):
			// Command started. A C without a preceding AtollCmd means the
			// shell marks boundaries but does not report text; we still open
			// the bracket so the exit code has something to close.
			s.started = true
			*done = append(*done, Event{Kind: EventStart, Offset: s.at, Text: s.cmd, Cwd: s.cwd})
		case strings.HasPrefix(rest, "D"):
			if !s.started {
				// A D with no open bracket is the very first prompt after
				// login. Nothing ran;恒不造一条空记录。
				s.cmd = ""
				return
			}
			c := Event{Kind: EventEnd, Offset: s.at, Text: s.cmd, Cwd: s.cwd}
			if i := strings.Index(rest, ";"); i >= 0 {
				if code, err := strconv.Atoi(strings.TrimSpace(rest[i+1:])); err == nil {
					c.ExitCode = code
					c.HasExit = true
				}
			}
			if c.Text != "" || c.HasExit {
				*done = append(*done, c)
			}
			s.started = false
			s.cmd = ""
		}
	case strings.HasPrefix(body, "1337;AtollCmd="):
		s.cmd = unquoteShell(body[len("1337;AtollCmd="):])
	case strings.HasPrefix(body, "1337;AtollCwd="):
		s.cwd = unquoteShell(body[len("1337;AtollCwd="):])
	}
}

// unquoteShell undoes zsh's ${(q)} quoting well enough to store the text. It
// is deliberately lenient: this value is a record for humans and agents to
// read, never something re-executed, so a residual backslash is cosmetic and
//恒不值得为它建一个完整的 shell 词法器。
func unquoteShell(in string) string {
	in = strings.TrimSpace(in)
	if len(in) >= 2 && in[0] == '\'' && in[len(in)-1] == '\'' {
		in = in[1 : len(in)-1]
		in = strings.ReplaceAll(in, `'\''`, `'`)
		return in
	}
	var b strings.Builder
	for i := 0; i < len(in); i++ {
		if in[i] == '\\' && i+1 < len(in) {
			i++
			b.WriteByte(in[i])
			continue
		}
		b.WriteByte(in[i])
	}
	return b.String()
}


// StripControl removes escape sequences from a captured output tail. The tail
// is a record for people and agents to READ — colour codes and cursor moves
// are noise there, and an OSC 133 mark surviving into it would be worse than
// noise: a later reader could mistake a recorded mark for a live one.
//
// The live stream keeps every byte; only this copy is cleaned.
func StripControl(in string) string {
	var b strings.Builder
	b.Grow(len(in))
	for i := 0; i < len(in); i++ {
		c := in[i]
		if c != esc {
			if c == bel {
				continue
			}
			b.WriteByte(c)
			continue
		}
		if i+1 >= len(in) {
			break
		}
		switch in[i+1] {
		case ']': // OSC — runs until BEL or ST
			i += 2
			for i < len(in) {
				if in[i] == bel {
					break
				}
				if in[i] == esc && i+1 < len(in) && in[i+1] == '\\' {
					i++
					break
				}
				i++
			}
		case '[': // CSI — runs until a final byte in @..~
			i += 2
			for i < len(in) && (in[i] < 0x40 || in[i] > 0x7e) {
				i++
			}
		default:
			i++
		}
	}
	return b.String()
}
