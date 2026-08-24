package terminal

import (
	"bytes"
	"testing"
)

func osc(body string) []byte { return []byte("\x1b]" + body + "\x1b\\") }

func feedAll(s *Scanner, chunks ...[]byte) ([]byte, []Event) {
	var out bytes.Buffer
	var all []Event
	for _, c := range chunks {
		passed, evs := s.Feed(c)
		out.Write(passed)
		for _, e := range evs {
			if e.Kind == EventEnd {
				all = append(all, e)
			}
		}
	}
	return out.Bytes(), all
}

func TestScannerPassesEveryByteThrough(t *testing.T) {
	// The marks must reach xterm.js too — a tap that swallowed them would
	// break the terminal's own prompt navigation (design §4.1).
	in := append([]byte("hello"), osc("133;C")...)
	in = append(in, []byte("world")...)
	in = append(in, osc("133;D;0")...)
	var s Scanner
	out, _ := s.Feed(in)
	if !bytes.Equal(out, in) {
		t.Fatalf("scanner altered the stream:\n got %q\nwant %q", out, in)
	}
}

func TestScannerCapturesCommandAndExitCode(t *testing.T) {
	var s Scanner
	_, cmds := feedAll(&s,
		osc("1337;AtollCmd='make test'"),
		osc("133;C"),
		[]byte("...build output..."),
		osc("133;D;1"),
	)
	if len(cmds) != 1 {
		t.Fatalf("want 1 command, got %d (%+v)", len(cmds), cmds)
	}
	if cmds[0].Text != "make test" {
		t.Errorf("text = %q, want %q", cmds[0].Text, "make test")
	}
	if !cmds[0].HasExit || cmds[0].ExitCode != 1 {
		t.Errorf("exit = %d (has=%v), want 1", cmds[0].ExitCode, cmds[0].HasExit)
	}
}

func TestScannerHandlesSequenceSplitAcrossReads(t *testing.T) {
	// An OSC can straddle any number of reads; partial state must survive.
	full := append(osc("1337;AtollCmd='go build ./...'"), osc("133;C")...)
	full = append(full, osc("133;D;0")...)
	var s Scanner
	var chunks [][]byte
	for i := 0; i < len(full); i++ {
		chunks = append(chunks, full[i:i+1]) // one byte at a time — worst case
	}
	out, cmds := feedAll(&s, chunks...)
	if !bytes.Equal(out, full) {
		t.Fatalf("byte-at-a-time feed altered the stream")
	}
	if len(cmds) != 1 || cmds[0].Text != "go build ./..." || cmds[0].ExitCode != 0 {
		t.Fatalf("got %+v, want one {go build ./..., 0}", cmds)
	}
}

func TestScannerAcceptsBELTerminator(t *testing.T) {
	var s Scanner
	_, cmds := feedAll(&s,
		[]byte("\x1b]1337;AtollCmd='ls'\x07"),
		[]byte("\x1b]133;C\x07"),
		[]byte("\x1b]133;D;0\x07"),
	)
	if len(cmds) != 1 || cmds[0].Text != "ls" {
		t.Fatalf("BEL-terminated OSC not recognised: %+v", cmds)
	}
}

func TestScannerIgnoresUnrelatedOSC(t *testing.T) {
	// Terminals carry many OSCs (title, colour, hyperlink) — none are ours.
	var s Scanner
	_, cmds := feedAll(&s,
		osc("0;window title"),
		osc("8;;https://example.com"),
		osc("10;?"),
	)
	if len(cmds) != 0 {
		t.Fatalf("unrelated OSC produced commands: %+v", cmds)
	}
}

func TestScannerDoesNotInventARecordForTheFirstPrompt(t *testing.T) {
	// The D that follows login closes nothing —恒不造一条空记录.
	var s Scanner
	_, cmds := feedAll(&s, osc("133;A"), osc("133;D;0"))
	if len(cmds) != 0 {
		t.Fatalf("first prompt produced a phantom command: %+v", cmds)
	}
}

func TestScannerRecordsBoundaryEvenWithoutCommandText(t *testing.T) {
	// A shell that marks boundaries but does not report text still gives us
	// an exit code, which is worth a row.
	var s Scanner
	_, cmds := feedAll(&s, osc("133;C"), osc("133;D;130"))
	if len(cmds) != 1 || cmds[0].Text != "" || cmds[0].ExitCode != 130 {
		t.Fatalf("got %+v, want one {\"\", 130}", cmds)
	}
}

func TestScannerSurvivesOverlongSequence(t *testing.T) {
	var s Scanner
	junk := append([]byte("\x1b]"), bytes.Repeat([]byte("x"), maxOSC+64)...)
	out, evs := s.Feed(junk)
	cmds := evs[:0]
	if !bytes.Equal(out, junk) {
		t.Fatal("overlong OSC altered the stream")
	}
	if len(cmds) != 0 {
		t.Fatalf("overlong OSC produced commands: %+v", cmds)
	}
	// Still usable afterwards.
	_, cmds = feedAll(&s, osc("1337;AtollCmd='pwd'"), osc("133;C"), osc("133;D;0"))
	if len(cmds) != 1 || cmds[0].Text != "pwd" {
		t.Fatalf("scanner did not recover: %+v", cmds)
	}
}

func TestUnquoteShell(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`'make test'`, "make test"},
		{`'it'\''s'`, "it's"},
		{`plain`, "plain"},
		{`a\ b`, "a b"},
	} {
		if got := unquoteShell(tc.in); got != tc.want {
			t.Errorf("unquoteShell(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestScannerReportsOffsetsSoOutputCanBeAttributed(t *testing.T) {
	// One read routinely carries both the C and the D of a fast command.
	// Inferring from end-of-chunk state would mis-file every such command,
	// so the boundaries must come back with offsets.
	var chunk []byte
	chunk = append(chunk, osc("1337;AtollCmd='true'")...)
	chunk = append(chunk, osc("133;C")...)
	mid := len(chunk)
	chunk = append(chunk, []byte("OUT")...)
	chunk = append(chunk, osc("133;D;0")...)

	var s Scanner
	passed, events := s.Feed(chunk)
	if !bytes.Equal(passed, chunk) {
		t.Fatal("stream altered")
	}
	var start, end *Event
	for i := range events {
		switch events[i].Kind {
		case EventStart:
			start = &events[i]
		case EventEnd:
			end = &events[i]
		}
	}
	if start == nil || end == nil {
		t.Fatalf("want both a start and an end, got %+v", events)
	}
	if start.Offset != mid {
		t.Errorf("start offset = %d, want %d", start.Offset, mid)
	}
	if got := string(chunk[start.Offset:end.Offset]); !bytes.Contains([]byte(got), []byte("OUT")) {
		t.Errorf("output between boundaries = %q, want it to contain OUT", got)
	}
	if end.Text != "true" || end.ExitCode != 0 {
		t.Errorf("end = %+v, want {true, 0}", *end)
	}
}

func TestStripControlLeavesTextAndRemovesMarks(t *testing.T) {
	// A recorded tail must not carry an OSC 133 mark: a later reader could
	// not tell it from a live one.
	raw := "before" + string(osc("133;C")) + "\x1b[31mred\x1b[0m" + string(osc("1337;AtollCmd='x'")) + "after"
	got := StripControl(raw)
	if got != "beforeredafter" {
		t.Fatalf("StripControl = %q, want %q", got, "beforeredafter")
	}
}

func TestScannerCapturesCwd(t *testing.T) {
	var s Scanner
	_, cmds := feedAll(&s,
		osc("1337;AtollCmd='go build'"),
		osc("1337;AtollCwd='/home/me/proj'"),
		osc("133;C"),
		osc("133;D;0"),
	)
	if len(cmds) != 1 || cmds[0].Cwd != "/home/me/proj" {
		t.Fatalf("cwd not captured: %+v", cmds)
	}
}
