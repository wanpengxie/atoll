package link

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

// TestOpClosedSet pins the mux opcode set at exactly three members with fixed
// wire bytes. A stream's life is open → data* → close; nothing reserved. If a
// value drifts or an op is added/removed, this trips — the two endpoints must
// agree on the exact byte (mirror of ipc TestKindClosedSet).
func TestOpClosedSet(t *testing.T) {
	want := map[Op]uint8{
		OpOpen:  1,
		OpData:  2,
		OpClose: 3,
	}
	for op, b := range want {
		if uint8(op) != b {
			t.Errorf("Op %v wire byte = %d, want %d", op, uint8(op), b)
		}
	}
	if len(want) != 3 {
		t.Fatalf("expected exactly 3 opcodes, guard lists %d", len(want))
	}
}

// TestFrameRoundTrip proves encode/decode preserves stream id, op, and payload
// (payload opaque: arbitrary bytes survive, including non-JSON).
func TestFrameRoundTrip(t *testing.T) {
	payload := []byte{0x00, 0xff, 0x7b, 0x6e, 0x6f, 0x74, 0x6a, 0x73, 0x6f, 0x6e}
	enc := encodeFrame(0x01020304, OpData, payload)
	stream, op, got, err := decodeFrame(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if stream != 0x01020304 {
		t.Fatalf("stream = %#x, want 0x01020304", stream)
	}
	if op != OpData {
		t.Fatalf("op = %v, want OpData", op)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %v, want %v", got, payload)
	}
}

// TestDecodeShortFrame proves a frame shorter than the header is rejected (a
// malformed mux frame is a protocol violation, never silently accepted).
func TestDecodeShortFrame(t *testing.T) {
	if _, _, _, err := decodeFrame([]byte{0x00, 0x01}); err == nil {
		t.Fatal("expected error for short frame")
	}
}

// TestStreamReadWritePipe proves a stream behaves as an io.ReadWriteCloser: data
// pushed in is readable, and close surfaces as io.EOF once drained — the seam
// ipc.Codec runs over.
func TestStreamReadWritePipe(t *testing.T) {
	var sink bytes.Buffer
	var mu sync.Mutex
	s := newStream(7, func(_ uint32, op Op, p []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if op == OpData {
			sink.Write(p)
		}
		return nil
	}, nil)

	// Write goes out as OpData on the sink.
	if _, err := s.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	mu.Lock()
	if got := sink.String(); got != "hello" {
		mu.Unlock()
		t.Fatalf("sink = %q, want hello", got)
	}
	mu.Unlock()

	// Push inbound data; Read drains it.
	s.push([]byte("world"))
	buf := make([]byte, 5)
	n, err := s.Read(buf)
	if err != nil || string(buf[:n]) != "world" {
		t.Fatalf("read = %q (%v), want world", buf[:n], err)
	}

	// After close, a drained Read returns EOF.
	s.markClosed()
	if _, err := s.Read(buf); err != io.EOF {
		t.Fatalf("read after close = %v, want io.EOF", err)
	}
}

// TestStreamReadBlocksUntilData proves Read blocks until bytes arrive (the
// behaviour ipc.Codec's io.ReadFull depends on).
func TestStreamReadBlocksUntilData(t *testing.T) {
	s := newStream(1, func(uint32, Op, []byte) error { return nil }, nil)
	done := make(chan string, 1)
	go func() {
		buf := make([]byte, 16)
		n, _ := s.Read(buf)
		done <- string(buf[:n])
	}()
	select {
	case <-done:
		t.Fatal("Read returned before any data")
	case <-time.After(20 * time.Millisecond):
	}
	s.push([]byte("late"))
	select {
	case got := <-done:
		if got != "late" {
			t.Fatalf("read = %q, want late", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Read never woke on push")
	}
}
