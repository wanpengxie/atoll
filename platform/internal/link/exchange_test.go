package link

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"testing"
	"time"
)

func TestExchangeBytesRoundTrip(t *testing.T) {
	want := bytes.Repeat([]byte("x"), MaxExchangeChunk+17)
	var framed bytes.Buffer
	if err := WriteExchangeBytes(&framed, bytes.NewReader(want)); err != nil {
		t.Fatal(err)
	}
	wantFramed := append([]byte(nil), framed.Bytes()...)
	var relayed bytes.Buffer
	if err := RelayExchangeBytes(&relayed, &framed); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(relayed.Bytes(), wantFramed) {
		t.Fatal("relay changed framed bytes")
	}
}

func TestExchangeReaderRequiresSuccessfulTerminal(t *testing.T) {
	left, right := net.Pipe()
	go func() {
		defer right.Close()
		_ = WriteExchangeChunk(right, []byte("data"))
		_ = WriteExchangeChunk(right, nil)
		_ = WriteExchangeControl(right, ExchangeStatus{OK: false, Code: "failed", Detail: "no"})
	}()
	_, err := io.ReadAll(NewExchangeReader(left))
	var terminal *ExchangeTerminalError
	if !errors.As(err, &terminal) {
		t.Fatalf("error=%v, want terminal", err)
	}
}

func TestExchangeReaderReportsChunkBoundaryDisconnectAsTruncation(t *testing.T) {
	left, right := net.Pipe()
	go func() {
		_ = WriteExchangeChunk(right, []byte("complete-chunk"))
		_ = right.Close() // no zero-length terminator and no successful terminal
	}()
	got, err := io.ReadAll(NewExchangeReader(left))
	if string(got) != "complete-chunk" {
		t.Fatalf("bytes = %q", got)
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestRelayExchangeBytesIsCutThrough(t *testing.T) {
	sourceRead, sourceWrite := net.Pipe()
	sinkRead, sinkWrite := net.Pipe()
	allowSourceFinish := make(chan struct{})
	sourceFinished := make(chan struct{})
	relayDone := make(chan error, 1)

	go func() {
		defer sourceWrite.Close()
		if err := WriteExchangeChunk(sourceWrite, []byte("x")); err != nil {
			close(sourceFinished)
			return
		}
		<-allowSourceFinish
		_ = WriteExchangeChunk(sourceWrite, nil)
		close(sourceFinished)
	}()
	go func() {
		relayDone <- RelayExchangeBytes(sinkWrite, sourceRead)
		_ = sinkWrite.Close()
	}()

	var firstFrame [5]byte
	if _, err := io.ReadFull(sinkRead, firstFrame[:]); err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint32(firstFrame[:4]); got != 1 || firstFrame[4] != 'x' {
		t.Fatalf("first frame = %v", firstFrame)
	}
	select {
	case <-sourceFinished:
		t.Fatal("source finished before the first payload byte reached the sink")
	default:
	}
	close(allowSourceFinish)
	var terminator [4]byte
	if _, err := io.ReadFull(sinkRead, terminator[:]); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-relayDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("relay did not join after the segment terminator")
	}
}

// framedZeros synthesizes a valid framed byte segment without retaining the
// represented file. It lets the test exercise very large logical files while
// measuring the relay itself rather than fixture allocation.
type framedZeros struct {
	left       int64
	header     [4]byte
	headerAt   int
	payload    int
	finalFrame bool
	done       bool
}

func newFramedZeros(size int64) *framedZeros {
	r := &framedZeros{left: size}
	r.nextFrame()
	return r
}

func (r *framedZeros) nextFrame() {
	r.headerAt = 0
	r.payload = int(min(r.left, int64(MaxExchangeChunk)))
	r.left -= int64(r.payload)
	r.finalFrame = r.payload == 0
	binary.BigEndian.PutUint32(r.header[:], uint32(r.payload))
}

func (r *framedZeros) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	written := 0
	for len(p) > 0 && !r.done {
		if r.headerAt < len(r.header) {
			n := copy(p, r.header[r.headerAt:])
			r.headerAt += n
			written += n
			p = p[n:]
			if r.headerAt < len(r.header) {
				break
			}
			if r.finalFrame {
				r.done = true
			}
			continue
		}
		if r.payload > 0 {
			n := min(len(p), r.payload)
			clear(p[:n])
			r.payload -= n
			written += n
			p = p[n:]
			if r.payload == 0 {
				r.nextFrame()
			}
		}
	}
	if written == 0 && r.done {
		return 0, io.EOF
	}
	return written, nil
}

func TestRelayExchangeBytesHasConstantMemoryBound(t *testing.T) {
	for _, size := range []int64{1 << 20, 64 << 20, 512 << 20} {
		t.Run(fmtBytes(size), func(t *testing.T) {
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			if err := RelayExchangeBytes(io.Discard, newFramedZeros(size)); err != nil {
				t.Fatal(err)
			}
			runtime.ReadMemStats(&after)
			// Total allocation is an upper bound on additional live/peak memory.
			// Keeping even this stronger cumulative number below 8 MiB proves the
			// relay does not retain input-sized buffers.
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated >= 8<<20 {
				t.Fatalf("relay allocated %d bytes for a %d-byte file; want < 8 MiB", allocated, size)
			}
		})
	}
}

func fmtBytes(size int64) string {
	if size >= 1<<20 {
		return fmt.Sprintf("%dMiB", size/(1<<20))
	}
	return "bytes"
}
