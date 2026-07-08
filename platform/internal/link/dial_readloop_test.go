package link

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

// nopRWC is a throwaway io.ReadWriteCloser for an actorStream.stream in read-loop
// unit tests: the read loop drives inbound frames through as.codec (built over
// the test's own pipes), and only ever calls as.stream.Close() directly — which
// here is a harmless no-op.
type nopRWC struct{}

func (nopRWC) Read([]byte) (int, error)    { return 0, io.EOF }
func (nopRWC) Write(p []byte) (int, error) { return len(p), nil }
func (nopRWC) Close() error                { return nil }

func testReadLoopStream(id actor.ActorID, r io.Reader, w io.Writer) *actorStream {
	codec := ipc.NewCodec(r, w)
	return &actorStream{
		id:     id,
		stream: nopRWC{},
		codec:  codec,
		writer: NewRemoteWriter(codec),
		access: newRelayClient(codec, ipc.KindAccess),
		sched:  newRelayClient(codec, ipc.KindSchedule),
	}
}

func testDialer() *Dialer {
	return &Dialer{
		streams: map[actor.ActorID]*actorStream{},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// The read loop's teardown must delete the streams-table entry ONLY if it is
// still this stream (pointer-guarded): a reconnect may have already registered
// a successor under the same actor id, and a bare delete-by-id would tear the
// successor's entry out from under it. (Pre-fix: delete(d.streams, as.id)
// unconditionally.)
func TestStreamReadLoop_PointerGuardedDelete(t *testing.T) {
	d := testDialer()
	pr, pw := io.Pipe()
	_ = pr.Close()
	_ = pw.Close() // codec.Read fails immediately → loop exits straight into its defer
	old := testReadLoopStream("actor:a", pr, pw)
	successor := &actorStream{id: "actor:a"}
	d.streams["actor:a"] = successor // reconnect already replaced the entry

	d.streamReadLoop(old, nil)

	if d.streams["actor:a"] != successor {
		t.Fatal("old stream's teardown deleted the successor's table entry (alias delete-by-id)")
	}
}

// An out-of-closed-set frame kind must fail-closed (close the stream, exit the
// loop) mirroring the home port's discipline: an unknown frame may be an
// unmatchable ack occupying a FIFO slot, and skipping it would silently shift
// an ack arm by one. (Pre-fix: warn-and-continue.)
func TestStreamReadLoop_UnknownKindFailsClosed(t *testing.T) {
	d := testDialer()
	pr, pw := io.Pipe()
	as := testReadLoopStream("actor:a", pr, io.Discard)
	d.streams["actor:a"] = as

	done := make(chan struct{})
	go func() { d.streamReadLoop(as, nil); close(done) }()

	feeder := ipc.NewCodec(nil, pw)
	if err := feeder.Write(ipc.Frame{Kind: ipc.Kind("bogus.kind")}); err != nil {
		t.Fatalf("feed unknown frame: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("read loop kept running past an unknown frame kind (warn-and-continue is back)")
	}
	d.mu.Lock()
	_, still := d.streams["actor:a"]
	d.mu.Unlock()
	if still {
		t.Fatal("failed-closed stream must be removed from the streams table")
	}
}
