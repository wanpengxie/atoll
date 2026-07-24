package link

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

type closeTrackingRWC struct {
	once   sync.Once
	closed chan struct{}
}

func newCloseTrackingRWC() *closeTrackingRWC {
	return &closeTrackingRWC{closed: make(chan struct{})}
}
func (*closeTrackingRWC) Read([]byte) (int, error)    { return 0, io.EOF }
func (*closeTrackingRWC) Write(p []byte) (int, error) { return len(p), nil }
func (c *closeTrackingRWC) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func testReadLoopStream(id actor.ActorID, stream io.ReadWriteCloser, r io.Reader, w io.Writer) *actorStream {
	codec := ipc.NewCodec(r, w)
	return &actorStream{
		id: id, stream: stream, codec: codec,
		writer: NewRemoteWriter(codec),
		access: newRelayClient(codec, ipc.KindAccess),
		sched:  newRelayClient(codec, ipc.KindSchedule),
		done:   make(chan struct{}),
	}
}

func testDialer() *Dialer {
	return &Dialer{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestExactStreamEOFClosesOnlyThatObject(t *testing.T) {
	first := newCloseTrackingRWC()
	second := newCloseTrackingRWC()
	d := testDialer()
	d.streamReadLoop(testReadLoopStream("actor:a", first, io.LimitReader(first, 0), io.Discard), nil)
	select {
	case <-first.closed:
	default:
		t.Fatal("EOF did not close exact stream")
	}
	select {
	case <-second.closed:
		t.Fatal("one exact stream teardown reached an unrelated stream")
	default:
	}
}

func TestStreamReadLoopUnknownKindFailsExactStreamClosed(t *testing.T) {
	d := testDialer()
	reader, writer := io.Pipe()
	stream := newCloseTrackingRWC()
	as := testReadLoopStream("actor:a", stream, reader, io.Discard)
	done := make(chan struct{})
	go func() {
		d.streamReadLoop(as, nil)
		close(done)
	}()
	feeder := ipc.NewCodec(nil, writer)
	if err := feeder.Write(ipc.Frame{Kind: ipc.Kind("bogus.kind")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("unknown kind did not stop exact stream")
	}
	select {
	case <-stream.closed:
	default:
		t.Fatal("unknown kind did not close exact stream")
	}
}
