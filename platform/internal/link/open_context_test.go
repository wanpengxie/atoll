package link

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

func openContextSessionPair(t *testing.T) (*linkSession, *yamux.Session) {
	t.Helper()
	a, b := net.Pipe()
	client, err := yamux.Client(a, linkYamuxConfig())
	if err != nil {
		t.Fatalf("yamux client: %v", err)
	}
	server, err := yamux.Server(b, linkYamuxConfig())
	if err != nil {
		t.Fatalf("yamux server: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return &linkSession{ys: client}, server
}

func TestOpenAdmission_CanceledWaiterDoesNotIssueOrKill(t *testing.T) {
	ls, server := openContextSessionPair(t)
	ls.openGateOnce.Do(func() { ls.openGate = make(chan struct{}, 1) })
	ls.openGate <- struct{}{} // another attempt owns admission

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := ls.openLane(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued open error = %v, want context deadline", err)
	}
	select {
	case <-ls.closed():
		t.Fatal("cancellation before admission killed the link")
	default:
	}
	select {
	case <-server.CloseChan():
		t.Fatal("canceled waiter issued an open or killed the peer session")
	default:
	}
	<-ls.openGate
}

func TestOpenStream_CancelAfterOpenIssuedKillsLink(t *testing.T) {
	ls, server := openContextSessionPair(t)
	handshakeSeen := make(chan ipc.HandshakePayload, 1)
	go func() {
		conn, err := server.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var hdr streamHeader
		if err := readLaneJSON(conn, &hdr); err != nil || hdr.Kind != streamActor {
			return
		}
		codec := ipc.NewCodec(conn, io.Discard)
		frame, err := codec.Read()
		if err != nil || frame.Kind != ipc.KindHandshake {
			return
		}
		var hp ipc.HandshakePayload
		if json.Unmarshal(frame.Payload, &hp) == nil {
			handshakeSeen <- hp
		}
		// Deliberately never send handshake_ack. The caller's context must tear
		// down the whole link, which unblocks this stream.
		<-ls.closed()
	}()

	d := &Dialer{lc: ls, streams: map[actor.ActorID]*actorStream{}}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err := d.OpenStream(ctx, actor.ActorID("tool-a"), 17, func(*message.Envelope) error { return nil }, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("OpenStream error = %v, want context deadline", err)
	}
	select {
	case hp := <-handshakeSeen:
		if hp.LeaseID != "tool-a" || hp.Epoch != 17 {
			t.Fatalf("handshake = %+v, want lease tool-a epoch 17", hp)
		}
	case <-time.After(time.Second):
		t.Fatal("peer did not receive handshake before cancellation")
	}
	select {
	case <-ls.closed():
	case <-time.After(time.Second):
		t.Fatal("cancellation after Open was issued did not kill the link")
	}
}
