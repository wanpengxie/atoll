package link

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/ipc"
)

func TestServerActorEndpointDirectWriteIsBudgeted(t *testing.T) {
	local, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	conn := newActorStreamConn(local, nil)
	const budget = 40 * time.Millisecond
	conn.budget = budget
	endpoint := newServerActorEndpoint(
		context.Background(), "actor-a", "attempt-a", conn, nil, serverActorHandlers{},
	)
	t.Cleanup(func() { _ = endpoint.Close() })

	started := time.Now()
	err := endpoint.Deliver(&message.Envelope{ID: "message-a"})
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("direct Deliver to a non-reading peer reported success")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("Deliver error = %v, want transport write timeout", err)
	}
	if elapsed < budget*8/10 {
		t.Fatalf("Deliver returned after %v, before its %v write budget", elapsed, budget)
	}
	if elapsed > time.Second {
		t.Fatalf("Deliver exceeded its local write budget: %v", elapsed)
	}

	// The timed-out direct write closes this actor stream. A second send must
	// fail immediately rather than enter an application queue.
	started = time.Now()
	if err := endpoint.Deliver(&message.Envelope{ID: "message-b"}); err == nil {
		t.Fatal("Deliver on the closed actor stream reported success")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("second Deliver waited behind stale writer state for %v", elapsed)
	}
}

func TestCancelForwardOnWedgedStreamIsBoundedAndLocal(t *testing.T) {
	wedgedLocal, wedgedPeer := net.Pipe()
	t.Cleanup(func() { _ = wedgedPeer.Close() })
	wedged := newActorStreamConn(wedgedLocal, nil)
	const budget = 40 * time.Millisecond
	wedged.budget = budget
	forward := NewRemoteWriter(ipc.NewCodec(wedged, wedged))

	started := time.Now()
	err := forward.sendCancel("request-wedged")
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("cancel forward to a non-reading peer reported success")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("cancel forward error = %v, want transport write timeout", err)
	}
	if elapsed < budget*8/10 || elapsed > time.Second {
		t.Fatalf("cancel forward elapsed = %v, want local %v budget", elapsed, budget)
	}

	healthyLocal, healthyPeer := net.Pipe()
	t.Cleanup(func() {
		_ = healthyLocal.Close()
		_ = healthyPeer.Close()
	})
	healthy := newActorStreamConn(healthyLocal, nil)
	received := make(chan ipc.Frame, 1)
	go func() {
		frame, _ := ipc.NewCodec(healthyPeer, healthyPeer).Read()
		received <- frame
	}()
	if err := NewRemoteWriter(ipc.NewCodec(healthy, healthy)).sendCancel("request-healthy"); err != nil {
		t.Fatalf("healthy sibling cancel failed after wedged stream closed: %v", err)
	}
	select {
	case frame := <-received:
		if frame.Kind != ipc.KindCancelRequest {
			t.Fatalf("healthy sibling frame kind = %q, want %q", frame.Kind, ipc.KindCancelRequest)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy sibling did not receive cancel")
	}

	started = time.Now()
	if err := forward.sendCancel("request-wedged-again"); err == nil {
		t.Fatal("second cancel on closed stream reported success")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("closed actor stream retained blocked write state for %v", elapsed)
	}
}

func TestServeStreamsDoesNotLetHalfOpenHeaderBlockSibling(t *testing.T) {
	serverTransport, clientTransport := net.Pipe()
	serverSession, err := yamux.Server(serverTransport, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	clientSession, err := yamux.Client(clientTransport, linkYamuxConfig())
	if err != nil {
		_ = serverSession.Close()
		t.Fatal(err)
	}
	carrier := &ServerCarrier{newRawCarrier(serverSession, nil, nil)}
	releaseHandshake := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseHandshake:
		default:
			close(releaseHandshake)
		}
		_ = carrier.Close()
		_ = clientSession.Close()
	})

	handled := make(chan DeviceStreamHeader, 1)
	handshakeEntered := make(chan struct{})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- carrier.ServeStreams(func(conn net.Conn, header DeviceStreamHeader) {
			defer conn.Close()
			if _, ok := conn.(*actorStreamConn); !ok {
				t.Errorf("accepted actor stream is %T, want write-budget owner", conn)
			}
			if header.Channel == "channel-a" {
				close(handshakeEntered)
				<-releaseHandshake // models a valid header whose IPC handshake stalls
				return
			}
			handled <- header
		})
	}()

	halfOpen, err := clientSession.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = halfOpen.Close() })
	// halfOpen deliberately sends no header.

	stalledHandshake, err := clientSession.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stalledHandshake.Close() })
	if err := writeStreamJSON(stalledHandshake, DeviceStreamHeader{
		Kind: DeviceStreamActor, Channel: "channel-a", LaneGen: "generation-a",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handshakeEntered:
	case <-time.After(time.Second):
		t.Fatal("stream with a complete header never entered its handler")
	}

	healthy, err := clientSession.Open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = healthy.Close() })
	if err := writeStreamJSON(healthy, DeviceStreamHeader{
		Kind: DeviceStreamActor, Channel: "channel-b", LaneGen: "generation-b",
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case header := <-handled:
		if header.Channel != "channel-b" || header.LaneGen != "generation-b" {
			t.Fatalf("handled sibling header = %+v", header)
		}
	case <-time.After(time.Second):
		t.Fatal("half-open stream header blocked admission of its sibling")
	}

	close(releaseHandshake)
	_ = carrier.Close()
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("stream workers did not join after carrier close")
	}
}

func TestOpenActorReturnsWriteBudgetOwner(t *testing.T) {
	clientTransport, serverTransport := net.Pipe()
	clientSession, err := yamux.Client(clientTransport, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	serverSession, err := yamux.Server(serverTransport, linkYamuxConfig())
	if err != nil {
		_ = clientSession.Close()
		t.Fatal(err)
	}
	carrier := &ClientCarrier{newRawCarrier(clientSession, nil, nil)}
	t.Cleanup(func() {
		_ = carrier.Close()
		_ = serverSession.Close()
	})

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := serverSession.Accept()
		accepted <- conn
	}()
	conn, err := carrier.OpenActor(context.Background(), "channel-a", "generation-a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	owner, ok := conn.(*actorStreamConn)
	if !ok {
		t.Fatalf("OpenActor returned %T, want actorStreamConn", conn)
	}
	if owner.budget != 0 {
		t.Fatalf("production actor budget override = %v, want shared default", owner.budget)
	}
	if streamWriteBudget != 10*time.Second {
		t.Fatalf("streamWriteBudget = %v, want 10s", streamWriteBudget)
	}
	peer := <-accepted
	if peer != nil {
		_ = peer.Close()
	}
}
