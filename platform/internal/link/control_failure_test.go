package link

import (
	"net"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

func TestControlWorkerPanicKillsSessionWithoutEscaping(t *testing.T) {
	carrierA, carrierB := net.Pipe()
	client, err := yamux.Client(carrierA, linkYamuxConfig())
	if err != nil {
		t.Fatalf("yamux client: %v", err)
	}
	server, err := yamux.Server(carrierB, linkYamuxConfig())
	if err != nil {
		t.Fatalf("yamux server: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ls := &linkSession{ys: client, onControl: func([]byte) { panic("handler exploded") }}
	reader, writer := net.Pipe()
	done := make(chan struct{})
	go func() {
		ls.readControl(reader)
		close(done)
	}()
	if _, err := writer.Write([]byte("{}\n")); err != nil {
		t.Fatalf("write control: %v", err)
	}
	select {
	case <-client.CloseChan():
	case <-time.After(2 * time.Second):
		t.Fatal("control worker panic did not kill the session")
	}
	_ = writer.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("control reader did not exit after session death")
	}
}
