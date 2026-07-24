package link

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
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
