package engineboot

import (
	"context"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Serve returning is the lifecycle boundary: the listener and every component
// assembled behind it have stopped by the time the call returns.
func TestServeLifecycleInvariant(t *testing.T) {
	eng, err := Boot(Config{
		ChannelDBDir: filepath.Join(t.TempDir(), "channels"),
		Addr:         "127.0.0.1:0",
		RootPassword: "test-root-password",
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("boot: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- eng.Serve(ctx) }()

	select {
	case <-eng.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("Ready never closed")
	}
	addr := eng.BoundAddr()
	if addr == "" || strings.HasSuffix(addr, ":0") {
		t.Fatalf("BoundAddr must be resolved, got %q", addr)
	}
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("engine not dialable at %s: %v", addr, err)
	}
	_ = conn.Close()

	cancel()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("clean shutdown: %v", err)
		}
	case <-time.After(35 * time.Second):
		t.Fatal("Serve did not return after cancellation")
	}
	if _, err := net.DialTimeout("tcp", addr, 500*time.Millisecond); err == nil {
		t.Fatalf("port %s still accepts after Serve returned", addr)
	}
}
