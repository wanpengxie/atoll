package link

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/runtime/ipc"
)

// These tests pin the relayCore SLOT semantics directly — which of the two
// error returns (transportErr = unconfirmed vs definiteErr = provably not
// executed) a settlement lands in. The adapter-level tests can only see the
// single collapsed error a Pen returns, so they cannot catch a regression
// that keeps the sentinel identity but moves it into the wrong box (codex
// B2 终审 P2: dropping coreResult.transport would stay green under an
// identity-only assertion).

// TestRelayCoreCloseBoxesInFlightAsTransport: a close with a round-trip
// genuinely in flight (host has provably READ the frame) settles it in the
// transportErr slot carrying the dialect's closed sentinel — an unconfirmed
// outcome, never a definite verdict.
func TestRelayCoreCloseBoxesInFlightAsTransport(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("test: core closed")
	hostConn, remoteConn := net.Pipe()
	defer func() { _ = hostConn.Close(); _ = remoteConn.Close() }()
	hostGotFrame := make(chan struct{}, 1)
	go func() {
		c := ipc.NewCodec(hostConn, hostConn)
		for {
			if _, err := c.Read(); err != nil {
				return
			}
			select {
			case hostGotFrame <- struct{}{}:
			default:
			}
			// read the frame, never ack
		}
	}()
	core := newRelayCore[struct{}](ipc.NewCodec(remoteConn, remoteConn), ipc.KindEmit, sentinel)

	type settled struct{ transportErr, definiteErr error }
	done := make(chan settled, 1)
	go func() {
		_, transportErr, definiteErr := core.roundTrip(context.Background(), []byte(`{}`))
		done <- settled{transportErr, definiteErr}
	}()
	select {
	case <-hostGotFrame:
	case <-time.After(2 * time.Second):
		t.Fatal("host never received the in-flight frame")
	}
	core.close()
	select {
	case s := <-done:
		if !errors.Is(s.transportErr, sentinel) {
			t.Fatalf("transportErr = %v, want the closed sentinel (unconfirmed box)", s.transportErr)
		}
		if s.definiteErr != nil {
			t.Fatalf("definiteErr = %v, want nil (a teardown settlement is never a definite verdict)", s.definiteErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight roundTrip never unblocked after close")
	}
}

// TestRelayCorePreSendCancelIsDefinite: a ctx cancelled BEFORE the send is a
// provable non-execution — it settles in the definiteErr slot and the
// transportErr slot stays empty (the exact opposite boxing of the in-flight
// teardown above).
func TestRelayCorePreSendCancelIsDefinite(t *testing.T) {
	t.Parallel()
	hostConn, remoteConn := net.Pipe()
	defer func() { _ = hostConn.Close(); _ = remoteConn.Close() }()
	core := newRelayCore[struct{}](ipc.NewCodec(remoteConn, remoteConn), ipc.KindEmit, errors.New("test: core closed"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, transportErr, definiteErr := core.roundTrip(ctx, []byte(`{}`))
	if definiteErr != context.Canceled {
		t.Fatalf("definiteErr = %v, want context.Canceled (pre-send cancel is a definite non-execution)", definiteErr)
	}
	if transportErr != nil {
		t.Fatalf("transportErr = %v, want nil (nothing was ever in flight)", transportErr)
	}
}
