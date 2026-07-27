package compute

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
)

type sessionTestIngress struct{}

func (sessionTestIngress) Emit(context.Context, actor.ActorID, actorhost.AttemptKey, *message.Envelope) (harness.WriteResult, error) {
	return harness.WriteResult{}, nil
}

func (sessionTestIngress) Access(context.Context, actor.ActorID, actorhost.AttemptKey, remoteingress.AccessRequest) (remoteingress.AccessResponse, error) {
	return remoteingress.AccessResponse{}, nil
}

func (sessionTestIngress) Schedule(context.Context, actor.ActorID, remoteingress.ScheduleRequest) (remoteingress.ScheduleResponse, error) {
	return remoteingress.ScheduleResponse{}, nil
}

func (sessionTestIngress) Fork(context.Context, actor.ActorID, actorhost.AttemptKey, remoteingress.ForkRequest) (actor.ActorID, error) {
	return "", nil
}

func (sessionTestIngress) EndSelf(context.Context, actor.ActorID, actorhost.AttemptKey, actorcaps.EndSelfRequest) error {
	return nil
}

func newSessionTestServer(t *testing.T) (*link.Acceptor, *httptest.Server) {
	t.Helper()
	acceptor, err := link.NewAcceptor(link.Config{
		Ingress:   sessionTestIngress{},
		ChannelID: channel.ID("channel:test"),
		AuthorizeAttach: func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain) error {
			return nil
		},
		AttachBinding: func(actor.ActorID, actorhost.AttemptKey, actorhost.ExecutionDomain, actorhost.Binding) error {
			return nil
		},
		BindingDown: func(actor.ActorID, actorhost.Binding) {},
		Plan: func(context.Context, string) ([]platform.PlanActor, error) {
			return nil, nil
		},
		CanAttach: func(context.Context, string) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acceptor.Serve(w, r, "daemon:test")
	}))
	t.Cleanup(func() {
		_ = acceptor.Close()
		server.Close()
	})
	return acceptor, server
}

func TestDaemonOutboundAcceptsOnlyLiveHomeMintedSessionAuthority(t *testing.T) {
	acceptor, server := newSessionTestServer(t)
	dialer, err := link.Dial(context.Background(), "ws"+server.URL[4:], link.DialConfig{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !acceptor.IsAttached("daemon:test") {
		t.Fatal("accepted reply became visible before the home committed current")
	}
	snapshots := acceptor.Sessions()
	if len(snapshots) != 1 || snapshots[0].State != link.SessionActive {
		t.Fatalf("home session ledger after attach=%+v", snapshots)
	}
	session, err := link.NewAuthenticatedLinkSession(link.AuthenticatedLinkSessionConfig{
		Peer: "server", Authority: dialer.Authority(),
		CloseTransport: dialer.Close, TransportDone: dialer.Done(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if session.Generation() != snapshots[0].Generation {
		t.Fatalf("daemon generation=%s home=%s", session.Generation(), snapshots[0].Generation)
	}
	outbound := NewDaemonOutbound(DaemonOutboundConfig{})
	if err := outbound.SetSession(session); err != nil {
		t.Fatalf("live home-minted session rejected: %v", err)
	}

	if err := dialer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("sealed daemon session did not collect")
	}
	if err := outbound.SetSession(session); !errors.Is(err, ErrOutboundDisconnected) {
		t.Fatalf("sealed authority SetSession error=%v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for acceptor.IsAttached("daemon:test") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if acceptor.IsAttached("daemon:test") {
		t.Fatal("home current pointer survived carrier loss")
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := outbound.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}

func TestCurrentLossNeverFallsBackToOlderActiveSession(t *testing.T) {
	acceptor, server := newSessionTestServer(t)
	ledger := link.NewRemoteSessionLedger(nil)
	first, err := link.Dial(context.Background(), "ws"+server.URL[4:], link.DialConfig{SessionLedger: ledger}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	firstSession, err := link.NewAuthenticatedLinkSession(link.AuthenticatedLinkSessionConfig{
		Peer: "server", Authority: first.Authority(),
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := link.Dial(context.Background(), "ws"+server.URL[4:], link.DialConfig{SessionLedger: ledger}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	secondSession, err := link.NewAuthenticatedLinkSession(link.AuthenticatedLinkSessionConfig{
		Peer: "server", Authority: second.Authority(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if firstSession.IsCurrent() || !secondSession.IsCurrent() {
		t.Fatal("shared daemon ledger did not displace the predecessor")
	}

	snapshots := acceptor.Sessions()
	if len(snapshots) != 2 {
		t.Fatalf("sessions=%d want 2", len(snapshots))
	}
	var newest link.SessionSnapshot
	for _, snapshot := range snapshots {
		if snapshot.AttachedAt.After(newest.AttachedAt) {
			newest = snapshot
		}
	}
	if !acceptor.KickSession(newest.Generation) {
		t.Fatal("exact successor kick was rejected")
	}
	deadline := time.Now().Add(2 * time.Second)
	for (acceptor.IsAttached("daemon:test") || secondSession.IsCurrent()) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if acceptor.IsAttached("daemon:test") {
		t.Fatal("current fell back to the older active carrier")
	}
	if firstSession.IsCurrent() || secondSession.IsCurrent() {
		t.Fatal("daemon ledger fell back after current loss")
	}
	select {
	case <-first.Done():
		t.Fatal("successor kick collected the displaced carrier")
	default:
	}
}
