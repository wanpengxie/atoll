package dataplane

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
)

func TestTicketRouteAndRepeatedRedemption(t *testing.T) {
	issue, redeem, bind, closePlane := New()
	t.Cleanup(func() { _ = closePlane(context.Background()) })
	if err := bind.BindHostStreamOpener(testOpener{online: true}); err != nil {
		t.Fatal(err)
	}
	grant, err := issue.Issue(t.Context(), IssueSpec{
		ResourceID: "daemon://host/x", ChannelID: "c", Mode: access.OpRead,
		HostID: "d", HostName: "host", CallerHostID: "other", Coord: "coord",
	})
	if err != nil || grant.Route != RouteRemote {
		t.Fatalf("grant=%+v err=%v", grant, err)
	}
	for range 2 {
		if _, err := redeem.Resolve("c", grant.Ticket); err != nil {
			t.Fatalf("repeated redemption: %v", err)
		}
	}
	if _, err := redeem.Resolve("other-channel", grant.Ticket); !errors.Is(err, ErrInvalidTicket) {
		t.Fatalf("cross-channel redemption error=%v", err)
	}
	p := issue.(issuer).p
	p.mu.Lock()
	p.tickets[grant.Ticket] = func(ticket Ticket) Ticket { ticket.Expires = p.now(); return ticket }(p.tickets[grant.Ticket])
	p.mu.Unlock()
	if _, err := redeem.Resolve("c", grant.Ticket); !errors.Is(err, ErrInvalidTicket) {
		t.Fatalf("expired redemption error=%v", err)
	}
}

func TestIssuerRejectsRemoteDirectories(t *testing.T) {
	issue, _, bind, closePlane := New()
	defer closePlane(context.Background())
	if err := bind.BindHostStreamOpener(testOpener{online: true}); err != nil {
		t.Fatal(err)
	}
	_, err := issue.Issue(t.Context(), IssueSpec{ResourceID: "daemon://host/tree", ChannelID: "c", Mode: access.OpRead, HostID: "host-id", HostName: "host", CallerHostID: "other", Coord: "coord", Dir: true})
	if !errors.Is(err, ErrRemoteDirectory) {
		t.Fatalf("remote directory error=%v", err)
	}
}

func TestHTTPRedemptionRejectsAddressAndModeMismatch(t *testing.T) {
	issue, redeem, bind, closePlane := New()
	defer closePlane(context.Background())
	if err := bind.BindHostStreamOpener(testOpener{online: true}); err != nil {
		t.Fatal(err)
	}
	grant, err := issue.Issue(t.Context(), IssueSpec{ResourceID: "daemon://host/x", ChannelID: "c", Mode: access.OpRead, HostID: "host-id", HostName: "host", Coord: "coord"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		address string
		mode    access.Operation
	}{
		{address: "daemon://host/other", mode: access.OpRead},
		{address: "daemon://host/x", mode: access.OpWrite},
	} {
		err := redeem.ServeHTTP(t.Context(), resource.ResourceID(tc.address), grant.Ticket, tc.mode, io.Discard, bytes.NewReader(nil))
		if !errors.Is(err, ErrInvalidTicket) {
			t.Fatalf("address=%q mode=%q error=%v", tc.address, tc.mode, err)
		}
	}
}

type testOpener struct{ online bool }

func (o testOpener) Online(string, channel.ID) bool { return o.online }
func (testOpener) OpenHost(context.Context, Ticket) (io.ReadWriteCloser, error) {
	return nil, errors.New("unused")
}
func (testOpener) Complete(context.Context, Ticket) error { return nil }

type exchangeOpener struct {
	complete int
	serve    func(io.ReadWriteCloser)
}

func (*exchangeOpener) Online(string, channel.ID) bool { return true }
func (o *exchangeOpener) OpenHost(_ context.Context, ticket Ticket) (io.ReadWriteCloser, error) {
	client, host := net.Pipe()
	go o.serve(host)
	if err := link.WriteExchangeControl(client, link.ExchangeHostHeader{Coord: "coord", Mode: ticket.Mode, ReservationID: ticket.ReservationID}); err != nil {
		return nil, err
	}
	return client, nil
}
func (o *exchangeOpener) Complete(context.Context, Ticket) error {
	o.complete++
	return nil
}

func TestRemoteReadAndWriteStateMachines(t *testing.T) {
	t.Run("read", func(t *testing.T) {
		want := bytes.Repeat([]byte("cut-through"), 4096)
		opener := &exchangeOpener{serve: func(conn io.ReadWriteCloser) {
			defer conn.Close()
			var head link.ExchangeHostHeader
			_ = link.ReadExchangeControl(conn, &head)
			_ = link.WriteExchangeBytes(conn, bytes.NewReader(want))
			_ = link.WriteExchangeControl(conn, link.ExchangeStatus{OK: true})
		}}
		issue, redeem, bind, closePlane := New()
		if err := bind.BindHostStreamOpener(opener); err != nil {
			t.Fatal(err)
		}
		defer closePlane(context.Background())
		grant, err := issue.Issue(t.Context(), IssueSpec{ResourceID: "daemon://host/x", ChannelID: "c", Mode: access.OpRead, HostID: "host-id", HostName: "host", CallerHostID: "other", Coord: "coord"})
		if err != nil {
			t.Fatal(err)
		}
		caller, server := net.Pipe()
		go redeem.ServeExchange(t.Context(), "c", server)
		if err := link.WriteExchangeControl(caller, link.ExchangeTicketHeader{Ticket: grant.Ticket}); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(link.NewExchangeReader(caller))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("read len=%d err=%v", len(got), err)
		}
	})

	t.Run("write and complete", func(t *testing.T) {
		want := bytes.Repeat([]byte("write"), 8192)
		landed := make(chan []byte, 1)
		opener := &exchangeOpener{serve: func(conn io.ReadWriteCloser) {
			defer conn.Close()
			var head link.ExchangeHostHeader
			_ = link.ReadExchangeControl(conn, &head)
			var got bytes.Buffer
			if err := link.ReadExchangeBytes(&got, conn); err == nil {
				landed <- got.Bytes()
				_ = link.WriteExchangeControl(conn, link.ExchangeStatus{OK: true})
			}
		}}
		issue, redeem, bind, closePlane := New()
		if err := bind.BindHostStreamOpener(opener); err != nil {
			t.Fatal(err)
		}
		defer closePlane(context.Background())
		grant, err := issue.Issue(t.Context(), IssueSpec{ResourceID: "daemon://host/x", ChannelID: "c", Mode: access.OpWrite, HostID: "host-id", HostName: "host", CallerHostID: "other", Coord: "coord", ReservationID: "reservation"})
		if err != nil {
			t.Fatal(err)
		}
		caller, server := net.Pipe()
		go redeem.ServeExchange(t.Context(), "c", server)
		if err := link.WriteExchangeControl(caller, link.ExchangeTicketHeader{Ticket: grant.Ticket}); err != nil {
			t.Fatal(err)
		}
		handle := link.NewExchangeWriteHandle(caller)
		if _, err := handle.Write(want); err != nil {
			t.Fatal(err)
		}
		if err := handle.Commit(); err != nil {
			t.Fatal(err)
		}
		if got := <-landed; !bytes.Equal(got, want) || opener.complete != 1 {
			t.Fatalf("landed=%d complete=%d", len(got), opener.complete)
		}
	})
}

func TestRemoteWriteForwardsEarlyHostFailureAndStopsUpload(t *testing.T) {
	opener := &exchangeOpener{serve: func(conn io.ReadWriteCloser) {
		defer conn.Close()
		var head link.ExchangeHostHeader
		_ = link.ReadExchangeControl(conn, &head)
		_ = link.WriteExchangeControl(conn, link.ExchangeStatus{OK: false, Code: "write_rejected", Detail: "staging unavailable"})
	}}
	issue, redeem, bind, closePlane := New()
	if err := bind.BindHostStreamOpener(opener); err != nil {
		t.Fatal(err)
	}
	defer closePlane(context.Background())
	grant, err := issue.Issue(t.Context(), IssueSpec{
		ResourceID: "daemon://host/x", ChannelID: "c", Mode: access.OpWrite,
		HostID: "host-id", HostName: "host", CallerHostID: "other", Coord: "coord",
	})
	if err != nil {
		t.Fatal(err)
	}
	caller, server := net.Pipe()
	serveDone := make(chan struct{})
	go func() {
		redeem.ServeExchange(t.Context(), "c", server)
		close(serveDone)
	}()
	if err := link.WriteExchangeControl(caller, link.ExchangeTicketHeader{Ticket: grant.Ticket}); err != nil {
		t.Fatal(err)
	}
	uploadDone := make(chan error, 1)
	payload := make([]byte, link.MaxExchangeChunk)
	go func() {
		for range 100 {
			if err := link.WriteExchangeChunk(caller, payload); err != nil {
				uploadDone <- err
				return
			}
		}
		uploadDone <- nil
	}()
	var status link.ExchangeStatus
	if err := link.ReadExchangeControl(caller, &status); err != nil {
		t.Fatal(err)
	}
	if status.OK || status.Code != "write_rejected" {
		t.Fatalf("early status = %+v", status)
	}
	select {
	case err := <-uploadDone:
		if err == nil {
			t.Fatal("upload reached its source end after the host had rejected it")
		}
	case <-time.After(time.Second):
		t.Fatal("upload did not stop after the early host terminal")
	}
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("exchange pump did not join after early failure")
	}
}

type endlessUpload struct{ closed chan struct{} }

func (u *endlessUpload) Read(p []byte) (int, error) {
	select {
	case <-u.closed:
		return 0, io.EOF
	default:
		clear(p)
		return len(p), nil
	}
}

func (u *endlessUpload) Close() error {
	select {
	case <-u.closed:
	default:
		close(u.closed)
	}
	return nil
}

func TestHTTPWriteStopsRequestBodyOnEarlyHostFailure(t *testing.T) {
	opener := &exchangeOpener{serve: func(conn io.ReadWriteCloser) {
		defer conn.Close()
		var head link.ExchangeHostHeader
		_ = link.ReadExchangeControl(conn, &head)
		_ = link.WriteExchangeControl(conn, link.ExchangeStatus{OK: false, Code: "write_rejected", Detail: "staging unavailable"})
	}}
	issue, redeem, bind, closePlane := New()
	if err := bind.BindHostStreamOpener(opener); err != nil {
		t.Fatal(err)
	}
	defer closePlane(context.Background())
	address := resource.ResourceID("daemon://host/x")
	grant, err := issue.Issue(t.Context(), IssueSpec{
		ResourceID: address, ChannelID: "c", Mode: access.OpWrite,
		HostID: "host-id", HostName: "host", CallerHostID: "browser", Coord: "coord",
	})
	if err != nil {
		t.Fatal(err)
	}
	upload := &endlessUpload{closed: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- redeem.ServeHTTP(t.Context(), address, grant.Ticket, access.OpWrite, io.Discard, upload)
	}()
	select {
	case err := <-done:
		var terminal *link.ExchangeTerminalError
		if !errors.As(err, &terminal) || terminal.Code != "write_rejected" {
			t.Fatalf("error = %v, want write_rejected terminal", err)
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP upload did not stop after the early host terminal")
	}
	select {
	case <-upload.closed:
	default:
		t.Fatal("early host failure did not close the HTTP request body")
	}
}
