package dataplane

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type testOpener struct {
	online  bool
	openErr error
	serve   func(io.ReadWriteCloser)
}

func (o *testOpener) Online(string, channel.ID) bool { return o.online }

func (o *testOpener) OpenHost(_ context.Context, ticket Ticket) (io.ReadWriteCloser, error) {
	if o.openErr != nil {
		return nil, o.openErr
	}
	client, host := net.Pipe()
	go o.serve(host)
	if err := link.WriteExchangeControl(client, link.ExchangeHostHeader{
		Path: "docs/a.txt", Mode: ticket.Mode,
	}); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func openTestPlane(t *testing.T, opener *testOpener) (Issuer, Redeemer) {
	t.Helper()
	issue, redeem, bind, closePlane := New()
	if err := bind.BindHostStreamOpener(opener); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		bind.UnbindHostStreamOpener()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := closePlane(ctx); err != nil {
			t.Errorf("close plane: %v", err)
		}
	})
	return issue, redeem
}

const testCaller = actor.ActorID("human:alice:7")

func issueTestTicket(t *testing.T, issue Issuer, mode access.Operation) Grant {
	t.Helper()
	grant, err := issue.Issue(t.Context(), IssueSpec{
		Address: "daemon://host/c0.test/docs/a.txt", Path: "docs/a.txt",
		ChannelID: "channel-a", Mode: mode,
		HostID:    "daemon-a", HostName: "host",
		Caller:    testCaller,
	})
	if err != nil || grant.Ticket == "" {
		t.Fatalf("Issue = (%+v, %v)", grant, err)
	}
	return grant
}

func TestTicketLedgerKeepsOnlyRemoteByteFacts(t *testing.T) {
	issue, redeem := openTestPlane(t, &testOpener{online: true})
	grant := issueTestTicket(t, issue, access.OpRead)
	ticket, err := redeem.Resolve("channel-a", testCaller, grant.Ticket)
	if err != nil {
		t.Fatal(err)
	}
	if ticket.Address != "daemon://host/c0.test/docs/a.txt" || ticket.HostID != "daemon-a" || ticket.Mode != access.OpRead {
		t.Fatalf("ticket=%+v", ticket)
	}
	// Path is where those bytes live inside the channel's directory on that
	// machine — a fact about the remote bytes, like the host and the mode.
	if ticket.Path != "docs/a.txt" {
		t.Fatalf("ticket path=%q", ticket.Path)
	}
	if ticket.Caller != testCaller {
		t.Fatalf("ticket subject=%q", ticket.Caller)
	}
	// The enumeration is the claim: this table holds a route and the scope it
	// belongs to (ChannelID + Caller), and is not a place other facts about a
	// person or a channel accumulate.
	want := []string{"ChannelID", "Address", "Path", "Mode", "HostID", "Caller", "Expires"}
	typ := reflect.TypeOf(Ticket{})
	got := make([]string, typ.NumField())
	for i := range got {
		got[i] = typ.Field(i).Name
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ticket fields=%v want=%v", got, want)
	}
}

// A ticket's scope is (channel, actor) and both halves are checked on the way
// out. Neither is optional and neither is inferred from the ticket itself —
// reading the scope off the ticket and then comparing it to itself would agree
// every time.
func TestTicketRejectsExpiryAndUseOutsideItsScope(t *testing.T) {
	issue, redeem := openTestPlane(t, &testOpener{online: true})

	scoped := issueTestTicket(t, issue, access.OpRead)
	for _, tc := range []struct {
		name   string
		ch     channel.ID
		caller actor.ActorID
	}{
		{name: "another channel", ch: "channel-b", caller: testCaller},
		{name: "another actor", ch: "channel-a", caller: "human:mallory:3"},
		{name: "no actor", ch: "channel-a", caller: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := redeem.Resolve(tc.ch, tc.caller, scoped.Ticket); !errors.Is(err, ErrInvalidTicket) {
				t.Fatalf("Resolve error = %v, want ErrInvalidTicket", err)
			}
		})
	}

	expired := issueTestTicket(t, issue, access.OpRead)
	p := issue.(issuer).p
	p.mu.Lock()
	ticket := p.tickets[expired.Ticket]
	ticket.Expires = p.now()
	p.tickets[expired.Ticket] = ticket
	p.mu.Unlock()
	if _, err := redeem.Resolve("channel-a", testCaller, expired.Ticket); !errors.Is(err, ErrInvalidTicket) {
		t.Fatalf("expired Resolve error = %v, want ErrInvalidTicket", err)
	}
}

// An access without an actor as its subject is not a weaker grant, it is not a
// grant — so the table refuses to hold one at all.
func TestATicketWithoutAnActorIsNeverMinted(t *testing.T) {
	issue, _ := openTestPlane(t, &testOpener{online: true})
	if _, err := issue.Issue(t.Context(), IssueSpec{
		Address: "daemon://host/c0.test/docs/a.txt", Path: "docs/a.txt",
		ChannelID: "channel-a", Mode: access.OpRead,
		HostID: "daemon-a", HostName: "host",
	}); err == nil {
		t.Fatal("a ticket with no actor was minted")
	}
}

// The door granted one actor one direction on one file. Bytes arrive later, on
// a connection the door never saw, so this is where the grant is matched to
// what showed up: another actor, no actor, another channel, or the other
// direction are each a different operation than the one that was decided.
func TestRedemptionAnswersOnlyWithinItsScopeAndDirection(t *testing.T) {
	issue, redeem := openTestPlane(t, &testOpener{online: true})
	grant := issueTestTicket(t, issue, access.OpRead)
	tests := []struct {
		name   string
		ch     channel.ID
		caller actor.ActorID
		mode   access.Operation
	}{
		{name: "another actor", ch: "channel-a", caller: "human:mallory:3", mode: access.OpRead},
		{name: "no actor", ch: "channel-a", caller: "", mode: access.OpRead},
		{name: "another channel", ch: "channel-b", caller: testCaller, mode: access.OpRead},
		{name: "the other direction", ch: "channel-a", caller: testCaller, mode: access.OpWrite},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := redeem.ServeHTTP(t.Context(), test.ch, test.caller, grant.Ticket, test.mode, io.Discard, bytes.NewReader(nil))
			if !errors.Is(err, ErrInvalidTicket) {
				t.Fatalf("ServeHTTP error = %v, want ErrInvalidTicket", err)
			}
			if _, err := redeem.OpenTransfer(t.Context(), test.ch, test.caller, grant.Ticket, test.mode); !errors.Is(err, ErrInvalidTicket) {
				t.Fatalf("OpenTransfer error = %v, want ErrInvalidTicket", err)
			}
		})
	}
}

func TestHostOpenFailureIsReturnedToHTTPAndExchangeCallers(t *testing.T) {
	wantErr := errors.New("open file: disk unavailable")
	issue, redeem := openTestPlane(t, &testOpener{online: true, openErr: wantErr})

	t.Run("HTTP", func(t *testing.T) {
		grant := issueTestTicket(t, issue, access.OpRead)
		err := redeem.ServeHTTP(t.Context(), "channel-a", testCaller, grant.Ticket, access.OpRead, io.Discard, nil)
		if !errors.Is(err, wantErr) {
			t.Fatalf("ServeHTTP error = %v, want host open error", err)
		}
	})

	t.Run("exchange", func(t *testing.T) {
		grant := issueTestTicket(t, issue, access.OpRead)
		caller, server := net.Pipe()
		serveDone := make(chan struct{})
		go func() {
			redeem.ServeExchange(t.Context(), "channel-a", server)
			close(serveDone)
		}()
		if err := link.WriteExchangeControl(caller, link.ExchangeTicketHeader{Ticket: grant.Ticket, Caller: string(testCaller)}); err != nil {
			t.Fatal(err)
		}
		var status link.ExchangeStatus
		if err := link.ReadExchangeControl(caller, &status); err != nil {
			t.Fatal(err)
		}
		if status.OK || status.Code != "unavailable" || status.Detail != wantErr.Error() {
			t.Fatalf("status = %+v", status)
		}
		select {
		case <-serveDone:
		case <-time.After(time.Second):
			t.Fatal("exchange did not join after host open failure")
		}
	})
}

func TestExchangeReadDistinguishesSuccessfulEOFAndTruncation(t *testing.T) {
	tests := []struct {
		name        string
		serve       func(io.ReadWriteCloser)
		want        string
		wantErrCode string
	}{
		{
			name: "successful terminal becomes ordinary EOF",
			serve: func(conn io.ReadWriteCloser) {
				defer conn.Close()
				var head link.ExchangeHostHeader
				_ = link.ReadExchangeControl(conn, &head)
				_ = link.WriteExchangeBytes(conn, bytes.NewBufferString("complete"))
				_ = link.WriteExchangeControl(conn, link.ExchangeStatus{OK: true})
			},
			want: "complete",
		},
		{
			name: "transport EOF before terminator is truncation",
			serve: func(conn io.ReadWriteCloser) {
				var head link.ExchangeHostHeader
				_ = link.ReadExchangeControl(conn, &head)
				_ = link.WriteExchangeChunk(conn, []byte("partial"))
				_ = conn.Close()
			},
			want: "partial", wantErrCode: "transfer_failed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			issue, redeem := openTestPlane(t, &testOpener{online: true, serve: test.serve})
			grant := issueTestTicket(t, issue, access.OpRead)
			caller, server := net.Pipe()
			serveDone := make(chan struct{})
			go func() {
				redeem.ServeExchange(t.Context(), "channel-a", server)
				close(serveDone)
			}()
			if err := link.WriteExchangeControl(caller, link.ExchangeTicketHeader{Ticket: grant.Ticket, Caller: string(testCaller)}); err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(link.NewExchangeReader(caller))
			if string(got) != test.want {
				t.Fatalf("bytes = %q, want %q", got, test.want)
			}
			if test.wantErrCode == "" {
				if err != nil {
					t.Fatalf("completed read error = %v, want ordinary EOF completion", err)
				}
			} else {
				var terminal *link.ExchangeTerminalError
				if !errors.As(err, &terminal) || terminal.Code != test.wantErrCode || errors.Is(err, io.EOF) {
					t.Fatalf("truncated read error = %v, want non-EOF %q terminal", err, test.wantErrCode)
				}
			}
			select {
			case <-serveDone:
			case <-time.After(time.Second):
				t.Fatal("exchange read pump did not join")
			}
		})
	}
}

type partialErrorReader struct {
	data []byte
	err  error
}

func (r *partialErrorReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}

func TestInterruptedHTTPWriteAfterPartialBytesNeverSucceeds(t *testing.T) {
	landed := make(chan []byte, 1)
	opener := &testOpener{online: true, serve: func(conn io.ReadWriteCloser) {
		defer conn.Close()
		var head link.ExchangeHostHeader
		_ = link.ReadExchangeControl(conn, &head)
		var got bytes.Buffer
		_ = link.ReadExchangeBytes(&got, conn)
		landed <- got.Bytes()
	}}
	issue, redeem := openTestPlane(t, opener)
	grant := issueTestTicket(t, issue, access.OpWrite)
	wantErr := errors.New("upload interrupted")
	err := redeem.ServeHTTP(t.Context(), "channel-a", testCaller, grant.Ticket, access.OpWrite,
		io.Discard, &partialErrorReader{data: []byte("partial"), err: wantErr})
	if err == nil {
		t.Fatal("partially transferred write returned success")
	}
	select {
	case got := <-landed:
		if string(got) != "partial" {
			t.Fatalf("host received %q, want partial prefix", got)
		}
	case <-time.After(time.Second):
		t.Fatal("host did not observe interrupted write")
	}
}

func TestExchangeWriteForwardsEarlyFailureAndJoins(t *testing.T) {
	opener := &testOpener{online: true, serve: func(conn io.ReadWriteCloser) {
		defer conn.Close()
		var head link.ExchangeHostHeader
		_ = link.ReadExchangeControl(conn, &head)
		_ = link.WriteExchangeControl(conn, link.ExchangeStatus{
			OK: false, Code: "write_rejected", Detail: "open failed",
		})
	}}
	issue, redeem := openTestPlane(t, opener)
	grant := issueTestTicket(t, issue, access.OpWrite)
	caller, server := net.Pipe()
	serveDone := make(chan struct{})
	go func() {
		redeem.ServeExchange(t.Context(), "channel-a", server)
		close(serveDone)
	}()
	if err := link.WriteExchangeControl(caller, link.ExchangeTicketHeader{Ticket: grant.Ticket, Caller: string(testCaller)}); err != nil {
		t.Fatal(err)
	}
	uploadDone := make(chan error, 1)
	go func() {
		payload := make([]byte, link.MaxExchangeChunk)
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
		t.Fatalf("early terminal = %+v", status)
	}
	select {
	case err := <-uploadDone:
		if err == nil {
			t.Fatal("upload reached its source end after early host failure")
		}
	case <-time.After(time.Second):
		t.Fatal("upload did not stop after early host failure")
	}
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("exchange did not join after early host failure")
	}
}
