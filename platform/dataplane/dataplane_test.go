package dataplane

import (
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type testOpener struct{}

func (testOpener) Online(string, channel.ID) bool                               { return true }
func (testOpener) OpenHost(context.Context, Ticket) (io.ReadWriteCloser, error) { return nil, io.EOF }

func TestTicketLedgerKeepsOnlyRemoteByteFacts(t *testing.T) {
	issue, redeem, bind, closePlane := New()
	t.Cleanup(func() { _ = closePlane(context.Background()) })
	if err := bind.BindHostStreamOpener(testOpener{}); err != nil {
		t.Fatal(err)
	}
	grant, err := issue.Issue(t.Context(), IssueSpec{Address: "daemon://host/docs/a.txt", ChannelID: "c", Mode: access.OpRead, HostID: "d", HostName: "host"})
	if err != nil || grant.Ticket == "" {
		t.Fatalf("grant=%+v err=%v", grant, err)
	}
	ticket, err := redeem.Resolve("c", grant.Ticket)
	if err != nil {
		t.Fatal(err)
	}
	if ticket.Address != "daemon://host/docs/a.txt" || ticket.HostID != "d" || ticket.Mode != access.OpRead {
		t.Fatalf("ticket=%+v", ticket)
	}
	want := []string{"ChannelID", "Address", "Mode", "HostID", "Expires"}
	typ := reflect.TypeOf(Ticket{})
	got := make([]string, typ.NumField())
	for i := range got {
		got[i] = typ.Field(i).Name
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ticket fields=%v want=%v", got, want)
	}
}
