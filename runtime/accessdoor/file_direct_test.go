package accessdoor

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

type directMounts struct{}

func (directMounts) ResolveStorageDaemon(context.Context, channel.ID, string) (StorageMount, bool, error) {
	return StorageMount{DaemonID: "daemon-a", Name: "laptop-a", Online: true}, true, nil
}

type countingTransfers struct {
	calls int
	last  TransferSpec
}

func (c *countingTransfers) IssueTransfer(_ context.Context, spec TransferSpec) (string, error) {
	c.calls++
	c.last = spec
	return "ticket", nil
}

func TestColocatedFileRouteCarriesPathAndMintsNoTicket(t *testing.T) {
	transfers := &countingTransfers{}
	d := &door{deps: Deps{Registry: &fakeRegistry{}, Drivers: DriverTable{resourcespec.KindKV: &fakeDriver{}}, Authority: &fakeMembership{lookupFound: true, lookupHost: "daemon-a"}, State: &fakeStateStore{}, ChannelID: "c", ChannelName: "c0.c", StorageMounts: directMounts{}, TransferControl: transfers}}
	route, err := d.resolveFileRoute(t.Context(), "agent:a", "daemon://laptop-a/c0.c/docs/report.txt", access.OpWrite)
	if err != nil {
		t.Fatal(err)
	}
	if route.Redeem != FileRedeemLocal || route.Path != "docs/report.txt" || route.Token != "" {
		t.Fatalf("route=%+v", route)
	}
	if transfers.calls != 0 {
		t.Fatalf("ticket ledger writes=%d want 0", transfers.calls)
	}
}

func TestFileDoorRejectsAddressForAnotherChannelBeforeMembership(t *testing.T) {
	membership := &fakeMembership{lookupFound: true, lookupHost: "daemon-a"}
	d := &door{deps: Deps{
		Registry: &fakeRegistry{}, Drivers: DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Authority: membership, State: &fakeStateStore{}, ChannelID: "channel-a", ChannelName: "c0.channel-a",
		StorageMounts: directMounts{}, TransferControl: &countingTransfers{},
	}}
	if _, err := d.resolveFileRoute(t.Context(), "agent:a", "daemon://laptop-a/c0.channel-b/docs/report.txt", access.OpRead); !errors.Is(err, ErrMalformed) {
		t.Fatalf("mismatched address error=%v, want ErrMalformed", err)
	}
	if membership.calls != 0 {
		t.Fatalf("membership consulted %d times before address-channel rejection", membership.calls)
	}
}

// A remote transfer is decided here and finished somewhere else, later, on a
// connection this door never sees. Whoever arrives there has to be answerable
// as the actor this route was decided for, so the decision has to travel with
// its subject — a route that forgets who it was for is a grant to everybody.
func TestARemoteFileRouteCarriesTheActorItWasDecidedFor(t *testing.T) {
	transfers := &countingTransfers{}
	d := &door{deps: Deps{
		Registry: &fakeRegistry{}, Drivers: DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Authority:     &fakeMembership{lookupFound: true, lookupHost: "somewhere-else", principal: "alice"},
		State:         &fakeStateStore{},
		ChannelID:     "c", ChannelName: "c0.c",
		StorageMounts: directMounts{}, TransferControl: transfers,
	}}
	route, err := d.resolveFileRoute(t.Context(), "human:alice:7", "daemon://laptop-a/c0.c/docs/report.txt", access.OpWrite)
	if err != nil {
		t.Fatal(err)
	}
	if route.Redeem != FileRedeemRemote {
		t.Fatalf("redeem=%q, want a remote route", route.Redeem)
	}
	if transfers.last.Caller != "human:alice:7" || transfers.last.Principal != "alice" {
		t.Fatalf("transfer subject=(%q,%q)", transfers.last.Caller, transfers.last.Principal)
	}
}

// An actor that answers for no person (an agent, a tool) still gets its route —
// it redeems on its own device lane. What must not happen is a principal being
// invented for it, which would make its transfer finishable at a human door.
func TestAPersonlessActorsRouteNamesNoPerson(t *testing.T) {
	transfers := &countingTransfers{}
	d := &door{deps: Deps{
		Registry: &fakeRegistry{}, Drivers: DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Authority:     &fakeMembership{lookupFound: true, lookupHost: "somewhere-else"},
		State:         &fakeStateStore{},
		ChannelID:     "c", ChannelName: "c0.c",
		StorageMounts: directMounts{}, TransferControl: transfers,
	}}
	if _, err := d.resolveFileRoute(t.Context(), "agent:a:1", "daemon://laptop-a/c0.c/docs/report.txt", access.OpRead); err != nil {
		t.Fatal(err)
	}
	if transfers.last.Principal != "" {
		t.Fatalf("principal=%q, want none", transfers.last.Principal)
	}
}
