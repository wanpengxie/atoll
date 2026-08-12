package accessdoor

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

type directMounts struct{}

func (directMounts) ResolveStorageDaemon(context.Context, channel.ID, string) (StorageMount, bool, error) {
	return StorageMount{DaemonID: "daemon-a", Name: "laptop-a", Online: true}, true, nil
}

type countingTransfers struct{ calls int }

func (c *countingTransfers) IssueTransfer(context.Context, resource.ResourceID, string, string, access.Operation) (string, error) {
	c.calls++
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
