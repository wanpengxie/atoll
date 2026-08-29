package accessdoor

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

type directMounts struct {
	root   string
	others []StorageMount
}

func (m directMounts) ResolveStorageDaemon(context.Context, channel.ID, string) (StorageMount, bool, error) {
	return StorageMount{DaemonID: "daemon-a", Name: "laptop-a", Online: true, Root: m.root}, true, nil
}

func (m directMounts) ListStorageMounts(context.Context, channel.ID) ([]StorageMount, error) {
	out := []StorageMount{{DaemonID: "daemon-a", Name: "laptop-a", Online: true, Root: m.root}}
	return append(out, m.others...), nil
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
		Authority: &fakeMembership{lookupFound: true, lookupHost: "somewhere-else"},
		State:     &fakeStateStore{},
		ChannelID: "c", ChannelName: "c0.c",
		StorageMounts: directMounts{}, TransferControl: transfers,
	}}
	route, err := d.resolveFileRoute(t.Context(), "human:alice:7", "daemon://laptop-a/c0.c/docs/report.txt", access.OpWrite)
	if err != nil {
		t.Fatal(err)
	}
	if route.Redeem != FileRedeemRemote {
		t.Fatalf("redeem=%q, want a remote route", route.Redeem)
	}
	if transfers.last.Caller != "human:alice:7" {
		t.Fatalf("transfer subject=%q", transfers.last.Caller)
	}
}

// An actor whose route was decided in this process can turn it into bytes here.
// The stub this replaces answered capability_unavailable, which left a
// server-resident actor holding a grant it could not act on.
func TestAnActorInThisProcessRedeemsItsOwnRoute(t *testing.T) {
	redeem := &recordingRedeem{}
	d := &door{deps: Deps{
		Registry: &fakeRegistry{}, Drivers: DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Authority: &fakeMembership{lookupFound: true, lookupHost: "somewhere-else"},
		State:     &fakeStateStore{},
		ChannelID: "c", ChannelName: "c0.c",
		StorageMounts: directMounts{}, TransferControl: &countingTransfers{}, TransferRedeem: redeem,
	}}
	h := boundHandle{door: d, caller: "agent:steward:9", authority: accessAuthority("agent:steward:9")}
	fa, out, err := h.Open(t.Context(), "daemon://laptop-a/c0.c/docs/report.txt", access.OpRead)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Accepted() || out.Route == nil {
		t.Fatalf("outcome=%+v", out)
	}
	if _, ok := fa.Reader(); !ok {
		t.Fatal("Open decided a route but handed back no bytes")
	}
	// The redemption runs under the handle's welded caller, never one carried in
	// on the route.
	if redeem.caller != "agent:steward:9" {
		t.Fatalf("redeemed as %q", redeem.caller)
	}
}

type recordingRedeem struct {
	caller actor.ActorID
	route  FileRoute
}

func (r *recordingRedeem) RedeemTransfer(_ context.Context, caller actor.ActorID, route FileRoute) (FileAccess, error) {
	r.caller, r.route = caller, route
	return FileAccess{Remote: &RemoteFile{Read: io.NopCloser(strings.NewReader("bytes"))}}, nil
}

// A door assembled without redemption cannot produce bytes and says so. This
// was once the permanent answer for every in-process handle — the face existed
// only to satisfy the three-avatar parity rule — which is how an actor here
// could be granted a file route it had no way to act on.
func TestAnUnwiredDoorRefusesInsteadOfPretending(t *testing.T) {
	d := &door{deps: Deps{
		Registry: &fakeRegistry{}, Drivers: DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Authority: &fakeMembership{lookupFound: true, lookupHost: "somewhere-else"},
		State:     &fakeStateStore{},
		ChannelID: "c", ChannelName: "c0.c",
		StorageMounts: directMounts{}, TransferControl: &countingTransfers{},
	}}
	h := boundHandle{door: d, caller: "agent:steward:9", authority: accessAuthority("agent:steward:9")}
	if _, _, err := h.Open(t.Context(), "daemon://laptop-a/c0.c/docs/report.txt", access.OpRead); !errors.Is(err, ErrFileCapabilityUnavailable) {
		t.Fatalf("unwired file face err=%v, want capability unavailable", err)
	}
}
