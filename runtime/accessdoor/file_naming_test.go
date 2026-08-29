package accessdoor

import (
	"context"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// askedMounts records the device segment it was resolved by, which the other
// doubles here deliberately ignore.
type askedMounts struct {
	asked []string
	mount StorageMount
}

func (m *askedMounts) ResolveStorageDaemon(_ context.Context, _ channel.ID, deviceName string) (StorageMount, bool, error) {
	m.asked = append(m.asked, deviceName)
	return m.mount, true, nil
}

func (m *askedMounts) ListStorageMounts(context.Context, channel.ID) ([]StorageMount, error) {
	return []StorageMount{m.mount}, nil
}

// A daemon:// address spells its device by NAME; the id stays the authority for
// bindings and routing. Both are plain strings, so handing the resolver an id
// compiles, type-checks, and then finds nothing — the failure is a lookup miss
// at runtime, far from the line that confused them.
//
// What this pins is the one thing that is actually observable here: the door
// resolves by the segment the address carries, and never by the mount's id. The
// other mount doubles in this package ignore their argument entirely, so
// nothing else can catch a regression to id-passing.
//
// It deliberately does NOT assert which string lands in the ticket's HostName:
// a real resolver answers a name lookup with a mount whose Name is that same
// name, so mount.Name and address.Host are equal in every reachable state and
// an assertion on them would pass either way. Preferring the mount's own name
// is a clarity choice, not a behaviour a test can defend.
func TestTheDoorResolvesADeviceByTheNameInTheAddress(t *testing.T) {
	mounts := &askedMounts{mount: StorageMount{
		DaemonID: "b068f503-8596-4a01-bf3f-eba9e41860b8", // an id never appears in an address
		Name:     "laptop-a",
		Online:   true,
	}}
	transfers := &countingTransfers{}
	d := &door{deps: Deps{
		Registry:        &fakeRegistry{},
		Authority:       &fakeMembership{lookupFound: true, lookupHost: "daemon-elsewhere"},
		State:           &fakeStateStore{},
		ChannelID:       "c",
		ChannelName:     "c0.c",
		StorageMounts:   mounts,
		TransferControl: transfers,
	}}

	if _, err := d.resolveFileRoute(
		t.Context(), "agent:a", "daemon://laptop-a/c0.c/docs/report.txt", access.OpRead,
	); err != nil {
		t.Fatal(err)
	}

	if len(mounts.asked) != 1 || mounts.asked[0] != "laptop-a" {
		t.Fatalf("resolver was asked for %v, want exactly the address's device name [laptop-a]", mounts.asked)
	}
	for _, asked := range mounts.asked {
		if strings.Contains(asked, mounts.mount.DaemonID) {
			t.Fatalf("resolver was asked for the device id %q — a daemon:// segment is a name", asked)
		}
	}
	// Routing still travels by id: the name got us to the mount, and the mount's
	// id is what the transfer is addressed to.
	if transfers.last.HostID != mounts.mount.DaemonID {
		t.Fatalf("ticket HostID=%q, want the mount's id %q", transfers.last.HostID, mounts.mount.DaemonID)
	}
}
