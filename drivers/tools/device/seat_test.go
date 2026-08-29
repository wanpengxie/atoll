package device

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

// The regression this pins cost the class its entire existence: construct used
// to ignore spec.ID and derive "device:"+DeviceName instead. A daemon host
// refuses a decl whose id is not the planned one, and a seat id ("tool:device:
// <ts>") can never equal a two-segment derived one — so every seating of this
// class failed to build, retried on the host's backoff, and never came up on
// any daemon. It is not enough to test that the actor WORKS; nothing could
// reach the working actor.
func TestConstructFillsThePlannedSeat(t *testing.T) {
	const seat = actor.ActorID("tool:device:1787907279757")
	decl, err := construct(
		registry.InstanceSpec{ID: seat},
		registry.Deps{DeviceID: "some-laptop", WorkspaceDir: t.TempDir()},
	)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	if decl.ID != seat {
		t.Fatalf("id = %q, want the planned seat %q — a daemon host refuses a decl that renames itself", decl.ID, seat)
	}
	if decl.Kind != actor.KindTool {
		t.Fatalf("kind = %q, want %q", decl.Kind, actor.KindTool)
	}
}

// The planned seat no longer derives its id, but a stable device identity is
// still required: this class is daemon-placed and only a daemon host supplies
// one, so an empty identity means the
// body is being built somewhere it was never meant to run.
func TestConstructRefusesWithoutADeviceIdentity(t *testing.T) {
	if _, err := construct(
		registry.InstanceSpec{ID: "tool:device:1"},
		registry.Deps{WorkspaceDir: t.TempDir()},
	); err == nil {
		t.Fatal("construct accepted an empty device id")
	}
}
