package all

import (
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/registry"
)

func TestSituationCarriesDeviceAndWorkspaceFacts(t *testing.T) {
	deps := registry.Deps{
		ChannelID:    channelspec.C0ChannelID,
		DeviceID:     "local-device",
		DeviceName:   "desk",
		WorkspaceDir: "/var/atoll/channels/c0",
	}
	got := situation(registry.InstanceSpec{ID: "agent:steward:1"}, deps, "codex")
	if got.DeviceID != deps.DeviceID || got.DeviceName != deps.DeviceName || got.WorkspaceDir != deps.WorkspaceDir {
		t.Fatalf("situation=%+v, want host device/workspace facts", got)
	}
}
