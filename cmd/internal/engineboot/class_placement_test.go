package engineboot

import (
	"testing"

	_ "github.com/wanpengxie/atoll/drivers/tools/device"
	_ "github.com/wanpengxie/atoll/drivers/tools/kimi"
	_ "github.com/wanpengxie/atoll/drivers/tools/mcp"
	_ "github.com/wanpengxie/atoll/drivers/tools/xhs"
	"github.com/wanpengxie/atoll/protocol/channel"
	classregistry "github.com/wanpengxie/atoll/registry"
)

func TestRegisteredProductionClassesAreDaemonPlaced(t *testing.T) {
	for _, class := range classregistry.RegisteredClasses() {
		if class == svcactorTestReceiverClass {
			continue
		}
		placement, ok := classregistry.ClassPlacement(class)
		if !ok || placement != channel.PlacementDaemon {
			t.Errorf("class %s placement=%q present=%v", class, placement, ok)
		}
	}
}
