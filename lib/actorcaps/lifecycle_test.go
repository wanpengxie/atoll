package actorcaps

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type lifecycleShape struct{}

func (lifecycleShape) Fork(context.Context, message.ID, ForkSpec) (actor.ActorID, error) {
	return "", nil
}
func (lifecycleShape) EndSelf(context.Context, EndSelfRequest) error {
	return nil
}

var _ LifecycleHandle = lifecycleShape{}

func TestForkSpecUsesPublicPlacement(t *testing.T) {
	t.Parallel()

	spec := ForkSpec{
		Kind:     actor.KindAgent,
		Class:    "worker",
		NameHint: "child",
		Config:   json.RawMessage(`{"x":1}`),
		Placement: &channel.Placement{
			Kind:        channel.PlacementDaemon,
			DesiredHost: "daemon-a",
		},
	}
	if err := spec.Placement.Validate(); err != nil {
		t.Fatalf("public placement: %v", err)
	}
}
