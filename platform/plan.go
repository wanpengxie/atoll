package platform

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// PlanActor is the daemon-build snapshot carried over the authenticated link.
type PlanActor struct {
	InstanceID actor.ActorID   `json:"instance_id"`
	Class      string          `json:"class"`
	Config     json.RawMessage `json:"config_json,omitempty"`
	Kind       actor.Kind      `json:"kind"`
	Binding    actor.Binding   `json:"binding"`
	Epoch      int64           `json:"epoch"`
}
