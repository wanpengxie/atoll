package platform

import (
	"encoding/json"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

// PlanActor is the daemon-build snapshot carried over the authenticated link.
type PlanActor struct {
	ActorID    actor.ActorID        `json:"actor_id"`
	AttemptKey actorhost.AttemptKey `json:"attempt_key"`
	Kind       actor.Kind           `json:"kind"`
	Class      string               `json:"class"`
	Config     json.RawMessage      `json:"config_json,omitempty"`
	Idle       time.Duration        `json:"idle_timeout"`
}
