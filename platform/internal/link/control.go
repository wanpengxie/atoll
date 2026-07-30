package link

import (
	"fmt"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

type PlanReply struct {
	Actors []platform.PlanActor `json:"actors"`
	Error  string               `json:"error,omitempty"`
}

func (m PlanReply) validate() error {
	for index, row := range m.Actors {
		if row.ActorID == "" {
			return fmt.Errorf("link: plan actor %d has no id", index)
		}
		if _, err := actorhost.ParseAttemptKey(string(row.AttemptKey)); err != nil {
			return err
		}
		if _, ok := actor.ParseKind(string(row.Kind)); !ok {
			return fmt.Errorf("link: plan actor %d has invalid kind", index)
		}
		if row.Class == "" {
			return fmt.Errorf("link: plan actor %d has no class", index)
		}
	}
	return nil
}

func requiredControlField(name, value string) error {
	if value == "" {
		return fmt.Errorf("link: control field %s is required", name)
	}
	return nil
}
