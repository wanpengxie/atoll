package home

import (
	"encoding/json"
	"fmt"
	"github.com/wanpengxie/atoll/platform/channelmember"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/peeractor"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// A relation key is derived from its resolved definition, never its declaration
// alias or singleton flag. This does not create a second relation registry.
func relationKey(def storespec.ActorDefinition) (string, error) {
	switch def.Class {
	case channelmember.SeatClass:
		cfg, err := channelmember.ParseSeatConfig(def.Config)
		if err != nil {
			return "", err
		}
		return "seat:" + string(cfg.Body), nil
	case channelmember.HandleClass:
		cfg, err := channelmember.ParseHandleConfig(def.Config)
		if err != nil {
			return "", err
		}
		return "handle:" + string(cfg.Host), nil
	case "peeractor":
		target, err := peeractor.ValidateConfig(json.RawMessage(def.Config))
		if err != nil {
			return "", err
		}
		return "peer:" + string(target), nil
	}
	return "", nil
}
func (a *actorSystem) checkRelation(def storespec.ActorDefinition, except actor.ActorID) error {
	key, err := relationKey(def)
	if err != nil {
		return err
	}
	if key == "" {
		return nil
	}
	instances, err := a.home.controller.DeclaredReconcileList()
	if err != nil {
		return err
	}
	for _, instance := range instances {
		if instance.ID == except {
			continue
		}
		other, err := relationKey(instance.Definition)
		if err != nil {
			return err
		}
		if key == other {
			return &channelspec.OperationError{Code: channelspec.ErrCodeConflictExists, Detail: fmt.Sprintf("relation %s already represented by %s", key, instance.ID)}
		}
	}
	return nil
}
