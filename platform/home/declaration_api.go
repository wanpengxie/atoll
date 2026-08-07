package home

import (
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// DeclareRequest is bootstrap input only. Serving-time declaration changes
// enter actorctl.ApplyDeclaration/Introduce and never mutate the store behind
// Controller's back.
type DeclareRequest struct {
	SourceDeclID string
	Kind         actor.Kind
	Class        string
	Config       *json.RawMessage
	Placement    storespec.Placement
	CreatedAt    int64
}

func validateDeclareRequest(in DeclareRequest) error {
	if in.SourceDeclID == "" || in.Class == "" || in.CreatedAt <= 0 ||
		in.Placement.Validate() != nil {
		return errors.New("platform: invalid declaration request")
	}
	return nil
}
