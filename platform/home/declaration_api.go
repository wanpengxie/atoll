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
	Seed         string
	Kind         actor.Kind
	Principal    string
	Class        string
	Config       *json.RawMessage
	Placement    storespec.Placement
	Singleton    bool
	CreatedAt    int64
}

func validateDeclareRequest(in DeclareRequest) error {
	if in.SourceDeclID == "" || in.Seed == "" || in.Class == "" || in.CreatedAt <= 0 ||
		in.Placement.Validate() != nil {
		return errors.New("platform: invalid declaration request")
	}
	if in.Principal != "" && in.Kind != actor.KindAgent {
		return errors.New("platform: only an agent declaration may carry a principal")
	}
	return nil
}
