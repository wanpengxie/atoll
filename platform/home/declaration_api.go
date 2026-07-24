package home

import (
	"encoding/json"
	"errors"
	"time"

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
	TIdle        int64
	MakeDefault  bool
	CreatedAt    int64
}

type DeclareResult struct {
	Row           storespec.ActorControlRow
	Created       bool
	ConfigUpdated bool
}

func validateDeclareRequest(in DeclareRequest) error {
	if in.SourceDeclID == "" || in.Class == "" || in.CreatedAt <= 0 ||
		in.TIdle < 0 || in.Placement.Validate() != nil {
		return errors.New("platform: invalid declaration request")
	}
	return nil
}

func durationMillis(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }
