package actorbase

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/harness"
)

func encodeRequestPayload(caller *harness.Caller, args any) (json.RawMessage, error) {
	raw, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	wrapped := struct {
		Context *harness.Context `json:"_context,omitempty"`
		Body    json.RawMessage  `json:"body"`
	}{Body: raw}
	if caller != nil {
		wrapped.Context = &harness.Context{Caller: *caller}
	}
	return json.Marshal(wrapped)
}

type TargetResolveError struct {
	Code   string
	Target string
}

func (e *TargetResolveError) Error() string {
	return fmt.Sprintf("actorbase: %s target %q", e.Code, e.Target)
}

func (e *TargetResolveError) ErrorCode() string { return e.Code }

func (e *engine) resolveTarget(target actor.ActorID) (actor.ActorID, error) {
	if target == actor.SystemActorID {
		return target, nil
	}
	if e.hooks.ResolveTarget == nil {
		return target, nil
	}
	resolved, err := e.hooks.ResolveTarget(string(target))
	if err == nil {
		return resolved, nil
	}
	var targetErr *TargetResolveError
	if errors.As(err, &targetErr) {
		return "", targetErr
	}
	var coded interface{ ErrorCode() string }
	if errors.As(err, &coded) {
		return "", &TargetResolveError{Code: coded.ErrorCode(), Target: string(target)}
	}
	return "", err
}

func (e *engine) resolveAudience(in []actor.ActorID) ([]actor.ActorID, error) {
	out := make([]actor.ActorID, len(in))
	for i, target := range in {
		resolved, err := e.resolveTarget(target)
		if err != nil {
			return nil, err
		}
		out[i] = resolved
	}
	return out, nil
}
