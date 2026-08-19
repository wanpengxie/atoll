package actorbase

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// DecodeStrict decodes a standard protocol body. Standard bodies are JSON
// objects: unknown fields, null/scalar/array values, and trailing documents are
// rejected so misspelled arguments fail at the receiver instead of becoming a
// different request.
func DecodeStrict(raw json.RawMessage, out any) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("request body must be a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body contains multiple JSON values")
		}
		return err
	}
	return nil
}

// DecodeStrictEmpty is the no-argument variant: a canonical body:null means
// the same thing as {}, while every non-empty body is still closed and strict.
func DecodeStrictEmpty(raw json.RawMessage, out any) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		raw = json.RawMessage(`{}`)
	}
	return DecodeStrict(raw, out)
}

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
