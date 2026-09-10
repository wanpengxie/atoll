package harness

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/wanpengxie/atoll/protocol/message"
)

const PayloadContextKey = "_context"

// Caller identifies the concrete member for whom a framework-authored request
// was written.
type Caller = message.Caller
type Context = message.Context

// Payload is the canonical payload envelope for every message kind. Context is
// always present on the ledger, even when it has no fields; actors only see
// Body after actorbase projects a message into a Msg.
type Payload struct {
	Context Context         `json:"_context"`
	Body    json.RawMessage `json:"body"`
}

func WrapPayload(ctx Context, body json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		trimmed = []byte("{}")
	}
	if trimmed[0] != '{' {
		return nil, errors.New("payload body must be a JSON object")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &object); err != nil || object == nil {
		if err == nil {
			err = errors.New("null object")
		}
		return nil, fmt.Errorf("payload body must be a JSON object: %w", err)
	}
	return json.Marshal(Payload{Context: ctx, Body: append(json.RawMessage(nil), trimmed...)})
}

func UnwrapPayload(raw json.RawMessage) (Context, json.RawMessage, error) {
	var wrapped struct {
		Context json.RawMessage `json:"_context"`
		Body    json.RawMessage `json:"body"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&wrapped); err != nil {
		return Context{}, nil, err
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return Context{}, nil, err
	}
	if len(wrapped.Context) == 0 || bytes.Equal(bytes.TrimSpace(wrapped.Context), []byte("null")) {
		return Context{}, nil, errors.New("_context object required")
	}
	if len(wrapped.Body) == 0 {
		return Context{}, nil, errors.New("body field required")
	}
	var ctx Context
	dec = json.NewDecoder(bytes.NewReader(wrapped.Context))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ctx); err != nil {
		return Context{}, nil, fmt.Errorf("_context: %w", err)
	}
	if ctx.Caller != nil && (ctx.Caller.Channel == "" || ctx.Caller.Actor == "") {
		return Context{}, nil, errors.New("_context.caller requires non-empty channel and actor")
	}
	if ctx.Session != "" && string(bytes.TrimSpace([]byte(ctx.Session))) != ctx.Session {
		return Context{}, nil, errors.New("_context.session must be non-blank and trimmed")
	}
	trimmed := bytes.TrimSpace(wrapped.Body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return Context{}, nil, errors.New("body must be a JSON object")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &object); err != nil || object == nil {
		return Context{}, nil, errors.New("body must be a JSON object")
	}
	return ctx, append(json.RawMessage(nil), trimmed...), nil
}
