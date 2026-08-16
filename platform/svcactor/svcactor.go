package svcactor

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/peerproto"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

const (
	Class  = "svcactor"
	DeclID = "atoll-internal:svcactor"
)

type Endpoint struct {
	Name     string
	Receiver string
}

type Audit func(context.Context, map[string]any) error

type Deps struct {
	Port           *Port
	Self           channel.ID
	Core           channel.ID
	RegistrarClass string
	Endpoints      func(context.Context) ([]Endpoint, error)
	Instances      func(context.Context, string) ([]actor.ActorID, error)
	Parent         func(context.Context) (channel.ID, error)
	ReceiverClass  func(context.Context, string) (string, error)
	Card           func(context.Context, channel.ID) (introspect.Describe, error)
	Audit          Audit
}

func Def(deps Deps) actorbase.Def {
	return actorbase.Def{Doc: "external service for channel " + string(deps.Self), New: func() (actorbase.Proc, error) {
		if deps.Port == nil || deps.Self == "" || deps.Core == "" || deps.Endpoints == nil || deps.Instances == nil || deps.Parent == nil || deps.ReceiverClass == nil || deps.Card == nil || deps.Audit == nil {
			return nil, errors.New("svcactor: incomplete dependencies")
		}
		return func(sys actorbase.Sys) error { return serve(sys, deps) }, nil
	}}
}

func serve(sys actorbase.Sys, deps Deps) error {
	go servePort(sys, deps)
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		if msg.Kind != message.KindRequest {
			continue
		}
		if msg.Type == introspect.QueryDescribe {
			card, err := deps.Card(msg.Ctx(), deps.Self)
			if err != nil {
				_, _ = sys.Fail(msg, "channel_unavailable", err.Error())
			} else {
				_, _ = sys.Reply(msg, card)
			}
			continue
		}
		_, _ = sys.Fail(msg, "external-facing", "svcactor only accepts requests through its channel port")
	}
}

func servePort(sys actorbase.Sys, deps Deps) {
	for {
		req, err := deps.Port.receive(sys.Life())
		if err != nil {
			return
		}
		result := dispatch(req.ctx, sys, deps, req.caller, req.frame)
		select {
		case req.done <- result:
		case <-sys.Life().Done():
		}
	}
}

func dispatch(ctx context.Context, sys actorbase.Sys, deps Deps, caller channel.ID, req peerproto.Request) peerproto.Result {
	if req.Origin.Channel != caller {
		return failure("bad_origin", "origin channel does not match the bound caller")
	}
	if req.Type == introspect.QueryDescribe {
		card, err := deps.Card(ctx, caller)
		if err != nil {
			return failure("channel_unavailable", err.Error())
		}
		raw, err := json.Marshal(card)
		if err != nil {
			return failure("internal_error", err.Error())
		}
		return peerproto.Result{Body: raw}
	}

	var target actor.ActorID
	payload := any(json.RawMessage(req.Payload))
	if native(req.Type) {
		parent, err := deps.Parent(ctx)
		if err != nil {
			return failure("channel_unavailable", err.Error())
		}
		if caller != deps.Core && caller != parent {
			return failure("forbidden", "management requests require core or parent")
		}
		target = actor.SystemActorID
	} else {
		endpoints, err := deps.Endpoints(ctx)
		if err != nil {
			return failure("channel_unavailable", err.Error())
		}
		var endpoint *Endpoint
		for i := range endpoints {
			if endpoints[i].Name == req.Type {
				endpoint = &endpoints[i]
				break
			}
		}
		if endpoint == nil {
			return failure("endpoint_not_found", "channel endpoint not found")
		}
		instances, err := deps.Instances(ctx, endpoint.Receiver)
		if err != nil {
			return failure("channel_unavailable", err.Error())
		}
		if len(instances) == 0 {
			return failure("receiver_unavailable", "endpoint receiver has no active instance")
		}
		if len(instances) != 1 {
			return failure("receiver_ambiguous", "endpoint receiver is not unique")
		}
		target = instances[0]
		class, err := deps.ReceiverClass(ctx, endpoint.Receiver)
		if err != nil {
			return failure("receiver_unavailable", err.Error())
		}
		if class == deps.RegistrarClass {
			payload = struct {
				Origin peerproto.Origin `json:"origin"`
				Args   json.RawMessage  `json:"args"`
			}{Origin: req.Origin, Args: append(json.RawMessage(nil), req.Payload...)}
		}
	}

	pending, err := sys.Call(target, req.Type, payload)
	if err != nil {
		return failure("receiver_unavailable", err.Error())
	}
	terminal, err := pending.Wait(ctx, 0)
	if err != nil {
		return failure("receiver_unavailable", err.Error())
	}
	_ = deps.Audit(ctx, map[string]any{"origin": req.Origin, "type": req.Type, "local_request_id": terminal.ParentID})
	return terminalResult(terminal.Payload)
}

func native(word string) bool {
	switch word {
	case "channel.introduce_actor", "channel.remove_actor", "channel.restart_actor":
		return true
	default:
		return false
	}
}

func terminalResult(raw json.RawMessage) peerproto.Result {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return failure("receiver_unavailable", "invalid receiver terminal")
	}
	var status string
	_ = json.Unmarshal(fields["status"], &status)
	if status == message.StatusFailed {
		var code, detail string
		_ = json.Unmarshal(fields["error_code"], &code)
		_ = json.Unmarshal(fields["detail"], &detail)
		if code == "" {
			code = "receiver_unavailable"
		}
		return failure(code, detail)
	}
	if status != message.StatusCompleted {
		return failure("receiver_unavailable", "receiver returned a non-terminal response")
	}
	delete(fields, "status")
	delete(fields, "reason")
	body, err := json.Marshal(fields)
	if err != nil {
		return failure("receiver_unavailable", err.Error())
	}
	return peerproto.Result{Body: body}
}

func failure(code, detail string) peerproto.Result {
	return peerproto.Result{Fail: &peerproto.Failure{Code: code, Detail: detail}}
}

func mustJSON(v any) json.RawMessage {
	raw, _ := json.Marshal(v)
	return raw
}
