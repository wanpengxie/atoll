package channelmember

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

const (
	SeatClass    = "channel-seat"
	HandleClass  = "channel-handle"
	HandleDeclID = "channel-handle"
	HandleSeed   = "host"
	HandleCall   = "channel.call"
	HandlePost   = "channel.post"
	HandleEmit   = "channel.emit"
	InboundEvent = "channelmember.inbound"
)

// Members is the body's existing membership authority, not a relation registry.
type Members interface {
	MemberOfDeclaration(string) (actor.ActorID, error)
	ActorFacts(context.Context, actor.ActorID) (channelspec.ActorFacts, bool, error)
}

func SeatDef(hub *Hub, host channel.ID, cfg SeatConfig, unavailable ...func(context.Context) error) actorbase.Def {
	pair := Pair{Host: host, Body: cfg.Body}
	projection := &manifestProjection{err: ErrUnreachable}
	describe := func(ctx context.Context) (map[string]introspect.WordSpec, error) {
		words, err := projection.read()
		if errors.Is(err, ErrUnreachable) && len(unavailable) > 0 && unavailable[0] != nil {
			if reason := unavailable[0](ctx); reason != nil {
				return nil, fmt.Errorf("%w: %v", ErrUnreachable, reason)
			}
		}
		return words, err
	}
	return actorbase.Def{Manifest: introspect.Manifest{Class: SeatClass, Interfaces: []string{"actor", "channel"}, Words: map[string]introspect.WordSpec{}, Dynamic: func(ctx context.Context) (map[string]introspect.WordSpec, error) {
		words, err := describe(ctx)
		if errors.Is(err, ErrUnreachable) {
			return map[string]introspect.WordSpec{}, nil
		}
		return words, err
	}}, New: func() (actorbase.Proc, error) {
		if hub == nil || !pair.valid() {
			return nil, errors.New("invalid seat binding")
		}
		return func(sys actorbase.Sys) error {
			releaseProjection, err := hub.WatchHandleBinding(pair, func(b HandleBinding) { projection.refresh(sys.Life(), b) })
			if err != nil {
				return err
			}
			defer releaseProjection()
			release, err := hub.AttachSeat(pair, func(ctx context.Context, req Request) (Response, error) { return actLocal(ctx, sys, host, req) })
			if err != nil {
				return err
			}
			defer release()
			return serveConcurrent(sys, func(msg actorbase.Msg) {
				if msg.Type == InboundEvent && msg.Sender.ID == sys.Self() {
					return
				}
				if msg.Kind == message.KindRequest {
					words, err := describe(msg.Ctx())
					if err != nil {
						relay(sys, msg, Response{}, err)
						return
					}
					if _, ok := words[msg.Type]; !ok {
						_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("body does not declare %q", msg.Type))
						return
					}
				}
				response, err := hub.Deliver(msg.Ctx(), pair, Request{Envelope: wireEnvelope(msg.Envelope), Await: msg.Kind == message.KindRequest, OnProgress: progressRelay(sys, msg)})
				if msg.Kind == message.KindRequest {
					relay(sys, msg, response, err)
				}
			})
		}, nil
	}}
}
func HandleDef(hub *Hub, body channel.ID, cfg HandleConfig, members Members) actorbase.Def {
	words := map[string]introspect.WordSpec{}
	for _, word := range []string{HandleCall, HandlePost, HandleEmit} {
		words[word] = introspect.WordSpec{
			Description: "act as this body's member Seat in the host channel",
			InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false,"required":["type","payload"],"properties":{"type":{"type":"string","minLength":1},"payload":{},"audience":{"type":"array","items":{"type":"string"}},"visibility":{"type":"string"}}}`),
		}
	}
	pair := Pair{Host: cfg.Host, Body: body}
	return actorbase.Def{Manifest: introspect.Manifest{Class: HandleClass, Interfaces: []string{"actor", "channel-handle"}, Words: words}, New: func() (actorbase.Proc, error) {
		if hub == nil || members == nil || !pair.valid() {
			return nil, errors.New("invalid handle binding")
		}
		return func(sys actorbase.Sys) error {
			release, err := hub.AttachHandle(pair, func(ctx context.Context, req Request) (Response, error) {
				if req.Type == introspect.QueryDescribe {
					raw, _ := json.Marshal(map[string]any{"words": cfg.ManifestWords()})
					return Response{Payload: raw}, nil
				}
				if word, ok := cfg.Words[req.Type]; ok {
					target, err := resolveWordTarget(ctx, sys.State(), members, req.Type, word.Target)
					if err != nil {
						return Response{}, err
					}
					req.Audience = message.Audience{target}
				} else if req.Kind == message.KindEvent {
					req.Audience = nil
				} else {
					return Response{}, &actorbase.TargetResolveError{Code: "type_unsupported", Target: req.Type}
				}
				return actLocal(ctx, sys, body, req)
			})
			if err != nil {
				return err
			}
			defer release()
			return serveConcurrent(sys, func(msg actorbase.Msg) {
				// Incoming events are notifications, never outbound drive instructions.
				if msg.Kind != message.KindRequest {
					return
				}
				if msg.Type != HandleCall && msg.Type != HandlePost && msg.Type != HandleEmit {
					_, _ = sys.Fail(msg, "type_unsupported", "unknown handle word")
					return
				}
				caller := actorbase.EffectiveCaller(msg)
				if caller.Channel != body {
					_, _ = sys.Fail(msg, "forbidden", "driver must be a body channel member")
					return
				}
				facts, found, err := members.ActorFacts(msg.Ctx(), caller.Actor)
				if err != nil {
					relay(sys, msg, Response{}, err)
					return
				}
				allowed := found && facts.Active
				if allowed && len(cfg.Drivers) > 0 {
					allowed = false
					for _, decl := range cfg.Drivers {
						if facts.SourceDeclID == decl {
							allowed = true
							break
						}
					}
				}
				if !allowed {
					_, _ = sys.Fail(msg, "forbidden", "driver is inactive or not permitted")
					return
				}
				var input struct {
					Type       string             `json:"type"`
					Payload    json.RawMessage    `json:"payload"`
					Audience   message.Audience   `json:"audience"`
					Visibility message.Visibility `json:"visibility"`
				}
				if err := actorbase.DecodeStrict(msg.Payload, &input); err != nil || input.Type == "" || len(input.Payload) == 0 || (msg.Type == HandleCall && len(input.Audience) != 1) {
					_, _ = sys.Fail(msg, "invalid_args", "type and payload required; call requires exactly one audience member")
					return
				}
				env := msg.Envelope
				env.Type = input.Type
				env.Payload = input.Payload
				env.Audience = input.Audience
				env.Visibility = input.Visibility
				env.Kind = message.KindRequest
				if msg.Type == HandleEmit {
					env.Kind = message.KindEvent
				}
				response, err := hub.Drive(msg.Ctx(), pair, Request{Envelope: wireEnvelope(env), Await: msg.Type == HandleCall, OnProgress: progressRelay(sys, msg)})
				relay(sys, msg, response, err)
			})
		}, nil
	}}
}
func serveConcurrent(sys actorbase.Sys, fn func(actorbase.Msg)) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		if msg.Kind != message.KindRequest && msg.Kind != message.KindEvent {
			continue
		}
		wg.Add(1)
		go func() { defer wg.Done(); fn(msg) }()
	}
}
func actLocal(ctx context.Context, sys actorbase.Sys, local channel.ID, req Request) (Response, error) {
	if req.Type == "" {
		return Response{}, errors.New("message type required")
	}
	if req.Kind == message.KindRequest {
		var wrapped struct {
			Body json.RawMessage `json:"body"`
		}
		if err := actorbase.DecodeStrict(req.Payload, &wrapped); err != nil {
			return Response{}, err
		}
		if len(wrapped.Body) == 0 {
			return Response{}, errors.New("request body required")
		}
		req.Payload = wrapped.Body
	}
	spec := behavior.RequestSpec{Cause: message.Root(), Type: req.Type, Payload: req.Payload, Audience: req.Audience, Visibility: req.Visibility, ExpiresAt: req.ExpiresAt}
	var id message.ID
	var pending actorbase.Pending
	var err error
	switch {
	case req.Kind == message.KindEvent:
		id, err = sys.Emit(behavior.EventSpec{Cause: message.Root(), Type: req.Type, Payload: req.Payload, Audience: req.Audience, Visibility: req.Visibility})
	case req.Kind == message.KindRequest && !req.Await:
		id, err = sys.Post(spec)
	case req.Kind == message.KindRequest && len(req.Audience) == 1:
		// The existing full-spec API preserves deadline/visibility. Caller is
		// explicitly the LOCAL organ, never the foreign driver.
		pending, err = sys.CallSpecFor(harness.Caller{Channel: local, Actor: sys.Self()}, spec)
		if err == nil {
			id = pending.RequestID()
		}
	default:
		return Response{}, errors.New("call requires exactly one audience member")
	}
	if err != nil {
		return Response{}, err
	}
	if req.ID != "" {
		audit, _ := behavior.EventSpecJSON(message.Anchored(id, id), InboundEvent, map[string]any{"from": map[string]any{"channel": req.ChannelID, "actor": req.Sender.ID, "request": req.ID}, "type": req.Type, "local_request_id": id})
		audit.Audience = message.Audience{sys.Self()}
		if _, err = sys.Emit(audit); err != nil {
			if pending != nil {
				_ = pending.Cancel()
			}
			return Response{}, err
		}
	}
	if pending == nil {
		raw, _ := json.Marshal(map[string]any{"message_id": id})
		return Response{Payload: raw}, nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for p := range pending.Progress() {
			if req.OnProgress != nil {
				status, payload := splitStatus(p.Payload)
				req.OnProgress(Progress{Status: status, Payload: payload})
			}
		}
	}()
	terminal, err := pending.Wait(ctx, 0)
	if err != nil {
		_ = pending.Cancel()
	}
	<-done
	if err != nil {
		return Response{}, err
	}
	return Response{Payload: append([]byte(nil), terminal.Payload...)}, nil
}

// Msg exposes an already-unwrapped application payload. Reconstitute the
// ordinary request envelope at the seam; foreign caller attribution never
// crosses as local authority. The receiving organ unwraps before its own Call.
func wireEnvelope(env message.Envelope) message.Envelope {
	if env.Kind == message.KindRequest {
		env.Payload, _ = json.Marshal(struct {
			Body json.RawMessage `json:"body"`
		}{env.Payload})
	}
	return env
}
func progressRelay(sys actorbase.Sys, msg actorbase.Msg) func(Progress) {
	return func(p Progress) {
		if msg.Kind != message.KindRequest {
			return
		}
		var body any = json.RawMessage(p.Payload)
		if len(p.Payload) == 0 {
			body = map[string]any{}
		}
		_, _ = sys.Progress(msg, p.Status, body)
	}
}
func splitStatus(payload []byte) (string, []byte) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return message.StatusProcessing, append([]byte(nil), payload...)
	}
	status := message.StatusProcessing
	if raw := fields["status"]; len(raw) > 0 {
		_ = json.Unmarshal(raw, &status)
	}
	delete(fields, "status")
	body, err := json.Marshal(fields)
	if err != nil {
		return status, payload
	}
	return status, body
}
func relay(sys actorbase.Sys, msg actorbase.Msg, response Response, err error) {
	if err != nil {
		code := "channel_unavailable"
		var targetErr *actorbase.TargetResolveError
		if errors.As(err, &targetErr) {
			code = targetErr.Code
		}
		_, _ = sys.Fail(msg, code, err.Error())
		return
	}
	var terminal struct {
		Status string `json:"status"`
		message.Failure
	}
	if json.Unmarshal(response.Payload, &terminal) == nil && terminal.Status == message.StatusFailed {
		code := terminal.ErrorCode
		if code == "" {
			code = "receiver_internal_error"
		}
		_, _ = sys.Fail(msg, code, terminal.Detail)
		return
	}
	body := json.RawMessage(response.Payload)
	if len(body) == 0 {
		body = json.RawMessage("{}")
	}
	_, _ = sys.Reply(msg, body)
}
