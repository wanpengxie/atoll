package channelmember

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
				response, err := hub.Deliver(msg.Ctx(), pair, Request{Envelope: wireEnvelope(msg.Envelope, msg.Context()), Await: msg.Kind == message.KindRequest, OnProgress: progressRelay(sys, msg)})
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
				response, err := hub.Drive(msg.Ctx(), pair, Request{Envelope: wireEnvelope(env, msg.Context()), Await: msg.Type == HandleCall, OnProgress: progressRelay(sys, msg)})
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

var errInvalidRequest = errors.New("invalid channel message")

// deliveryError preserves the receipt when waiting fails after a successful
// dispatch. Cancellation of the wait does not prove the receiver did no work.
type deliveryError struct {
	id  message.ID
	err error
}

func (e *deliveryError) Error() string {
	return fmt.Sprintf("request %s was dispatched; outcome unknown: %v", e.id, e.err)
}
func (e *deliveryError) Unwrap() error { return e.err }

func actLocal(ctx context.Context, sys actorbase.Sys, local channel.ID, req Request) (Response, error) {
	if req.Type == "" {
		return Response{}, fmt.Errorf("%w: message type required", errInvalidRequest)
	}
	app, body, unwrapErr := harness.UnwrapPayload(req.Payload)
	if unwrapErr != nil {
		return Response{}, fmt.Errorf("%w: %v", errInvalidRequest, unwrapErr)
	}
	req.Payload = body
	// This boundary owns the local caller; foreign caller metadata is not inherited.
	app.Caller = nil
	if req.Kind == message.KindRequest && req.Await {
		app.Caller = &harness.Caller{Channel: local, Actor: sys.Self()}
	}
	var err error

	spec := behavior.RequestSpec{Cause: message.Root(), Context: app, Type: req.Type, Payload: req.Payload, Audience: req.Audience, Visibility: req.Visibility, ExpiresAt: req.ExpiresAt}
	var id message.ID
	var pending actorbase.Pending
	switch {
	case req.Kind == message.KindEvent:
		id, err = sys.Emit(behavior.EventSpec{Cause: message.Root(), Context: app, Type: req.Type, Payload: req.Payload, Audience: req.Audience, Visibility: req.Visibility})
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
		return Response{}, fmt.Errorf("%w: call requires exactly one audience member", errInvalidRequest)
	}
	if err != nil {
		return Response{}, err
	}
	if req.ID != "" {
		audit, _ := behavior.EventSpecJSON(message.Anchored(id, id), app, InboundEvent, map[string]any{"from": map[string]any{"channel": req.ChannelID, "actor": req.Sender.ID, "request": req.ID}, "type": req.Type, "local_request_id": id})
		audit.Audience = message.Audience{sys.Self()}
		if _, err = sys.Emit(audit); err != nil {
			// The action is already committed. Failure of this auxiliary record
			// cannot undo dispatch or justify cancelling a successful Call.
			slog.Error("channelmember.audit_failed", "channel", local, "actor", sys.Self(), "local_request_id", id, "source_request_id", req.ID, "err", err)
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
		return Response{}, &deliveryError{id: id, err: err}
	}
	return Response{Payload: append([]byte(nil), terminal.Payload...)}, nil
}

// Msg exposes an already-unwrapped application payload. Reconstitute the
// ordinary request envelope at the seam; foreign caller attribution never
// crosses as local authority. The receiving organ unwraps before its own Call.
func wireEnvelope(env message.Envelope, contexts ...harness.Context) message.Envelope {
	app := harness.Context{}
	if len(contexts) > 0 {
		app = contexts[0]
	}
	env.Payload, _ = harness.WrapPayload(app, env.Payload)
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
		code := "internal_error"
		var coded interface{ ErrorCode() string }
		var rejected *actorbase.WriteRejected
		var visibility *actorbase.InvalidVisibilityError
		switch {
		case errors.As(err, &coded):
			code = coded.ErrorCode()
		case errors.As(err, &rejected):
			code = rejected.Reason
		case errors.Is(err, errInvalidRequest), errors.As(err, &visibility):
			code = "bad_payload"
		case errors.Is(err, context.Canceled):
			code = "cancelled"
		case errors.Is(err, context.DeadlineExceeded):
			code = "deadline_exceeded"
		case errors.Is(err, actorbase.ErrCallClosed):
			code = "call_closed"
		case errors.Is(err, ErrUnreachable):
			code = "channel_unavailable"
		}
		if code == "" {
			code = "internal_error"
		}
		var dispatched *deliveryError
		if errors.As(err, &dispatched) {
			_, _ = sys.Fail(msg, code, err.Error(), map[string]any{"delivery": "dispatched", "local_request_id": dispatched.id, "outcome": "unknown"})
			return
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
		var fields map[string]any
		_ = json.Unmarshal(response.Payload, &fields)
		delete(fields, "status")
		delete(fields, "error_code")
		delete(fields, "detail")
		_, _ = sys.Fail(msg, code, terminal.Detail, fields)
		return
	}
	body := json.RawMessage(response.Payload)
	if len(body) == 0 {
		body = json.RawMessage("{}")
	}
	_, _ = sys.Reply(msg, body)
}
