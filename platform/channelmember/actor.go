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
	InboundEvent = "channelmember.inbound"
)

type Config struct {
	Host     channel.ID `json:"host_channel"`
	Body     channel.ID `json:"body_channel"`
	Receiver string     `json:"receiver,omitempty"`
}

func ParseConfig(raw json.RawMessage) (Config, error) {
	var cfg Config
	if err := actorbase.DecodeStrict(raw, &cfg); err != nil {
		return Config{}, err
	}
	if cfg.Host == "" || cfg.Body == "" || cfg.Host == cfg.Body {
		return Config{}, errors.New("host_channel and distinct body_channel required")
	}
	return cfg, nil
}

func SeatDef(hub *Hub, actualHost channel.ID, cfg Config) actorbase.Def {
	pair := Pair{Host: cfg.Host, Body: cfg.Body}
	return actorbase.Def{Manifest: introspect.Manifest{
		Class: SeatClass, Interfaces: []string{"actor", "channel"}, Words: map[string]introspect.WordSpec{},
		Dynamic: func(ctx context.Context) (map[string]introspect.WordSpec, error) {
			response, err := hub.Deliver(ctx, pair, Request{Type: introspect.QueryDescribe, Payload: []byte(`{}`)})
			if err != nil {
				return nil, err
			}
			var described struct {
				Status string                         `json:"status"`
				Words  map[string]introspect.WordSpec `json:"words"`
			}
			if err := json.Unmarshal(response.Payload, &described); err != nil {
				return nil, fmt.Errorf("channel-seat: decode body manifest: %w", err)
			}
			if described.Status != message.StatusCompleted {
				return nil, errors.New("channel-seat: body manifest unavailable")
			}
			return described.Words, nil
		},
	}, New: func() (actorbase.Proc, error) {
		if hub == nil || actualHost != cfg.Host {
			return nil, errors.New("channel-seat: host mismatch")
		}
		return func(sys actorbase.Sys) error { return runSeat(sys, hub, cfg) }, nil
	}}
}

func HandleDef(hub *Hub, actualBody channel.ID, cfg Config) actorbase.Def {
	return actorbase.Def{Manifest: introspect.Manifest{Class: HandleClass, Interfaces: []string{"actor", "channel-handle"}, Words: map[string]introspect.WordSpec{
		HandleCall: {
			Description: "act through this body channel's Seat in its host; target may be any host member, including system for discovery",
			InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false,"required":["target","type","payload"],"properties":{"target":{"type":"string"},"type":{"type":"string"},"payload":{}}}`),
			Examples: []json.RawMessage{
				json.RawMessage(`{"target":"system","type":"system.member.list","payload":{}}`),
				json.RawMessage(`{"target":"tool:device:1","type":"workspace.read","payload":{"path":"README.md"}}`),
			},
		},
	}}, New: func() (actorbase.Proc, error) {
		if hub == nil || actualBody != cfg.Body || cfg.Receiver == "" {
			return nil, errors.New("channel-handle: body/receiver mismatch")
		}
		return func(sys actorbase.Sys) error { return runHandle(sys, hub, cfg) }, nil
	}}
}

func runSeat(sys actorbase.Sys, hub *Hub, cfg Config) error {
	pair := Pair{Host: cfg.Host, Body: cfg.Body}
	release, err := hub.AttachSeat(pair, func(ctx context.Context, req Request) (Response, error) {
		return callLocal(ctx, sys, req)
	})
	if err != nil {
		return err
	}
	defer release()
	releaseWatch, err := hub.WatchHandle(pair, func(online bool) {
		_ = sys.PublishObs(introspect.ObsDevicePresence, introspect.MarshalDevicePresence(online))
	})
	if err != nil {
		return err
	}
	defer releaseWatch()
	return serveConcurrent(sys, func(msg actorbase.Msg) {
		caller := actorbase.EffectiveCaller(msg)
		response, err := hub.Deliver(msg.Ctx(), pair, Request{Type: msg.Type, Payload: append([]byte(nil), msg.Payload...), CallerChannel: caller.Channel, CallerActor: string(caller.Actor), CallerRequestID: string(msg.ID), Deadline: deadline(msg), OnProgress: progressRelay(sys, msg)})
		relay(sys, msg, response, err)
	})
}

func runHandle(sys actorbase.Sys, hub *Hub, cfg Config) error {
	pair := Pair{Host: cfg.Host, Body: cfg.Body}
	release, err := hub.AttachHandle(pair, func(ctx context.Context, req Request) (Response, error) {
		req.Target = cfg.Receiver
		return callLocal(ctx, sys, req)
	})
	if err != nil {
		return err
	}
	defer release()
	return serveConcurrent(sys, func(msg actorbase.Msg) {
		if msg.Type != HandleCall {
			_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("channel handle does not answer %q", msg.Type))
			return
		}
		var body struct {
			Target  string          `json:"target"`
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := actorbase.DecodeStrict(msg.Payload, &body); err != nil || body.Target == "" || body.Type == "" {
			_, _ = sys.Fail(msg, "invalid_args", "target and type required")
			return
		}
		caller := actorbase.EffectiveCaller(msg)
		response, err := hub.Drive(msg.Ctx(), pair, Request{Target: body.Target, Type: body.Type, Payload: append([]byte(nil), body.Payload...), CallerChannel: caller.Channel, CallerActor: string(caller.Actor), CallerRequestID: string(msg.ID), Deadline: deadline(msg), OnProgress: progressRelay(sys, msg)})
		relay(sys, msg, response, err)
	})
}

func serveConcurrent(sys actorbase.Sys, fn func(actorbase.Msg)) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		if msg.Kind != message.KindRequest {
			continue
		}
		wg.Add(1)
		go func() { defer wg.Done(); fn(msg) }()
	}
}

func callLocal(ctx context.Context, sys actorbase.Sys, req Request) (Response, error) {
	if req.Target == "" || req.Type == "" {
		return Response{}, errors.New("channelmember: target and type required")
	}
	spec := behavior.RequestSpec{Cause: message.Root(), Type: req.Type, Payload: json.RawMessage(req.Payload), Audience: message.Audience{actor.ActorID(req.Target)}}
	if req.Deadline > 0 {
		spec.ExpiresAt = &req.Deadline
	}
	var pending actorbase.Pending
	var err error
	if req.CallerChannel == "" && req.CallerActor == "" {
		pending, err = sys.Call(message.Root(), actor.ActorID(req.Target), req.Type, json.RawMessage(req.Payload))
	} else {
		pending, err = sys.CallSpecFor(harness.Caller{Channel: req.CallerChannel, Actor: actor.ActorID(req.CallerActor)}, spec)
	}
	if err != nil {
		return Response{}, err
	}
	if req.CallerRequestID != "" {
		audit, auditErr := behavior.EventSpecJSON(
			message.Anchored(pending.RequestID(), pending.RequestID()),
			InboundEvent,
			map[string]any{
				"from": map[string]any{
					"channel": req.CallerChannel,
					"actor":   req.CallerActor,
					"request": req.CallerRequestID,
				},
				"type":             req.Type,
				"local_request_id": pending.RequestID(),
			},
		)
		if auditErr != nil {
			_ = pending.Cancel()
			return Response{}, auditErr
		}
		if _, auditErr = sys.Emit(audit); auditErr != nil {
			_ = pending.Cancel()
			return Response{}, auditErr
		}
	}
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		for progress := range pending.Progress() {
			if req.OnProgress == nil {
				continue
			}
			status, payload := splitStatus(progress.Payload)
			req.OnProgress(Progress{Status: status, Payload: payload})
		}
	}()
	terminal, err := pending.Wait(ctx, 0)
	if err != nil {
		_ = pending.Cancel()
		<-progressDone
		return Response{}, err
	}
	<-progressDone
	return Response{Payload: append([]byte(nil), terminal.Payload...)}, nil
}

func progressRelay(sys actorbase.Sys, msg actorbase.Msg) func(Progress) {
	return func(progress Progress) {
		body := any(json.RawMessage(progress.Payload))
		if len(progress.Payload) == 0 {
			body = map[string]any{}
		}
		_, _ = sys.Progress(msg, progress.Status, body)
	}
}

func splitStatus(payload []byte) (string, []byte) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return message.StatusProcessing, append([]byte(nil), payload...)
	}
	status := message.StatusProcessing
	if raw := fields["status"]; len(raw) != 0 {
		_ = json.Unmarshal(raw, &status)
	}
	delete(fields, "status")
	body, err := json.Marshal(fields)
	if err != nil {
		return status, append([]byte(nil), payload...)
	}
	return status, body
}

func relay(sys actorbase.Sys, msg actorbase.Msg, response Response, err error) {
	if err != nil {
		_, _ = sys.Fail(msg, "channel_unavailable", err.Error())
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
		body = json.RawMessage(`{}`)
	}
	_, _ = sys.Reply(msg, body)
}

func deadline(msg actorbase.Msg) int64 {
	if msg.ExpiresAt == nil {
		return 0
	}
	return *msg.ExpiresAt
}
