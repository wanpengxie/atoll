package peeractor

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type Seam func(context.Context, channel.ID, channel.ID, channel.Request, func(channel.Progress)) (channel.Result, error)
type Describe func(context.Context, channel.ID, channel.ID, channel.Describe) (channel.Card, error)

type Deps struct {
	Caller   channel.ID
	Target   channel.ID
	Seam     Seam
	Describe Describe
}

func ValidateConfig(raw json.RawMessage) (channel.ID, error) {
	var cfg struct {
		Channel channel.ID `json:"channel"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil || cfg.Channel == "" {
		return "", errors.New("peeractor: config.channel required")
	}
	return cfg.Channel, nil
}

func Def(deps Deps) actorbase.Def {
	return actorbase.Def{Manifest: introspect.Manifest{
		Class: "peeractor", Interfaces: []string{"actor", "peer"}, Words: map[string]introspect.WordSpec{},
		Dynamic: func(ctx context.Context) (map[string]introspect.WordSpec, error) {
			card, err := deps.Describe(ctx, deps.Caller, deps.Target, channel.Describe{From: channel.DescribeFrom{Channel: deps.Caller}})
			if err != nil {
				return nil, err
			}
			words := make(map[string]introspect.WordSpec, len(card.Words))
			for name, raw := range card.Words {
				var spec introspect.WordSpec
				if err := json.Unmarshal(raw, &spec); err != nil {
					return nil, err
				}
				words[name] = spec
			}
			return words, nil
		},
	}, New: func() (actorbase.Proc, error) {
		if deps.Caller == "" || deps.Target == "" || deps.Seam == nil || deps.Describe == nil {
			return nil, errors.New("peeractor: incomplete dependencies")
		}
		return func(sys actorbase.Sys) error { return serve(sys, deps) }, nil
	}}
}

func serve(sys actorbase.Sys, deps Deps) error {
	var inFlight sync.WaitGroup
	defer inFlight.Wait()
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		if msg.Kind != message.KindRequest {
			continue
		}
		inFlight.Add(1)
		go func(msg actorbase.Msg) {
			defer inFlight.Done()
			handle(sys, deps, msg)
		}(msg)
	}
}

func handle(sys actorbase.Sys, deps Deps, msg actorbase.Msg) {
	caller := actorbase.EffectiveCaller(msg)
	request := channel.Request{
		From: channel.From{Channel: caller.Channel, Actor: string(caller.Actor), RequestID: string(msg.ID)},
		Type: msg.Type, Payload: append(json.RawMessage(nil), msg.Payload...),
	}
	if msg.ExpiresAt != nil {
		request.Deadline = *msg.ExpiresAt
	}
	result, err := deps.Seam(msg.Ctx(), deps.Caller, deps.Target, request, func(progress channel.Progress) {
		body := any(json.RawMessage(progress.Body))
		if len(progress.Body) == 0 {
			body = map[string]any{}
		}
		_, _ = sys.Progress(msg, progress.Status, body)
	})
	if err != nil {
		_, _ = sys.Fail(msg, "channel_unavailable", err.Error())
		return
	}
	if result.Fail != nil {
		_, _ = sys.Fail(msg, result.Fail.Code, result.Fail.Detail)
		return
	}
	body := result.Body
	if len(body) == 0 {
		body = json.RawMessage(`{}`)
	}
	_, _ = sys.Reply(msg, body)
}
