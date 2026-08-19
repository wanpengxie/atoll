package sysactor

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

// Peer is the gate's one injected external dependency. The caller channel is
// welded by the assembly closure; the gate supplies only the frame and the
// optional ordered progress sink.
type Peer func(context.Context, channel.Request, func(channel.Progress)) (channel.Result, error)

func (s *SystemActor) routeSpace(sys actorbase.Sys, msg actorbase.Msg) {
	caller := actorbase.EffectiveCaller(msg)
	go func() {
		if msg.ChannelID == channelspec.C0ChannelID {
			pending, err := sys.CallFor(caller, actor.ActorID("system:registrar"), msg.Type, json.RawMessage(msg.Payload))
			if err != nil {
				_, _ = sys.Fail(msg, routeErrorCode(err), err.Error())
				return
			}
			terminal, err := pending.Wait(msg.Ctx(), 0)
			if err != nil {
				_, _ = sys.Fail(msg, "channel_unavailable", err.Error())
				return
			}
			relayTerminal(sys, msg, terminal.Payload)
			return
		}
		if s.peer == nil {
			_, _ = sys.Fail(msg, "channel_unavailable", "c0 peer port unavailable")
			return
		}
		frame := channel.Request{
			From: channel.From{
				Channel: msg.ChannelID, Actor: string(caller.Actor), RequestID: string(msg.ID),
			},
			Type: msg.Type, Payload: append(json.RawMessage(nil), msg.Payload...),
		}
		if msg.ExpiresAt != nil {
			frame.Deadline = *msg.ExpiresAt
		}
		result, err := s.peer(msg.Ctx(), frame, func(progress channel.Progress) {
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
		body := any(json.RawMessage(result.Body))
		if len(result.Body) == 0 {
			body = map[string]any{}
		}
		_, _ = sys.Reply(msg, body)
	}()
}

func routeErrorCode(err error) string {
	var targetErr *actorbase.TargetResolveError
	if errors.As(err, &targetErr) {
		return targetErr.Code
	}
	return "internal_error"
}

func relayTerminal(sys actorbase.Sys, request actorbase.Msg, raw json.RawMessage) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		_, _ = sys.Fail(request, "internal_error", "registrar returned an invalid terminal")
		return
	}
	var status string
	_ = json.Unmarshal(fields["status"], &status)
	if status == message.StatusFailed {
		var failure message.Failure
		_ = json.Unmarshal(raw, &failure)
		if failure.ErrorCode == "" {
			failure.ErrorCode = "internal_error"
		}
		_, _ = sys.Fail(request, failure.ErrorCode, failure.Detail)
		return
	}
	if status != message.StatusCompleted {
		_, _ = sys.Fail(request, "internal_error", "registrar returned a non-terminal response")
		return
	}
	delete(fields, "status")
	delete(fields, "reason")
	body, err := json.Marshal(fields)
	if err != nil {
		_, _ = sys.Fail(request, "internal_error", err.Error())
		return
	}
	_, _ = sys.Reply(request, json.RawMessage(body))
}
