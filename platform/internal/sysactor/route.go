package sysactor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
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
		payload := json.RawMessage(msg.Payload)
		if msg.Type == message.TypeSystemChannelCreate {
			var err *createIntentError
			if caller.Channel == msg.ChannelID {
				payload, err = s.resolveChannelCreate(msg.Ctx(), msg.Payload)
			} else {
				// A cross-channel request has already crossed its source sysactor.
				// Decode the closed internal shape here so a malformed relay never
				// reaches Registrar as an apparently trusted plan.
				var resolved lagoon.ResolvedChannelCreate
				if decodeErr := actorbase.DecodeStrict(msg.Payload, &resolved); decodeErr != nil {
					err = &createIntentError{code: "invalid_args", detail: "resolved channel create payload is invalid: " + decodeErr.Error()}
				}
			}
			if err != nil {
				_, _ = sys.Fail(msg, err.code, err.detail)
				return
			}
		}
		if msg.ChannelID == channelspec.C0ChannelID {
			// A relay continues the errand that arrived here; it does not start
			// one. The registrar hop belongs to the caller's tree.
			pending, err := sys.CallFor(msg.Cause(), caller, actor.ActorID("system:registrar"), msg.Type, payload)
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
			_, _ = sys.Fail(msg, "channel_unavailable", "this channel has no open port to the registry channel, so space words cannot be forwarded from here right now; retry once the link is back")
			return
		}
		frame := channel.Request{
			From: channel.From{
				Channel: msg.ChannelID, Actor: string(caller.Actor), RequestID: string(msg.ID),
			},
			Type: msg.Type, Payload: append(json.RawMessage(nil), payload...),
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

type createIntentError struct {
	code   string
	detail string
}

func (s *SystemActor) resolveChannelCreate(ctx context.Context, raw json.RawMessage) (json.RawMessage, *createIntentError) {
	// A pointer preserves the protocol distinction between an explicitly empty
	// list (create an empty channel) and a missing/null list (the caller did not
	// make the membership decision).
	var wire struct {
		Name            string           `json:"name"`
		Recipe          json.RawMessage  `json:"recipe"`
		InitialActorIDs *[]actor.ActorID `json:"initial_actor_ids"`
	}
	if err := actorbase.DecodeStrict(raw, &wire); err != nil {
		return nil, &createIntentError{code: "invalid_args", detail: "invalid channel create payload: " + err.Error()}
	}
	if wire.InitialActorIDs == nil {
		return nil, &createIntentError{code: "invalid_args", detail: "initial_actor_ids is required; send [] to explicitly create a channel without initial members"}
	}
	if len(*wire.InitialActorIDs) > 64 {
		return nil, &createIntentError{code: "invalid_args", detail: "initial_actor_ids accepts at most 64 current-channel actors"}
	}
	var recipe lagoon.ChannelCreateIntent
	canonical, marshalErr := json.Marshal(map[string]any{
		"name": wire.Name, "recipe": wire.Recipe, "initial_actor_ids": *wire.InitialActorIDs,
	})
	if marshalErr != nil || actorbase.DecodeStrict(canonical, &recipe) != nil {
		return nil, &createIntentError{code: "invalid_args", detail: "recipe must be a JSON object"}
	}
	if s.facts == nil {
		return nil, &createIntentError{code: "internal_error", detail: "channel actor facts authority is unavailable"}
	}
	seats := make([]lagoon.InitialSeatIntent, 0, len(recipe.InitialActorIDs))
	seen := make(map[actor.ActorID]struct{}, len(recipe.InitialActorIDs))
	for index, id := range recipe.InitialActorIDs {
		parsedKind, valid := initialActorIDKind(id)
		if !valid {
			return nil, &createIntentError{
				code:   "actor_id_invalid",
				detail: fmt.Sprintf("initial_actor_ids[%d]=%q is not a complete actor id; use system.member.list and pass the full current-channel actor id", index, id),
			}
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, &createIntentError{code: "duplicate_actor_id", detail: fmt.Sprintf("initial_actor_ids[%d]=%q is duplicated", index, id)}
		}
		seen[id] = struct{}{}
		facts, active, err := s.facts.ActorFacts(ctx, id)
		if err != nil {
			return nil, &createIntentError{code: "internal_error", detail: "current-channel actor lookup failed: " + err.Error()}
		}
		if !active {
			return nil, &createIntentError{
				code:   "actor_not_in_channel",
				detail: fmt.Sprintf("initial_actor_ids[%d]=%q is not an active member of this channel; refresh with system.member.list", index, id),
			}
		}
		if facts.Kind != parsedKind {
			return nil, &createIntentError{code: "internal_error", detail: fmt.Sprintf("actor %q has inconsistent id and registry kinds", id)}
		}
		switch facts.Kind {
		case actor.KindHuman:
			if facts.Principal == "" || facts.SourceDeclID != "" {
				return nil, &createIntentError{code: "actor_identity_incomplete", detail: fmt.Sprintf("human actor %q has no valid principal identity", id)}
			}
		case actor.KindAgent:
			if facts.SourceDeclID == "" {
				return nil, &createIntentError{code: "actor_identity_incomplete", detail: fmt.Sprintf("agent actor %q has no source declaration", id)}
			}
		case actor.KindTool:
			if facts.SourceDeclID == "" || facts.Principal != "" {
				return nil, &createIntentError{code: "actor_identity_incomplete", detail: fmt.Sprintf("tool actor %q has incomplete declaration identity", id)}
			}
		default:
			return nil, &createIntentError{code: "actor_kind_not_importable", detail: fmt.Sprintf("initial_actor_ids[%d]=%q is %s; peer and system actors are created by the platform", index, id, facts.Kind)}
		}
		seats = append(seats, lagoon.InitialSeatIntent{
			SourceActorID: id, Kind: facts.Kind, Principal: facts.Principal, DeclID: facts.SourceDeclID,
		})
	}
	resolved := lagoon.ResolvedChannelCreate{Name: recipe.Name, Recipe: recipe.Recipe, InitialSeats: seats}
	out, err := json.Marshal(resolved)
	if err != nil {
		return nil, &createIntentError{code: "internal_error", detail: err.Error()}
	}
	return out, nil
}

func initialActorIDKind(id actor.ActorID) (actor.Kind, bool) {
	if id == actor.SystemActorID {
		return actor.KindSystem, true
	}
	parts := strings.Split(string(id), ":")
	if len(parts) != 3 || parts[1] == "" {
		return "", false
	}
	kind, ok := actor.ParseKind(parts[0])
	if !ok || kind == actor.KindSystem {
		return "", false
	}
	stamp, err := strconv.ParseInt(parts[2], 10, 64)
	return kind, err == nil && stamp > 0
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
		_, _ = sys.Fail(request, "internal_error", "the registry answered with a payload this door could not read; the request may or may not have taken effect, so check the current state before sending it again")
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
		_, _ = sys.Fail(request, "internal_error", "the registry answered without reaching a final verdict; the request may or may not have taken effect, so check the current state before sending it again")
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
