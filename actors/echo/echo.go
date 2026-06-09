// Package echo provides a minimal actor for dev/test. It echoes back the
// received payload as a response — no external dependencies, no credentials.
package echo

import (
	"context"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

const DefaultActorID actor.ActorID = "echo"
const TypePing = "echo.ping"

const actorDescription = "Minimal dev/test actor that echoes payloads for echo.ping and answers actor.describe with its self-description."

const actorSkillDoc = "" +
	"# echo\n" +
	"\n" +
	"Minimal dev/test actor.\n" +
	"\n" +
	"## Tool surface\n" +
	"\n" +
	"- `echo.ping` — echo the payload back in a completed response.\n" +
	"\n" +
	"## Describe surface\n" +
	"\n" +
	"- `actor.describe` — returns the actor id, skill doc, and one type entry for `echo.ping`.\n"

type Actor struct {
	writer  harness.Writer
	actorID actor.ActorID
	clock   func() time.Time
}

func NewActor(w harness.Writer) *Actor {
	return &Actor{writer: w, actorID: DefaultActorID, clock: time.Now}
}

func (a *Actor) Receive(ctx context.Context, env *message.Envelope) error {
	switch env.Type {
	case TypePing:
		return a.respond(ctx, env, map[string]any{
			"echo":          true,
			"original_id":   string(env.ID),
			"original_type": env.Type,
		})
	case introspect.QueryDescribe:
		return a.handleDescribe(ctx, env)
	default:
		return a.fail(ctx, env, "type_unsupported", fmt.Sprintf("echo actor does not handle %s", env.Type))
	}
}

var _ actorrt.Actor = (*Actor)(nil)

func (a *Actor) sender() message.Sender {
	return message.Sender{ID: a.actorID, Kind: actor.KindTool}
}

func (a *Actor) respond(ctx context.Context, env *message.Envelope, result any) error {
	_, err := behavior.RespondJSON(ctx, a.writer, a.clock, env, a.sender(), result)
	return err
}

func (a *Actor) fail(ctx context.Context, env *message.Envelope, errorCode, detail string) error {
	_, err := behavior.Fail(ctx, a.writer, a.clock, env, a.sender(), errorCode, detail)
	return err
}

func (a *Actor) handleDescribe(ctx context.Context, env *message.Envelope) error {
	req, err := introspect.ParseDescribeRequest(env.Payload)
	if err != nil {
		return a.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode describe payload: %v", err))
	}
	answer, ok := introspect.AnswerDescribe(introspect.Describe{
		ActorID:     string(a.actorID),
		Description: actorDescription,
		SkillDoc:    actorSkillDoc,
		Types: map[string]introspect.TypeMeta{
			TypePing: {
				Description:  "Echo the request payload back in a completed response.",
				AllowedKinds: []string{string(message.KindRequest)},
				MaxPendingMs: 30_000,
				Notes:        "Generic probe for daemon-attached dev/test flows.",
			},
		},
	}, req)
	if !ok {
		return a.fail(ctx, env, "type_unsupported", fmt.Sprintf("echo actor does not handle %s", req.Type))
	}
	return a.respond(ctx, env, answer)
}
