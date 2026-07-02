// Package echo provides a minimal actor for dev/test. It echoes back the
// received payload as a response — no external dependencies, no credentials.
package echo

import (
	"context"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
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
	pen     harness.Pen
	actorID actor.ActorID
	clock   func() time.Time
}

func NewActor(w harness.Pen) *Actor {
	return &Actor{pen: w, actorID: DefaultActorID, clock: time.Now}
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

func (a *Actor) respond(ctx context.Context, env *message.Envelope, result any) error {
	_, err := behavior.RespondJSON(ctx, a.pen, a.clock, env, result)
	return err
}

func (a *Actor) fail(ctx context.Context, env *message.Envelope, errorCode, detail string) error {
	_, err := behavior.Fail(ctx, a.pen, a.clock, env, errorCode, detail)
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
