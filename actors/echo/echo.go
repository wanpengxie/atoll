// Package echo provides a minimal actor for dev/test. It echoes back the
// received payload as a response — no external dependencies, no credentials.
package echo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

const DefaultActorID actor.ActorID = "echo"

type Actor struct {
	writer  harness.Writer
	actorID actor.ActorID
}

func NewActor(w harness.Writer) *Actor {
	return &Actor{writer: w, actorID: DefaultActorID}
}

func (a *Actor) Receive(ctx context.Context, env *message.Envelope) error {
	raw, _ := json.Marshal(map[string]any{
		"status":      "completed",
		"echo":        true,
		"original_id": string(env.ID),
	})
	now := time.Now().UnixMilli()
	resp := &message.Envelope{
		ID:            message.ID(uuid.NewString()),
		TS:            now,
		Type:          env.Type + ".response",
		Sender:        message.Sender{ID: a.actorID, Kind: actor.KindTool},
		Audience:      message.Audience{env.Sender.ID},
		ParentID:      env.ID,
		CorrelationID: env.CorrelationID,
		ChannelID:     env.ChannelID,
		Kind:          message.KindResponse,
		Payload:       raw,
	}
	_, err := a.writer.Write(ctx, resp)
	return err
}

var _ actorrt.Actor = (*Actor)(nil)
