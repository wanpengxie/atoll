package feishu

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// DefaultActorID is the actor_registry row this adapter binds to.
const DefaultActorID actor.ActorID = "tool:feishu-adapter"

// DefaultMaxPendingMs is the per-request timeout (30s) used when the
// caller does not override.
const DefaultMaxPendingMs int64 = 30_000

// Actor implements actorrt.Actor for the feishu outbound API.
type Actor struct {
	writer  harness.Writer
	actorID actor.ActorID
	client  *client
	clock   func() time.Time
	logger  *slog.Logger
	creds   CredentialBundle
	tokens  *tokenCache
}

// NewActor constructs a feishu Actor. Credentials are loaded from the
// CredentialBundle (caller provides). writer is the harness writer for
// committing responses to truth.
func NewActor(writer harness.Writer, creds CredentialBundle, logger *slog.Logger) (*Actor, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	tokens := newTokenCache(time.Now)
	httpClient := &http.Client{Timeout: 30 * time.Second}
	c := newClientWithHTTP(httpClient, creds, tokens, logger)
	return &Actor{
		writer:  writer,
		actorID: DefaultActorID,
		client:  c,
		clock:   time.Now,
		logger:  logger,
		creds:   creds,
		tokens:  tokens,
	}, nil
}

// Receive dispatches by env.Type. Unknown types are rejected with a
// failed terminal so the caller observes a definite outcome instead of
// waiting for the framework timer.
func (a *Actor) Receive(ctx context.Context, env *message.Envelope) error {
	switch env.Type {
	case TypeChatSend:
		return a.handleChatSend(ctx, env)
	case TypeChatCreate:
		return a.handleChatCreate(ctx, env)
	}
	return a.fail(ctx, env, "type_unsupported", fmt.Sprintf("feishu actor does not handle %s", env.Type))
}

var _ actorrt.Actor = (*Actor)(nil)
