package store_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/internal/store"
)

// testChannelID is the channel every helper-opened sqlite is bound to.
const testChannelID channel.ID = "C-test"

// openTestChannel opens a fresh temp-dir channel sqlite (full DDL) and
// registers cleanup. Uses the real sqlite store per the test brief — no fakes.
func openTestChannel(t *testing.T) *store.ChannelStores {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	cs, err := store.OpenChannel(ctx, testChannelID, filepath.Join(dir, "messages.sqlite"), store.OpenOptions{}, nil)
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// openTestChannelOnCommit is openTestChannel with a post-commit signal sink
// wired — the commit-signal coverage tests assert which write paths fire it.
func openTestChannelOnCommit(t *testing.T, onCommit func()) *store.ChannelStores {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	cs, err := store.OpenChannel(ctx, testChannelID, filepath.Join(dir, "messages.sqlite"), store.OpenOptions{}, onCommit)
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// envOpt mutates an envelope under construction.
type envOpt func(*message.Envelope)

// newEnv builds a minimally-valid envelope (request payloads use the canonical
// body envelope)
// addressed to `audience`, with the given id/kind. Options override fields.
func newEnv(id string, kind message.Kind, audience message.Audience, opts ...envOpt) *message.Envelope {
	env := &message.Envelope{
		ID:         message.ID(id),
		TS:         1000,
		TSReceived: 1000,
		ChannelID:  testChannelID,
		Sender:     message.Sender{Kind: actor.KindHuman, ID: "alice"},
		Kind:       kind,
		Type:       "human.text",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   audience,
	}
	for _, o := range opts {
		o(env)
	}
	if kind == message.KindRequest {
		body := append(json.RawMessage(nil), env.Payload...)
		env.Payload, _ = json.Marshal(struct {
			Body json.RawMessage `json:"body"`
		}{Body: body})
	}
	return env
}

func withSender(kind actor.Kind, id actor.ActorID) envOpt {
	return func(e *message.Envelope) { e.Sender = message.Sender{Kind: kind, ID: id} }
}
func withParent(parent message.ID) envOpt {
	return func(e *message.Envelope) { e.ParentID = parent }
}
func withType(typ string) envOpt { return func(e *message.Envelope) { e.Type = typ } }
func withPayload(p string) envOpt {
	return func(e *message.Envelope) { e.Payload = json.RawMessage(p) }
}
func withVisibility(v message.Visibility) envOpt {
	return func(e *message.Envelope) { e.Visibility = v }
}
func withExpiresAt(at int64) envOpt { return func(e *message.Envelope) { e.ExpiresAt = &at } }
func withCorrelation(c message.ID) envOpt {
	return func(e *message.Envelope) { e.CorrelationID = c }
}
