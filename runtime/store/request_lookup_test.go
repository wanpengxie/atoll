package store_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/store"
)

// TestRequestLookup_FindByID covers the happy path (Append → FindByID
// returns a pointer with matching fields).
func TestRequestLookup_FindByID(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	msgs := store.NewMessages(db)
	lookup := store.NewRequestLookup(msgs, channel.ID("ch-1"))

	env := &message.Envelope{
		ID:         "req-1",
		TS:         1000,
		TSReceived: 1000,
		ChannelID:  "ch-1",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:author"},
		Kind:       message.KindRequest,
		Type:       "xhs.publish",
		Payload:    json.RawMessage(`{"title":"hello"}`),
		Visibility: message.VisibilityPrivate,
		Audience:   message.Audience{"tool:xhs-adapter"},
	}
	if _, err := msgs.Append(ctx, env, klog.FencingTuple{}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, ok, err := lookup.FindByID(ctx, "req-1")
	if err != nil || !ok {
		t.Fatalf("FindByID ok=%v err=%v", ok, err)
	}
	if got == nil {
		t.Fatal("FindByID returned nil envelope despite ok=true")
	}
	if got.ID != "req-1" || got.Type != "xhs.publish" {
		t.Errorf("envelope mismatch got=%+v", got)
	}
}

// TestRequestLookup_Missing returns ok=false with nil envelope.
func TestRequestLookup_Missing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	defer func() { _ = db.Close() }()

	lookup := store.NewRequestLookup(store.NewMessages(db), channel.ID("ch-1"))
	env, ok, err := lookup.FindByID(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if ok {
		t.Error("expected ok=false")
	}
	if env != nil {
		t.Errorf("expected nil envelope, got %+v", env)
	}
}
