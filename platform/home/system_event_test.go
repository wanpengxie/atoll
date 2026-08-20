package home

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

type penFunc func(context.Context, *message.Envelope) (harness.WriteResult, error)

func (f penFunc) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	return f(ctx, env)
}

func TestEmitSystemEventSealsEnvelopeAndChecksAccepted(t *testing.T) {
	var captured *message.Envelope
	h := &Home{
		channelID: "c1", nowMs: func() int64 { return 1234 },
		logger: slog.New(slog.DiscardHandler),
		systemPen: penFunc(func(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
			copy := *env
			captured = &copy
			return harness.WriteResult{MessageID: env.ID, Seq: 7}, nil
		}),
	}
	if err := h.emitSystemEvent(context.Background(), message.Root(), "test.event", map[string]any{"x": 1}); err != nil {
		t.Fatal(err)
	}
	if captured == nil || captured.ID == "" || captured.TS != 1234 ||
		captured.Kind != message.KindEvent || captured.Type != "test.event" ||
		captured.Visibility != message.VisibilitySystem ||
		captured.Audience == nil || len(captured.Audience) != 0 ||
		string(captured.Payload) != `{"x":1}` {
		t.Fatalf("captured=%+v", captured)
	}
	wire, _ := json.Marshal(captured)
	if !strings.Contains(string(wire), `"audience":[]`) {
		t.Fatalf("wire audience is not []: %s", wire)
	}

	h.systemPen = penFunc(func(context.Context, *message.Envelope) (harness.WriteResult, error) {
		return harness.WriteResult{
			RejectReason: harness.HarnessTypeUnknown, RejectDetail: "rejected",
		}, nil
	})
	err := h.emitSystemEvent(context.Background(), message.Root(), "test.rejected", map[string]any{})
	var rejected *systemEventWriteError
	if !errors.As(err, &rejected) || rejected.Reason != harness.HarnessTypeUnknown {
		t.Fatalf("reject err=%v", err)
	}
}

func TestLifecycleNarrationLandsThroughSystemEventMouth(t *testing.T) {
	h, err := Open(completeHomeTestConfig(Config{
		ChannelID: "system-events", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		ReconcileInterval: time.Hour, Bootstrap: true,
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.closeInternal("test") }()
	ctx := context.Background()

	h.announceRegistered(ctx, message.Root(), "agent:new:1", map[string]any{"kind": actor.KindAgent})
	h.announceEnded(ctx, message.Root(), []actor.ActorID{"agent:new:1"}, "test", actor.SystemActorID)
	for _, typ := range []string{
		message.TypeSystemMemberCreated,
		message.TypeSystemMemberDeleted,
	} {
		row, found, err := h.query.LatestBySenderAndType(ctx, actor.SystemActorID, typ)
		if err != nil || !found || row.Envelope.Kind != message.KindEvent ||
			row.Envelope.Visibility != message.VisibilitySystem {
			t.Fatalf("type %s row=%+v found=%v err=%v", typ, row, found, err)
		}
	}
}
