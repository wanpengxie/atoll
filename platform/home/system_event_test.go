package home

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/humancell"
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
	if err := h.emitSystemEvent(context.Background(), "test.event", map[string]any{"x": 1}); err != nil {
		t.Fatal(err)
	}
	if captured == nil || captured.ID == "" || captured.TS != 1234 ||
		captured.Kind != message.KindEvent || captured.Type != "test.event" ||
		captured.Visibility != message.VisibilitySystem ||
		len(captured.Audience) != 1 || captured.Audience[0] != actor.SystemActorID ||
		string(captured.Payload) != `{"x":1}` {
		t.Fatalf("captured=%+v", captured)
	}

	h.systemPen = penFunc(func(context.Context, *message.Envelope) (harness.WriteResult, error) {
		return harness.WriteResult{
			RejectReason: harness.HarnessAudienceEmpty, RejectDetail: "rejected",
		}, nil
	})
	err := h.emitSystemEvent(context.Background(), "test.rejected", map[string]any{})
	var rejected *systemEventWriteError
	if !errors.As(err, &rejected) || rejected.Reason != harness.HarnessAudienceEmpty {
		t.Fatalf("reject err=%v", err)
	}
}

func TestDefaultAgentFoldDoesNotAdvanceOnRejectedWrite(t *testing.T) {
	h := &Home{
		channelID: "c1", nowMs: func() int64 { return 1234 },
		logger: slog.New(slog.DiscardHandler),
		systemPen: penFunc(func(context.Context, *message.Envelope) (harness.WriteResult, error) {
			return harness.WriteResult{
				RejectReason: harness.HarnessAudienceEmpty, RejectDetail: "rejected",
			}, nil
		}),
	}
	fold := &defaultAgentFold{
		home: h,
		state: humancell.RoutingSnapshot{
			State: humancell.RoutingConfigured, Target: "agent:old",
		},
	}
	if err := fold.set(context.Background(), "agent:new", "human:alice"); err == nil {
		t.Fatal("rejected authoritative write unexpectedly succeeded")
	}
	got := fold.snapshot()
	if got.State != humancell.RoutingConfigured || got.Target != "agent:old" {
		t.Fatalf("fold advanced on reject: %+v", got)
	}
}

func TestLifecycleAndAuditNarrationLandThroughSystemEventMouth(t *testing.T) {
	h, err := Open(Config{
		ChannelID: "system-events", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: routingResolver{}, IntroductionResolver: inertIntroductionResolver{},
		ReconcileInterval: time.Hour, Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.closeInternal("test") }()
	ctx := context.Background()

	h.announceRegistered(ctx, "agent:new", actor.KindAgent)
	h.announceEnded(ctx, []actor.ActorID{"agent:new"}, "test", actor.SystemActorID)
	h.announceAudit(ctx, "test_audit", map[string]any{"ok": true})
	for _, typ := range []string{
		actor.ReservedSystemActorRegistered,
		actor.ReservedSystemActorEnded,
		sysOpAuditType,
	} {
		row, found, err := h.query.LatestBySenderAndType(ctx, actor.SystemActorID, typ)
		if err != nil || !found || row.Envelope.Kind != message.KindEvent ||
			row.Envelope.Visibility != message.VisibilitySystem {
			t.Fatalf("type %s row=%+v found=%v err=%v", typ, row, found, err)
		}
	}
}
