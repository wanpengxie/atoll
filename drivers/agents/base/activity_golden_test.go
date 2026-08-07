package base

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/registry"
)

func TestActivityEnvelopeGoldenAcrossOwnershipTransfer(t *testing.T) {
	l, _ := newUnitLoop()
	first := bufferedMsg("request-1", "actor:user", 1)
	first.trigger.CorrelationID = "correlation-1"
	l.active, l.lastOwner = first, first
	if err := l.emitActivity(registry.ActivityTurnStarted, registry.ActivityTurnStartedPayload{
		TurnIndex: 1,
		Status:    registry.ActivityStartedStatus,
	}); err != nil {
		t.Fatal(err)
	}
	emits := l.sys.(*testSys).emits
	if len(emits) != 1 {
		t.Fatalf("emitted %d activities, want 1", len(emits))
	}
	got := emits[0]
	if got.Audience == nil || len(got.Audience) != 0 {
		t.Fatalf("audience = %#v, want non-nil empty", got.Audience)
	}
	if got.Visibility != message.VisibilityPublic {
		t.Fatalf("visibility = %q, want public", got.Visibility)
	}
	env, err := behavior.BuildEvent(func() time.Time { return time.UnixMilli(1) }, got)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(wire, []byte(`"audience":[]`)) {
		t.Fatalf("activity wire lacks explicit empty audience: %s", wire)
	}
	if got.ParentID != "request-1" || got.CorrelationID != "correlation-1" {
		t.Fatalf("first activity ownership = parent %q correlation %q", got.ParentID, got.CorrelationID)
	}

	second := bufferedMsg("request-2", "actor:user", 1)
	second.trigger.CorrelationID = "correlation-2"
	l.reply(l.active, map[string]any{"preempted_by": second.msg.ID})
	l.active, l.lastOwner = second, second
	if err := l.emitTurnEnded(TurnStatusOK); err != nil {
		t.Fatal(err)
	}
	got = l.sys.(*testSys).emits[1]
	if got.ParentID != "request-2" || got.CorrelationID != "correlation-2" {
		t.Fatalf("transferred activity ownership = parent %q correlation %q", got.ParentID, got.CorrelationID)
	}
}

func TestActivityWriteFailureOnlyLogsAndDoesNotStopIncarnation(t *testing.T) {
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	l, engine := newUnitLoop()
	sys := l.sys.(*testSys)
	sys.emitErr = errors.New("activity store unavailable")
	owner := bufferedMsg("request", "actor:user", 1)
	l.committing["start"] = &operation{kind: "start", item: owner}
	l.state = stateStarting

	l.handleProviderEvent(providerEvent{kind: eventTurnStarted, op: "start", turnID: "turn"})

	if engine.terminates != 0 || l.state != stateTurnActive || l.turnID != "turn" || l.active != owner {
		t.Fatalf("activity failure changed incarnation: terminates=%d state=%v turn=%q active=%#v", engine.terminates, l.state, l.turnID, l.active)
	}
}
