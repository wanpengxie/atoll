package platform_test

// actorbase-spec-v1.md §4 S5a: echo is the "concept budget" consumer — this
// is the DoD's real out-generation path (Home.Spawn, the cell host) proving
// a bare actorbase.Proc closes a full call->reply loop through the SAME
// admission every other actor goes through, not a hand-rolled harness.

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/actors/echo"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

const echoTestChannelID = channel.ID("test-echo-actorbase")

func newEchoTestHome(t *testing.T) *platform.Home {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "echo.sqlite")
	h, err := platform.Open(platform.HomeConfig{ChannelID: echoTestChannelID, DBPath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// TestEcho_CallReplyClosesThroughRealCellHost spawns echo.Def() over the
// production Home.Spawn path and drives one echo.say request end to end:
// pen write -> harness -> cell delivery -> actorbase engine -> echo's Proc ->
// sys.Reply -> pen write of the terminal.
func TestEcho_CallReplyClosesThroughRealCellHost(t *testing.T) {
	ch := newEchoTestHome(t)

	callerID := actor.ActorID("user:caller")
	echoID := echo.DefaultActorID

	callerPen := spawnWithPen(t, ch, callerID, actor.KindHuman)
	if err := ch.Spawn(t.Context(), echoID, actor.KindTool, platform.ActorFactory{Proc: echo.Def()}); err != nil {
		t.Fatalf("spawn echo actor: %v", err)
	}

	reqID := writeRequest(t, callerPen, echoID, echo.TypeSay, nil)

	term := pollForTerminal(t, ch, reqID, 5*time.Second)
	if term.Kind != message.KindResponse {
		t.Fatalf("terminal kind=%s, want response", term.Kind)
	}
	if term.Sender.ID != echoID {
		t.Fatalf("terminal sender.id=%s, want %s", term.Sender.ID, echoID)
	}
	var payload struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(term.Payload, &payload); err != nil {
		t.Fatalf("unmarshal terminal payload: %v (raw=%s)", err, term.Payload)
	}
	if payload.Status != "completed" {
		t.Fatalf("terminal payload.status=%q, want completed", payload.Status)
	}
}

// TestEcho_UnknownTypeFails drives an unsupported request type through the
// same real cell host and asserts the failed terminal (type_unsupported).
func TestEcho_UnknownTypeFails(t *testing.T) {
	ch := newEchoTestHome(t)

	callerID := actor.ActorID("user:caller2")
	echoID := echo.DefaultActorID

	callerPen := spawnWithPen(t, ch, callerID, actor.KindHuman)
	if err := ch.Spawn(t.Context(), echoID, actor.KindTool, platform.ActorFactory{Proc: echo.Def()}); err != nil {
		t.Fatalf("spawn echo actor: %v", err)
	}

	reqID := writeRequest(t, callerPen, echoID, "echo.nope", nil)

	term := pollForTerminal(t, ch, reqID, 5*time.Second)
	var payload struct {
		Status    string `json:"status"`
		ErrorCode string `json:"error_code"`
	}
	if err := json.Unmarshal(term.Payload, &payload); err != nil {
		t.Fatalf("unmarshal terminal payload: %v (raw=%s)", err, term.Payload)
	}
	if payload.Status != "failed" {
		t.Fatalf("terminal payload.status=%q, want failed", payload.Status)
	}
	if payload.ErrorCode != "type_unsupported" {
		t.Fatalf("terminal payload.error_code=%q, want type_unsupported", payload.ErrorCode)
	}
}
