package home_test

// actorbase-spec-v1.md §4 S5a: echo is the "concept budget" consumer — this
// is the DoD's real out-generation path (Home.SpawnIfAbsent, the cell host) proving
// a bare actorbase.Proc closes a full call->reply loop through the SAME
// admission every other actor goes through, not a hand-rolled harness.

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

const echoTestChannelID = channel.ID("test-echo-actorbase")

// echoProbeType / echoProbeID / echoProbeDef are an inline echo-equivalent Proc
// (reply payload unchanged on echoProbeType, else fail type_unsupported). The
// concept-budget consumer these host tests exercise only needs SOME bare
// actorbase.Proc, not the drivers/tools/echo package — inlining it keeps
// home_test from importing downstream (drivers/*), which the drivers fence
// forbids for everyone but the assembly root cmd/*.
const echoProbeType = "echo.say"

const echoProbeID = actor.ActorID("echo")

func echoProbeDef() actorbase.Def {
	return actorbase.Def{
		Doc: "test-only inline echo: echo.say replies payload unchanged, else type_unsupported",
		New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error {
				for {
					msg, err := sys.Recv()
					if err != nil {
						return err
					}
					switch msg.Type {
					case echoProbeType:
						_, _ = sys.Reply(msg, msg.Payload)
					default:
						_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("echo actor does not handle %s", msg.Type))
					}
				}
			}, nil
		},
	}
}

func newEchoTestHome(t *testing.T) *home.Home {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "echo.sqlite")
	h, err := home.Open(home.Config{ChannelID: echoTestChannelID, DBPath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// TestEcho_CallReplyClosesThroughRealCellHost spawns echoProbeDef() over the
// production Home.SpawnIfAbsent path and drives one echo.say request end to end:
// pen write -> harness -> cell delivery -> actorbase engine -> echo's Proc ->
// sys.Reply -> pen write of the terminal.
func TestEcho_CallReplyClosesThroughRealCellHost(t *testing.T) {
	ch := newEchoTestHome(t)

	callerID := actor.ActorID("user:caller")
	echoID := echoProbeID

	callerPen := spawnWithPen(t, ch, &callerID, actor.KindHuman)
	echoID, err := home.SpawnForTest(ch, echoID, actor.KindTool, platform.ActorFactory{Proc: echoProbeDef()})
	if err != nil {
		t.Fatalf("spawn echo actor: %v", err)
	}

	reqID := writeRequest(t, callerPen, echoID, echoProbeType, nil)

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
	echoID := echoProbeID

	callerPen := spawnWithPen(t, ch, &callerID, actor.KindHuman)
	echoID, err := home.SpawnForTest(ch, echoID, actor.KindTool, platform.ActorFactory{Proc: echoProbeDef()})
	if err != nil {
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
