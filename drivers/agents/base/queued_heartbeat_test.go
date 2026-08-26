package base

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/protocol/message"
)

// A request waiting behind a long turn must not fall silent: every real
// runtime event of the running turn re-affirms the buffered rows, throttled
// to queuedHeartbeatEvery per row. No event → no heartbeat, so a stuck
// runtime still lets the caller's sliding deadline fire.
func TestQueuedHeartbeatFollowsRealProgressOnly(t *testing.T) {
	l, sys, _ := newV7Loop(t, nil)
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	l.nowFn = func() time.Time { return now }

	v7Activate(t, l, "owner")
	l.handleIntake(v7Request("r1", TypeAsk, "caller", `{"text":"one"}`))
	l.handleIntake(v7Request("r2", TypeAsk, "caller", `{"text":"two"}`))
	if got := l.state.Buffer; len(got) != 2 {
		t.Fatalf("buffer=%v", got)
	}
	queued := func(id string) []v7Progress {
		var out []v7Progress
		for _, p := range sys.progresses(id) {
			if p.status == message.StatusQueued {
				out = append(out, p)
			}
		}
		return out
	}
	toolEvent := func(call string) {
		l.handleRuntimeEvent(runtimeEvent{kind: evTool, turnID: "turn-owner", tool: runtimeproto.ToolEvent{
			CallID: call, Phase: processStarted, Name: "bash", Input: json.RawMessage(`{}`),
		}})
	}

	// Admission wrote exactly one queued frame each.
	if len(queued("r1")) != 1 || len(queued("r2")) != 1 {
		t.Fatalf("admission frames r1=%v r2=%v", queued("r1"), queued("r2"))
	}

	// Inside the throttle window a real event does not re-affirm.
	now = now.Add(10 * time.Second)
	toolEvent("c1")
	if len(queued("r1")) != 1 || len(queued("r2")) != 1 {
		t.Fatalf("early heartbeat r1=%v r2=%v", queued("r1"), queued("r2"))
	}

	// Past the window, the next real event re-affirms every buffered row once.
	now = now.Add(55 * time.Second)
	toolEvent("c2")
	for i, id := range []string{"r1", "r2"} {
		got := queued(id)
		if len(got) != 2 {
			t.Fatalf("%s frames=%v", id, got)
		}
		value, _ := got[1].value.(map[string]any)
		if value["heartbeat"] != true || value["position"] != i+1 {
			t.Fatalf("%s heartbeat value=%v", id, value)
		}
		if _, ok := value["controls"]; !ok {
			t.Fatalf("%s heartbeat must carry controls: %v", id, value)
		}
		if value["current_turn_since"] != l.state.Turn.StartedAtMs {
			t.Fatalf("%s current_turn_since=%v want %d", id, value["current_turn_since"], l.state.Turn.StartedAtMs)
		}
	}

	// Immediately after, another event is throttled again.
	toolEvent("c3")
	if len(queued("r1")) != 2 || len(queued("r2")) != 2 {
		t.Fatalf("throttle after beat r1=%v r2=%v", queued("r1"), queued("r2"))
	}

	// Silence: time passes with no runtime event — nothing is written, the
	// waiting callers' sliding deadlines are free to fire.
	now = now.Add(10 * time.Minute)
	if len(queued("r1")) != 2 || len(queued("r2")) != 2 {
		t.Fatalf("silent heartbeat r1=%v r2=%v", queued("r1"), queued("r2"))
	}

	// Stage events count as real progress too.
	l.handleRuntimeEvent(runtimeEvent{kind: evProgress, turnID: "turn-owner", progress: runtimeproto.ProgressEvent{Kind: "thinking", Text: "…"}})
	if len(queued("r1")) != 3 || len(queued("r2")) != 3 {
		t.Fatalf("stage heartbeat r1=%v r2=%v", queued("r1"), queued("r2"))
	}
}
