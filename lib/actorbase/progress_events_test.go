package actorbase

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

func TestJobTableProgressEventsPreserveCallerLedgerOrderBeforeFinal(t *testing.T) {
	e := newTestEngine(t, &fakePen{self: "agent:caller:1"}, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	registerEntry(e, "req-progress")
	progress := e.ProgressEvents("req-progress")
	for step := 1; step <= 2; step++ {
		raw, _ := json.Marshal(map[string]any{"status": message.StatusProcessing, "step": step})
		env := responseEnv("req-progress", message.StatusProcessing)
		env.ID = message.ID("progress-" + string(rune('0'+step)))
		env.Payload = raw
		if !e.call.match(env) {
			t.Fatalf("progress step %d did not match the caller ledger", step)
		}
	}
	if !e.call.match(responseEnv("req-progress", message.StatusCompleted)) {
		t.Fatal("final did not match the caller ledger")
	}
	steps := []int{}
	for env := range progress {
		var payload struct {
			Step int `json:"step"`
		}
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		steps = append(steps, payload.Step)
	}
	if len(steps) != 2 || steps[0] != 1 || steps[1] != 2 {
		t.Fatalf("caller ledger progress order=%v", steps)
	}
	final, ok, err := e.Await(context.Background(), "req-progress", time.Second)
	if err != nil || !ok || final == nil {
		t.Fatalf("final=%+v ok=%v err=%v", final, ok, err)
	}
}
