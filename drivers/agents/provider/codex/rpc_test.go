package codex

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestUnknownServerRequestAlwaysGetsErrorResponse(t *testing.T) {
	_, err := handleServerRequest("future/request", nil)
	if err == nil || err.Code != -32601 {
		t.Fatalf("err=%+v", err)
	}
}

func TestControlDoneFollowsSubmissionOrder(t *testing.T) {
	p := mockProcess(t, func(method string, _ map[string]any, call int) (any, *rpcError) {
		if method == "initialize" {
			return map[string]any{}, nil
		}
		if call == 1 {
			time.Sleep(20 * time.Millisecond)
		}
		return map[string]any{}, nil
	})
	events := &recordingEvents{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &engine{cfg: Config{Logger: slog.New(slog.DiscardHandler), processFactory: func(context.Context, Config) (*childProcess, error) { return p, nil }}, events: events, life: ctx}
	c, err := e.openConnection(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.current, e.threadID = c, "thread"
	c.turnID = "turn"
	go e.controlWorker()
	trigger := base.Trigger{Envelope: message.Envelope{Payload: []byte(`{"text":"x"}`)}}
	_ = e.Steer("first", trigger)
	_ = e.Steer("second", trigger)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		records := events.snapshot()
		if len(records) >= 2 {
			if records[0].op != "first" || records[1].op != "second" {
				t.Fatalf("control order=%#v", records)
			}
			_ = e.Close()
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("controls incomplete: %#v", events.snapshot())
}

func TestControlSubmissionDoesNotBlockAtFormerChannelCapacity(t *testing.T) {
	e := &engine{life: context.Background()}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 128; i++ {
			if err := e.Interrupt(base.OpID(fmt.Sprintf("op-%d", i))); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("control submission blocked behind the provider RPC worker")
	}
	e.controlMu.Lock()
	queued := len(e.controlQueue)
	e.controlMu.Unlock()
	if queued != 128 {
		t.Fatalf("queued=%d, want 128", queued)
	}
}
func TestApprovalRequestsAreDeclinedAndCurrentTimeAnswered(t *testing.T) {
	for _, m := range []string{"item/commandExecution/requestApproval", "item/fileChange/requestApproval", "execCommandApproval", "applyPatchApproval"} {
		result, err := handleServerRequest(m, nil)
		if err != nil || result == nil {
			t.Fatalf("%s=(%v,%v)", m, result, err)
		}
	}
	result, err := handleServerRequest("currentTime/read", nil)
	if err != nil || result == nil {
		t.Fatalf("time=(%v,%v)", result, err)
	}
}
