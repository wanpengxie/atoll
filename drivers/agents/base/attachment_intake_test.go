package base

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base/internal/book"
	"github.com/wanpengxie/atoll/drivers/agents/effectcap"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

type askIntakeSys struct {
	actorbase.Sys
	failures chan string
}

func (*askIntakeSys) Self() actor.ActorID { return "agent:test:1" }
func (s *askIntakeSys) Fail(_ actorbase.Msg, code, _ string) (message.ID, error) {
	s.failures <- code
	return "failure", nil
}
func (*askIntakeSys) Progress(actorbase.Msg, string, any) (message.ID, error) {
	return "progress", nil
}

func runAskIntake(t *testing.T, payload string) (*captureRuntime, <-chan string) {
	t.Helper()
	rt := &captureRuntime{}
	sys := &askIntakeSys{failures: make(chan string, 1)}
	vault := effectcap.NewVault()
	exec := newExecutor(sys, vault)
	exec.bindRuntime(rt)
	local, cancel := context.WithCancel(context.Background())
	l := &agentLoop{
		def: definition{
			cfg: Config{
				RequestMaxCount: 8,
				BufferMaxCount:  8,
				BufferMaxBytes:  1 << 20,
				BatchMaxCount:   8,
				ReceiptDeadline: time.Hour,
			},
			controls: map[string]struct{}{TypeAsk: {}},
		},
		sys: sys, rt: rt, exec: exec, vault: vault, state: book.New(),
		inbox: newLoopInbox(16), local: local,
		receipts: map[string]receiptRow{}, receiptTimers: map[string]*time.Timer{}, logger: testLogger(),
	}
	t.Cleanup(func() {
		cancel()
		for _, timer := range l.receiptTimers {
			timer.Stop()
		}
	})
	msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: "ask", Sender: message.Sender{ID: "human:root:1"}, Kind: message.KindRequest,
		Type: TypeAsk, Payload: json.RawMessage(`{"body":` + payload + `}`),
	})
	l.handleIntake(msg)
	return rt, sys.failures
}

func TestAskAcceptsFrontendAttachmentFields(t *testing.T) {
	rt, _ := runAskIntake(t, `{
		"text":"read this",
		"attachments":[{
			"resource_id":"resource-1",
			"address":"daemon://local-device/c0.proj/uploads/research.md",
			"name":"研究 文档.md",
			"media_type":"text/markdown",
			"size":42
		}]
	}`)
	if len(rt.starts) != 1 || len(rt.starts[0].Messages) != 1 {
		t.Fatalf("starts=%+v, want one accepted message", rt.starts)
	}
	input := rt.starts[0].Messages[0]
	want := runtimeproto.Attachment{Address: "daemon://local-device/c0.proj/uploads/research.md", Name: "研究 文档.md"}
	if input.Text != "read this" || len(input.Attachments) != 1 || input.Attachments[0] != want {
		t.Fatalf("input=%+v, want text and attachment %+v", input, want)
	}
}

func TestAskDropsOnlyAttachmentWithoutAddress(t *testing.T) {
	rt, _ := runAskIntake(t, `{
		"text":"keep going",
		"attachments":[
			{"name":"missing.md","size":1},
			{"address":"daemon://local-device/c0.proj/uploads/kept.md","name":"kept.md","size":2}
		]
	}`)
	if len(rt.starts) != 1 || len(rt.starts[0].Messages) != 1 {
		t.Fatalf("starts=%+v, want the message to remain accepted", rt.starts)
	}
	input := rt.starts[0].Messages[0]
	if input.Text != "keep going" || len(input.Attachments) != 1 || input.Attachments[0].Name != "kept.md" {
		t.Fatalf("input=%+v, want body and only the valid attachment", input)
	}
}

func TestAskStillRejectsUnknownTopLevelField(t *testing.T) {
	rt, failures := runAskIntake(t, `{"text":"read this","unexpected":true}`)
	select {
	case code := <-failures:
		if code != "invalid_args" {
			t.Fatalf("failure code=%q, want invalid_args", code)
		}
	case <-time.After(time.Second):
		t.Fatal("unknown top-level field was not rejected")
	}
	if len(rt.starts) != 0 {
		t.Fatalf("runtime starts=%d, want none", len(rt.starts))
	}
}
