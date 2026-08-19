package actorbase

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestDescribeIsProjectedBeforeProcDelivery(t *testing.T) {
	pen := &fakePen{self: "tool:device:1"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	e.actorCtx = &fakeActorContext{self: "tool:device:1"}
	e.occupant.Store(int32(occupantRunning))
	e.def.Manifest = introspect.Manifest{
		Class: "device", Interfaces: []string{"actor"}, Capabilities: map[string]bool{"files": true},
		Words: map[string]introspect.WordSpec{"device.read": {Description: "read"}},
	}

	env := &message.Envelope{ID: "describe", Kind: message.KindRequest, Type: introspect.QueryDescribe, Payload: json.RawMessage(`{"body":{}}`)}
	if err := e.Receive(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if queued, ok := e.workQ.pop(); ok || queued != nil {
		t.Fatalf("describe reached Proc queue: %+v", queued)
	}
	var terminal struct {
		Status string `json:"status"`
		introspect.Describe
	}
	if last := pen.last(); last == nil || json.Unmarshal(last.Payload, &terminal) != nil || terminal.Status != message.StatusCompleted || terminal.Class != "device" || len(terminal.Words) != 1 {
		t.Fatalf("terminal=%+v envelope=%+v", terminal, pen.last())
	}
	nullBody := &message.Envelope{ID: "describe-null", Kind: message.KindRequest, Type: introspect.QueryDescribe, Payload: json.RawMessage(`{"body":null}`)}
	if err := e.Receive(context.Background(), nullBody); err != nil {
		t.Fatal(err)
	}
	if last := pen.last(); last == nil || json.Unmarshal(last.Payload, &terminal) != nil || terminal.Status != message.StatusCompleted {
		t.Fatalf("null-body describe terminal=%+v envelope=%+v", terminal, last)
	}

	unknown := &message.Envelope{ID: "unknown", Kind: message.KindRequest, Type: introspect.QueryDescribe, Payload: json.RawMessage(`{"body":{"type":"missing.word"}}`)}
	if err := e.Receive(context.Background(), unknown); err != nil {
		t.Fatal(err)
	}
	var failure struct {
		Status    string `json:"status"`
		ErrorCode string `json:"error_code"`
	}
	if err := json.Unmarshal(pen.last().Payload, &failure); err != nil || failure.Status != message.StatusFailed || failure.ErrorCode != "invalid_args" {
		t.Fatalf("unknown selector=%s", pen.last().Payload)
	}

	extra := &message.Envelope{ID: "extra", Kind: message.KindRequest, Type: introspect.QueryDescribe, Payload: json.RawMessage(`{"body":{"type":"device.read","typo":true}}`)}
	if err := e.Receive(context.Background(), extra); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(pen.last().Payload, &failure); err != nil || failure.Status != message.StatusFailed || failure.ErrorCode != "invalid_args" {
		t.Fatalf("unknown describe field=%s", pen.last().Payload)
	}
}

func TestManifestStatePutRejectsGateCodes(t *testing.T) {
	e := newTestEngine(t, &fakePen{self: "tool:device:1"}, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	e.def.Manifest = introspect.Manifest{Class: "device", Interfaces: []string{"actor"}, Words: map[string]introspect.WordSpec{"device.read": {}}}
	raw := []byte(`{"device.dynamic":{"error_codes":["endpoint_not_found"]}}`)
	if _, err := e.State().Put(ManifestStateKey, raw); err == nil {
		t.Fatal("dynamic manifest accepted a gate error code")
	}
}
