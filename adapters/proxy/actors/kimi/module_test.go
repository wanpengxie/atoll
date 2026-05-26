package kimi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/adapters/proxy/actorapi"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestModuleHandleCommandSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/command" {
			http.NotFound(w, r)
			return
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode command: %v", err)
		}
		if req["action"] != "snapshot" || req["session"] != "kimi" {
			t.Fatalf("command request = %+v", req)
		}
		_, _ = w.Write([]byte(`{"ok":true,"data":{"tree":"button \"Send\" @e1"}}`))
	}))
	defer server.Close()

	mod := New()
	rawCfg, _ := json.Marshal(Config{BaseURL: server.URL, DefaultSession: "kimi"})
	if err := mod.Init(context.Background(), actorapi.ModuleConfig{Raw: rawCfg}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	resp, err := mod.Handle(context.Background(), message.Envelope{
		ID:         "req-kimi",
		TS:         time.Now().UnixMilli(),
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:test"},
		Kind:       message.KindRequest,
		Type:       TypeCommand,
		Payload:    json.RawMessage(`{"action":"snapshot","args":{}}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{DefaultAdapterActorID},
	})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	var payload struct {
		Status string `json:"status"`
		Tree   string `json:"tree"`
	}
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("payload decode: %v raw=%s", err, string(resp.Payload))
	}
	if payload.Status != "completed" || payload.Tree == "" {
		t.Fatalf("payload = %+v raw=%s", payload, string(resp.Payload))
	}
}

func TestModuleReadiness(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"running":true,"extension_connected":true}`))
	}))
	defer server.Close()

	mod := New()
	rawCfg, _ := json.Marshal(Config{BaseURL: server.URL})
	if err := mod.Init(context.Background(), actorapi.ModuleConfig{Raw: rawCfg}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	ready, reason, err := mod.Readiness(context.Background())
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if !ready || reason != "ok" {
		t.Fatalf("ready=%v reason=%q", ready, reason)
	}
}
