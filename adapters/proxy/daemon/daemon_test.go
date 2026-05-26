package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/adapters/proxy/actorapi"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestDaemonRunRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readyCh := make(chan DeviceFrame, 1)
	respCh := make(chan message.Envelope, 1)
	errCh := make(chan error, 1)
	upgrader := websocket.Upgrader{Subprotocols: []string{WSSubprotocolV2}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != WSPathV2 {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get(QueryParamAPIKey) != "dk_test" {
			http.Error(w, "bad key", http.StatusUnauthorized)
			return
		}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = ws.Close() }()

		var ready DeviceFrame
		if err := ws.ReadJSON(&ready); err != nil {
			errCh <- err
			return
		}
		readyCh <- ready

		req := message.Envelope{
			ID:         "req-1",
			TS:         time.Now().UnixMilli(),
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:test"},
			Kind:       message.KindRequest,
			Type:       "fake.echo",
			Payload:    json.RawMessage(`{"prompt":"ping"}`),
			Visibility: message.VisibilityPublic,
			Audience:   message.Audience{"tool:fake"},
		}
		raw, _ := json.Marshal(req)
		if err := ws.WriteJSON(DeviceFrame{
			Direction: "to_device",
			ActorID:   "tool:fake",
			Payload:   raw,
		}); err != nil {
			errCh <- err
			return
		}
		for {
			var frame DeviceFrame
			if err := ws.ReadJSON(&frame); err != nil {
				errCh <- err
				return
			}
			var env message.Envelope
			if len(frame.Payload) == 0 || json.Unmarshal(frame.Payload, &env) != nil {
				continue
			}
			if env.Kind == message.KindResponse {
				respCh <- env
				cancel()
				return
			}
		}
	}))
	defer server.Close()

	reg := NewRegistry()
	if err := reg.Register("tool:fake", func() (actorapi.ActorModule, error) {
		return fakeModule{}, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	d, err := New(Config{
		APIKey:        "dk_test",
		ServerWS:      server.URL,
		EnabledActors: []actor.ActorID{"tool:fake"},
	}, Options{
		Registry:           reg,
		DisableLocalListen: true,
		ReadinessInterval:  time.Hour,
		HeartbeatInterval:  time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- d.Run(ctx) }()

	select {
	case ready := <-readyCh:
		if ready.FrameType != FrameTypeReady || len(ready.Actors) != 1 || ready.Actors[0].ActorID != "tool:fake" {
			t.Fatalf("ready frame = %+v", ready)
		}
	case err := <-errCh:
		t.Fatalf("server error before ready: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("ready frame timeout")
	}
	select {
	case resp := <-respCh:
		if resp.Kind != message.KindResponse || resp.ParentID != "req-1" || resp.Sender.ID != "tool:fake" {
			t.Fatalf("response envelope = %+v", resp)
		}
		var payload struct {
			Status string `json:"status"`
			Echo   string `json:"echo"`
		}
		if err := json.Unmarshal(resp.Payload, &payload); err != nil {
			t.Fatalf("response payload decode: %v", err)
		}
		if payload.Status != "completed" || payload.Echo != "ping" {
			t.Fatalf("payload = %+v raw=%s", payload, string(resp.Payload))
		}
	case err := <-errCh:
		t.Fatalf("server error before response: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("response timeout")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

type fakeModule struct{}

func (fakeModule) ActorID() actor.ActorID { return "tool:fake" }

func (fakeModule) Declaration() adapter.Declaration {
	return adapter.Declaration{
		Name:         "fake",
		ActorID:      "tool:fake",
		Types:        []string{"fake.echo"},
		Binding:      actor.BindingRuntimeInboundViaRelay,
		MaxPendingMs: 30_000,
		TypeDeclarations: map[string]adapter.TypeDeclaration{
			"fake.echo": {
				AllowedKinds:       []message.Kind{message.KindRequest, message.KindResponse},
				TerminalConvention: string(adapter.TerminalPayloadStatus),
			},
		},
	}
}

func (fakeModule) Init(context.Context, actorapi.ModuleConfig) error { return nil }
func (fakeModule) Shutdown(context.Context) error                    { return nil }
func (fakeModule) Readiness(context.Context) (bool, string, error)   { return true, "ok", nil }

func (fakeModule) Handle(_ context.Context, req message.Envelope) (message.Envelope, error) {
	payload := json.RawMessage(`{"status":"completed","echo":"ping"}`)
	return responseEnvelopeForTest(req, "tool:fake", payload), nil
}

func responseEnvelopeForTest(req message.Envelope, sender actor.ActorID, payload json.RawMessage) message.Envelope {
	now := time.Now().UnixMilli()
	return message.Envelope{
		ID:            "resp-1",
		TS:            now,
		Sender:        message.Sender{Kind: actor.KindTool, ID: sender},
		Kind:          message.KindResponse,
		Type:          req.Type,
		ParentID:      req.ID,
		CorrelationID: req.ID,
		Payload:       payload,
		Visibility:    message.VisibilityPublic,
		Audience:      message.Audience{req.Sender.ID},
	}
}
