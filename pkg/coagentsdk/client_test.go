package coagentsdk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestCallActorHappyPath(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newMockSDKServer(t, mockConfig{
		ResponsePayload: json.RawMessage(`{"status":"completed","note_id":"n-1","url":"https://example.invalid/n-1"}`),
	})
	defer srv.Close()

	res, err := (&Client{BaseURL: srv.URL, SessionToken: "tok"}).CallActor(context.Background(), CallActorRequest{
		ChannelID: "ch-1",
		ActorID:   "tool:xhs",
		Type:      "xhs.publish",
		Payload:   json.RawMessage(`{"title":"hello"}`),
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("CallActor: %v", err)
	}
	if !res.OK {
		t.Fatalf("OK=false: %+v", res.Error)
	}
	assertJSONEqual(t, res.Data, `{"note_id":"n-1","url":"https://example.invalid/n-1"}`)
	assertJSONEqual(t, res.Raw, `{"status":"completed","note_id":"n-1","url":"https://example.invalid/n-1"}`)
}

func TestCallActorFailedResponse(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newMockSDKServer(t, mockConfig{
		ResponsePayload: json.RawMessage(`{"status":"failed","error_code":"boom","detail":"bad input","recovery_hint":"retry later"}`),
	})
	defer srv.Close()

	res, err := (&Client{BaseURL: srv.URL}).CallActor(context.Background(), CallActorRequest{
		ChannelID: "ch-1",
		ActorID:   "tool:xhs",
		Type:      "xhs.publish",
		Payload:   json.RawMessage(`{"title":"hello"}`),
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("CallActor: %v", err)
	}
	if res.OK || res.Error == nil {
		t.Fatalf("expected failed result, got %+v", res)
	}
	if res.Error.Code != "boom" || res.Error.Message != "bad input" || res.Error.RecoveryHint != "retry later" {
		t.Fatalf("error=%+v", res.Error)
	}
}

func TestCallActorTimeout(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newMockSDKServer(t, mockConfig{NoResponse: true})
	defer srv.Close()

	res, err := (&Client{BaseURL: srv.URL}).CallActor(context.Background(), CallActorRequest{
		ChannelID: "ch-1",
		ActorID:   "tool:xhs",
		Type:      "xhs.publish",
		Payload:   json.RawMessage(`{"title":"hello"}`),
		Timeout:   30 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("CallActor: %v", err)
	}
	if res.OK || res.Error == nil || res.Error.Code != "timeout" {
		t.Fatalf("result=%+v want timeout", res)
	}
}

func TestCallActorHTTPErrorOnEmit(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newMockSDKServer(t, mockConfig{EmitStatus: http.StatusInternalServerError})
	defer srv.Close()

	_, err := (&Client{BaseURL: srv.URL}).CallActor(context.Background(), CallActorRequest{
		ChannelID: "ch-1",
		ActorID:   "tool:xhs",
		Type:      "xhs.publish",
		Payload:   json.RawMessage(`{"title":"hello"}`),
		Timeout:   time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("err=%v want HTTP 500", err)
	}
}

func TestListActorsDecodesReadiness(t *testing.T) {
	srv := newMockSDKServer(t, mockConfig{
		ActorListPayload: json.RawMessage(`{
			"channel_id": "ch-1",
			"actors": [{
				"actor_id": "tool:xhs",
				"kind": "tool",
				"binding": "runtime_inbound_via_relay",
				"ready": false,
				"ready_reason": "device_offline",
				"ready_detail": {"device_state": "offline"},
				"last_ready_at": 1700000001000,
				"last_state_change_at": 1700000002000,
				"types": [{
					"type": "xhs.publish",
					"allowed_kinds": ["request","response"],
					"max_pending_ms": 30000
				}]
			}]
		}`),
	})
	defer srv.Close()

	actors, err := (&Client{BaseURL: srv.URL, SessionToken: "tok"}).ListActors(context.Background(), "ch-1")
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	if len(actors) != 1 {
		t.Fatalf("actors len=%d", len(actors))
	}
	got := actors[0]
	if got.ActorID != "tool:xhs" || got.Ready || got.ReadyReason != "device_offline" {
		t.Fatalf("actor=%+v", got)
	}
	if got.LastReadyAt != 1700000001000 || got.LastStateChangeAt != 1700000002000 {
		t.Fatalf("timestamps=%+v", got)
	}
	if len(got.Types) != 1 || got.Types[0].Type != "xhs.publish" || got.Types[0].MaxPendingMs != 30000 {
		t.Fatalf("types=%+v", got.Types)
	}
}

func TestActorStatusUsesReservedActorStatusCall(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newMockSDKServer(t, mockConfig{
		WantRequestType: "actor.status",
		WantAudience:    "tool:xhs",
		ResponsePayload: json.RawMessage(`{
			"status":"completed",
			"available":false,
			"reason":"extension_disconnected",
			"kind":"tool",
			"binding":"runtime_outbound",
			"last_ready_at":1700000001000,
			"last_state_change_at":1700000002000,
			"detail":{"extension_connected":false},
			"checked_at":1700000003000
		}`),
	})
	defer srv.Close()

	status, err := (&Client{BaseURL: srv.URL}).ActorStatus(context.Background(), "ch-1", "tool:xhs")
	if err != nil {
		t.Fatalf("ActorStatus: %v", err)
	}
	if status.Available || status.Reason != "extension_disconnected" || status.Kind != "tool" || status.Binding != "runtime_outbound" {
		t.Fatalf("status=%+v", status)
	}
	if status.CheckedAt != 1700000003000 || len(status.Raw) == 0 {
		t.Fatalf("status timestamps/raw=%+v raw=%s", status, string(status.Raw))
	}
	var detail map[string]bool
	if err := json.Unmarshal(status.Detail, &detail); err != nil {
		t.Fatalf("detail JSON: %v", err)
	}
	if detail["extension_connected"] {
		t.Fatalf("detail=%+v", detail)
	}
}

func TestCallActorWebSocketConnectError(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newMockSDKServer(t, mockConfig{WSReject: true})
	defer srv.Close()

	_, err := (&Client{BaseURL: srv.URL}).CallActor(context.Background(), CallActorRequest{
		ChannelID: "ch-1",
		ActorID:   "tool:xhs",
		Type:      "xhs.publish",
		Payload:   json.RawMessage(`{"title":"hello"}`),
		Timeout:   time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "websocket connect") {
		t.Fatalf("err=%v want websocket connect error", err)
	}
}

type mockConfig struct {
	ResponsePayload  json.RawMessage
	ActorListPayload json.RawMessage
	WantRequestType  string
	WantAudience     string
	NoResponse       bool
	EmitStatus       int
	WSReject         bool
}

func newMockSDKServer(t *testing.T, cfg mockConfig) *httptest.Server {
	t.Helper()
	type emittedRequest struct {
		id  string
		typ string
	}
	requests := make(chan emittedRequest, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/channels/ch-1/cursor", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"last_received_seq": 0})
	})
	mux.HandleFunc("/api/channels/ch-1/actors", func(w http.ResponseWriter, r *http.Request) {
		payload := cfg.ActorListPayload
		if len(payload) == 0 {
			payload = json.RawMessage(`{"channel_id":"ch-1","actors":[]}`)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	})
	mux.HandleFunc("/api/channels/ch-1/messages", func(w http.ResponseWriter, r *http.Request) {
		if cfg.EmitStatus != 0 {
			http.Error(w, "emit failed", cfg.EmitStatus)
			return
		}
		var body emitRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if body.Kind != string(message.KindRequest) {
			t.Errorf("kind=%q want request", body.Kind)
		}
		wantType := cfg.WantRequestType
		if wantType == "" {
			wantType = "xhs.publish"
		}
		if body.Type != wantType {
			t.Errorf("type=%q want %q", body.Type, wantType)
		}
		wantAudience := cfg.WantAudience
		if wantAudience == "" {
			wantAudience = "tool:xhs"
		}
		if len(body.Audience) != 1 || body.Audience[0] != wantAudience {
			t.Errorf("audience=%v", body.Audience)
		}
		if string(body.Payload) == "" {
			t.Error("payload empty")
		}
		select {
		case requests <- emittedRequest{id: body.ID, typ: body.Type}:
		default:
		}
		_ = json.NewEncoder(w).Encode(emitAck{MessageID: body.ID, Accepted: true})
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if cfg.WSReject {
			http.Error(w, "no ws", http.StatusInternalServerError)
			return
		}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = ws.Close() }()
		var sub map[string]any
		if err := ws.ReadJSON(&sub); err != nil {
			return
		}
		if sub["type"] != "subscribe" || sub["channel_id"] != "ch-1" {
			t.Errorf("subscribe frame=%v", sub)
		}
		if cfg.NoResponse {
			<-r.Context().Done()
			return
		}
		req := <-requests
		payload := cfg.ResponsePayload
		if len(payload) == 0 {
			payload = json.RawMessage(`{"status":"completed"}`)
		}
		frame := wsPushFrame{
			Type:      "message",
			ChannelID: "ch-1",
			Seq:       2,
			Envelope: mustMarshal(t, message.Envelope{
				ID:        "resp-1",
				TS:        time.Now().UnixMilli(),
				ChannelID: channel.ID("ch-1"),
				Sender: message.Sender{
					Kind: actor.KindTool,
					ID:   actor.ActorID("tool:xhs"),
				},
				Kind:       message.KindResponse,
				Type:       req.typ,
				Payload:    payload,
				ParentID:   message.ID(req.id),
				Visibility: message.VisibilityPublic,
				Audience:   message.Audience{},
			}),
		}
		_ = ws.WriteJSON(frame)
	})
	return httptest.NewServer(mux)
}

func withNoSubscribeDelay(t *testing.T) {
	t.Helper()
	old := subscribeSettleDelay
	subscribeSettleDelay = 0
	t.Cleanup(func() { subscribeSettleDelay = old })
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func assertJSONEqual(t *testing.T, got json.RawMessage, want string) {
	t.Helper()
	var gotV any
	if err := json.Unmarshal(got, &gotV); err != nil {
		t.Fatalf("got not JSON: %v raw=%s", err, string(got))
	}
	var wantV any
	if err := json.Unmarshal([]byte(want), &wantV); err != nil {
		t.Fatalf("want not JSON: %v", err)
	}
	gotRaw, _ := json.Marshal(gotV)
	wantRaw, _ := json.Marshal(wantV)
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("json=%s want %s", gotRaw, wantRaw)
	}
}
