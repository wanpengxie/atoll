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
		ActorID:   "tool:xhs-adapter",
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
		ActorID:   "tool:xhs-adapter",
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
		ActorID:   "tool:xhs-adapter",
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
		ActorID:   "tool:xhs-adapter",
		Type:      "xhs.publish",
		Payload:   json.RawMessage(`{"title":"hello"}`),
		Timeout:   time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("err=%v want HTTP 500", err)
	}
}

func TestCallActorWebSocketConnectError(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newMockSDKServer(t, mockConfig{WSReject: true})
	defer srv.Close()

	_, err := (&Client{BaseURL: srv.URL}).CallActor(context.Background(), CallActorRequest{
		ChannelID: "ch-1",
		ActorID:   "tool:xhs-adapter",
		Type:      "xhs.publish",
		Payload:   json.RawMessage(`{"title":"hello"}`),
		Timeout:   time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "websocket connect") {
		t.Fatalf("err=%v want websocket connect error", err)
	}
}

type mockConfig struct {
	ResponsePayload json.RawMessage
	NoResponse      bool
	EmitStatus      int
	WSReject        bool
}

func newMockSDKServer(t *testing.T, cfg mockConfig) *httptest.Server {
	t.Helper()
	requestIDs := make(chan string, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/channels/ch-1/cursor", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"last_received_seq": 0})
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
		if len(body.Audience) != 1 || body.Audience[0] != "tool:xhs-adapter" {
			t.Errorf("audience=%v", body.Audience)
		}
		if string(body.Payload) == "" {
			t.Error("payload empty")
		}
		select {
		case requestIDs <- body.ID:
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
		reqID := <-requestIDs
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
					ID:   actor.ActorID("tool:xhs-adapter"),
				},
				Kind:       message.KindResponse,
				Type:       "xhs.publish",
				Payload:    payload,
				ParentID:   message.ID(reqID),
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
