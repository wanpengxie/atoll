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

// streamConfig drives newStreamingSDKServer, which supports emitting a
// scripted sequence of envelopes (provisional + final) for one request.
type streamConfig struct {
	// Stream is the ordered list of payloads to write back. Each entry
	// is the full response payload JSON the server pushes as a
	// kind=response envelope.
	Stream []json.RawMessage
	// FinalAfterStream, when non-empty, is written after the Stream
	// entries with a small delay (lets tests verify that Await only
	// resolves on the final response while Watch sees everything).
	FinalAfterStream json.RawMessage
	// EmitDelay sleeps before sending each stream frame.
	EmitDelay time.Duration
}

// newStreamingSDKServer is a richer mock than newMockSDKServer — it
// streams multiple response envelopes per request so tests can verify
// provisional-vs-final dispatching.
func newStreamingSDKServer(t *testing.T, cfg streamConfig) *httptest.Server {
	t.Helper()
	type emittedRequest struct {
		id  string
		typ string
	}
	requests := make(chan emittedRequest, 4)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/channels/ch-1/cursor", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"last_received_seq": 0})
	})
	mux.HandleFunc("/api/channels/ch-1/messages", func(w http.ResponseWriter, r *http.Request) {
		var body emitRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		select {
		case requests <- emittedRequest{id: body.ID, typ: body.Type}:
		default:
		}
		_ = json.NewEncoder(w).Encode(emitAck{MessageID: body.ID, Accepted: true})
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
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
		req := <-requests
		writeEnvelope := func(id string, payload json.RawMessage) {
			frame := wsPushFrame{
				Type:      "message",
				ChannelID: "ch-1",
				Seq:       2,
				Envelope: mustMarshal(t, message.Envelope{
					ID:        message.ID(id),
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
		}
		for i, payload := range cfg.Stream {
			if cfg.EmitDelay > 0 {
				time.Sleep(cfg.EmitDelay)
			}
			writeEnvelope("resp-"+itoaShort(i), payload)
		}
		if len(cfg.FinalAfterStream) > 0 {
			if cfg.EmitDelay > 0 {
				time.Sleep(cfg.EmitDelay)
			}
			writeEnvelope("resp-final", cfg.FinalAfterStream)
		}
		<-r.Context().Done()
	})
	return httptest.NewServer(mux)
}

func itoaShort(i int) string {
	if i == 0 {
		return "0"
	}
	digits := ""
	for i > 0 {
		digits = string(rune('0'+i%10)) + digits
		i /= 10
	}
	return digits
}

// TestCallActorSkipsProvisionalAndResolvesOnFinal verifies that the
// sync CallActor surface ignores provisional responses (Layer 2 core +
// Layer 3 extension) and only returns when the final response arrives.
func TestCallActorSkipsProvisionalAndResolvesOnFinal(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newStreamingSDKServer(t, streamConfig{
		Stream: []json.RawMessage{
			json.RawMessage(`{"status":"received"}`),
			json.RawMessage(`{"status":"processing","progress_percent":0.4}`),
			json.RawMessage(`{"status":"xhs.login_queued","queue_position":3}`),
		},
		FinalAfterStream: json.RawMessage(`{"status":"completed","note_id":"n-1"}`),
	})
	defer srv.Close()

	res, err := (&Client{BaseURL: srv.URL}).CallActor(context.Background(), CallActorRequest{
		ChannelID: "ch-1",
		ActorID:   "tool:xhs",
		Type:      "xhs.publish",
		Payload:   json.RawMessage(`{"title":"hi"}`),
		Timeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("CallActor: %v", err)
	}
	if !res.OK {
		t.Fatalf("expected OK, got %+v", res.Error)
	}
	assertJSONEqual(t, res.Data, `{"note_id":"n-1"}`)
}

// TestSubmitAndWatchObserveProvisionalAndFinal verifies the streaming
// Submit + Watch surface delivers every envelope (provisional + final)
// in order, with IsFinal set correctly.
func TestSubmitAndWatchObserveProvisionalAndFinal(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newStreamingSDKServer(t, streamConfig{
		Stream: []json.RawMessage{
			json.RawMessage(`{"status":"received"}`),
			json.RawMessage(`{"status":"processing","progress_percent":0.4}`),
		},
		FinalAfterStream: json.RawMessage(`{"status":"completed","note_id":"n-1"}`),
	})
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := &Client{BaseURL: srv.URL}

	// Start Watch first so we don't race the server reading from
	// requests channel (Watch must subscribe before Submit completes).
	// Use a known request id so Watch can filter without round-trip.
	requestID := "req-watch-1"
	go func() {
		// Submit after a brief delay so Watch has time to subscribe.
		time.Sleep(20 * time.Millisecond)
		if _, err := client.Submit(ctx, SubmitRequest{
			ChannelID: "ch-1",
			ActorID:   "tool:xhs",
			Type:      "xhs.publish",
			Payload:   json.RawMessage(`{"title":"hi"}`),
			RequestID: requestID,
		}); err != nil {
			t.Errorf("Submit: %v", err)
		}
	}()

	watch, err := client.Watch(ctx, "ch-1", requestID)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer watch.Close()

	var statuses []string
	var sawFinal bool
	for ev := range watch.Events() {
		if ev.Err != nil {
			t.Fatalf("watch err: %v", ev.Err)
		}
		if ev.Envelope == nil {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(ev.Envelope.Payload, &payload); err != nil {
			t.Fatalf("payload unmarshal: %v", err)
		}
		statuses = append(statuses, payload["status"].(string))
		if ev.IsFinal {
			sawFinal = true
			break
		}
	}
	if !sawFinal {
		t.Fatalf("expected final response, got statuses=%v", statuses)
	}
	want := []string{"received", "processing", "completed"}
	if !equalStringSlices(statuses, want) {
		t.Fatalf("statuses=%v want %v", statuses, want)
	}
}

// TestAwaitResolvesOnFinalIgnoringProvisional verifies that Await
// returns only when the final response arrives and ignores provisionals.
func TestAwaitResolvesOnFinalIgnoringProvisional(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newStreamingSDKServer(t, streamConfig{
		Stream: []json.RawMessage{
			json.RawMessage(`{"status":"received"}`),
			json.RawMessage(`{"status":"queued","queue_position":1}`),
		},
		FinalAfterStream: json.RawMessage(`{"status":"completed","note_id":"n-2"}`),
	})
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := &Client{BaseURL: srv.URL}

	requestID := "req-await-1"
	go func() {
		time.Sleep(20 * time.Millisecond)
		if _, err := client.Submit(ctx, SubmitRequest{
			ChannelID: "ch-1",
			ActorID:   "tool:xhs",
			Type:      "xhs.publish",
			Payload:   json.RawMessage(`{"title":"hi"}`),
			RequestID: requestID,
		}); err != nil {
			t.Errorf("Submit: %v", err)
		}
	}()

	res, err := client.Await(ctx, "ch-1", requestID, time.Second)
	if err != nil {
		t.Fatalf("Await: %v", err)
	}
	if !res.OK {
		t.Fatalf("expected OK final result, got %+v", res.Error)
	}
	assertJSONEqual(t, res.Data, `{"note_id":"n-2"}`)
}

// TestAwaitTimesOutWhenOnlyProvisionalArrives — when no final
// response arrives before the timeout, Await returns a timeout result
// (substrate may still emit final later; this matches CallActor
// behaviour).
func TestAwaitTimesOutWhenOnlyProvisionalArrives(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newStreamingSDKServer(t, streamConfig{
		Stream: []json.RawMessage{
			json.RawMessage(`{"status":"received"}`),
			json.RawMessage(`{"status":"processing"}`),
		},
	})
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := &Client{BaseURL: srv.URL}

	requestID := "req-await-timeout"
	go func() {
		time.Sleep(20 * time.Millisecond)
		_, _ = client.Submit(ctx, SubmitRequest{
			ChannelID: "ch-1",
			ActorID:   "tool:xhs",
			Type:      "xhs.publish",
			Payload:   json.RawMessage(`{"title":"hi"}`),
			RequestID: requestID,
		})
	}()

	res, err := client.Await(ctx, "ch-1", requestID, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("Await: %v", err)
	}
	if res.OK || res.Error == nil || res.Error.Code != "timeout" {
		t.Fatalf("expected timeout result, got %+v", res)
	}
}

func TestSubmitGeneratesRequestIDWhenAbsent(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newStreamingSDKServer(t, streamConfig{})
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	client := &Client{BaseURL: srv.URL}
	res, err := client.Submit(ctx, SubmitRequest{
		ChannelID: "ch-1",
		ActorID:   "tool:xhs",
		Type:      "xhs.publish",
		Payload:   json.RawMessage(`{"title":"hi"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !strings.HasPrefix(res.RequestID, "req-") {
		t.Fatalf("RequestID=%q want prefix req-", res.RequestID)
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
