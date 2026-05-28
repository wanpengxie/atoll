package coagentsdk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// withNoSubscribeDelay is preserved as a no-op so existing call sites
// don't churn. The SDK no longer has a subscribe-settle delay — server
// subscribe replay covers the race window between WS subscribe and
// emit. Remove call sites lazily as their containing tests are updated.
func withNoSubscribeDelay(t *testing.T) { t.Helper() }

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// TestWatchContextTimeoutExitsCleanly is the regression test for the
// gorilla "repeated read on failed websocket connection" panic. The
// scenario: caller drives Watch with context.WithTimeout, server never
// emits a final response, ctx expires. The old SDK implementation used
// per-iter SetReadDeadline → nextReadWindow shrank to 1ns once ctx was
// past → gorilla's read state corrupts after the first i/o timeout
// (`hideTempErr` drops Temporary()) and any subsequent ReadMessage
// counts toward the 1000-call panic counter. With the ws-close-on-ctx
// pattern the read goroutine must unwind cleanly without panic.
func TestWatchContextTimeoutExitsCleanly(t *testing.T) {
	withNoSubscribeDelay(t)
	// Server keeps the connection open and never writes; ctx timeout
	// is the only path that ends the watch.
	srv := newSilentSDKServer(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client := &Client{BaseURL: srv.URL, SessionToken: "tok"}
	watch, err := client.Watch(ctx, "ch-1", "req-watch-timeout")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Drain events channel until it closes. With the fix, ctx
	// expiring closes the ws which unblocks ReadMessage → goroutine
	// exits → events channel closes. No panic.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-watch.Events():
			if !ok {
				// Channel closed — clean exit.
				watch.Close()
				return
			}
			// Any event before timeout is unexpected (server is silent).
		case <-deadline:
			t.Fatalf("watch did not exit within 2s after ctx timeout (100ms)")
		}
	}
}

// TestWatchExplicitCloseExitsCleanly verifies Close() on a live Watch
// shuts down the read goroutine without panic. Complements
// TestWatchContextTimeoutExitsCleanly which exercises the ctx path.
func TestWatchExplicitCloseExitsCleanly(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newSilentSDKServer(t)
	defer srv.Close()

	client := &Client{BaseURL: srv.URL, SessionToken: "tok"}
	watch, err := client.Watch(context.Background(), "ch-1", "req-watch-close")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Let the read goroutine settle into ReadMessage.
	time.Sleep(20 * time.Millisecond)

	closed := make(chan struct{})
	go func() {
		watch.Close()
		close(closed)
	}()

	select {
	case <-closed:
		// Close returned — goroutine exited.
	case <-time.After(2 * time.Second):
		t.Fatalf("watch.Close did not return within 2s")
	}

	// Events channel must be closed after Close returns.
	select {
	case _, ok := <-watch.Events():
		if ok {
			t.Fatalf("events channel still open after Close")
		}
	default:
		t.Fatalf("events channel not closed after Close")
	}
}

// TestCallActorContextCancelExits verifies CallActor's waitResponse
// path also unwinds cleanly when the parent ctx is cancelled
// mid-flight (the same race lived in waitResponse via nextReadWindow).
func TestCallActorContextCancelExits(t *testing.T) {
	withNoSubscribeDelay(t)
	srv := newMockSDKServer(t, mockConfig{NoResponse: true})
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	done := make(chan struct {
		res *CallActorResult
		err error
	}, 1)
	go func() {
		res, err := (&Client{BaseURL: srv.URL}).CallActor(ctx, CallActorRequest{
			ChannelID: "ch-1",
			ActorID:   "tool:xhs",
			Type:      "xhs.publish",
			Payload:   json.RawMessage(`{"title":"hello"}`),
			Timeout:   5 * time.Second, // larger than ctx timeout
		})
		done <- struct {
			res *CallActorResult
			err error
		}{res, err}
	}()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("CallActor err=%v", got.err)
		}
		if got.res == nil || got.res.OK || got.res.Error == nil || got.res.Error.Code != "timeout" {
			t.Fatalf("expected timeout result, got %+v", got.res)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("CallActor did not return within 3s after ctx cancel (150ms)")
	}
}

// newSilentSDKServer is a minimal mock server that handles cursor +
// websocket subscribe but never sends a frame back. Used to exercise
// the Watch/CallActor read-loop ctx-cancel exit path.
func newSilentSDKServer(t *testing.T) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/channels/ch-1/cursor", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"last_received_seq": 0})
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = ws.Close() }()
		// Consume the subscribe frame but never write back.
		var sub map[string]any
		if err := ws.ReadJSON(&sub); err != nil {
			return
		}
		<-r.Context().Done()
	})
	return httptest.NewServer(mux)
}

// TestSubmitAwait_FastFinalRecoveredViaReplay is the D18 race
// regression: actor emits the final response BEFORE the client's
// Watch/Await subscribes the WS, but the server's since_seq=N replay
// (driven by SubmitResult.SinceSeq) covers the gap so Await still
// returns the final result.
//
// We simulate the race by:
//  1. fake server returns cursor=10 at Submit-time
//  2. POST /messages: server "processes" instantly and appends the
//     final response to its in-memory channel log at seq=11 BEFORE
//     responding to the client
//  3. client then calls Await — by the time the WS subscribes, the
//     final is already persisted. Server-side replay (since_seq=10)
//     pushes seq=11 down to the client.
//
// Without the fix (server ignored since_seq) the client would block on
// the WS waiting for a live push that already happened, then time out.
func TestSubmitAwait_FastFinalRecoveredViaReplay(t *testing.T) {
	t.Parallel()
	srv := newReplayFakeServer(t, replayFakeConfig{
		CursorAtSubmit:  10,
		FinalSeq:        11,
		FinalPayload:    json.RawMessage(`{"status":"completed","note_id":"n-fast"}`),
		EmitBeforeReply: true,
	})
	defer srv.Close()

	client := &Client{BaseURL: srv.URL, SessionToken: "tok"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sub, err := client.Submit(ctx, SubmitRequest{
		ChannelID: "ch-1",
		ActorID:   "tool:xhs",
		Type:      "xhs.publish",
		Payload:   json.RawMessage(`{"title":"hi"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if sub.SinceSeq != 10 {
		t.Fatalf("SinceSeq=%d want 10", sub.SinceSeq)
	}

	res, err := client.Await(ctx, "ch-1", sub.RequestID, 2*time.Second, WithSinceSeq(sub.SinceSeq))
	if err != nil {
		t.Fatalf("Await: %v", err)
	}
	if !res.OK {
		t.Fatalf("Await result not OK: %+v", res.Error)
	}
	assertJSONEqual(t, res.Data, `{"note_id":"n-fast"}`)
}

// TestSubmitAndAwait_FastFinalRecoveredViaReplay is the NF3 regression:
// the one-call SubmitAndAwait sugar MUST thread the submit-time cursor
// into the watch automatically, so a fast final emitted before the WS
// subscribe is recovered via replay WITHOUT the caller passing
// WithSinceSeq manually. Same race scenario as the D18 Submit+Await test.
func TestSubmitAndAwait_FastFinalRecoveredViaReplay(t *testing.T) {
	t.Parallel()
	srv := newReplayFakeServer(t, replayFakeConfig{
		CursorAtSubmit:  10,
		FinalSeq:        11,
		FinalPayload:    json.RawMessage(`{"status":"completed","note_id":"n-sugar"}`),
		EmitBeforeReply: true,
	})
	defer srv.Close()

	client := &Client{BaseURL: srv.URL, SessionToken: "tok"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	res, err := client.SubmitAndAwait(ctx, SubmitRequest{
		ChannelID: "ch-1",
		ActorID:   "tool:xhs",
		Type:      "xhs.publish",
		Payload:   json.RawMessage(`{"title":"hi"}`),
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("SubmitAndAwait: %v", err)
	}
	if !res.OK {
		t.Fatalf("result not OK: %+v", res.Error)
	}
	assertJSONEqual(t, res.Data, `{"note_id":"n-sugar"}`)
}

// TestSubmitAndWatch_StreamsFinal verifies the one-call SubmitAndWatch
// sugar opens a stream that delivers the (fast) final response, again
// without the caller threading SinceSeq by hand (NF3).
func TestSubmitAndWatch_StreamsFinal(t *testing.T) {
	t.Parallel()
	srv := newReplayFakeServer(t, replayFakeConfig{
		CursorAtSubmit:  10,
		FinalSeq:        11,
		FinalPayload:    json.RawMessage(`{"status":"completed","note_id":"n-stream"}`),
		EmitBeforeReply: true,
	})
	defer srv.Close()

	client := &Client{BaseURL: srv.URL, SessionToken: "tok"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	watch, err := client.SubmitAndWatch(ctx, SubmitRequest{
		ChannelID: "ch-1",
		ActorID:   "tool:xhs",
		Type:      "xhs.publish",
		Payload:   json.RawMessage(`{"title":"hi"}`),
	})
	if err != nil {
		t.Fatalf("SubmitAndWatch: %v", err)
	}
	defer watch.Close()

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatalf("SubmitAndWatch: timed out waiting for final response")
		case ev, ok := <-watch.Events():
			if !ok {
				t.Fatalf("SubmitAndWatch: stream closed before final")
			}
			if ev.Err != nil {
				t.Fatalf("SubmitAndWatch event err: %v", ev.Err)
			}
			if ev.Envelope == nil || !ev.IsFinal {
				continue
			}
			res, err := resultFromResponse(*ev.Envelope)
			if err != nil {
				t.Fatalf("resultFromResponse: %v", err)
			}
			if !res.OK {
				t.Fatalf("final not OK: %+v", res.Error)
			}
			assertJSONEqual(t, res.Data, `{"note_id":"n-stream"}`)
			return
		}
	}
}

// replayFakeConfig parameterises newReplayFakeServer for the
// fast-final race scenario.
type replayFakeConfig struct {
	// CursorAtSubmit is what GET /cursor returns BEFORE the emit POST.
	// Submit captures this as SubmitResult.SinceSeq.
	CursorAtSubmit int64
	// FinalSeq is the seq the final response is "persisted" at.
	FinalSeq int64
	// FinalPayload is the response envelope payload.
	FinalPayload json.RawMessage
	// EmitBeforeReply: if true, the emit POST appends the final to the
	// server's persistent log BEFORE replying to the client — the
	// scenario where the actor races the WS subscribe.
	EmitBeforeReply bool
}

func newReplayFakeServer(t *testing.T, cfg replayFakeConfig) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	type persistedEnvelope struct {
		seq int64
		env message.Envelope
	}
	var (
		mu        sync.Mutex
		persisted []persistedEnvelope
		// requestID seen by the emit POST, captured so the WS handler
		// can stamp it as parent_id on the final response.
		emittedReqID string
		emitDone     = make(chan struct{})
	)

	persist := func(seq int64, parentID string) {
		mu.Lock()
		defer mu.Unlock()
		persisted = append(persisted, persistedEnvelope{
			seq: seq,
			env: message.Envelope{
				ID:        "resp-fast",
				TS:        time.Now().UnixMilli(),
				ChannelID: channel.ID("ch-1"),
				Sender: message.Sender{
					Kind: actor.KindTool,
					ID:   actor.ActorID("tool:xhs"),
				},
				Kind:       message.KindResponse,
				Type:       "xhs.publish",
				Payload:    cfg.FinalPayload,
				ParentID:   message.ID(parentID),
				Visibility: message.VisibilityPublic,
				Audience:   message.Audience{},
			},
		})
	}

	snapshotPersisted := func() []persistedEnvelope {
		mu.Lock()
		defer mu.Unlock()
		out := make([]persistedEnvelope, len(persisted))
		copy(out, persisted)
		return out
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/channels/ch-1/cursor", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"last_received_seq": cfg.CursorAtSubmit})
	})
	mux.HandleFunc("/api/channels/ch-1/messages", func(w http.ResponseWriter, r *http.Request) {
		var body emitRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		emittedReqID = body.ID
		mu.Unlock()
		if cfg.EmitBeforeReply {
			persist(cfg.FinalSeq, body.ID)
			close(emitDone)
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
		if sub["type"] != "subscribe" {
			return
		}
		// Server replay contract: re-emit every persisted envelope with
		// seq > since_seq, ASC.
		sinceSeq := int64(0)
		switch v := sub["since_seq"].(type) {
		case float64:
			sinceSeq = int64(v)
		case int64:
			sinceSeq = v
		}
		// Wait for emit so we know which envelopes have been persisted
		// before serving the replay window.
		select {
		case <-emitDone:
		case <-r.Context().Done():
			return
		}
		_ = emittedReqID // captured under mu; not used outside
		for _, p := range snapshotPersisted() {
			if p.seq <= sinceSeq {
				continue
			}
			frame := wsPushFrame{
				Type:      "message",
				ChannelID: "ch-1",
				Seq:       p.seq,
				Envelope:  mustMarshal(t, p.env),
			}
			if err := ws.WriteJSON(frame); err != nil {
				return
			}
		}
		<-r.Context().Done()
	})
	return httptest.NewServer(mux)
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
