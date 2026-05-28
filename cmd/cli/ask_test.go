package main_test

// ask_test.go covers M1.6-T5 phase-4 `coagent ask|emit|answer`
// subcommands. Each test boots a tiny in-process HTTP server to stand
// in for the gateway and exec's the freshly built cmd/cli binary
// against it. The assertions cover (i) flag validation surface, (ii)
// happy-path POST shape and stdout envelope, and (iii) the
// reject_reason → exit code mapping used by adapters/device/xhs/cli's
// real_provider classifyExit logic.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeGateway is a tiny stub for POST /api/channels/:chID/messages.
// It captures the inbound body so tests can verify the wrapper shape,
// then returns whatever response (status + body) the test configured.
type fakeGateway struct {
	resp       string
	statusCode int

	lastPath   string
	lastBody   string
	lastAuth   string
	lastMethod string
}

func newFakeGateway(t *testing.T, status int, body string) (*httptest.Server, *fakeGateway) {
	t.Helper()
	fg := &fakeGateway{resp: body, statusCode: status}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fg.lastMethod = r.Method
		fg.lastPath = r.URL.Path
		fg.lastAuth = r.Header.Get("Authorization")
		raw, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		fg.lastBody = string(raw)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(fg.statusCode)
		_, _ = io.WriteString(w, fg.resp)
	}))
	t.Cleanup(srv.Close)
	return srv, fg
}

// runCLI execs the CLI binary with args + env additions and returns
// (stdout, stderr, exitCode). It doesn't pipe stdin because none of
// the subcommands consume any.
func runCLI(t *testing.T, bin string, env, args []string) (string, string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append([]string{}, env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if exit, ok := err.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatalf("run cli: %v", err)
	}
	return stdout.String(), stderr.String(), code
}

// TestAsk_HappyPath asserts the POST body shape (type/kind/audience/
// payload) AND that stdout carries id/correlation_id from the gateway
// ack — the contract xhs-cli/internal/xhs/real_provider depends on.
func TestAsk_HappyPath(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	srv, fg := newFakeGateway(t, http.StatusOK, `{
		"frame_id": "frame-abc",
		"daemon_ack_id": "ack-1",
		"message_id": "msg-xyz",
		"correlation_id": "msg-xyz",
		"accepted": true,
		"seq": 42
	}`)

	stdout, stderr, code := runCLI(t, bin,
		[]string{
			"COAGENT_SERVER_URL=" + srv.URL,
			"COAGENT_SESSION_TOKEN=tok-1",
			"COAGENT_CHANNEL_ID=ch-1",
		},
		[]string{
			"ask",
			"--type", "xhs.publish",
			"--audience", "tool:xhs",
			"--payload", `{"title":"hi"}`,
		})
	if code != 0 {
		t.Fatalf("exit=%d (want 0) stderr=%q stdout=%q", code, stderr, stdout)
	}

	if fg.lastMethod != "POST" {
		t.Errorf("method=%q want POST", fg.lastMethod)
	}
	if fg.lastPath != "/api/channels/ch-1/messages" {
		t.Errorf("path=%q", fg.lastPath)
	}
	if fg.lastAuth != "Bearer tok-1" {
		t.Errorf("auth=%q", fg.lastAuth)
	}

	var got struct {
		ID       string          `json:"id"`
		Type     string          `json:"type"`
		Kind     string          `json:"kind"`
		Audience []string        `json:"audience"`
		Payload  json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal([]byte(fg.lastBody), &got); err != nil {
		t.Fatalf("decode posted body: %v\nraw=%s", err, fg.lastBody)
	}
	// R4-3: caller MUST supply envelope.id; CLI defaults to a fresh uuid.
	if got.ID == "" {
		t.Errorf("posted body missing required envelope.id (R4-3); raw=%s", fg.lastBody)
	}
	if got.Type != "xhs.publish" || got.Kind != "request" {
		t.Errorf("type/kind=%q/%q", got.Type, got.Kind)
	}
	if len(got.Audience) != 1 || got.Audience[0] != "tool:xhs" {
		t.Errorf("audience=%v", got.Audience)
	}
	if string(got.Payload) != `{"title":"hi"}` {
		t.Errorf("payload=%s", string(got.Payload))
	}

	var out struct {
		ID            string `json:"id"`
		CorrelationID string `json:"correlation_id"`
		Kind          string `json:"kind"`
		FrameID       string `json:"frame_id"`
		Seq           int    `json:"seq"`
	}
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("decode stdout: %v\nraw=%s", err, stdout)
	}
	if out.ID != "msg-xyz" || out.CorrelationID != "msg-xyz" || out.Kind != "request" {
		t.Errorf("stdout id/correlation_id/kind=%q/%q/%q", out.ID, out.CorrelationID, out.Kind)
	}
	if out.FrameID != "frame-abc" {
		t.Errorf("stdout frame_id=%q", out.FrameID)
	}
	if out.Seq != 42 {
		t.Errorf("stdout seq=%d", out.Seq)
	}
}

// TestAsk_MissingType: --type required → exit=2, reject JSON on stderr.
func TestAsk_MissingType(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	_, stderr, code := runCLI(t, bin,
		[]string{"COAGENT_CHANNEL_ID=ch-1"},
		[]string{"ask", "--audience", "a", "--payload", `{}`},
	)
	if code != 2 {
		t.Errorf("exit=%d want 2", code)
	}
	if !strings.Contains(stderr, `"reason":"usage_error"`) {
		t.Errorf("stderr missing usage_error reject: %q", stderr)
	}
}

// TestAsk_MissingAudience: kind=request must have exactly 1 concrete
// audience. Empty → usage_error.
func TestAsk_MissingAudience(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	_, stderr, code := runCLI(t, bin,
		[]string{"COAGENT_CHANNEL_ID=ch-1"},
		[]string{"ask", "--type", "x.y", "--payload", `{}`},
	)
	if code != 2 || !strings.Contains(stderr, "usage_error") {
		t.Errorf("exit=%d stderr=%q", code, stderr)
	}
}

// TestAsk_MissingChannel: --channel + COAGENT_CHANNEL_ID both empty →
// usage_error.
func TestAsk_MissingChannel(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	_, stderr, code := runCLI(t, bin,
		[]string{}, // no COAGENT_CHANNEL_ID
		[]string{"ask", "--type", "x.y", "--audience", "a", "--payload", `{}`},
	)
	if code != 2 || !strings.Contains(stderr, "COAGENT_CHANNEL_ID") {
		t.Errorf("exit=%d stderr=%q", code, stderr)
	}
}

// TestAsk_BadPayload: --payload that is not valid JSON → exit=6
// (flag_format_error) so the agent can distinguish from a missing flag.
func TestAsk_BadPayload(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	_, stderr, code := runCLI(t, bin,
		[]string{"COAGENT_CHANNEL_ID=ch-1"},
		[]string{"ask", "--type", "x.y", "--audience", "a", "--payload", "not json"},
	)
	if code != 6 {
		t.Errorf("exit=%d want 6 (flag_format)", code)
	}
	if !strings.Contains(stderr, "flag_format_error") {
		t.Errorf("stderr missing flag_format_error: %q", stderr)
	}
}

// TestAsk_HarnessReject: server returns 409 with reject_reason; expect
// exit=3 and the reason verbatim in the stderr reject JSON.
func TestAsk_HarnessReject(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	srv, _ := newFakeGateway(t, http.StatusConflict, `{
		"reject_reason": "harness_kind_not_allowed_for_type",
		"reject_detail": "type x.y rejects kind=request"
	}`)
	_, stderr, code := runCLI(t, bin,
		[]string{
			"COAGENT_SERVER_URL=" + srv.URL,
			"COAGENT_SESSION_TOKEN=tok",
			"COAGENT_CHANNEL_ID=ch-1",
		},
		[]string{"ask", "--type", "x.y", "--audience", "a", "--payload", `{}`},
	)
	if code != 3 {
		t.Errorf("exit=%d want 3", code)
	}
	if !strings.Contains(stderr, `"reason":"harness_kind_not_allowed_for_type"`) {
		t.Errorf("stderr missing reason: %q", stderr)
	}
}

// TestEmit_HappyPath: kind=event requires explicit audience and pipes
// the payload through. Verifies the body's `kind` key flips to "event".
func TestEmit_HappyPath(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	srv, fg := newFakeGateway(t, http.StatusOK, `{"frame_id":"f1","daemon_ack_id":"a1","message_id":"e1","correlation_id":"e1","accepted":true}`)

	stdout, stderr, code := runCLI(t, bin,
		[]string{
			"COAGENT_SERVER_URL=" + srv.URL,
			"COAGENT_SESSION_TOKEN=t",
			"COAGENT_CHANNEL_ID=ch-1",
		},
		[]string{"emit", "--type", "core.system_event", "--audience", "agent:alpha", "--payload", `{"event":"foo"}`},
	)
	if code != 0 {
		t.Fatalf("exit=%d stderr=%q", code, stderr)
	}
	var got map[string]any
	_ = json.Unmarshal([]byte(fg.lastBody), &got)
	if got["kind"] != "event" {
		t.Errorf("kind=%v want event", got["kind"])
	}
	if !reflect.DeepEqual(got["audience"], []any{"agent:alpha"}) {
		t.Errorf("audience=%v want [agent:alpha]; body=%s", got["audience"], fg.lastBody)
	}
	var out map[string]any
	_ = json.Unmarshal([]byte(stdout), &out)
	if out["kind"] != "event" {
		t.Errorf("stdout kind=%v", out["kind"])
	}
}

func TestEmit_MissingAudience(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	_, stderr, code := runCLI(t, bin,
		[]string{"COAGENT_CHANNEL_ID=ch-1"},
		[]string{"emit", "--type", "core.system_event", "--payload", `{"event":"foo"}`},
	)
	if code != 2 || !strings.Contains(stderr, "--audience is required") {
		t.Errorf("exit=%d stderr=%q", code, stderr)
	}
}

// TestAnswer_RequiresParent: kind=response without --parent-id → usage.
func TestAnswer_RequiresParent(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	_, stderr, code := runCLI(t, bin,
		[]string{"COAGENT_CHANNEL_ID=ch-1"},
		[]string{"answer", "--type", "x.y", "--audience", "agent:alpha", "--payload", `{}`},
	)
	if code != 2 || !strings.Contains(stderr, "parent-id") {
		t.Errorf("exit=%d stderr=%q", code, stderr)
	}
}

// TestAnswer_HappyPath: with --parent-id, the body must carry parent_id
// and kind=response.
func TestAnswer_HappyPath(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	srv, fg := newFakeGateway(t, http.StatusOK, `{"frame_id":"f1","daemon_ack_id":"a1","message_id":"r1","accepted":true}`)

	_, stderr, code := runCLI(t, bin,
		[]string{
			"COAGENT_SERVER_URL=" + srv.URL,
			"COAGENT_SESSION_TOKEN=t",
			"COAGENT_CHANNEL_ID=ch-1",
		},
		[]string{"answer", "--type", "xhs.publish", "--audience", "agent:alpha", "--parent-id", "req-1", "--payload", `{"status":"completed","note_id":"n-1"}`},
	)
	if code != 0 {
		t.Fatalf("exit=%d stderr=%q", code, stderr)
	}
	var got map[string]any
	_ = json.Unmarshal([]byte(fg.lastBody), &got)
	if got["kind"] != "response" || got["parent_id"] != "req-1" {
		t.Errorf("body kind/parent_id=%v/%v raw=%s", got["kind"], got["parent_id"], fg.lastBody)
	}
}

// =====================================================================
// `--watch` surface (response-multitype-refactor §3.7 G).
// =====================================================================

// watchFakeServer simulates the gateway HTTP POST + cursor + WS push
// surface the SDK's Watch() consumes. The test scripts a sequence of
// response envelopes (provisional + final) the WS handler emits after
// the SDK subscribes; the request id the gateway saw at POST time is
// stamped as parent_id on every emitted envelope.
type watchFakeServer struct {
	srv *httptest.Server

	mu          sync.Mutex
	postedReqID string
	posted      chan struct{}
}

// runWatchFakeServer boots a watchFakeServer scripted to emit the
// supplied response payloads (each `{"status":"...","detail":"..."}`).
// kind=response envelopes are pushed in order with parent_id == the
// envelope id the CLI POST'd.
func runWatchFakeServer(t *testing.T, payloads []json.RawMessage) *watchFakeServer {
	t.Helper()
	w := &watchFakeServer{posted: make(chan struct{}, 1)}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/channels/ch-1/cursor", func(rw http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(rw).Encode(map[string]int64{"last_received_seq": 0})
	})
	mux.HandleFunc("/api/channels/ch-1/messages", func(rw http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		var body struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(raw, &body)
		w.mu.Lock()
		w.postedReqID = body.ID
		w.mu.Unlock()
		select {
		case w.posted <- struct{}{}:
		default:
		}
		_ = json.NewEncoder(rw).Encode(map[string]any{
			"message_id":     body.ID,
			"correlation_id": body.ID,
			"frame_id":       "frame-1",
			"accepted":       true,
		})
	})
	mux.HandleFunc("/ws", func(rw http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(rw, r, nil)
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
		// Wait for POST so the parent_id is known before we emit.
		select {
		case <-w.posted:
		case <-time.After(5 * time.Second):
			return
		}
		w.mu.Lock()
		parentID := w.postedReqID
		w.mu.Unlock()

		for i, payload := range payloads {
			// Tiny gap between frames so the CLI side definitely sees
			// each frame as a separate read on stderr.
			time.Sleep(10 * time.Millisecond)
			env := map[string]any{
				"id":         "resp-" + watchTestItoa(i),
				"channel_id": "ch-1",
				"sender":     map[string]any{"kind": "tool", "id": "tool:xhs"},
				"kind":       "response",
				"type":       "xhs.publish",
				"audience":   []string{"user:alice"},
				"payload":    payload,
				"parent_id":  parentID,
				"ts":         time.Now().UnixMilli(),
				"visibility": "public",
			}
			envRaw, _ := json.Marshal(env)
			frame := map[string]any{
				"type":       "message",
				"channel_id": "ch-1",
				"seq":        int64(i + 1),
				"envelope":   json.RawMessage(envRaw),
			}
			if err := ws.WriteJSON(frame); err != nil {
				return
			}
		}
		// Keep the socket open until the client closes — the CLI side
		// closes after observing the final response.
		<-r.Context().Done()
	})
	w.srv = httptest.NewServer(mux)
	t.Cleanup(w.srv.Close)
	return w
}

func watchTestItoa(i int) string {
	if i == 0 {
		return "0"
	}
	out := ""
	for i > 0 {
		out = string(rune('0'+i%10)) + out
		i /= 10
	}
	return out
}

// TestAsk_Watch_StreamsProvisionalAndFinal — the canonical happy path.
// Stream is `received` → `processing` → `completed`. CLI must:
//   - stream the two provisional statuses to stderr (⏳ lines)
//   - print the final payload JSON to stdout
//   - exit 0
//   - keep the legacy emit-ack trace on stderr too
func TestAsk_Watch_StreamsProvisionalAndFinal(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	fake := runWatchFakeServer(t, []json.RawMessage{
		json.RawMessage(`{"status":"received"}`),
		json.RawMessage(`{"status":"processing","detail":"40% done"}`),
		json.RawMessage(`{"status":"completed","note_id":"n-1"}`),
	})

	stdout, stderr, code := runCLI(t, bin,
		[]string{
			"COAGENT_SERVER_URL=" + fake.srv.URL,
			"COAGENT_SESSION_TOKEN=tok",
			"COAGENT_CHANNEL_ID=ch-1",
		},
		[]string{
			"ask",
			"--type", "xhs.publish",
			"--audience", "tool:xhs",
			"--payload", `{"title":"hi"}`,
			"--watch",
			"--timeout", "5s",
		})
	if code != 0 {
		t.Fatalf("exit=%d (want 0) stderr=%q stdout=%q", code, stderr, stdout)
	}

	// stdout must carry the final payload (and ONLY the final payload —
	// the legacy ack moves to stderr under --watch).
	var finalPayload map[string]any
	if err := json.Unmarshal([]byte(stdout), &finalPayload); err != nil {
		t.Fatalf("stdout final payload not JSON: %v\nraw=%q", err, stdout)
	}
	if finalPayload["status"] != "completed" || finalPayload["note_id"] != "n-1" {
		t.Errorf("final payload=%v want status=completed,note_id=n-1", finalPayload)
	}

	// stderr must carry both provisional traces in order, plus the
	// emitted-ack line.
	if !strings.Contains(stderr, "coagent: emitted ") {
		t.Errorf("stderr missing emitted-ack trace: %q", stderr)
	}
	if !strings.Contains(stderr, "⏳ received") {
		t.Errorf("stderr missing 'received' provisional: %q", stderr)
	}
	idxR := strings.Index(stderr, "⏳ received")
	idxP := strings.Index(stderr, "⏳ processing")
	if idxP < 0 || idxP < idxR {
		t.Errorf("stderr order broken: received=%d processing=%d full=%q", idxR, idxP, stderr)
	}
}

// TestAsk_Watch_FinalFailedMapsExitCode — when the final response is
// status=failed, the CLI exits with askExitWatchFailed (8) and emits a
// reject blob with reason="response_failed" + detail mirroring
// payload.reason (proto-layer0 §2.6 terminal failure closed set).
func TestAsk_Watch_FinalFailedMapsExitCode(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	fake := runWatchFakeServer(t, []json.RawMessage{
		json.RawMessage(`{"status":"received"}`),
		json.RawMessage(`{"status":"failed","reason":"receiver_unavailable","detail":"xhs offline"}`),
	})

	stdout, stderr, code := runCLI(t, bin,
		[]string{
			"COAGENT_SERVER_URL=" + fake.srv.URL,
			"COAGENT_SESSION_TOKEN=tok",
			"COAGENT_CHANNEL_ID=ch-1",
		},
		[]string{
			"ask",
			"--type", "xhs.publish",
			"--audience", "tool:xhs",
			"--payload", `{"title":"hi"}`,
			"--watch",
			"--timeout", "5s",
		})
	if code != 8 {
		t.Fatalf("exit=%d want 8; stderr=%q stdout=%q", code, stderr, stdout)
	}
	// stdout still carries the final payload so callers can parse the
	// failure detail programmatically.
	if !strings.Contains(stdout, `"status":"failed"`) {
		t.Errorf("stdout missing failed payload: %q", stdout)
	}
	if !strings.Contains(stderr, `"reason":"response_failed"`) {
		t.Errorf("stderr missing response_failed reject: %q", stderr)
	}
	if !strings.Contains(stderr, "receiver_unavailable") {
		t.Errorf("stderr missing terminal failure reason detail: %q", stderr)
	}
}

// TestAsk_Watch_TimeoutWhenNoFinal — when only provisionals arrive
// before the timeout, CLI exits with askExitWatchTimeout (7).
func TestAsk_Watch_TimeoutWhenNoFinal(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	fake := runWatchFakeServer(t, []json.RawMessage{
		json.RawMessage(`{"status":"received"}`),
		json.RawMessage(`{"status":"processing"}`),
	})

	_, stderr, code := runCLI(t, bin,
		[]string{
			"COAGENT_SERVER_URL=" + fake.srv.URL,
			"COAGENT_SESSION_TOKEN=tok",
			"COAGENT_CHANNEL_ID=ch-1",
		},
		[]string{
			"ask",
			"--type", "xhs.publish",
			"--audience", "tool:xhs",
			"--payload", `{"title":"hi"}`,
			"--watch",
			"--timeout", "300ms",
		})
	if code != 7 {
		t.Fatalf("exit=%d want 7 (watch_timeout); stderr=%q", code, stderr)
	}
	if !strings.Contains(stderr, `"reason":"watch_timeout"`) {
		t.Errorf("stderr missing watch_timeout reject: %q", stderr)
	}
}

// TestAsk_Watch_FastFinalRecoveredViaReplay is the F27 race regression:
// the actor's final response lands in the channel log BEFORE the CLI's
// WS subscribe completes server-side. Without server-side replay the
// CLI would block on the WS waiting for a push that already happened,
// then time out. With the fix the CLI captures cursor before POST and
// passes it as since_seq=N to the subscribe; the server's replay window
// pushes the persisted final down to the CLI on subscribe.
func TestAsk_Watch_FastFinalRecoveredViaReplay(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	fake := runFastFinalFakeServer(t, json.RawMessage(`{"status":"completed","note_id":"n-fast"}`))

	stdout, stderr, code := runCLI(t, bin,
		[]string{
			"COAGENT_SERVER_URL=" + fake.srv.URL,
			"COAGENT_SESSION_TOKEN=tok",
			"COAGENT_CHANNEL_ID=ch-1",
		},
		[]string{
			"ask",
			"--type", "xhs.publish",
			"--audience", "tool:xhs",
			"--payload", `{"title":"hi"}`,
			"--watch",
			"--timeout", "3s",
		})
	if code != 0 {
		t.Fatalf("exit=%d (want 0 — replay should have recovered fast-final) stderr=%q stdout=%q", code, stderr, stdout)
	}
	var finalPayload map[string]any
	if err := json.Unmarshal([]byte(stdout), &finalPayload); err != nil {
		t.Fatalf("stdout final payload not JSON: %v\nraw=%q", err, stdout)
	}
	if finalPayload["status"] != "completed" || finalPayload["note_id"] != "n-fast" {
		t.Errorf("final payload=%v want status=completed,note_id=n-fast", finalPayload)
	}
}

// fastFinalFakeServer simulates the F27 race: the server appends the
// final response to its in-memory log AT EMIT TIME, then the WS handler
// honours since_seq replay so the persisted final can be re-served to
// a late subscriber.
type fastFinalFakeServer struct {
	srv *httptest.Server
}

func runFastFinalFakeServer(t *testing.T, finalPayload json.RawMessage) *fastFinalFakeServer {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	type persistedFrame struct {
		seq int64
		raw json.RawMessage
	}
	var (
		mu        sync.Mutex
		persisted []persistedFrame
		// Capture the request id seen at POST so the WS handler can
		// stamp it as parent_id on the persisted response.
		postedID    string
		emitDoneCh  = make(chan struct{})
		emitDoneRef = &sync.Once{}
	)
	closeEmitDoneOnce := func() {
		emitDoneRef.Do(func() { close(emitDoneCh) })
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/channels/ch-1/cursor", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		cur := int64(0)
		if len(persisted) > 0 {
			cur = persisted[len(persisted)-1].seq
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]int64{"last_received_seq": cur})
	})
	mux.HandleFunc("/api/channels/ch-1/messages", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		var body struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(raw, &body)
		// "Fast final": persist the response at seq=1 INSIDE the emit
		// POST so any later WS subscribe with since_seq=0 can replay it.
		env := map[string]any{
			"id":         "resp-fast",
			"channel_id": "ch-1",
			"sender":     map[string]any{"kind": "tool", "id": "tool:xhs"},
			"kind":       "response",
			"type":       "xhs.publish",
			"audience":   []string{"user:alice"},
			"payload":    finalPayload,
			"parent_id":  body.ID,
			"ts":         time.Now().UnixMilli(),
			"visibility": "public",
		}
		envRaw, _ := json.Marshal(env)
		mu.Lock()
		postedID = body.ID
		persisted = append(persisted, persistedFrame{seq: 1, raw: envRaw})
		mu.Unlock()
		closeEmitDoneOnce()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message_id":     body.ID,
			"correlation_id": body.ID,
			"frame_id":       "frame-1",
			"accepted":       true,
		})
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
		sinceSeq := int64(0)
		switch v := sub["since_seq"].(type) {
		case float64:
			sinceSeq = int64(v)
		case int64:
			sinceSeq = v
		}
		// Wait for the emit to land before serving replay so the test
		// deterministically exercises the late-subscribe path.
		select {
		case <-emitDoneCh:
		case <-r.Context().Done():
			return
		}
		_ = postedID // captured under mu; not needed for the test
		mu.Lock()
		snapshot := make([]persistedFrame, len(persisted))
		copy(snapshot, persisted)
		mu.Unlock()
		for _, p := range snapshot {
			if p.seq <= sinceSeq {
				continue
			}
			frame := map[string]any{
				"type":       "message",
				"channel_id": "ch-1",
				"seq":        p.seq,
				"envelope":   p.raw,
			}
			if err := ws.WriteJSON(frame); err != nil {
				return
			}
		}
		<-r.Context().Done()
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &fastFinalFakeServer{srv: srv}
}

// TestAsk_NoWatchKeepsLegacyBehavior — explicit guard that the default
// (no --watch flag) path is unchanged: stdout carries the gateway ack,
// stderr is silent, exit is 0. Existing xhs-cli RealProvider depends on
// this.
func TestAsk_NoWatchKeepsLegacyBehavior(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	srv, _ := newFakeGateway(t, http.StatusOK, `{
		"frame_id": "frame-no-watch",
		"message_id": "msg-no-watch",
		"correlation_id": "msg-no-watch",
		"accepted": true
	}`)

	stdout, stderr, code := runCLI(t, bin,
		[]string{
			"COAGENT_SERVER_URL=" + srv.URL,
			"COAGENT_SESSION_TOKEN=tok",
			"COAGENT_CHANNEL_ID=ch-1",
		},
		[]string{
			"ask",
			"--type", "xhs.publish",
			"--audience", "tool:xhs",
			"--payload", `{"title":"hi"}`,
		})
	if code != 0 {
		t.Fatalf("exit=%d stderr=%q stdout=%q", code, stderr, stdout)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("stdout: %v raw=%q", err, stdout)
	}
	if out["id"] != "msg-no-watch" {
		t.Errorf("stdout id=%v want msg-no-watch", out["id"])
	}
	if strings.Contains(stderr, "⏳") || strings.Contains(stderr, "emitted ") {
		t.Errorf("stderr should be silent without --watch: %q", stderr)
	}
}
