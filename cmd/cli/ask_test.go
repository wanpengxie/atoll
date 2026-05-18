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
	"strings"
	"testing"
	"time"
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
			"--audience", "tool:xhs-adapter",
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
		Type     string          `json:"type"`
		Kind     string          `json:"kind"`
		Audience []string        `json:"audience"`
		Payload  json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal([]byte(fg.lastBody), &got); err != nil {
		t.Fatalf("decode posted body: %v\nraw=%s", err, fg.lastBody)
	}
	if got.Type != "xhs.publish" || got.Kind != "request" {
		t.Errorf("type/kind=%q/%q", got.Type, got.Kind)
	}
	if len(got.Audience) != 1 || got.Audience[0] != "tool:xhs-adapter" {
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
		"reject_reason": "kind_not_allowed",
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
	if !strings.Contains(stderr, `"reason":"kind_not_allowed"`) {
		t.Errorf("stderr missing reason: %q", stderr)
	}
}

// TestEmit_NoAudience: kind=event allows empty audience and pipes the
// payload through. Verifies the body's `kind` key flips to "event".
func TestEmit_NoAudience(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	srv, fg := newFakeGateway(t, http.StatusOK, `{"frame_id":"f1","daemon_ack_id":"a1","message_id":"e1","correlation_id":"e1","accepted":true}`)

	stdout, stderr, code := runCLI(t, bin,
		[]string{
			"COAGENT_SERVER_URL=" + srv.URL,
			"COAGENT_SESSION_TOKEN=t",
			"COAGENT_CHANNEL_ID=ch-1",
		},
		[]string{"emit", "--type", "system.event", "--payload", `{"event":"foo"}`},
	)
	if code != 0 {
		t.Fatalf("exit=%d stderr=%q", code, stderr)
	}
	var got map[string]any
	_ = json.Unmarshal([]byte(fg.lastBody), &got)
	if got["kind"] != "event" {
		t.Errorf("kind=%v want event", got["kind"])
	}
	if _, hasAud := got["audience"]; hasAud {
		t.Errorf("emit should not stamp audience by default; body=%s", fg.lastBody)
	}
	var out map[string]any
	_ = json.Unmarshal([]byte(stdout), &out)
	if out["kind"] != "event" {
		t.Errorf("stdout kind=%v", out["kind"])
	}
}

// TestAnswer_RequiresParent: kind=response without --parent-id → usage.
func TestAnswer_RequiresParent(t *testing.T) {
	t.Parallel()
	bin := buildCLI(t)
	_, stderr, code := runCLI(t, bin,
		[]string{"COAGENT_CHANNEL_ID=ch-1"},
		[]string{"answer", "--type", "x.y", "--payload", `{}`},
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
		[]string{"answer", "--type", "xhs.publish", "--parent-id", "req-1", "--payload", `{"status":"completed","note_id":"n-1"}`},
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
