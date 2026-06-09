package device

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// recordingWriter is a harness.Writer test double.
type recordingWriter struct {
	mu     sync.Mutex
	writes []*message.Envelope
}

func (w *recordingWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, env)
	return harness.WriteResult{MessageID: env.ID}, nil
}

func (w *recordingWriter) last(t *testing.T) *message.Envelope {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.writes) == 0 {
		t.Fatal("no response written")
	}
	return w.writes[len(w.writes)-1]
}

const testChannel = channel.ID("ch-test")

func newTestActor(t *testing.T) (*Actor, *recordingWriter) {
	t.Helper()
	w := &recordingWriter{}
	a := NewActor(w, actor.ActorID("device:test"), t.TempDir(), nil)
	return a, w
}

func request(typ string, payload any) *message.Envelope {
	raw, _ := json.Marshal(payload)
	return &message.Envelope{
		ID:        message.ID("req-" + typ),
		ChannelID: testChannel,
		Kind:      message.KindRequest,
		Type:      typ,
		Sender:    message.Sender{Kind: actor.KindAgent, ID: "agent:test"},
		Payload:   raw,
	}
}

// decodeResponse unmarshals the merged response payload and returns the
// status field plus the raw payload for further decoding.
func decodeResponse(t *testing.T, env *message.Envelope) (status string, raw map[string]json.RawMessage) {
	t.Helper()
	if env.Kind != message.KindResponse {
		t.Fatalf("response kind = %s; want response", env.Kind)
	}
	if err := json.Unmarshal(env.Payload, &raw); err != nil {
		t.Fatalf("decode response payload: %v", err)
	}
	_ = json.Unmarshal(raw["status"], &status)
	return status, raw
}

func errorCode(t *testing.T, raw map[string]json.RawMessage) string {
	t.Helper()
	var code string
	_ = json.Unmarshal(raw["error_code"], &code)
	return code
}

func TestWriteReadRoundtrip(t *testing.T) {
	a, w := newTestActor(t)
	ctx := context.Background()

	_ = a.Receive(ctx, request(TypeFileWrite, FileWritePayload{
		Path: "notes/plan.md", Content: "line1\nline2\nline3",
	}))
	status, _ := decodeResponse(t, w.last(t))
	if status != "completed" {
		t.Fatalf("write status = %s", status)
	}

	_ = a.Receive(ctx, request(TypeFileRead, FileReadPayload{Path: "notes/plan.md"}))
	status, raw := decodeResponse(t, w.last(t))
	if status != "completed" {
		t.Fatalf("read status = %s", status)
	}
	var content string
	_ = json.Unmarshal(raw["content"], &content)
	if content != "line1\nline2\nline3" {
		t.Fatalf("content = %q", content)
	}
}

func TestReadLineSlice(t *testing.T) {
	a, w := newTestActor(t)
	ctx := context.Background()

	_ = a.Receive(ctx, request(TypeFileWrite, FileWritePayload{
		Path: "f.txt", Content: "a\nb\nc\nd",
	}))
	_ = a.Receive(ctx, request(TypeFileRead, FileReadPayload{Path: "f.txt", Offset: 1, Limit: 2}))
	_, raw := decodeResponse(t, w.last(t))
	var content string
	_ = json.Unmarshal(raw["content"], &content)
	if content != "b\nc" {
		t.Fatalf("sliced content = %q; want b\\nc", content)
	}
}

func TestPathConfinement(t *testing.T) {
	a, w := newTestActor(t)
	ctx := context.Background()

	for _, p := range []string{"../escape.txt", "/etc/passwd", "a/../../escape.txt"} {
		_ = a.Receive(ctx, request(TypeFileRead, FileReadPayload{Path: p}))
		status, raw := decodeResponse(t, w.last(t))
		if status != "failed" || errorCode(t, raw) != "path_invalid" {
			t.Fatalf("path %q: status=%s code=%s; want failed/path_invalid", p, status, errorCode(t, raw))
		}
	}
}

func TestEditUniqueness(t *testing.T) {
	a, w := newTestActor(t)
	ctx := context.Background()

	_ = a.Receive(ctx, request(TypeFileWrite, FileWritePayload{
		Path: "e.txt", Content: "foo bar foo",
	}))

	// Multiple occurrences without replace_all -> old_string_not_unique.
	_ = a.Receive(ctx, request(TypeFileEdit, FileEditPayload{
		Path: "e.txt", OldString: "foo", NewString: "baz",
	}))
	status, raw := decodeResponse(t, w.last(t))
	if status != "failed" || errorCode(t, raw) != "old_string_not_unique" {
		t.Fatalf("dup edit: status=%s code=%s", status, errorCode(t, raw))
	}

	// Zero occurrences -> old_string_not_found.
	_ = a.Receive(ctx, request(TypeFileEdit, FileEditPayload{
		Path: "e.txt", OldString: "missing", NewString: "x",
	}))
	status, raw = decodeResponse(t, w.last(t))
	if status != "failed" || errorCode(t, raw) != "old_string_not_found" {
		t.Fatalf("missing edit: status=%s code=%s", status, errorCode(t, raw))
	}

	// replace_all succeeds with 2 replacements.
	_ = a.Receive(ctx, request(TypeFileEdit, FileEditPayload{
		Path: "e.txt", OldString: "foo", NewString: "baz", ReplaceAll: true,
	}))
	status, raw = decodeResponse(t, w.last(t))
	if status != "completed" {
		t.Fatalf("replace_all status = %s", status)
	}
	var n int
	_ = json.Unmarshal(raw["replacements"], &n)
	if n != 2 {
		t.Fatalf("replacements = %d; want 2", n)
	}

	// Unique occurrence edits in place.
	_ = a.Receive(ctx, request(TypeFileEdit, FileEditPayload{
		Path: "e.txt", OldString: "bar", NewString: "qux",
	}))
	status, _ = decodeResponse(t, w.last(t))
	if status != "completed" {
		t.Fatalf("unique edit status = %s", status)
	}
	_ = a.Receive(ctx, request(TypeFileRead, FileReadPayload{Path: "e.txt"}))
	_, raw = decodeResponse(t, w.last(t))
	var content string
	_ = json.Unmarshal(raw["content"], &content)
	if content != "baz qux baz" {
		t.Fatalf("final content = %q", content)
	}
}

func TestExec(t *testing.T) {
	a, w := newTestActor(t)
	ctx := context.Background()

	// Basic stdout capture inside the workspace cwd.
	_ = a.Receive(ctx, request(TypeExec, ExecPayload{Command: "echo hello && pwd"}))
	status, raw := decodeResponse(t, w.last(t))
	if status != "completed" {
		t.Fatalf("exec status = %s", status)
	}
	var stdout string
	_ = json.Unmarshal(raw["stdout"], &stdout)
	if !strings.HasPrefix(stdout, "hello\n") || !strings.Contains(stdout, string(testChannel)) {
		t.Fatalf("stdout = %q; want hello + workspace cwd", stdout)
	}

	// Non-zero exit code is a completed result.
	_ = a.Receive(ctx, request(TypeExec, ExecPayload{Command: "exit 3"}))
	status, raw = decodeResponse(t, w.last(t))
	if status != "completed" {
		t.Fatalf("nonzero-exit status = %s", status)
	}
	var code int
	_ = json.Unmarshal(raw["exit_code"], &code)
	if code != 3 {
		t.Fatalf("exit_code = %d; want 3", code)
	}

	// Timeout fails with exec_timeout.
	_ = a.Receive(ctx, request(TypeExec, ExecPayload{Command: "sleep 2", TimeoutMs: 100}))
	status, raw = decodeResponse(t, w.last(t))
	if status != "failed" || errorCode(t, raw) != "exec_timeout" {
		t.Fatalf("timeout: status=%s code=%s", status, errorCode(t, raw))
	}

	// cwd escape rejected.
	_ = a.Receive(ctx, request(TypeExec, ExecPayload{Command: "ls", Cwd: "../"}))
	status, raw = decodeResponse(t, w.last(t))
	if status != "failed" || errorCode(t, raw) != "path_invalid" {
		t.Fatalf("cwd escape: status=%s code=%s", status, errorCode(t, raw))
	}
}

func TestDescribeAndUnknownType(t *testing.T) {
	a, w := newTestActor(t)
	ctx := context.Background()

	_ = a.Receive(ctx, request("actor.describe", map[string]any{}))
	status, raw := decodeResponse(t, w.last(t))
	if status != "completed" {
		t.Fatalf("describe status = %s", status)
	}
	var actorID string
	_ = json.Unmarshal(raw["actor_id"], &actorID)
	if actorID != string(a.actorID) {
		t.Fatalf("describe actor_id = %q; want %q", actorID, a.actorID)
	}
	var skillDoc string
	_ = json.Unmarshal(raw["skill_doc"], &skillDoc)
	if skillDoc == "" {
		t.Fatal("describe skill_doc empty")
	}
	// Kind/binding are registry truth (actor.list), deliberately absent here.
	if _, ok := raw["kind"]; ok {
		t.Fatal("describe must not restate registry kind")
	}
	var types map[string]json.RawMessage
	_ = json.Unmarshal(raw["types"], &types)
	for _, typ := range AllTypes {
		if _, ok := types[typ]; !ok {
			t.Fatalf("describe missing type %s", typ)
		}
	}

	_ = a.Receive(ctx, request("actor.describe", map[string]any{"type": TypeExec}))
	status, raw = decodeResponse(t, w.last(t))
	if status != "completed" {
		t.Fatalf("describe_type status = %s", status)
	}
	var typ string
	_ = json.Unmarshal(raw["type"], &typ)
	if typ != TypeExec {
		t.Fatalf("describe_type type = %q; want %q", typ, TypeExec)
	}
	var allowedKinds []string
	_ = json.Unmarshal(raw["allowed_kinds"], &allowedKinds)
	if len(allowedKinds) != 1 || allowedKinds[0] != string(message.KindRequest) {
		t.Fatalf("describe_type allowed_kinds = %v; want [request]", allowedKinds)
	}
	var maxPendingMs int64
	_ = json.Unmarshal(raw["max_pending_ms"], &maxPendingMs)
	if maxPendingMs != MaxExecTimeoutMs {
		t.Fatalf("describe_type max_pending_ms = %d; want %d", maxPendingMs, MaxExecTimeoutMs)
	}

	_ = a.Receive(ctx, request("device.nope", map[string]any{}))
	status, raw = decodeResponse(t, w.last(t))
	if status != "failed" || errorCode(t, raw) != "type_unsupported" {
		t.Fatalf("unknown type: status=%s code=%s", status, errorCode(t, raw))
	}
}
