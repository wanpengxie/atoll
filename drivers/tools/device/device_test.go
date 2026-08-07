package device

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

const testActorID = actor.ActorID("device:test")

// errFakeSysStopped is fakeSys.Recv's loop-termination signal (mirrors the
// engine's ErrRecvDone contract, spec §1.2).
var errFakeSysStopped = errors.New("fakeSys: stopped")

// replyRec / failRec record one recorded sys.Reply/sys.Fail call.
type replyRec struct {
	id message.ID
	v  any
}

type failRec struct {
	id           message.ID
	code, detail string
}

// fakeSys is a minimal actorbase.Sys double for the device Proc: it embeds the
// (nil) interface so any verb this actor never touches nil-panics loudly, and
// overrides only Recv/Reply/Fail/Self/Life. The device actor answers
// synchronously on the worker goroutine, so a single recorded reply/fail is
// available right after the worker drains the pushed request.
type fakeSys struct {
	actorbase.Sys

	selfID actor.ActorID
	recvCh chan actorbase.Msg
	quit   chan struct{}
	once   sync.Once

	mu      sync.Mutex
	replies []replyRec
	fails   []failRec
}

func newFakeSys(selfID actor.ActorID) *fakeSys {
	return &fakeSys{selfID: selfID, recvCh: make(chan actorbase.Msg, 16), quit: make(chan struct{})}
}

func (f *fakeSys) push(msg actorbase.Msg) {
	select {
	case f.recvCh <- msg:
	case <-f.quit:
	}
}

func (f *fakeSys) stop() { f.once.Do(func() { close(f.quit) }) }

func (f *fakeSys) Recv() (actorbase.Msg, error) {
	select {
	case msg := <-f.recvCh:
		return msg, nil
	case <-f.quit:
		return actorbase.Msg{}, errFakeSysStopped
	}
}

func (f *fakeSys) Reply(msg actorbase.Msg, v any) (message.ID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replies = append(f.replies, replyRec{id: msg.ID, v: v})
	return msg.ID, nil
}

func (f *fakeSys) Fail(msg actorbase.Msg, code, detail string) (message.ID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fails = append(f.fails, failRec{id: msg.ID, code: code, detail: detail})
	return msg.ID, nil
}

func (f *fakeSys) PublishObs(actorrt.ObsKind, actorrt.ObsValue) error { return nil }

func (f *fakeSys) Self() actor.ActorID { return f.selfID }

func (f *fakeSys) Life() context.Context { return context.Background() }

var _ actorbase.Sys = (*fakeSys)(nil)

func (f *fakeSys) repliesSnapshot() []replyRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]replyRec, len(f.replies))
	copy(out, f.replies)
	return out
}

func (f *fakeSys) failsSnapshot() []failRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]failRec, len(f.fails))
	copy(out, f.fails)
	return out
}

// waitTerminal polls for the recorded terminal (reply or fail) for id, then
// returns its result value / failure code as JSON-decodable data. status is
// "completed" for a Reply, "failed" for a Fail.
func waitTerminal(t *testing.T, f *fakeSys, id message.ID) (status, code string, result map[string]json.RawMessage) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		for _, r := range f.repliesSnapshot() {
			if r.id == id {
				raw, _ := json.Marshal(r.v)
				_ = json.Unmarshal(raw, &result)
				return "completed", "", result
			}
		}
		for _, r := range f.failsSnapshot() {
			if r.id == id {
				return "failed", r.code, nil
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no terminal recorded for %s", id)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

const testChannel = channel.ID("ch-test")

// startActor runs a device Proc against a fakeSys in a goroutine; cleanup stops
// it and joins run()'s return.
func startActor(t *testing.T) (*fakeSys, string) {
	t.Helper()
	root := t.TempDir()
	a := NewActor(root, nil)
	sys := newFakeSys(testActorID)
	done := make(chan error, 1)
	go func() { done <- a.run(sys) }()
	t.Cleanup(func() {
		sys.stop()
		<-done
	})
	return sys, root
}

func request(typ string, payload any) actorbase.Msg {
	raw, _ := json.Marshal(payload)
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID:        message.ID("req-" + typ),
		ChannelID: testChannel,
		Kind:      message.KindRequest,
		Type:      typ,
		Sender:    message.Sender{Kind: actor.KindAgent, ID: "agent:test"},
		Payload:   raw,
	})
}

func TestWriteReadRoundtrip(t *testing.T) {
	sys, _ := startActor(t)

	sys.push(request(TypeFileWrite, FileWritePayload{
		Path: "notes/plan.md", Content: "line1\nline2\nline3",
	}))
	status, _, _ := waitTerminal(t, sys, "req-"+TypeFileWrite)
	if status != "completed" {
		t.Fatalf("write status = %s", status)
	}

	sys.push(request(TypeFileRead, FileReadPayload{Path: "notes/plan.md"}))
	status, _, raw := waitTerminal(t, sys, "req-"+TypeFileRead)
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
	sys, _ := startActor(t)

	sys.push(request(TypeFileWrite, FileWritePayload{Path: "f.txt", Content: "a\nb\nc\nd"}))
	_, _, _ = waitTerminal(t, sys, "req-"+TypeFileWrite)

	// Same request id would collide in the recorder; use a distinct read msg.
	readMsg := request(TypeFileRead, FileReadPayload{Path: "f.txt", Offset: 1, Limit: 2})
	sys.push(readMsg)
	_, _, raw := waitTerminal(t, sys, readMsg.ID)
	var content string
	_ = json.Unmarshal(raw["content"], &content)
	if content != "b\nc" {
		t.Fatalf("sliced content = %q; want b\\nc", content)
	}
}

func TestPathConfinement(t *testing.T) {
	sys, _ := startActor(t)

	for i, p := range []string{"../escape.txt", "/etc/passwd", "a/../../escape.txt"} {
		msg := request(TypeFileRead, FileReadPayload{Path: p})
		msg.ID = message.ID("req-confine-" + string(rune('a'+i)))
		sys.push(msg)
		status, code, _ := waitTerminal(t, sys, msg.ID)
		if status != "failed" || code != "path_invalid" {
			t.Fatalf("path %q: status=%s code=%s; want failed/path_invalid", p, status, code)
		}
	}
}

func TestEditUniqueness(t *testing.T) {
	sys, _ := startActor(t)

	push := func(id string, typ string, payload any) (string, string, map[string]json.RawMessage) {
		msg := request(typ, payload)
		msg.ID = message.ID(id)
		sys.push(msg)
		return waitTerminal(t, sys, msg.ID)
	}

	_, _, _ = push("w", TypeFileWrite, FileWritePayload{Path: "e.txt", Content: "foo bar foo"})

	status, code, _ := push("e1", TypeFileEdit, FileEditPayload{Path: "e.txt", OldString: "foo", NewString: "baz"})
	if status != "failed" || code != "old_string_not_unique" {
		t.Fatalf("dup edit: status=%s code=%s", status, code)
	}

	status, code, _ = push("e2", TypeFileEdit, FileEditPayload{Path: "e.txt", OldString: "missing", NewString: "x"})
	if status != "failed" || code != "old_string_not_found" {
		t.Fatalf("missing edit: status=%s code=%s", status, code)
	}

	status, _, raw := push("e3", TypeFileEdit, FileEditPayload{Path: "e.txt", OldString: "foo", NewString: "baz", ReplaceAll: true})
	if status != "completed" {
		t.Fatalf("replace_all status = %s", status)
	}
	var n int
	_ = json.Unmarshal(raw["replacements"], &n)
	if n != 2 {
		t.Fatalf("replacements = %d; want 2", n)
	}

	status, _, _ = push("e4", TypeFileEdit, FileEditPayload{Path: "e.txt", OldString: "bar", NewString: "qux"})
	if status != "completed" {
		t.Fatalf("unique edit status = %s", status)
	}
	_, _, raw = push("r", TypeFileRead, FileReadPayload{Path: "e.txt"})
	var content string
	_ = json.Unmarshal(raw["content"], &content)
	if content != "baz qux baz" {
		t.Fatalf("final content = %q", content)
	}
}

func TestExec(t *testing.T) {
	sys, _ := startActor(t)

	push := func(id string, payload ExecPayload) (string, string, map[string]json.RawMessage) {
		msg := request(TypeExec, payload)
		msg.ID = message.ID(id)
		sys.push(msg)
		return waitTerminal(t, sys, msg.ID)
	}

	status, _, raw := push("x1", ExecPayload{Command: "echo hello && pwd"})
	if status != "completed" {
		t.Fatalf("exec status = %s", status)
	}
	var stdout string
	_ = json.Unmarshal(raw["stdout"], &stdout)
	if !strings.HasPrefix(stdout, "hello\n") || !strings.Contains(stdout, string(testChannel)) {
		t.Fatalf("stdout = %q; want hello + workspace cwd", stdout)
	}

	status, _, raw = push("x2", ExecPayload{Command: "exit 3"})
	if status != "completed" {
		t.Fatalf("nonzero-exit status = %s", status)
	}
	var code int
	_ = json.Unmarshal(raw["exit_code"], &code)
	if code != 3 {
		t.Fatalf("exit_code = %d; want 3", code)
	}

	status, ecode, _ := push("x3", ExecPayload{Command: "sleep 2", TimeoutMs: 100})
	if status != "failed" || ecode != "exec_timeout" {
		t.Fatalf("timeout: status=%s code=%s", status, ecode)
	}

	status, ecode, _ = push("x4", ExecPayload{Command: "ls", Cwd: "../"})
	if status != "failed" || ecode != "path_invalid" {
		t.Fatalf("cwd escape: status=%s code=%s", status, ecode)
	}
}

func TestDescribeAndUnknownType(t *testing.T) {
	sys, _ := startActor(t)

	pushRaw := func(id string, typ string, payload any) (string, string, map[string]json.RawMessage) {
		raw, _ := json.Marshal(payload)
		msg := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
			ID:        message.ID(id),
			ChannelID: testChannel,
			Kind:      message.KindRequest,
			Type:      typ,
			Sender:    message.Sender{Kind: actor.KindAgent, ID: "agent:test"},
			Payload:   raw,
		})
		sys.push(msg)
		return waitTerminal(t, sys, msg.ID)
	}

	status, _, raw := pushRaw("d1", "actor.describe", map[string]any{})
	if status != "completed" {
		t.Fatalf("describe status = %s", status)
	}
	var actorID string
	_ = json.Unmarshal(raw["actor_id"], &actorID)
	if actorID != string(testActorID) {
		t.Fatalf("describe actor_id = %q; want %q", actorID, testActorID)
	}
	var skillDoc string
	_ = json.Unmarshal(raw["skill_doc"], &skillDoc)
	if skillDoc == "" {
		t.Fatal("describe skill_doc empty")
	}
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

	status, _, raw = pushRaw("d2", "actor.describe", map[string]any{"type": TypeExec})
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

	status, code, _ := pushRaw("d3", "device.nope", map[string]any{})
	if status != "failed" || code != "type_unsupported" {
		t.Fatalf("unknown type: status=%s code=%s", status, code)
	}
}
