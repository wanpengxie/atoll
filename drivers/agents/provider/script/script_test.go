package script

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/registry"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// The behaviour-table unit tests: Call / Resource are faked (the true
// integration is the e2e loop harness); the assertions pin the two-row table's
// reply shapes and failure codes.

var errStop = errors.New("fakeSys: queue drained")

type replyCall struct {
	msg actorbase.Msg
	v   any
}

type failCall struct {
	msg          actorbase.Msg
	code, detail string
}

// fakePending returns a fixed terminal Msg.
type fakePending struct {
	term actorbase.Msg
	err  error
}

func (p *fakePending) Wait(context.Context, time.Duration) (actorbase.Msg, error) {
	return p.term, p.err
}
func (p *fakePending) Cancel() error { return nil }

// fakeWriteHandle records written bytes and the commit/abort disposition.
type fakeWriteHandle struct {
	buf       bytes.Buffer
	committed bool
	aborted   bool
}

func (w *fakeWriteHandle) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *fakeWriteHandle) Commit() error               { w.committed = true; return nil }
func (w *fakeWriteHandle) Abort() error                { w.aborted = true; return nil }

// fakeResource embeds the nil interface (unimplemented verbs panic loudly) and
// implements only CreateFile / Stat / Open.
type fakeResource struct {
	actorbase.ResourceHandle

	files map[resource.ResourceID][]byte // committed content
	// statMisses: per-id count of not_found verdicts to serve before visible
	// (exercises the verify poll loop).
	statMisses map[resource.ResourceID]int
	lastWrite  *fakeWriteHandle
	created    []resource.ResourceID
}

func newFakeResource() *fakeResource {
	return &fakeResource{files: map[resource.ResourceID][]byte{}, statMisses: map[resource.ResourceID]int{}}
}

func (r *fakeResource) CreateFile(id resource.ResourceID, dir, withContent bool) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	if _, dup := r.files[id]; dup {
		return accessdoor.FileAccess{}, accessdoor.Outcome{RejectReason: access.AlreadyExists}, nil
	}
	w := &fakeWriteHandle{}
	r.lastWrite = w
	r.created = append(r.created, id)
	// Commit lands the bytes into files on the caller's Commit() — approximated
	// by pointing the map at the buffer post-hoc in the test assertions.
	return accessdoor.FileAccess{Local: &accessdoor.LocalFile{Write: w}}, accessdoor.Outcome{}, nil
}

func (r *fakeResource) Stat(id resource.ResourceID) (accessdoor.StatResult, error) {
	if n := r.statMisses[id]; n > 0 {
		r.statMisses[id] = n - 1
		return accessdoor.StatResult{Reject: accessdoor.QueryNotFound}, nil
	}
	if _, ok := r.files[id]; !ok {
		return accessdoor.StatResult{Reject: accessdoor.QueryNotFound}, nil
	}
	return accessdoor.StatResult{}, nil
}

type nopReadSeekCloser struct{ *bytes.Reader }

func (nopReadSeekCloser) Close() error { return nil }

func (r *fakeResource) Open(id resource.ResourceID, mode access.Operation) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	b, ok := r.files[id]
	if !ok {
		return accessdoor.FileAccess{}, accessdoor.Outcome{RejectReason: access.ResourceNotFound}, nil
	}
	return accessdoor.FileAccess{Local: &accessdoor.LocalFile{Read: nopReadSeekCloser{bytes.NewReader(b)}}}, accessdoor.Outcome{}, nil
}

// fakeSys wires Recv/Reply/Fail/Call/Resource; everything else nil-panics.
type fakeSys struct {
	actorbase.Sys

	queue    []actorbase.Msg
	at       int
	replies  []replyCall
	fails    []failCall
	calls    []struct{ target, msgType, payload string }
	pending  *fakePending
	resource *fakeResource
}

func (f *fakeSys) Recv() (actorbase.Msg, error) {
	if f.at >= len(f.queue) {
		return actorbase.Msg{}, errStop
	}
	m := f.queue[f.at]
	f.at++
	return m, nil
}

func (f *fakeSys) Reply(msg actorbase.Msg, v any) (message.ID, error) {
	f.replies = append(f.replies, replyCall{msg, v})
	return msg.ID, nil
}

func (f *fakeSys) Fail(msg actorbase.Msg, code, detail string) (message.ID, error) {
	f.fails = append(f.fails, failCall{msg, code, detail})
	return msg.ID, nil
}

func (f *fakeSys) Call(target actor.ActorID, msgType string, payload any) (actorbase.Pending, error) {
	raw, _ := json.Marshal(payload)
	f.calls = append(f.calls, struct{ target, msgType, payload string }{string(target), msgType, string(raw)})
	return f.pending, nil
}

func (f *fakeSys) Resource() actorbase.ResourceHandle { return f.resource }

var _ actorbase.Sys = (*fakeSys)(nil)

func requestMsg(id, typ string, payload string) actorbase.Msg {
	return actorbase.NewMsg(context.Background(), message.Envelope{
		ID:      message.ID(id),
		Kind:    message.KindRequest,
		Type:    typ,
		Payload: json.RawMessage(payload),
	})
}

// completedTerminal fabricates the tool's completed terminal: the reply payload
// with status merged in (the RespondJSON shape the engine strips back off).
func completedTerminal(payload map[string]any) actorbase.Msg {
	payload["status"] = message.StatusCompleted
	raw, _ := json.Marshal(payload)
	return actorbase.NewMsg(context.Background(), message.Envelope{
		Kind: message.KindResponse, Payload: raw,
	})
}

func TestChat_CallsToolWritesResourceAndReplies(t *testing.T) {
	const payload = `{"text":"hello loop"}`
	msg := requestMsg("m-1", TypeChat, payload)
	res := newFakeResource()
	sys := &fakeSys{
		queue:    []actorbase.Msg{msg},
		pending:  &fakePending{term: completedTerminal(map[string]any{"text": "hello loop"})},
		resource: res,
	}
	if err := newRun("agent:echo-1")(sys); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v, want errStop", err)
	}
	if len(sys.fails) != 0 {
		t.Fatalf("fails = %v, want none", sys.fails)
	}
	if len(sys.calls) != 1 || sys.calls[0].target != "agent:echo-1" || sys.calls[0].msgType != toolSayType {
		t.Fatalf("calls = %+v, want one echo.say to agent:echo-1", sys.calls)
	}
	if sys.calls[0].payload != payload {
		t.Fatalf("call payload = %s, want verbatim %s", sys.calls[0].payload, payload)
	}
	// Resource id keyed off the message id; content = original payload bytes.
	wantRID := "file:loop/m-1"
	if len(res.created) != 1 || string(res.created[0]) != wantRID {
		t.Fatalf("created = %v, want [%s]", res.created, wantRID)
	}
	if !res.lastWrite.committed || res.lastWrite.aborted {
		t.Fatalf("write handle committed=%v aborted=%v, want committed only", res.lastWrite.committed, res.lastWrite.aborted)
	}
	if got := res.lastWrite.buf.String(); got != payload {
		t.Fatalf("written bytes = %s, want verbatim %s", got, payload)
	}
	if len(sys.replies) != 1 {
		t.Fatalf("replies = %d, want 1", len(sys.replies))
	}
	body := sys.replies[0].v.(map[string]any)
	if body["ok"] != true || body["resource_id"] != wantRID {
		t.Fatalf("reply = %+v", body)
	}
	echoed := body["echoed"].(map[string]any)
	if echoed["text"] != "hello loop" {
		t.Fatalf("echoed = %+v, want text=hello loop", echoed)
	}
	if _, still := echoed["status"]; still {
		t.Fatalf("echoed still carries protocol field status: %+v", echoed)
	}
}

func TestChat_NonObjectPayloadFailsBadPayload(t *testing.T) {
	sys := &fakeSys{queue: []actorbase.Msg{requestMsg("m-2", TypeChat, `[1,2]`)}}
	if err := newRun("agent:echo-1")(sys); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v", err)
	}
	if len(sys.fails) != 1 || sys.fails[0].code != "bad_payload" {
		t.Fatalf("fails = %+v, want one bad_payload", sys.fails)
	}
	if len(sys.calls) != 0 {
		t.Fatalf("tool called despite bad payload")
	}
}

func TestChat_ToolFailureFailsToolCallFailed(t *testing.T) {
	failedRaw, _ := json.Marshal(map[string]any{
		"status": message.StatusFailed, "error_code": "type_unsupported", "detail": "nope",
	})
	term := actorbase.NewMsg(context.Background(), message.Envelope{Kind: message.KindResponse, Payload: failedRaw})
	sys := &fakeSys{
		queue:    []actorbase.Msg{requestMsg("m-3", TypeChat, `{"a":1}`)},
		pending:  &fakePending{term: term},
		resource: newFakeResource(),
	}
	if err := newRun("agent:echo-1")(sys); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v", err)
	}
	if len(sys.fails) != 1 || sys.fails[0].code != "tool_call_failed" {
		t.Fatalf("fails = %+v, want one tool_call_failed", sys.fails)
	}
	if len(sys.resource.created) != 0 {
		t.Fatalf("resource created despite tool failure")
	}
}

func TestVerify_PollsStatThenReadsBytes(t *testing.T) {
	const rid = "file:loop/m-1"
	const content = `{"text":"hello loop"}`
	res := newFakeResource()
	res.files[resource.ResourceID(rid)] = []byte(content)
	res.statMisses[resource.ResourceID(rid)] = 2 // first two Stats invisible
	sys := &fakeSys{
		queue:    []actorbase.Msg{requestMsg("m-4", TypeVerify, `{"resource_id":"`+rid+`"}`)},
		resource: res,
	}
	if err := newRun("agent:echo-1")(sys); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v", err)
	}
	if len(sys.fails) != 0 {
		t.Fatalf("fails = %+v", sys.fails)
	}
	body := sys.replies[0].v.(map[string]any)
	if body["exists"] != true || body["content"] != content || body["size"] != len(content) {
		t.Fatalf("verify reply = %+v", body)
	}
}

// TestChat_DuplicateResourceIDRejectedNotSilentlyOverwritten pins the 防重发
// property: the resource id is keyed off the message id, so a REDELIVERED
// message (same id) hits already_exists and fails loud — never a silent
// second write; a harness retry (fresh message id) never collides at all.
func TestChat_DuplicateResourceIDRejectedNotSilentlyOverwritten(t *testing.T) {
	const payload = `{"text":"again"}`
	res := newFakeResource()
	res.files["file:loop/m-dup"] = []byte(payload) // first delivery already landed
	sys := &fakeSys{
		queue:    []actorbase.Msg{requestMsg("m-dup", TypeChat, payload)},
		pending:  &fakePending{term: completedTerminal(map[string]any{"text": "again"})},
		resource: res,
	}
	if err := newRun("agent:echo-1")(sys); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v", err)
	}
	if len(sys.fails) != 1 || sys.fails[0].code != "resource_failed" ||
		!strings.Contains(sys.fails[0].detail, string(access.AlreadyExists)) {
		t.Fatalf("fails = %+v, want one resource_failed carrying already_exists", sys.fails)
	}
	if len(res.created) != 0 || len(sys.replies) != 0 {
		t.Fatalf("duplicate delivery wrote/replied: created=%v replies=%d", res.created, len(sys.replies))
	}
}

func TestVerify_MissingResourceIDFailsBadPayload(t *testing.T) {
	sys := &fakeSys{queue: []actorbase.Msg{requestMsg("m-5", TypeVerify, `{}`)}}
	if err := newRun("agent:echo-1")(sys); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v", err)
	}
	if len(sys.fails) != 1 || sys.fails[0].code != "bad_payload" {
		t.Fatalf("fails = %+v, want one bad_payload", sys.fails)
	}
}

func TestUnknownTypeFailsTypeUnsupported(t *testing.T) {
	sys := &fakeSys{queue: []actorbase.Msg{requestMsg("m-6", "loop.nope", `{}`)}}
	if err := newRun("agent:echo-1")(sys); !errors.Is(err, errStop) {
		t.Fatalf("run returned %v", err)
	}
	if len(sys.fails) != 1 || sys.fails[0].code != "type_unsupported" {
		t.Fatalf("fails = %+v, want one type_unsupported", sys.fails)
	}
}

func TestConstruct_RequiresToolIDAndInstanceID(t *testing.T) {
	if _, err := construct(registry.InstanceSpec{ID: "agent:s"}, registry.Deps{}); err == nil || !strings.Contains(err.Error(), "tool_id") {
		t.Fatalf("construct without tool_id = %v, want tool_id error", err)
	}
	if _, err := construct(registry.InstanceSpec{Config: json.RawMessage(`{"tool_id":"agent:e"}`)}, registry.Deps{}); err == nil || !strings.Contains(err.Error(), "instance id") {
		t.Fatalf("construct without id = %v, want instance id error", err)
	}
	decl, err := construct(registry.InstanceSpec{ID: "agent:s", Config: json.RawMessage(`{"tool_id":"agent:e"}`)}, registry.Deps{})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	if decl.ID != "agent:s" || decl.Kind != actor.KindAgent {
		t.Fatalf("decl = %+v", decl)
	}
}
