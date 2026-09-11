package agentlooper

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

func TestBranchRuntimeStartsFromHostWorkspaceAndAddsOneHint(t *testing.T) {
	host := t.TempDir()
	l := &looper{
		initialEnv: environmentDefaults{
			host: "host-channel",
			lookup: func(id channel.ID) (string, bool) {
				return host, id == "host-channel"
			},
			variables: map[string]string{"Z": "last", "A": "first"},
		},
		branches: map[string]*branchRuntime{},
	}
	old := environmentHint(branchEnvironment{CWD: "/stale", Vars: map[string]string{"OLD": "yes"}})
	messages := []json.RawMessage{json.RawMessage(`{"role":"user","content":"history"}`), old}
	runtime, object, err := l.branchForStart(agentloop.StartRequest{SessionID: "branch"}, agentbase.ContextObject{Messages: messages})
	if err != nil {
		t.Fatal(err)
	}
	if len(object.Messages) != 1 || isEnvironmentHint(object.Messages[0]) {
		t.Fatalf("old hint survived Holder reconstruction: %s", mustJSON(object.Messages))
	}
	got := runtime.appendInitialEnvironmentHint(append(object.Messages, json.RawMessage(`{"role":"user","content":"first input"}`)))
	if runtime.Environment.CWD != filepath.Clean(host) || len(got) != 3 || !isEnvironmentHint(got[2]) {
		t.Fatalf("runtime=%+v messages=%s", runtime, mustJSON(got))
	}
	if text := string(got[2]); strings.Index(text, `A=first`) > strings.Index(text, `Z=last`) || strings.Contains(text, "OLD=yes") {
		t.Fatalf("hint is stale or unstable: %s", text)
	}
	(&assignment{branch: runtime}).setHistory(got)
	_, next, err := l.branchForStart(agentloop.StartRequest{SessionID: "branch"}, agentbase.ContextObject{Messages: got})
	again := next.Messages
	if err != nil || len(again) != len(got) || string(again[len(again)-1]) != string(got[len(got)-1]) {
		t.Fatalf("hint was appended again: %s err=%v", mustJSON(again), err)
	}
}

func TestBranchRuntimeContinuationUsesHolderContextAtTheLedgerVersion(t *testing.T) {
	root := t.TempDir()
	l := &looper{initialEnv: testEnvironmentDefaults(root, map[string]string{}), branches: map[string]*branchRuntime{}}
	runtime, _, err := l.branchForStart(agentloop.StartRequest{SessionID: "branch"}, agentbase.ContextObject{
		Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"ledger"}`)},
		Version:  "v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	initial := runtime.appendInitialEnvironmentHint(runtime.Context.Messages)
	(&assignment{branch: runtime, version: "v1"}).setHistory(initial)
	runtime.mu.Lock()
	runtime.Context.Messages = append(runtime.Context.Messages, json.RawMessage(`{"role":"assistant","content":"runtime-only"}`))
	runtime.mu.Unlock()
	_, got, err := l.branchForStart(agentloop.StartRequest{SessionID: "branch"}, agentbase.ContextObject{
		Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"rebuilt"}`)},
		Version:  "v1",
	})
	if err != nil || !strings.Contains(string(mustJSON(got.Messages)), "runtime-only") || strings.Contains(string(mustJSON(got.Messages)), "rebuilt") {
		t.Fatalf("Holder context was not continued: messages=%s err=%v", mustJSON(got.Messages), err)
	}

	_, reset, err := l.branchForStart(agentloop.StartRequest{SessionID: "branch"}, agentbase.ContextObject{
		Messages: []json.RawMessage{},
		Version:  "v2",
	})
	if err != nil || len(reset.Messages) != 0 {
		t.Fatalf("new ledger version did not replace Holder context: messages=%s err=%v", mustJSON(reset.Messages), err)
	}
}

func TestEnvironmentUpdateAndForkAreOwnedByLooper(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	l := &looper{initialEnv: testEnvironmentDefaults(root, map[string]string{"KEEP": "one"}), branches: map[string]*branchRuntime{}}
	parent, _, err := l.branchForStart(agentloop.StartRequest{SessionID: "parent"}, agentbase.ContextObject{Messages: []json.RawMessage{}})
	if err != nil {
		t.Fatal(err)
	}
	raw, failed := executeEnvironmentTool(resolvedTool{Name: environmentUpdateName}, json.RawMessage(`{"cwd":"nested","set":{"NEW":"two"},"unset":["KEEP"]}`), parent)
	if failed || parent.Environment.CWD != nested || parent.Environment.Vars["NEW"] != "two" {
		t.Fatalf("update=%s env=%+v failed=%v", raw, parent.Environment, failed)
	}
	child, _, err := l.branchForStart(agentloop.StartRequest{SessionID: "child", Open: &agentloop.OpenRequest{Base: &agentloop.BoundaryRef{Session: "parent"}}}, agentbase.ContextObject{Messages: []json.RawMessage{}})
	if err != nil {
		t.Fatal(err)
	}
	child.Environment.Vars["NEW"] = "child"
	if child.Environment.CWD != nested || parent.Environment.Vars["NEW"] != "two" {
		t.Fatalf("fork did not copy environment: parent=%+v child=%+v", parent.Environment, child.Environment)
	}
}

func TestWindowsEnvironmentNamesAreCaseInsensitive(t *testing.T) {
	variables := map[string]string{"Path": "old", "OTHER": "value"}
	setEnvironmentEntryForOS(variables, "PATH", "new", "windows")
	if len(variables) != 2 || variables["PATH"] != "new" {
		t.Fatalf("case-insensitive set left aliases behind: %+v", variables)
	}
	deleteEnvironmentEntryForOS(variables, "path", "windows")
	if _, found := variables["PATH"]; found || len(variables) != 1 {
		t.Fatalf("case-insensitive unset did not remove PATH: %+v", variables)
	}
	if validEnvironmentKeyForOS("Pwd", "windows") {
		t.Fatal("Windows Pwd alias was accepted")
	}
	if !validEnvironmentKeyForOS("Pwd", "linux") {
		t.Fatal("case-sensitive Unix environment name was rejected")
	}
	if duplicate := duplicateEnvironmentKeyForOS(map[string]string{"Path": "one", "PATH": "two"}, "windows"); duplicate == "" {
		t.Fatal("Windows environment aliases were not rejected")
	}
	if duplicate := duplicateEnvironmentKeyForOS(map[string]string{"Path": "one", "PATH": "two"}, "linux"); duplicate != "" {
		t.Fatalf("case-sensitive Unix variables were treated as duplicates: %q", duplicate)
	}
}

func testEnvironmentDefaults(cwd string, variables map[string]string) environmentDefaults {
	return environmentDefaults{
		host: "host",
		lookup: func(channel.ID) (string, bool) {
			return cwd, true
		},
		variables: variables,
	}
}

func testBranchRuntime() *branchRuntime {
	cwd, _ := os.Getwd()
	return &branchRuntime{Environment: branchEnvironment{CWD: cwd, Vars: map[string]string{}}, HintAdded: true}
}

type hostJobsTestBase struct {
	cancelled []message.ID
	submitID  message.ID
	listed    []message.ID
	awaitEnv  *message.Envelope
	awaitOK   bool
	awaitErr  error
}

func (b *hostJobsTestBase) Submit(behavior.RequestSpec) (message.ID, error) {
	if b.submitID != "" {
		return b.submitID, nil
	}
	return "host-request", nil
}
func (b *hostJobsTestBase) Await(context.Context, message.ID, time.Duration) (*message.Envelope, bool, error) {
	return b.awaitEnv, b.awaitOK, b.awaitErr
}
func (*hostJobsTestBase) ProgressEvents(message.ID) <-chan *message.Envelope {
	return make(chan *message.Envelope)
}
func (b *hostJobsTestBase) List() []message.ID {
	if b.listed != nil {
		return b.listed
	}
	return []message.ID{"internal-llm", "host-request"}
}
func (b *hostJobsTestBase) Cancel(id message.ID) error {
	b.cancelled = append(b.cancelled, id)
	return nil
}

func TestHostMetaJobAccountCannotSeeLooperInternalRequests(t *testing.T) {
	base := &hostJobsTestBase{submitID: "host-a"}
	branchA := testBranchRuntime()
	jobsA := hostJobTable{base: base, host: "host", branch: branchA}
	request := behavior.RequestSpec{Audience: message.Audience{"target"}, Payload: json.RawMessage(`{}`)}
	if _, err := jobsA.Submit(request); err != nil {
		t.Fatal(err)
	}
	branchB := testBranchRuntime()
	jobsB := hostJobTable{base: base, host: "host", branch: branchB}
	base.submitID = "host-b"
	if _, err := jobsB.Submit(request); err != nil {
		t.Fatal(err)
	}
	base.listed = []message.ID{"internal-llm", "host-a", "host-b"}
	if got := jobsA.List(); len(got) != 1 || got[0] != "host-a" {
		t.Fatalf("branch A Host job list leaked another account: %v", got)
	}
	if got := jobsB.List(); len(got) != 1 || got[0] != "host-b" {
		t.Fatalf("branch B Host job list leaked another account: %v", got)
	}
	if err := jobsA.Cancel("internal-llm"); err == nil || len(base.cancelled) != 0 {
		t.Fatalf("Host account cancelled an internal request: err=%v cancelled=%v", err, base.cancelled)
	}
	if _, _, err := jobsB.Await(context.Background(), "host-a", 0); err == nil {
		t.Fatal("another branch awaited a Host request it does not own")
	}
	if err := jobsA.Cancel("host-a"); err != nil {
		t.Fatal(err)
	}
	if !branchA.hasHostRequest("host-a") {
		t.Fatal("cancel detached a possibly buffered final before Await could observe it")
	}
	if !branchB.hasHostRequest("host-b") {
		t.Fatal("cancelling branch A detached branch B's Host request")
	}
}

func TestHostMetaJobAccountDropsOnlyClosedBranchReferences(t *testing.T) {
	branch := testBranchRuntime()
	base := &hostJobsTestBase{}
	jobs := hostJobTable{base: base, host: "host", branch: branch}
	if _, err := jobs.Submit(behavior.RequestSpec{Audience: message.Audience{"target"}, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := jobs.Await(context.Background(), "host-request", 0); err != nil || ok {
		t.Fatalf("open request await = (_, %v, %v), want (_, false, nil)", ok, err)
	}
	if !branch.hasHostRequest("host-request") {
		t.Fatal("a wait window expiring detached an in-flight Host request")
	}

	base.awaitErr = actorbase.ErrCallClosed
	if _, _, err := jobs.Await(context.Background(), "host-request", 0); !errors.Is(err, actorbase.ErrCallClosed) {
		t.Fatalf("closed request error = %v, want ErrCallClosed", err)
	}
	if branch.hasHostRequest("host-request") {
		t.Fatal("closed Host request remained attached to its branch")
	}

	base.awaitErr = nil
	base.awaitOK = true
	base.awaitEnv = &message.Envelope{ID: "response"}
	if _, err := jobs.Submit(behavior.RequestSpec{Audience: message.Audience{"target"}, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := jobs.Await(context.Background(), "host-request", time.Second); err != nil || !ok {
		t.Fatalf("final request await = (_, %v, %v), want (_, true, nil)", ok, err)
	}
	if branch.hasHostRequest("host-request") {
		t.Fatal("collected Host request remained attached to its branch")
	}
}

func TestHostMetaRequestUsesPrivateHandleEnvelope(t *testing.T) {
	expires := int64(123)
	original := behavior.RequestSpec{
		Cause:      message.Anchored("model-response", "turn"),
		Context:    harness.Context{Session: "branch"},
		Type:       "actor.ask",
		Payload:    json.RawMessage(`{"text":"hello"}`),
		Audience:   message.Audience{"agent:worker:1"},
		Visibility: message.VisibilityPublic,
		ExpiresAt:  &expires,
	}
	got, err := wrapHostRequest(original, "tool:host:1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != hostHandleCall || len(got.Audience) != 1 || got.Audience[0] != "tool:host:1" || got.Context.Session != "branch" || got.Cause != original.Cause || got.ExpiresAt != original.ExpiresAt {
		t.Fatalf("outer Host request lost routing facts: %+v", got)
	}
	var inner struct {
		Type       string             `json:"type"`
		Payload    json.RawMessage    `json:"payload"`
		Audience   message.Audience   `json:"audience"`
		Visibility message.Visibility `json:"visibility"`
	}
	if err := json.Unmarshal(got.Payload, &inner); err != nil || inner.Type != original.Type || string(inner.Payload) != string(original.Payload) || len(inner.Audience) != 1 || inner.Audience[0] != original.Audience[0] || inner.Visibility != original.Visibility {
		t.Fatalf("inner Host request mismatch: %+v err=%v", inner, err)
	}
}
