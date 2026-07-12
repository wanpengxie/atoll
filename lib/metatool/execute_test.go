package metatool_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// execute_test drives the SEVEN meta-tools against a fake Exec double. The
// out-station correlation machinery itself (Submit/Await/Cancel/List/the
// author#2 timer) is the engine's callLedger — tested in lib/actorbase. Here we
// pin only the tools' translation of params → JobTable/Call operations and the
// rendering of results.

// --- fake Exec -------------------------------------------------------------

// fakeJobs is a recording actorbase.JobTable double: Submit records the spec
// and mints a monotonic id; Await/Call return whatever a test scripted.
type fakeJobs struct {
	submitted []behavior.RequestSpec
	seq       int

	// awaitFn scripts each Await call; nil = still-pending (ok=false).
	awaitFn func(id message.ID) (*message.Envelope, bool, error)
	// list is what List returns.
	listIDs []message.ID
	// cancelled records ids passed to Cancel.
	cancelled []message.ID
}

func (f *fakeJobs) Submit(spec behavior.RequestSpec) (message.ID, error) {
	f.submitted = append(f.submitted, spec)
	f.seq++
	return message.ID("req-" + itoa(int64(f.seq))), nil
}

func (f *fakeJobs) Await(_ context.Context, id message.ID, _ time.Duration) (*message.Envelope, bool, error) {
	if f.awaitFn != nil {
		return f.awaitFn(id)
	}
	return nil, false, nil
}

func (f *fakeJobs) List() []message.ID { return f.listIDs }

func (f *fakeJobs) Cancel(id message.ID) error {
	f.cancelled = append(f.cancelled, id)
	return nil
}

var _ actorbase.JobTable = (*fakeJobs)(nil)

// newExec builds an Exec over a fake JobTable + a scripted Call.
func newExec(jobs *fakeJobs, call metatool.CallFunc) *metatool.Exec {
	return &metatool.Exec{
		Jobs:           jobs,
		Call:           call,
		Clock:          time.Now,
		FastPathWindow: 10 * time.Second,
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func finalResp(parentID message.ID, payload map[string]any) *message.Envelope {
	body, _ := json.Marshal(payload)
	return &message.Envelope{
		ID:       message.ID("resp-" + string(parentID)),
		Kind:     message.KindResponse,
		ParentID: parentID,
		Payload:  body,
	}
}

func defaultRC() metatool.RuntimeContext {
	return metatool.RuntimeContext{
		Trigger: metatool.Trigger{
			Envelope: message.Envelope{
				ID:   "trigger-1",
				Kind: message.KindRequest,
				Type: "agent.turn",
			},
			CorrelationID: "trigger-1",
		},
	}
}

func assertIsError(t *testing.T, rv metatool.ResultValue, code string) {
	t.Helper()
	if !rv.IsError {
		t.Fatalf("expected IsError, got %+v", rv.Value)
	}
	errObj, ok := rv.Value["error"].(map[string]any)
	if !ok {
		// list_actors uses a plain {"error": "..."} shape.
		if _, ok := rv.Value["error"].(string); ok {
			return
		}
		t.Fatalf("no error object in %+v", rv.Value)
	}
	if code != "" && errObj["code"] != code {
		t.Fatalf("error code = %v, want %v", errObj["code"], code)
	}
}

func assertNotError(t *testing.T, rv metatool.ResultValue) {
	t.Helper()
	if rv.IsError {
		t.Fatalf("unexpected error: %+v", rv.Value)
	}
}

// --- call_actor: validation ------------------------------------------------

func TestExecuteCallActor_NilExec(t *testing.T) {
	rv := metatool.ExecuteCallActor(context.Background(), nil, nil, defaultRC())
	assertIsError(t, rv, "internal_error")
}

func TestExecuteCallActor_OutsideTurn(t *testing.T) {
	x := newExec(&fakeJobs{}, nil)
	params := json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.search"}`)
	rv := metatool.ExecuteCallActor(context.Background(), params, x, metatool.RuntimeContext{})
	assertIsError(t, rv, "internal_error")
}

func TestExecuteCallActor_MissingActorID(t *testing.T) {
	x := newExec(&fakeJobs{}, nil)
	params := json.RawMessage(`{"type":"xhs.search"}`)
	rv := metatool.ExecuteCallActor(context.Background(), params, x, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteCallActor_MissingType(t *testing.T) {
	x := newExec(&fakeJobs{}, nil)
	params := json.RawMessage(`{"actor_id":"tool:xhs"}`)
	rv := metatool.ExecuteCallActor(context.Background(), params, x, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteCallActor_InvalidJSON(t *testing.T) {
	x := newExec(&fakeJobs{}, nil)
	rv := metatool.ExecuteCallActor(context.Background(), json.RawMessage(`{bad`), x, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

// --- call_actor: dispatch --------------------------------------------------

// TestExecuteCallActor_FanOutSubmitsRequest pins the behavior.RequestSpec the
// adapter builds: audience=target, type, ParentID=trigger, and an ExpiresAt
// derived from the deadline.
func TestExecuteCallActor_FanOutSubmitsRequest(t *testing.T) {
	jobs := &fakeJobs{}
	x := newExec(jobs, nil)
	params := json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.search","payload":{"keyword":"go"},"wait":false}`)
	rv := metatool.ExecuteCallActor(context.Background(), params, x, defaultRC())
	assertNotError(t, rv)
	if len(jobs.submitted) != 1 {
		t.Fatalf("expected 1 submit, got %d", len(jobs.submitted))
	}
	got := jobs.submitted[0]
	if got.Type != "xhs.search" {
		t.Fatalf("submit type = %q, want xhs.search", got.Type)
	}
	if len(got.Audience) != 1 || got.Audience[0] != actor.ActorID("tool:xhs") {
		t.Fatalf("submit audience = %v, want [tool:xhs]", got.Audience)
	}
	if got.ParentID != "trigger-1" {
		t.Fatalf("submit parent = %q, want trigger-1", got.ParentID)
	}
	if got.ExpiresAt == nil {
		t.Fatal("submit ExpiresAt nil — the closure deadline must be stamped")
	}
	// wait=false → immediate ack.
	if rv.Value["status"] != "accepted" {
		t.Fatalf("wait=false should ack; got %+v", rv.Value)
	}
}

// TestExecuteCallActor_TimeoutResolverOverridesDefault pins P13: a resolver
// answer is the deadline actually used (ExpiresAt reflects it).
func TestExecuteCallActor_TimeoutResolverOverridesDefault(t *testing.T) {
	jobs := &fakeJobs{}
	now := time.Now()
	x := &metatool.Exec{
		Jobs:  jobs,
		Clock: func() time.Time { return now },
		TimeoutResolver: func(target actor.ActorID, reqType string) (time.Duration, bool) {
			return 3 * time.Second, true
		},
	}
	params := json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.search","wait":false}`)
	metatool.ExecuteCallActor(context.Background(), params, x, defaultRC())
	got := jobs.submitted[0]
	wantExpiry := now.Add(3 * time.Second).UnixMilli()
	if got.ExpiresAt == nil || *got.ExpiresAt != wantExpiry {
		t.Fatalf("ExpiresAt = %v, want %d (now+3s)", got.ExpiresAt, wantExpiry)
	}
}

// TestExecuteCallActor_SyncResolvesInline pins the sync experience: a final in
// the window returns inline as the actor's completed result.
func TestExecuteCallActor_SyncResolvesInline(t *testing.T) {
	jobs := &fakeJobs{}
	jobs.awaitFn = func(id message.ID) (*message.Envelope, bool, error) {
		return finalResp(id, map[string]any{"status": "completed", "results": []any{}}), true, nil
	}
	x := newExec(jobs, nil)
	params := json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.search","wait":true}`)
	rv := metatool.ExecuteCallActor(context.Background(), params, x, defaultRC())
	assertNotError(t, rv)
	if rv.Value["status"] != "completed" {
		t.Fatalf("expected inline completed result, got %+v", rv.Value)
	}
}

// TestExecuteCallActor_TerminalFailureNormalized pins the actor-CLI error
// mapping for a terminal failure reason.
func TestExecuteCallActor_TerminalFailureNormalized(t *testing.T) {
	jobs := &fakeJobs{}
	jobs.awaitFn = func(id message.ID) (*message.Envelope, bool, error) {
		return finalResp(id, map[string]any{
			"status": "failed",
			"reason": string(message.TerminalReceiverUnavailable),
		}), true, nil
	}
	x := newExec(jobs, nil)
	params := json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.search","wait":true}`)
	rv := metatool.ExecuteCallActor(context.Background(), params, x, defaultRC())
	assertIsError(t, rv, "actor_unreachable")
}

// TestExecuteCallActor_SubmitError surfaces a Submit failure as internal_error.
func TestExecuteCallActor_SubmitError(t *testing.T) {
	x := newExec(&fakeJobs{}, nil)
	x.Jobs = &errJobs{}
	params := json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.search"}`)
	rv := metatool.ExecuteCallActor(context.Background(), params, x, defaultRC())
	assertIsError(t, rv, "internal_error")
}

type errJobs struct{ fakeJobs }

func (e *errJobs) Submit(spec behavior.RequestSpec) (message.ID, error) {
	return "", context.DeadlineExceeded
}

// --- await_result ----------------------------------------------------------

func TestExecuteAwaitResult_NilExec(t *testing.T) {
	rv := metatool.ExecuteAwaitResult(context.Background(), nil, nil, defaultRC())
	assertIsError(t, rv, "internal_error")
}

func TestExecuteAwaitResult_MissingID(t *testing.T) {
	x := newExec(&fakeJobs{}, nil)
	rv := metatool.ExecuteAwaitResult(context.Background(), json.RawMessage(`{}`), x, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

func TestExecuteAwaitResult_NotInFlight(t *testing.T) {
	jobs := &fakeJobs{}
	jobs.awaitFn = func(id message.ID) (*message.Envelope, bool, error) {
		return nil, false, actorbase.ErrCallClosed
	}
	x := newExec(jobs, nil)
	rv := metatool.ExecuteAwaitResult(context.Background(),
		json.RawMessage(`{"request_id":"req-9"}`), x, defaultRC())
	assertIsError(t, rv, "internal_error")
	blob, _ := json.Marshal(rv.Value)
	if !containsStr(string(blob), "not in flight") {
		t.Fatalf("expected 'not in flight' message, got %s", blob)
	}
}

func TestExecuteAwaitResult_StillPending(t *testing.T) {
	jobs := &fakeJobs{} // awaitFn nil → ok=false
	x := newExec(jobs, nil)
	rv := metatool.ExecuteAwaitResult(context.Background(),
		json.RawMessage(`{"request_id":"req-1"}`), x, defaultRC())
	assertNotError(t, rv)
	if rv.Value["status"] != "accepted" {
		t.Fatalf("still-pending should ack; got %+v", rv.Value)
	}
}

func TestExecuteAwaitResult_ResolvesFinal(t *testing.T) {
	jobs := &fakeJobs{}
	jobs.awaitFn = func(id message.ID) (*message.Envelope, bool, error) {
		return finalResp(id, map[string]any{"status": "completed"}), true, nil
	}
	x := newExec(jobs, nil)
	rv := metatool.ExecuteAwaitResult(context.Background(),
		json.RawMessage(`{"request_id":"req-1"}`), x, defaultRC())
	assertNotError(t, rv)
	if rv.Value["status"] != "completed" {
		t.Fatalf("expected completed, got %+v", rv.Value)
	}
}

// --- cancel ----------------------------------------------------------------

func TestExecuteCancel_NilExec(t *testing.T) {
	rv := metatool.ExecuteCancel(context.Background(), nil, nil, defaultRC())
	assertIsError(t, rv, "internal_error")
}

func TestExecuteCancel_CallsJobTableCancel(t *testing.T) {
	jobs := &fakeJobs{}
	x := newExec(jobs, nil)
	rv := metatool.ExecuteCancel(context.Background(),
		json.RawMessage(`{"request_id":"req-7"}`), x, defaultRC())
	assertNotError(t, rv)
	if len(jobs.cancelled) != 1 || jobs.cancelled[0] != "req-7" {
		t.Fatalf("expected Cancel(req-7), got %v", jobs.cancelled)
	}
	if rv.Value["cancelled"] != "req-7" {
		t.Fatalf("expected cancelled=req-7, got %+v", rv.Value)
	}
}

// --- list_pending ----------------------------------------------------------

func TestExecuteListPending_ReturnsJobTableList(t *testing.T) {
	jobs := &fakeJobs{listIDs: []message.ID{"req-1", "req-2"}}
	x := newExec(jobs, nil)
	rv := metatool.ExecuteListPending(context.Background(), nil, x, defaultRC())
	assertNotError(t, rv)
	if rv.Value["count"] != 2 {
		t.Fatalf("count = %v, want 2", rv.Value["count"])
	}
}

// --- describe_actor (sys.Call face) ----------------------------------------

func TestExecuteDescribeActor_NilCall(t *testing.T) {
	x := &metatool.Exec{Jobs: &fakeJobs{}} // Call nil
	rv := metatool.ExecuteDescribeActor(context.Background(),
		json.RawMessage(`{"actor_id":"tool:xhs"}`), x, defaultRC())
	assertIsError(t, rv, "internal_error")
}

func TestExecuteDescribeActor_UsesCallFace(t *testing.T) {
	var called bool
	call := func(_ context.Context, spec behavior.RequestSpec, _ time.Duration) (*message.Envelope, bool, error) {
		called = true
		if spec.Type != "actor.describe" {
			t.Fatalf("describe spec type = %q", spec.Type)
		}
		return finalResp("d", map[string]any{"actor_id": "tool:xhs", "description": "x"}), true, nil
	}
	x := &metatool.Exec{Jobs: &fakeJobs{}, Call: call, Clock: time.Now}
	rv := metatool.ExecuteDescribeActor(context.Background(),
		json.RawMessage(`{"actor_id":"tool:xhs"}`), x, defaultRC())
	assertNotError(t, rv)
	if !called {
		t.Fatal("describe_actor did not drive the Call face")
	}
	// A describe query MUST NOT become a durable job (it went through sys.Call).
	if jobs, ok := x.Jobs.(*fakeJobs); ok && len(jobs.submitted) != 0 {
		t.Fatalf("describe leaked into the JobTable: %d submits", len(jobs.submitted))
	}
}

// TestExecuteDescribeActor_TerminalFailureNormalized pins that describe_actor
// now routes its final through the SAME actor-CLI normalization as call_actor
// (#11): a failed describe response renders as the closed error set, not the
// raw透传 {error:reason} shape it did before.
func TestExecuteDescribeActor_TerminalFailureNormalized(t *testing.T) {
	call := func(_ context.Context, _ behavior.RequestSpec, _ time.Duration) (*message.Envelope, bool, error) {
		return finalResp("d", map[string]any{
			"status": "failed",
			"reason": string(message.TerminalReceiverUnavailable),
		}), true, nil
	}
	x := &metatool.Exec{Jobs: &fakeJobs{}, Call: call, Clock: time.Now}
	rv := metatool.ExecuteDescribeActor(context.Background(),
		json.RawMessage(`{"actor_id":"tool:xhs"}`), x, defaultRC())
	assertIsError(t, rv, "actor_unreachable")
}

// TestExecuteDescribeType_TerminalFailureNormalized: same normalization for the
// per-type query (#11).
func TestExecuteDescribeType_TerminalFailureNormalized(t *testing.T) {
	call := func(_ context.Context, _ behavior.RequestSpec, _ time.Duration) (*message.Envelope, bool, error) {
		return finalResp("d", map[string]any{
			"status": "failed",
			"reason": string(message.TerminalUnansweredTimeout),
		}), true, nil
	}
	x := &metatool.Exec{Jobs: &fakeJobs{}, Call: call, Clock: time.Now}
	rv := metatool.ExecuteDescribeType(context.Background(),
		json.RawMessage(`{"actor_id":"tool:xhs","type":"xhs.search"}`), x, defaultRC())
	assertIsError(t, rv, "timeout")
}

func TestExecuteDescribeType_MissingType(t *testing.T) {
	x := &metatool.Exec{Jobs: &fakeJobs{}, Call: func(_ context.Context, _ behavior.RequestSpec, _ time.Duration) (*message.Envelope, bool, error) {
		return nil, false, nil
	}, Clock: time.Now}
	rv := metatool.ExecuteDescribeType(context.Background(),
		json.RawMessage(`{"actor_id":"tool:xhs"}`), x, defaultRC())
	assertIsError(t, rv, "payload_invalid")
}

// --- list_actors (sys.Call raw) --------------------------------------------

func TestExecuteListActors_NilCall(t *testing.T) {
	x := &metatool.Exec{Jobs: &fakeJobs{}}
	rv := metatool.ExecuteListActors(context.Background(), nil, x, defaultRC())
	assertIsError(t, rv, "")
}

func TestExecuteListActors_OutsideTurn(t *testing.T) {
	x := &metatool.Exec{Jobs: &fakeJobs{}, Call: func(_ context.Context, _ behavior.RequestSpec, _ time.Duration) (*message.Envelope, bool, error) {
		return nil, false, nil
	}}
	rv := metatool.ExecuteListActors(context.Background(), nil, x, metatool.RuntimeContext{})
	assertIsError(t, rv, "")
}

func TestExecuteListActors_RendersCatalog(t *testing.T) {
	call := func(_ context.Context, spec behavior.RequestSpec, _ time.Duration) (*message.Envelope, bool, error) {
		if spec.Type != "actor.list" {
			t.Fatalf("list spec type = %q", spec.Type)
		}
		body, _ := json.Marshal(map[string]any{
			"actors": []map[string]any{
				{"actor_id": "tool:xhs", "kind": "tool", "present": true},
			},
		})
		return &message.Envelope{Kind: message.KindResponse, Payload: body}, true, nil
	}
	x := &metatool.Exec{Jobs: &fakeJobs{}, Call: call, Clock: time.Now}
	rv := metatool.ExecuteListActors(context.Background(), nil, x, defaultRC())
	assertNotError(t, rv)
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
