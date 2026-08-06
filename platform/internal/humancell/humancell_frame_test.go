package humancell

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// fakeSys implements only the verbs the frame interpreter drives; the rest of
// actorbase.Sys is embedded nil (a call the interpreter never makes would
// nil-panic, which is the honest test contract — assert the interpreter touches
// ONLY the write/schedule/resource face it is supposed to).
//
// NOTE for whoever edits the verb table next: because Sys is embedded nil here,
// ADDING or RENAMING a Sys method does NOT break this file's compilation — it
// breaks at run time. This package must be run explicitly after any verb-table
// change; a green `go build` proves nothing about it.
type fakeSys struct {
	actorbase.Sys
	self actor.ActorID
	life context.Context

	// unregistered writes (submit frame)
	emitSpec behavior.EventSpec
	postSpec behavior.RequestSpec
	emitted  bool
	posted   bool
	writeID  message.ID
	writeErr error

	// terminal writes (resolve / cancel frames)
	replyMsg   actorbase.Msg
	replyVal   any
	replied    bool
	failMsg    actorbase.Msg
	failCode   string
	failDetail string
	failed     bool
	terminalID message.ID
	writeTermE error

	// schedule arm
	afterD       time.Duration
	afterType    string
	afterPayload any
	afterHome    schedule.TimerHome
	afterID      schedule.TimerID
	cancelTID    schedule.TimerID

	rh  actorbase.ResourceHandle
	obs [][]byte
}

func (f *fakeSys) Self() actor.ActorID { return f.self }

func (f *fakeSys) Life() context.Context {
	if f.life == nil {
		f.life = context.Background()
	}
	return f.life
}

func (f *fakeSys) Emit(spec behavior.EventSpec) (message.ID, error) {
	f.emitSpec, f.emitted = spec, true
	return f.writeID, f.writeErr
}

func (f *fakeSys) Post(spec behavior.RequestSpec) (message.ID, error) {
	f.postSpec, f.posted = spec, true
	return f.writeID, f.writeErr
}

func (f *fakeSys) Reply(msg actorbase.Msg, v any) (message.ID, error) {
	f.replyMsg, f.replyVal, f.replied = msg, v, true
	return f.terminalID, f.writeTermE
}

func (f *fakeSys) Fail(msg actorbase.Msg, code, detail string) (message.ID, error) {
	f.failMsg, f.failCode, f.failDetail, f.failed = msg, code, detail, true
	return f.terminalID, f.writeTermE
}

func (f *fakeSys) After(d time.Duration, msgType string, payload any, home schedule.TimerHome) (schedule.TimerID, error) {
	f.afterD, f.afterType, f.afterPayload, f.afterHome = d, msgType, payload, home
	return f.afterID, nil
}

func (f *fakeSys) CancelTimer(id schedule.TimerID) error { f.cancelTID = id; return nil }
func (f *fakeSys) Resource() actorbase.ResourceHandle    { return f.rh }
func (f *fakeSys) PublishObs(kind actorrt.ObsKind, val actorrt.ObsValue) error {
	f.obs = append(f.obs, []byte(val))
	return nil
}

func newDeps(self actor.ActorID, req *message.Envelope, open bool) Deps {
	return Deps{
		Self:       self,
		Requests:   &fakeReq{req: req},
		OpenCheck:  func(context.Context, actor.ActorID, message.ID) (bool, error) { return open, nil },
		CancelHint: func(actor.ActorID, message.ID) {},
	}
}

type fakeReq struct{ req *message.Envelope }

func (f *fakeReq) FindByID(context.Context, message.ID) (*message.Envelope, bool, error) {
	return f.req, f.req != nil, nil
}

func decodeErr(t *testing.T, f subjectgate.Frame) subjectgate.ErrorPayload {
	t.Helper()
	var p subjectgate.ErrorPayload
	if err := f.DecodePayload(&p); err != nil {
		t.Fatalf("decode error payload: %v", err)
	}
	return p
}

func TestInterpretSubmit(t *testing.T) {
	fs := &fakeSys{self: "human:alice", writeID: "m1"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref-1", subjectgate.SubmitPayload{
		ChannelID: "c1", MsgType: "human.message", Audience: []string{"tool:kimi"}, Payload: json.RawMessage(`{"x":1}`),
	})
	got := interpretFrame(fs, newDeps("human:alice", nil, false), f)
	if got.Type != subjectgate.FrameReceipt || got.Ref != "ref-1" {
		t.Fatalf("unexpected receipt frame: %+v", got)
	}
	var rc subjectgate.SubmitReceipt
	_ = got.DecodePayload(&rc)
	if rc.MessageID != "m1" {
		t.Fatalf("receipt mismatch: %+v", rc)
	}
	// default kind = request → Post (never Emit); audience carried through.
	if !fs.posted || fs.emitted {
		t.Fatalf("default kind must Post, not Emit (posted=%v emitted=%v)", fs.posted, fs.emitted)
	}
	if len(fs.postSpec.Audience) != 1 || fs.postSpec.Audience[0] != "tool:kimi" {
		t.Fatalf("request spec not built correctly: %+v", fs.postSpec)
	}
	if fs.postSpec.Type != "human.message" || string(fs.postSpec.Payload) != `{"x":1}` {
		t.Fatalf("request spec not built correctly: %+v", fs.postSpec)
	}
}

func TestInterpretSubmitKeepsAgentIntentInsideOpaquePayload(t *testing.T) {
	const payload = `{"text":"stop and reconsider","intent":"interrupt","expected_turn_id":"turn-7","provider_field":{"x":1}}`
	fs := &fakeSys{self: "human:alice", writeID: "m-intent"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "intent-ref", subjectgate.SubmitPayload{
		ChannelID: "c1", ID: "client-id", MsgType: "human.message",
		Audience: []string{"agent:a"}, Payload: json.RawMessage(payload),
	})
	got := interpretFrame(fs, newDeps("human:alice", nil, false), f)
	if got.Type != subjectgate.FrameReceipt {
		t.Fatalf("intent submit failed: %+v", decodeErr(t, got))
	}
	if !fs.posted || fs.emitted {
		t.Fatalf("agent message must remain an ordinary request")
	}
	if string(fs.postSpec.Payload) != payload {
		t.Fatalf("agent payload changed in transit: got %s want %s", fs.postSpec.Payload, payload)
	}
	if fs.postSpec.ID != "client-id" || fs.postSpec.Type != "human.message" {
		t.Fatalf("intent leaked into standard message fields: %+v", fs.postSpec)
	}
}

// The submit frame's kind is a CLIENT-supplied string. Post/Emit make a
// kind=response unconstructible at the verb, so the whitelist that used to live
// on the deleted SubmitEnvelope now lives here — and answers bad_payload,
// because a refused kind is permanently malformed, not a transient outage.
func TestInterpretSubmitRefusesKindsOtherThanRequestOrEvent(t *testing.T) {
	for _, kind := range []string{"response", "wat"} {
		fs := &fakeSys{self: "human:alice", writeID: "m1"}
		f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref", subjectgate.SubmitPayload{
			ChannelID: "c1", MsgType: "x", Kind: kind, Audience: []string{"tool:kimi"},
		})
		e := decodeErr(t, interpretFrame(fs, newDeps("human:alice", nil, false), f))
		if e.Code != subjectgate.CodeBadPayload {
			t.Fatalf("kind=%q must be bad_payload, got %q", kind, e.Code)
		}
		if fs.posted || fs.emitted {
			t.Fatalf("kind=%q must not reach any write verb", kind)
		}
	}
}

// TestInterpretSubmitExpiresAt (P1-6): the submit frame's optional expires_at_ms
// rides through to RequestSpec.ExpiresAt verbatim (additive透传); absent → nil,
// so the substrate stamps its own long default TTL rather than a short
// caller-side one (Post resolves no timeout — that is why it is Post).
func TestInterpretSubmitExpiresAt(t *testing.T) {
	exp := int64(1_777_000_000_123)
	fs := &fakeSys{self: "human:alice", writeID: "m1"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref", subjectgate.SubmitPayload{
		ChannelID: "c1", MsgType: "human.approve", Kind: "request", Audience: []string{"tool:kimi"},
		Payload: json.RawMessage(`{}`), ExpiresAt: &exp,
	})
	if got := interpretFrame(fs, newDeps("human:alice", nil, false), f); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("submit with expires_at should succeed, got %+v", got)
	}
	if fs.postSpec.ExpiresAt == nil || *fs.postSpec.ExpiresAt != exp {
		t.Fatalf("ExpiresAt must透传 verbatim: got %v want %d", fs.postSpec.ExpiresAt, exp)
	}

	// Absent expires_at → nil (harness default TTL).
	fs2 := &fakeSys{self: "human:alice", writeID: "m2"}
	f2, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref2", subjectgate.SubmitPayload{
		ChannelID: "c1", MsgType: "human.message", Audience: []string{"tool:kimi"}, Payload: json.RawMessage(`{}`),
	})
	_ = interpretFrame(fs2, newDeps("human:alice", nil, false), f2)
	if fs2.postSpec.ExpiresAt != nil {
		t.Fatalf("absent expires_at must be nil, got %v", *fs2.postSpec.ExpiresAt)
	}
}

// The full submit surface (own id, parent, visibility) reaches the verb spec
// unchanged on BOTH arms — the event arm carries no ExpiresAt because an event
// has no closure to deadline.
func TestInterpretSubmitCarriesTheFullSurfaceOnBothArms(t *testing.T) {
	fs := &fakeSys{self: "human:alice", writeID: "m1"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref", subjectgate.SubmitPayload{
		ChannelID: "c1", MsgType: "chat.text", Kind: "event", Audience: []string{"agent:a"},
		ID: "own-id", ParentID: "parent-1", Visibility: "public", Payload: json.RawMessage(`{"t":"hi"}`),
	})
	if got := interpretFrame(fs, newDeps("human:alice", nil, false), f); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("event submit should receipt: %s", decodeErr(t, got).Code)
	}
	if !fs.emitted || fs.posted {
		t.Fatalf("kind=event must Emit, not Post")
	}
	got := fs.emitSpec
	if got.ID != "own-id" || got.ParentID != "parent-1" || got.Visibility != message.VisibilityPublic ||
		got.Type != "chat.text" || string(got.Payload) != `{"t":"hi"}` {
		t.Fatalf("event spec lost a field: %+v", got)
	}

	fs2 := &fakeSys{self: "human:alice", writeID: "m2"}
	f2, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref", subjectgate.SubmitPayload{
		ChannelID: "c1", MsgType: "human.approve", Kind: "request", Audience: []string{"agent:a"},
		ID: "own-id", ParentID: "parent-1", Visibility: "public",
	})
	if got := interpretFrame(fs2, newDeps("human:alice", nil, false), f2); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("request submit should receipt: %s", decodeErr(t, got).Code)
	}
	if fs2.postSpec.ID != "own-id" || fs2.postSpec.ParentID != "parent-1" ||
		fs2.postSpec.Visibility != message.VisibilityPublic {
		t.Fatalf("request spec lost a field: %+v", fs2.postSpec)
	}
}

// A client id collision is a websocket idempotency concern, never a leaked
// harness implementation word — on both write arms.
func TestSubmitMapsDuplicateRejectToIdempotencyConflictOnBothArms(t *testing.T) {
	for _, kind := range []string{"request", "event"} {
		fs := &fakeSys{
			self:     "human:alice",
			writeErr: &actorbase.WriteRejected{Reason: "harness_id_duplicate_conflict", Detail: "already exists"},
		}
		f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref", subjectgate.SubmitPayload{
			ChannelID: "c1", MsgType: "x", Kind: kind, Audience: []string{"tool:kimi"},
		})
		e := decodeErr(t, interpretFrame(fs, newDeps("human:alice", nil, false), f))
		if e.Code != subjectgate.CodeIdempotencyConflict || e.Detail != "already exists" {
			t.Fatalf("kind=%s: duplicate must map to idempotency_conflict, got %+v", kind, e)
		}
	}
}

func TestSubmitMapsInvalidVisibilityToPermanentBadPayload(t *testing.T) {
	fs := &fakeSys{
		self:     "human:alice",
		writeErr: &actorbase.InvalidVisibilityError{Visibility: message.Visibility("private")},
	}
	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref", subjectgate.SubmitPayload{
		ChannelID: "c1", MsgType: "x", Kind: "event", Visibility: "private",
	})
	e := decodeErr(t, interpretFrame(fs, newDeps("human:alice", nil, false), f))
	if e.Code != subjectgate.CodeBadPayload {
		t.Fatalf("private visibility code=%q, want bad_payload", e.Code)
	}
}

func TestSubmitFingerprintUsesCanonicalClientSemantics(t *testing.T) {
	request := func(id, kind, visibility, payload string) subjectgate.SubmitPayload {
		return subjectgate.SubmitPayload{
			ChannelID: "c1", ID: id, MsgType: "human.message", Kind: kind,
			Visibility: visibility, Audience: []string{"agent:a"}, Payload: json.RawMessage(payload),
		}
	}
	base, err := submitFingerprint(request("id-1", "", "", `{"z":1.0,"a":{"y":2,"x":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	equivalent, err := submitFingerprint(request("id-2", "request", "public", `{"a":{"x":1,"y":2.0},"z":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if base != equivalent {
		t.Fatalf("JSON key order/default spelling changed fingerprint:\n%s\n%s", base, equivalent)
	}

	changed := request("id-3", "request", "public", `{"a":{"x":1,"y":2},"z":1,"intent":"interrupt"}`)
	changedFingerprint, err := submitFingerprint(changed)
	if err != nil {
		t.Fatal(err)
	}
	if changedFingerprint == base {
		t.Fatal("payload intent must participate in the fingerprint")
	}

	// Frame ref never reaches SubmitPayload, and client id is the idempotency
	// key rather than fingerprint material.
	otherID := request("entirely-different-id", "", "", `{"z":1,"a":{"y":2,"x":1}}`)
	otherFingerprint, err := submitFingerprint(otherID)
	if err != nil || otherFingerprint != base {
		t.Fatalf("message id leaked into fingerprint: got %s err=%v want %s", otherFingerprint, err, base)
	}
}

// The interpreter runs its verbs on the cell's life ctx, so every write during
// teardown answers a bare context.Canceled. That must not reach a person's
// client as a Go runtime string — same retryable verdict, honest detail.
func TestTeardownWritesAnswerAStableUnavailable(t *testing.T) {
	fs := &fakeSys{self: "human:alice", writeErr: fmt.Errorf("pen write: %w", context.Canceled)}
	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref", subjectgate.SubmitPayload{
		ChannelID: "c1", MsgType: "human.approve", Audience: []string{"tool:kimi"},
	})
	e := decodeErr(t, interpretFrame(fs, newDeps("human:alice", nil, false), f))
	if e.Code != subjectgate.CodeUnavailable || e.Detail != "cell is stopping" {
		t.Fatalf("teardown write should be a stable unavailable, got %+v", e)
	}
}

func TestInterpretSubmitDefaultAudienceAtHumanMembrane(t *testing.T) {
	frame := func(kind string) subjectgate.Frame {
		f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "routing", subjectgate.SubmitPayload{
			ChannelID: "c1", MsgType: "chat.text", Kind: kind,
			Payload: json.RawMessage(`{"text":"hi"}`),
		})
		return f
	}
	base := func(snapshot RoutingSnapshot) (*fakeSys, Deps) {
		fs := &fakeSys{self: "human:alice", writeID: "m1"}
		deps := newDeps("human:alice", nil, false)
		deps.Routing = func() RoutingSnapshot { return snapshot }
		deps.IsActive = func(context.Context, actor.ActorID) (bool, error) { return true, nil }
		deps.Present = func(actor.ActorID) bool { return true }
		return fs, deps
	}

	t.Run("event stays empty and ignores default routing", func(t *testing.T) {
		fs, deps := base(RoutingSnapshot{State: RoutingConfigured, Target: "agent:default"})
		got := interpretFrame(fs, deps, frame("event"))
		if got.Type != subjectgate.FrameReceipt {
			t.Fatalf("got error: %+v", decodeErr(t, got))
		}
		if !fs.emitted || fs.posted {
			t.Fatalf("kind=event must go to Emit")
		}
		if fs.emitSpec.Audience == nil || len(fs.emitSpec.Audience) != 0 {
			t.Fatalf("emit=%+v", fs.emitSpec)
		}
	})

	t.Run("unset", func(t *testing.T) {
		fs, deps := base(RoutingSnapshot{State: RoutingUnset})
		err := decodeErr(t, interpretFrame(fs, deps, frame("request")))
		if err.Code != subjectgate.CodeRoutingUnavailable ||
			err.Detail != "未设置默认应答者，请设置或指名收件人" {
			t.Fatalf("error=%+v", err)
		}
	})

	for _, tc := range []struct {
		name     string
		snapshot RoutingSnapshot
		active   bool
		present  bool
	}{
		{"fold unavailable", RoutingSnapshot{State: RoutingUnavailable}, true, true},
		{"inactive", RoutingSnapshot{State: RoutingConfigured, Target: "agent:default"}, false, true},
		{"not present", RoutingSnapshot{State: RoutingConfigured, Target: "agent:default"}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, deps := base(tc.snapshot)
			deps.IsActive = func(context.Context, actor.ActorID) (bool, error) { return tc.active, nil }
			deps.Present = func(actor.ActorID) bool { return tc.present }
			err := decodeErr(t, interpretFrame(fs, deps, frame("request")))
			if err.Code != subjectgate.CodeRoutingUnavailable ||
				err.Detail != "默认应答者当前不可用，请重新设置一次" {
				t.Fatalf("error=%+v", err)
			}
		})
	}

	t.Run("active read failure", func(t *testing.T) {
		fs, deps := base(RoutingSnapshot{State: RoutingConfigured, Target: "agent:default"})
		deps.IsActive = func(context.Context, actor.ActorID) (bool, error) {
			return false, errors.New("ledger down")
		}
		err := decodeErr(t, interpretFrame(fs, deps, frame("request")))
		if err.Code != subjectgate.CodeUnavailable {
			t.Fatalf("error=%+v", err)
		}
	})
}

func TestInterpretResolveFiveStep(t *testing.T) {
	req := &message.Envelope{ID: "r1", Sender: message.Sender{ID: "tool:kimi"}, Audience: message.Audience{"human:alice"}}
	life := context.Background()
	fs := &fakeSys{self: "human:alice", life: life, terminalID: "resp1"}

	// happy path.
	f, _ := subjectgate.NewFrame(subjectgate.FrameResolve, "r", subjectgate.ResolvePayload{ChannelID: "c1", ReqID: "r1", Decision: "approved"})
	got := interpretFrame(fs, newDeps("human:alice", req, true), f)
	if got.Type != subjectgate.FrameReceipt {
		t.Fatalf("resolve happy path should receipt: %+v (%s)", got, decodeErr(t, got).Code)
	}
	if !fs.replied {
		t.Fatalf("resolve must close the request through Reply (the completed terminal)")
	}
	// The handle is built from the LOG-recovered envelope, on sys.Life() — the
	// person holds no mailbox delivery and the cell outlives them going offline.
	if fs.replyMsg.ID != "r1" || fs.replyMsg.Ctx() != life {
		t.Fatalf("reply handle must be the log-recovered request on sys.Life(): id=%q", fs.replyMsg.ID)
	}

	// invalid decision.
	bad, _ := subjectgate.NewFrame(subjectgate.FrameResolve, "r", subjectgate.ResolvePayload{ChannelID: "c1", ReqID: "r1", Decision: "maybe"})
	if e := decodeErr(t, interpretFrame(fs, newDeps("human:alice", req, true), bad)); e.Code != subjectgate.CodeInvalidDecision {
		t.Fatalf("want invalid_decision, got %q", e.Code)
	}
	// not in audience.
	other := newDeps("human:bob", req, true)
	if e := decodeErr(t, interpretFrame(fs, other, f)); e.Code != subjectgate.CodeNotInAudience {
		t.Fatalf("want not_in_audience, got %q", e.Code)
	}
	// already closed.
	if e := decodeErr(t, interpretFrame(fs, newDeps("human:alice", req, false), f)); e.Code != subjectgate.CodeAlreadyClosed {
		t.Fatalf("want already_closed, got %q", e.Code)
	}
	// not found.
	if e := decodeErr(t, interpretFrame(fs, newDeps("human:alice", nil, true), f)); e.Code != subjectgate.CodeRequestNotFound {
		t.Fatalf("want request_not_found, got %q", e.Code)
	}
}

// §7.1, the migration trap: Reply marshals its argument ONCE (behavior.
// RespondJSON), so the interpreter must hand it the merged VALUE — never the
// bytes of an already-marshalled copy. json.Marshal([]byte) emits a base64 JSON
// STRING, which would silently destroy the payload and lose `decision`
// altogether. This test marshals exactly as Reply does and asserts the result is
// still a decodable object carrying both the person's fields and the decision.
func TestResolvePayloadIsMarshalledExactlyOnce(t *testing.T) {
	req := &message.Envelope{ID: "r1", Sender: message.Sender{ID: "tool:kimi"}, Audience: message.Audience{"human:alice"}}
	fs := &fakeSys{self: "human:alice", terminalID: "resp1"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameResolve, "r", subjectgate.ResolvePayload{
		ChannelID: "c1", ReqID: "r1", Decision: "approved",
		Payload: json.RawMessage(`{"note":"看过了","n":7}`),
	})
	if got := interpretFrame(fs, newDeps("human:alice", req, true), f); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("resolve should receipt: %s", decodeErr(t, got).Code)
	}
	if _, isBytes := fs.replyVal.([]byte); isBytes {
		t.Fatalf("Reply must never be handed a []byte — it would be re-marshalled to base64")
	}
	// Reply's own single marshal, reproduced.
	raw, err := json.Marshal(fs.replyVal)
	if err != nil {
		t.Fatalf("marshal reply value: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("resolve payload is not a JSON object (base64 corruption?): %s", raw)
	}
	if out["decision"] != "approved" || out["note"] != "看过了" || out["n"] != float64(7) {
		t.Fatalf("resolve payload lost fields: %v", out)
	}
}

func TestInterpretCancelSenderGate(t *testing.T) {
	req := &message.Envelope{ID: "r1", Sender: message.Sender{ID: "human:alice"}, Audience: message.Audience{"tool:kimi"}}
	life := context.Background()
	fs := &fakeSys{self: "human:alice", life: life, terminalID: "resp1"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameCancel, "r", subjectgate.CancelPayload{ChannelID: "c1", ReqID: "r1"})
	if got := interpretFrame(fs, newDeps("human:alice", req, true), f); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("cancel by sender should receipt: %s", decodeErr(t, got).Code)
	}
	// Cancel writes the failed terminal through Fail, on the same log-origin
	// handle resolve uses. The interpreter supplies only code+detail: the
	// reason derivation (this identity SENT the request → the caller's own
	// unanswered_timeout) and the cancelled:true stamp are the engine's, in one
	// place — the three-field literal this function used to hand-write is gone.
	if !fs.failed || fs.replied {
		t.Fatalf("cancel must go through Fail, not Reply")
	}
	if fs.failMsg.ID != "r1" || fs.failMsg.Ctx() != life {
		t.Fatalf("fail handle must be the log-recovered request on sys.Life(): id=%q", fs.failMsg.ID)
	}
	if fs.failCode != string(message.TerminalUnansweredTimeout) || fs.failDetail != "cancelled by caller" {
		t.Fatalf("cancel words changed: code=%q detail=%q", fs.failCode, fs.failDetail)
	}
	// non-sender refused.
	if e := decodeErr(t, interpretFrame(fs, newDeps("human:bob", req, true), f)); e.Code != subjectgate.CodeUnauthorizedSender {
		t.Fatalf("want unauthorized_sender, got %q", e.Code)
	}
}

// §9-14: the打断 hint is best-effort and fires EXACTLY once per successful
// cancel — and never when the write itself errored. Fail does not run
// Hooks.Canceller (that hook belongs to callLedger.cancel, the in-process
// twin), so deps.CancelHint is the ONLY thing that can interrupt the receiver.
func TestCancelHintFiresExactlyOnce(t *testing.T) {
	req := &message.Envelope{ID: "r1", Sender: message.Sender{ID: "human:alice"}, Audience: message.Audience{"tool:kimi"}}
	f, _ := subjectgate.NewFrame(subjectgate.FrameCancel, "r", subjectgate.CancelPayload{ChannelID: "c1", ReqID: "r1"})

	var hints []actor.ActorID
	deps := newDeps("human:alice", req, true)
	deps.CancelHint = func(target actor.ActorID, id message.ID) {
		if id != "r1" {
			t.Fatalf("hint carried the wrong request id: %q", id)
		}
		hints = append(hints, target)
	}

	fs := &fakeSys{self: "human:alice", terminalID: "resp1"}
	if got := interpretFrame(fs, deps, f); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("cancel should receipt: %s", decodeErr(t, got).Code)
	}
	if len(hints) != 1 || hints[0] != "tool:kimi" {
		t.Fatalf("want exactly one hint to the receiver, got %v", hints)
	}

	// A verb-errored self-close closes nothing, so it hints nothing.
	hints = nil
	errored := &fakeSys{self: "human:alice", writeTermE: errors.New("membrane down")}
	if got := interpretFrame(errored, deps, f); got.Type != subjectgate.FrameError {
		t.Fatalf("failed cancel must answer an error frame, got %+v", got)
	}
	if len(hints) != 0 {
		t.Fatalf("a failed cancel must send no hint, got %v", hints)
	}
}

// TestCancelOwnRequestAcrossIncarnation (DoD-3, the cancel-authority half of the
// two-author-authority split): cancel authority系于 identity, NOT per-life state.
// The request being cancelled was authored in a PRIOR life — this fresh
// incarnation (a bare fakeSys with zero call-ledger memory of it) never Recv'd or
// sent it in-process. It is recovered purely from the durable log (FindByID), and
// the sender-identity gate (req.Sender.ID == self) grants the cancel. The
// log-origin Msg carries that recovery into the verb, and the terminal failed
// response lands — no per-life ledger was consulted.
// (Engine-side sibling: lib/actorbase's log-origin terminal-write coverage.)
func TestCancelOwnRequestAcrossIncarnation(t *testing.T) {
	// Request authored by human:alice in a life this incarnation has no memory of.
	priorLifeReq := &message.Envelope{
		ID:       "r-priorlife",
		Sender:   message.Sender{ID: "human:alice"},
		Audience: message.Audience{"tool:kimi"},
	}
	// Fresh incarnation: no call ledger, no serve ledger — authority is log-derived only.
	fs := &fakeSys{self: "human:alice", terminalID: "resp-x"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameCancel, "ref-c", subjectgate.CancelPayload{ChannelID: "c1", ReqID: "r-priorlife"})

	got := interpretFrame(fs, newDeps("human:alice", priorLifeReq, true), f)
	if got.Type != subjectgate.FrameReceipt {
		t.Fatalf("cross-incarnation cancel of own request should receipt: %s", decodeErr(t, got).Code)
	}
	// The terminal addressed the recovered request — truth closed from the log.
	if !fs.failed || fs.failMsg.ID != "r-priorlife" {
		t.Fatalf("cancel must fail the log-recovered request, got %+v", fs.failMsg.Envelope)
	}
	if fs.failMsg.Sender.ID != "human:alice" {
		t.Fatalf("the recovered envelope must ride into the verb verbatim — the sender is what picks the self-close arm")
	}
}

func TestInterpretAfterUsesTheDurableHome(t *testing.T) {
	fs := &fakeSys{self: "human:alice", afterID: "t-1"}
	payload := json.RawMessage(`{"note":"remind me"}`)
	f, _ := subjectgate.NewFrame(subjectgate.FrameAfter, "r", subjectgate.AfterPayload{
		ChannelID: "c1", DurationMs: 1500, MsgType: "human.remind", Payload: payload,
	})
	got := interpretFrame(fs, newDeps("human:alice", nil, false), f)
	if got.Type != subjectgate.FrameReceipt {
		t.Fatalf("after should receipt: %s", decodeErr(t, got).Code)
	}
	var rc subjectgate.AfterReceipt
	_ = got.DecodePayload(&rc)
	if rc.TimerID != "t-1" {
		t.Fatalf("timer id mismatch: %+v", rc)
	}
	// Durable: a person's reminder must outlive a Scheduler restart. home is
	// DURABILITY, not lifetime — there is no default, so this is a declaration.
	if fs.afterHome != schedule.TimerHomeDurable {
		t.Fatalf("human timers must be durable, got %q", fs.afterHome)
	}
	if fs.afterD != 1500*time.Millisecond || fs.afterType != "human.remind" {
		t.Fatalf("after args wrong: d=%v type=%q", fs.afterD, fs.afterType)
	}
	// The payload stays json.RawMessage all the way to the verb: After marshals
	// once, and RawMessage's MarshalJSON emits the bytes as-is. Handing over a
	// plain []byte here would base64 them (the §7.1 trap, on this line too).
	raw, ok := fs.afterPayload.(json.RawMessage)
	if !ok {
		t.Fatalf("After payload must stay json.RawMessage, got %T", fs.afterPayload)
	}
	out, err := json.Marshal(raw)
	if err != nil || string(out) != `{"note":"remind me"}` {
		t.Fatalf("timer payload would not survive After's marshal: %s (%v)", out, err)
	}

	// bounds still refused before the verb.
	for _, bad := range []int64{0, -1} {
		bf, _ := subjectgate.NewFrame(subjectgate.FrameAfter, "r", subjectgate.AfterPayload{
			ChannelID: "c1", DurationMs: bad, MsgType: "x",
		})
		if e := decodeErr(t, interpretFrame(fs, newDeps("human:alice", nil, false), bf)); e.Code != subjectgate.CodeBadPayload {
			t.Fatalf("duration %d must be bad_payload, got %q", bad, e.Code)
		}
	}
}

func TestInterpretCancelTimer(t *testing.T) {
	fs := &fakeSys{self: "human:alice"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameCancelTimer, "r", subjectgate.CancelTimerPayload{
		ChannelID: "c1", TimerID: "t-9",
	})
	got := interpretFrame(fs, newDeps("human:alice", nil, false), f)
	if got.Type != subjectgate.FrameReceipt {
		t.Fatalf("cancel_timer should receipt: %s", decodeErr(t, got).Code)
	}
	if fs.cancelTID != "t-9" {
		t.Fatalf("timer id not carried: %q", fs.cancelTID)
	}
}

func TestInterpretUnexpectedFrame(t *testing.T) {
	fs := &fakeSys{self: "human:alice"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameAttach, "r", subjectgate.AttachPayload{})
	if e := decodeErr(t, interpretFrame(fs, newDeps("human:alice", nil, false), f)); e.Code != subjectgate.CodeBadPayload {
		t.Fatalf("attach through the cell interpreter is unexpected, got %q", e.Code)
	}
}

func TestInterpretResourceCreate(t *testing.T) {
	fs := &fakeSys{self: "human:alice", rh: &fakeResource{out: accessdoor.Outcome{}}}
	f, _ := subjectgate.NewFrame(subjectgate.FrameResource, "r", subjectgate.ResourcePayload{ChannelID: "c1", Op: subjectgate.ResCreate, ResourceID: "res:1", Args: json.RawMessage(`{"a":1}`)})
	got := interpretFrame(fs, newDeps("human:alice", nil, false), f)
	if got.Type != subjectgate.FrameReceipt {
		t.Fatalf("resource create should receipt: %s", decodeErr(t, got).Code)
	}
	var o subjectgate.ResourceOutcome
	_ = got.DecodePayload(&o)
	if o.Status != "ok" {
		t.Fatalf("want ok outcome, got %+v", o)
	}
}

func TestPublishPresenceMapping(t *testing.T) {
	fs := &fakeSys{}
	publishPresence(fs, subjectgate.LevelOnline)
	publishPresence(fs, subjectgate.LevelOffline)
	if len(fs.obs) != 2 {
		t.Fatalf("want 2 obs pushes, got %d", len(fs.obs))
	}
	p1, ok1 := introspect.ParseDevicePresence(fs.obs[0])
	p2, ok2 := introspect.ParseDevicePresence(fs.obs[1])
	if !ok1 || !ok2 || !p1.Online || p2.Online {
		t.Fatalf("level→online mapping wrong: %+v %+v", p1, p2)
	}
}

var _ actorbase.ResourceHandle = (*fakeResource)(nil)

type fakeResource struct {
	actorbase.ResourceHandle
	out accessdoor.Outcome
}

func (f *fakeResource) Create(resource.ResourceID, []byte) (accessdoor.Outcome, error) {
	return f.out, nil
}
