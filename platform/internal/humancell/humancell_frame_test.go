package humancell

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

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

// fakeSys implements only the identity-dimension verbs (plus Self/PublishObs)
// the frame interpreter drives; the rest of actorbase.Sys is embedded nil (a
// call the interpreter never makes would nil-panic, which is the honest test
// contract — assert the interpreter touches ONLY the identity face).
type fakeSys struct {
	actorbase.Sys
	self actor.ActorID

	submitSpec  behavior.SubjectWriteSpec
	submitID    message.ID
	submitSeq   int64
	submitErr   error
	respondReq  *message.Envelope
	respondSpec behavior.ResponseSpec
	respondID   message.ID
	respondErr  error
	cancelTID   schedule.TimerID
	rh          actorbase.ResourceHandle
	obs         [][]byte
}

func (f *fakeSys) Self() actor.ActorID { return f.self }
func (f *fakeSys) SubmitEnvelope(spec behavior.SubjectWriteSpec) (message.ID, int64, error) {
	f.submitSpec = spec
	return f.submitID, f.submitSeq, f.submitErr
}
func (f *fakeSys) RespondEnvelope(req *message.Envelope, spec behavior.ResponseSpec) (message.ID, error) {
	f.respondReq, f.respondSpec = req, spec
	return f.respondID, f.respondErr
}
func (f *fakeSys) CancelTimerIdentity(id schedule.TimerID) error { f.cancelTID = id; return nil }
func (f *fakeSys) ResourceIdentity() actorbase.ResourceHandle    { return f.rh }
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
	fs := &fakeSys{self: "human:alice", submitID: "m1", submitSeq: 42}
	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref-1", subjectgate.SubmitPayload{
		ChannelID: "c1", MsgType: "human.message", Audience: []string{"tool:kimi"}, Payload: json.RawMessage(`{"x":1}`),
	})
	got := interpretFrame(fs, newDeps("human:alice", nil, false), f)
	if got.Type != subjectgate.FrameReceipt || got.Ref != "ref-1" {
		t.Fatalf("unexpected receipt frame: %+v", got)
	}
	var rc subjectgate.SubmitReceipt
	_ = got.DecodePayload(&rc)
	if rc.MessageID != "m1" || rc.Seq != 42 {
		t.Fatalf("receipt mismatch: %+v", rc)
	}
	// default kind = request; audience carried through.
	if fs.submitSpec.Kind != message.KindRequest || len(fs.submitSpec.Audience) != 1 {
		t.Fatalf("submit spec not built correctly: %+v", fs.submitSpec)
	}
}

// TestInterpretSubmitExpiresAt (P1-6): the submit frame's optional expires_at_ms
// rides through to SubjectWriteSpec.ExpiresAt verbatim (additive透传); absent → nil.
func TestInterpretSubmitExpiresAt(t *testing.T) {
	exp := int64(1_777_000_000_123)
	fs := &fakeSys{self: "human:alice", submitID: "m1", submitSeq: 1}
	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref", subjectgate.SubmitPayload{
		ChannelID: "c1", MsgType: "human.approve", Kind: "request", Audience: []string{"tool:kimi"},
		Payload: json.RawMessage(`{}`), ExpiresAt: &exp,
	})
	if got := interpretFrame(fs, newDeps("human:alice", nil, false), f); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("submit with expires_at should succeed, got %+v", got)
	}
	if fs.submitSpec.ExpiresAt == nil || *fs.submitSpec.ExpiresAt != exp {
		t.Fatalf("ExpiresAt must透传 verbatim: got %v want %d", fs.submitSpec.ExpiresAt, exp)
	}

	// Absent expires_at → nil (harness default TTL).
	fs2 := &fakeSys{self: "human:alice", submitID: "m2", submitSeq: 2}
	f2, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, "ref2", subjectgate.SubmitPayload{
		ChannelID: "c1", MsgType: "human.message", Audience: []string{"tool:kimi"}, Payload: json.RawMessage(`{}`),
	})
	_ = interpretFrame(fs2, newDeps("human:alice", nil, false), f2)
	if fs2.submitSpec.ExpiresAt != nil {
		t.Fatalf("absent expires_at must be nil, got %v", *fs2.submitSpec.ExpiresAt)
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
		fs := &fakeSys{self: "human:alice", submitID: "m1", submitSeq: 1}
		deps := newDeps("human:alice", nil, false)
		deps.Routing = func() RoutingSnapshot { return snapshot }
		deps.IsActive = func(context.Context, actor.ActorID) (bool, error) { return true, nil }
		deps.Present = func(actor.ActorID) bool { return true }
		return fs, deps
	}

	t.Run("configured preserves event kind", func(t *testing.T) {
		fs, deps := base(RoutingSnapshot{State: RoutingConfigured, Target: "agent:default"})
		got := interpretFrame(fs, deps, frame("event"))
		if got.Type != subjectgate.FrameReceipt {
			t.Fatalf("got error: %+v", decodeErr(t, got))
		}
		if fs.submitSpec.Kind != message.KindEvent ||
			len(fs.submitSpec.Audience) != 1 || fs.submitSpec.Audience[0] != "agent:default" {
			t.Fatalf("submit=%+v", fs.submitSpec)
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
	fs := &fakeSys{self: "human:alice", respondID: "resp1"}

	// happy path.
	f, _ := subjectgate.NewFrame(subjectgate.FrameResolve, "r", subjectgate.ResolvePayload{ChannelID: "c1", ReqID: "r1", Decision: "approved"})
	got := interpretFrame(fs, newDeps("human:alice", req, true), f)
	if got.Type != subjectgate.FrameReceipt {
		t.Fatalf("resolve happy path should receipt: %+v (%s)", got, decodeErr(t, got).Code)
	}
	if fs.respondSpec.Status != message.StatusCompleted {
		t.Fatalf("resolve must map to completed, got %q", fs.respondSpec.Status)
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

func TestInterpretCancelSenderGate(t *testing.T) {
	req := &message.Envelope{ID: "r1", Sender: message.Sender{ID: "human:alice"}, Audience: message.Audience{"tool:kimi"}}
	fs := &fakeSys{self: "human:alice", respondID: "resp1"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameCancel, "r", subjectgate.CancelPayload{ChannelID: "c1", ReqID: "r1"})
	if got := interpretFrame(fs, newDeps("human:alice", req, true), f); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("cancel by sender should receipt: %s", decodeErr(t, got).Code)
	}
	if fs.respondSpec.Status != message.StatusFailed {
		t.Fatalf("cancel must map to failed terminal, got %q", fs.respondSpec.Status)
	}
	// non-sender refused.
	if e := decodeErr(t, interpretFrame(fs, newDeps("human:bob", req, true), f)); e.Code != subjectgate.CodeUnauthorizedSender {
		t.Fatalf("want unauthorized_sender, got %q", e.Code)
	}
}

// TestCancelOwnRequestAcrossIncarnation (DoD-3, the cancel-authority half of the
// two-author-authority split): cancel authority系于 identity, NOT per-life state.
// The request being cancelled was authored in a PRIOR life — this fresh
// incarnation (a bare fakeSys with zero call-ledger memory of it) never Recv'd or
// sent it in-process. It is recovered purely from the durable log (FindByID), and
// the sender-identity gate (req.Sender.ID == self) grants the cancel. The terminal
// failed response lands, closing the request — no per-life ledger was consulted.
// (Paired sibling of TestRespondEnvelopeAcrossIncarnation, which covers the
// respond-authority half at the engine level.)
func TestCancelOwnRequestAcrossIncarnation(t *testing.T) {
	// Request authored by human:alice in a life this incarnation has no memory of.
	priorLifeReq := &message.Envelope{
		ID:       "r-priorlife",
		Sender:   message.Sender{ID: "human:alice"},
		Audience: message.Audience{"tool:kimi"},
	}
	// Fresh incarnation: no call ledger, no serve ledger — authority is log-derived only.
	fs := &fakeSys{self: "human:alice", respondID: "resp-x"}
	f, _ := subjectgate.NewFrame(subjectgate.FrameCancel, "ref-c", subjectgate.CancelPayload{ChannelID: "c1", ReqID: "r-priorlife"})

	got := interpretFrame(fs, newDeps("human:alice", priorLifeReq, true), f)
	if got.Type != subjectgate.FrameReceipt {
		t.Fatalf("cross-incarnation cancel of own request should receipt: %s", decodeErr(t, got).Code)
	}
	// The terminal response addressed the recovered request — truth closed from the log.
	if fs.respondReq == nil || fs.respondReq.ID != "r-priorlife" {
		t.Fatalf("cancel must respond to the log-recovered request, got %+v", fs.respondReq)
	}
	if fs.respondSpec.Status != message.StatusFailed {
		t.Fatalf("cancel must map to failed terminal, got %q", fs.respondSpec.Status)
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
