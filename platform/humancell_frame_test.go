package platform

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/internal/subjectgate"
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

func newDeps(self actor.ActorID, req *message.Envelope, open bool) humanDriverDeps {
	return humanDriverDeps{
		self:      self,
		requests:  &fakeReq{req: req},
		openCheck: func(context.Context, actor.ActorID, message.ID) (bool, error) { return open, nil },
		cancelHint: func(actor.ActorID, message.ID) {},
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
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")
	slot.SetBinding(3)
	fs := &fakeSys{self: "human:alice", submitID: "m1", submitSeq: 42}
	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, 0, "ref-1", subjectgate.SubmitPayload{
		MsgType: "human.message", Audience: []string{"tool:kimi"}, Payload: json.RawMessage(`{"x":1}`),
	})
	got := interpretFrame(fs, slot, newDeps("human:alice", nil, false), f, slot.BindingGen())
	if got.Type != subjectgate.FrameReceipt || got.Ref != "ref-1" || got.BindingGen != 3 {
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
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")
	exp := int64(1_777_000_000_123)
	fs := &fakeSys{self: "human:alice", submitID: "m1", submitSeq: 1}
	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, 0, "ref", subjectgate.SubmitPayload{
		MsgType: "human.approve", Kind: "request", Audience: []string{"tool:kimi"},
		Payload: json.RawMessage(`{}`), ExpiresAt: &exp,
	})
	if got := interpretFrame(fs, slot, newDeps("human:alice", nil, false), f, slot.BindingGen()); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("submit with expires_at should succeed, got %+v", got)
	}
	if fs.submitSpec.ExpiresAt == nil || *fs.submitSpec.ExpiresAt != exp {
		t.Fatalf("ExpiresAt must透传 verbatim: got %v want %d", fs.submitSpec.ExpiresAt, exp)
	}

	// Absent expires_at → nil (harness default TTL).
	fs2 := &fakeSys{self: "human:alice", submitID: "m2", submitSeq: 2}
	f2, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, 0, "ref2", subjectgate.SubmitPayload{
		MsgType: "human.message", Audience: []string{"tool:kimi"}, Payload: json.RawMessage(`{}`),
	})
	_ = interpretFrame(fs2, slot, newDeps("human:alice", nil, false), f2, slot.BindingGen())
	if fs2.submitSpec.ExpiresAt != nil {
		t.Fatalf("absent expires_at must be nil, got %v", *fs2.submitSpec.ExpiresAt)
	}
}

func TestInterpretResolveFiveStep(t *testing.T) {
	req := &message.Envelope{ID: "r1", Sender: message.Sender{ID: "tool:kimi"}, Audience: message.Audience{"human:alice"}}
	fs := &fakeSys{self: "human:alice", respondID: "resp1"}
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")

	// happy path.
	f, _ := subjectgate.NewFrame(subjectgate.FrameResolve, 0, "r", subjectgate.ResolvePayload{ReqID: "r1", Decision: "approved"})
	got := interpretFrame(fs, slot, newDeps("human:alice", req, true), f, slot.BindingGen())
	if got.Type != subjectgate.FrameReceipt {
		t.Fatalf("resolve happy path should receipt: %+v (%s)", got, decodeErr(t, got).Code)
	}
	if fs.respondSpec.Status != message.StatusCompleted {
		t.Fatalf("resolve must map to completed, got %q", fs.respondSpec.Status)
	}

	// invalid decision.
	bad, _ := subjectgate.NewFrame(subjectgate.FrameResolve, 0, "r", subjectgate.ResolvePayload{ReqID: "r1", Decision: "maybe"})
	if e := decodeErr(t, interpretFrame(fs, slot, newDeps("human:alice", req, true), bad, slot.BindingGen())); e.Code != subjectgate.CodeInvalidDecision {
		t.Fatalf("want invalid_decision, got %q", e.Code)
	}
	// not in audience.
	other := newDeps("human:bob", req, true)
	if e := decodeErr(t, interpretFrame(fs, slot, other, f, slot.BindingGen())); e.Code != subjectgate.CodeNotInAudience {
		t.Fatalf("want not_in_audience, got %q", e.Code)
	}
	// already closed.
	if e := decodeErr(t, interpretFrame(fs, slot, newDeps("human:alice", req, false), f, slot.BindingGen())); e.Code != subjectgate.CodeAlreadyClosed {
		t.Fatalf("want already_closed, got %q", e.Code)
	}
	// not found.
	if e := decodeErr(t, interpretFrame(fs, slot, newDeps("human:alice", nil, true), f, slot.BindingGen())); e.Code != subjectgate.CodeRequestNotFound {
		t.Fatalf("want request_not_found, got %q", e.Code)
	}
}

func TestInterpretCancelSenderGate(t *testing.T) {
	req := &message.Envelope{ID: "r1", Sender: message.Sender{ID: "human:alice"}, Audience: message.Audience{"tool:kimi"}}
	fs := &fakeSys{self: "human:alice", respondID: "resp1"}
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")
	f, _ := subjectgate.NewFrame(subjectgate.FrameCancel, 0, "r", subjectgate.CancelPayload{ReqID: "r1"})
	if got := interpretFrame(fs, slot, newDeps("human:alice", req, true), f, slot.BindingGen()); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("cancel by sender should receipt: %s", decodeErr(t, got).Code)
	}
	if fs.respondSpec.Status != message.StatusFailed {
		t.Fatalf("cancel must map to failed terminal, got %q", fs.respondSpec.Status)
	}
	// non-sender refused.
	if e := decodeErr(t, interpretFrame(fs, slot, newDeps("human:bob", req, true), f, slot.BindingGen())); e.Code != subjectgate.CodeUnauthorizedSender {
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
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")
	f, _ := subjectgate.NewFrame(subjectgate.FrameCancel, 0, "ref-c", subjectgate.CancelPayload{ReqID: "r-priorlife"})

	got := interpretFrame(fs, slot, newDeps("human:alice", priorLifeReq, true), f, slot.BindingGen())
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

// TestInterpretStaleBindingAtCommit (修复批四轮 P0, 递交线性化点核验): a frame whose
// carried绑定世代 no longer matches the slot's current层2世代 (a rebind superseded it
// while it sat in the queue) is refused stale_binding at the interpreter's commit
// path — and crucially NEVER 落账 (no SubmitEnvelope). The gate lives at the真线性化
// 点, so the enqueue-time fast check being non-authoritative is harmless.
func TestInterpretStaleBindingAtCommit(t *testing.T) {
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")
	genA := int64(5)
	slot.SetBinding(genA)
	fs := &fakeSys{self: "human:alice", submitID: "m1", submitSeq: 1}
	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, genA, "ref", subjectgate.SubmitPayload{
		MsgType: "human.message", Audience: []string{"tool:kimi"}, Payload: json.RawMessage(`{}`),
	})
	// Rebind (seal → fresh arm → SetBinding) lands BEFORE the interpreter commits.
	slot.SetBinding(genA + 1)
	got := interpretFrame(fs, slot, newDeps("human:alice", nil, false), f, genA)
	if e := decodeErr(t, got); got.Type != subjectgate.FrameError || e.Code != subjectgate.CodeStaleBinding {
		t.Fatalf("superseded frame must return stale_binding error frame, got %+v (%s)", got, e.Code)
	}
	if fs.submitSpec.Type != "" {
		t.Fatalf("a superseded frame must NOT 落账 (no SubmitEnvelope), but submitSpec was set: %+v", fs.submitSpec)
	}

	// DeliverAnyGen (trusted platform-internal shim) is exempt even under a mismatch.
	fs2 := &fakeSys{self: "human:alice", submitID: "m2", submitSeq: 2}
	if got := interpretFrame(fs2, slot, newDeps("human:alice", nil, false), f, subjectgate.DeliverAnyGen); got.Type != subjectgate.FrameReceipt {
		t.Fatalf("DeliverAnyGen must ride through the commit-point gate, got %+v", got)
	}
}

// TestStaleJobTraversalRefusedAtCommit (修复批四轮 P0, 真交错): the frame passes the
// enqueue-time fast check (slot at genA when Deliver runs), traverses the帧递交端
// queue, and the rebind to genA+1 lands via the interpreter闸门 AFTER the job is
// dequeued (enqueue done) but BEFORE the commit. The unbuffered frames channel makes
// the ordering deterministic (Deliver's send rendezvous with the dequeue). Assert the
// stale Job is refused stale_binding at commit and does NOT 落账 —穿越无害.
func TestStaleJobTraversalRefusedAtCommit(t *testing.T) {
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")
	genA := int64(7)
	slot.SetBinding(genA)
	fs := &fakeSys{self: "human:alice", submitID: "m1", submitSeq: 1}
	deps := newDeps("human:alice", nil, false)

	frames, _, release := slot.AttachInterpreter()
	defer release()

	rebound := make(chan struct{})
	go func() {
		job := <-frames           // rendezvous: unblocks Deliver's send (enqueue done)
		slot.SetBinding(genA + 1) // rebind lands after enqueue, before commit
		close(rebound)
		job.Reply(subjectgate.FrameResult{Frame: interpretFrame(fs, slot, deps, job.Frame, job.BindingGen)})
	}()

	f, _ := subjectgate.NewFrame(subjectgate.FrameSubmit, genA, "ref", subjectgate.SubmitPayload{
		MsgType: "human.message", Audience: []string{"tool:kimi"}, Payload: json.RawMessage(`{}`),
	})
	res, err := slot.Deliver(f, genA) // fast check passes (genA current), enqueues, blocks on reply
	if err != nil {
		t.Fatalf("Deliver returned Go error: %v", err)
	}
	<-rebound
	if e := decodeErr(t, res.Frame); res.Frame.Type != subjectgate.FrameError || e.Code != subjectgate.CodeStaleBinding {
		t.Fatalf("stale Job traversing the queue must be refused stale_binding at commit, got %+v (%s)", res.Frame, e.Code)
	}
	if fs.submitSpec.Type != "" {
		t.Fatalf("a stale Job must NOT 落账, but submitSpec was set: %+v", fs.submitSpec)
	}
}

func TestInterpretUnexpectedFrame(t *testing.T) {
	fs := &fakeSys{self: "human:alice"}
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")
	f, _ := subjectgate.NewFrame(subjectgate.FrameAttach, 0, "r", subjectgate.AttachPayload{ChannelID: "c"})
	if e := decodeErr(t, interpretFrame(fs, slot, newDeps("human:alice", nil, false), f, slot.BindingGen())); e.Code != subjectgate.CodeBadPayload {
		t.Fatalf("attach through the cell interpreter is unexpected, got %q", e.Code)
	}
}

func TestInterpretResourceCreate(t *testing.T) {
	fs := &fakeSys{self: "human:alice", rh: &fakeResource{out: accessdoor.Outcome{}}}
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")
	f, _ := subjectgate.NewFrame(subjectgate.FrameResource, 0, "r", subjectgate.ResourcePayload{Op: subjectgate.ResCreate, ResourceID: "res:1", Args: json.RawMessage(`{"a":1}`)})
	got := interpretFrame(fs, slot, newDeps("human:alice", nil, false), f, slot.BindingGen())
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
