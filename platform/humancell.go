package platform

import (
	"context"
	"encoding/json"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/lib/jsondepth"
	"github.com/wanpengxie/atoll/platform/internal/subjectgate"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
)

// Day-1 two of the three honest closure options (三层律 §3) a human cell
// declares per request type. fail-fast (device_unreachable) is not a human's
// option: the log IS the inbox, so a human is never structurally unreachable —
// an unrecognised type degrades to the deferred (收件箱) default rather than a
// fabricated failure.
const (
	// TypeHumanMessage is IMMEDIATE: a message to the human's inbox, answered
	// completed on receipt (durable delivery to the log IS the answer).
	TypeHumanMessage = "human.message"
	// TypeHumanApprove is DEFERRED: left OPEN until the person answers via the
	// door (the resolve frame). Closure is the sender's caller-scoped timer.
	TypeHumanApprove = "human.approve"
)

// humanCellFactory is the platform's built-in home-side human embodiment. user域
// supply is platform internal政 — a per-channel human member's authority lives
// only in this channel's registry (the app cannot enumerate it), so the reconcile
// ring keeps a live human cell up whenever the member is admitted, without any
// app-injected factory.
//
// Proc shape (through the actorbase engine, NOT a raw actorrt.Actor implementer —
// archtest wall): TWO input faces run concurrently (标准型, design §5.2) —
//
//   - the MAILBOX serve loop (humanServe): answers each delivered request per the
//     three-choice type table (immediate human.message / deferred human.approve /
//     describe self-answer);
//   - the FRAME interpreter (gateway 期 S2): the person's OWN actions arrive as
//     wire frames through the per-identity slot's帧递交端 and are driven onto this
//     cell's own caps via the identity-dimension Sys verbs (SubmitEnvelope/
//     RespondEnvelope/AfterIdentity/…). No slot (no gateway attach yet) → this
//     face is dormant and the cell is mailbox-only.
//
// The cell holds ZERO caller obligations (期12): a subject's own requests are
// closed by the substrate expiry reaper (义务归位 D3) — no per-user Caller, no
// Match plumbing.
func humanCellFactory(h *Home, id actor.ActorID) ActorFactory {
	return ActorFactory{Proc: actorbase.Def{
		Doc: "home-side human embodiment (subjectgate): callable; three-choice per-type closure (immediate human.message / deferred human.approve) + describe; the person drives own actions via wire frames through the slot",
		New: func() (actorbase.Proc, error) {
			return func(sys actorbase.Sys) error { return h.runHumanCell(id, sys) }, nil
		},
	}}
}

// subjectgateSlot returns the per-identity slot, ENSURING it exists (装配链
// step③). The slot must outlive incarnations and pre-exist any gateway attach:
// an always-on human cell is born by the reconcile ring at Admit — BEFORE any ws
// attach — so a lazy lookup would leave the cell permanently mailbox-only (a
// later attach's EnsureSlot would create a slot the already-running cell never
// observed). The cell owns its own frame-delivery endpoint's creation (the slot
// is the identity's, not a connection's); the gateway (S3) then looks it up at
// attach and drives it. nil registry (defensive) → mailbox-only.
func (h *Home) subjectgateSlot(id actor.ActorID) (*subjectgate.Slot, bool) {
	if h.subjectgate == nil {
		return nil, false
	}
	return h.subjectgate.EnsureSlot(id), true
}

// runHumanCell is the human cell's Proc body: it wires the frame interpreter +
// presence self-report against the slot (装配链 step③④) if one exists, then runs
// the mailbox serve loop. On serve-loop exit (Recv error = cooperative
// termination) it stops the interpreter goroutine and joins it (S1 纪律照 kimi:
// wg join +解阻 — closing stop detaches the slot so any blocked gateway Deliver
// unblocks with ErrNoOccupant).
func (h *Home) runHumanCell(id actor.ActorID, sys actorbase.Sys) error {
	var wg sync.WaitGroup
	stop := make(chan struct{})

	if slot, ok := h.subjectgateSlot(id); ok {
		deps := humanDriverDeps{
			self:       id,
			requests:   h.cs.Requests,
			openCheck:  h.isRequestOpen,
			cancelHint: h.CancelRequest,
		}
		// presence self-report (design §5.4): read the slot snapshot (step④) and
		// self-report its level; nothing published (unknown) → say nothing (fold
		// 无行 = unknown 诚实默认). Then observe edges by this incarnation's token —
		// an old cell's摘除 (its token) can never unregister this one.
		token := uuid.NewString()
		if level, _, _, present := slot.Snapshot(); present {
			publishPresence(sys, level)
		}
		slot.RegisterObserver(token, func(u subjectgate.PresenceUpdate) {
			// Only positive edges are self-reported; a revocation (Live=false) is
			// the容器 owner's证词账清洁 (Forget/epoch teardown, S4), not the cell's
			// to retract via PublishObs.
			if u.Live {
				publishPresence(sys, u.Level)
			}
		})
		frames, release := slot.AttachInterpreter()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer release()
			defer slot.RemoveObserver(token)
			interpretFrames(sys, slot, deps, frames, stop)
		}()
	}

	err := humanServe(sys)
	close(stop)
	wg.Wait()
	return err
}

func publishPresence(sys actorbase.Sys, level subjectgate.Level) {
	_ = sys.PublishObs(introspect.ObsDevicePresence, introspect.MarshalDevicePresence(level == subjectgate.LevelOnline))
}

// humanServe is the human cell's mailbox serve loop: delivered requests route
// through the three-choice type table. Returning on a Recv error is the
// cooperative termination contract (spec §1.6).
func humanServe(sys actorbase.Sys) error {
	for {
		msg, err := sys.Recv()
		if err != nil {
			return nil
		}
		switch msg.Kind {
		case message.KindRequest:
			humanServeRequest(sys, msg)
		}
	}
}

// humanServeRequest answers one delivered request per the three-choice type
// table. It NEVER fabricates a Reply it did not earn: human.approve and any
// unrecognised type are left OPEN (deferred) — the person's resolve frame is the
// real answer, and closure is the sender's caller-scoped timer.
func humanServeRequest(sys actorbase.Sys, msg actorbase.Msg) {
	switch msg.Type {
	case introspect.QueryDescribe:
		req, err := introspect.ParseDescribeRequest(msg.Payload)
		if err != nil {
			_, _ = sys.Fail(msg, "payload_invalid", err.Error())
			return
		}
		answer, ok := introspect.AnswerDescribe(humanDescribe(string(sys.Self())), req)
		if !ok {
			_, _ = sys.Fail(msg, "type_unsupported", "human cell does not serve "+req.Type)
			return
		}
		_, _ = sys.Reply(msg, answer)
	case TypeHumanMessage:
		// immediate: 收件即 completed 回执 (log 即收件箱).
		_, _ = sys.Reply(msg, map[string]any{"delivered": true})
	default:
		// default-deferred (D9, owner 拍定): human.approve AND any
		// unrecognised type are left OPEN — the log IS the inbox and a human
		// is never structurally unreachable, so the honest default is "held
		// for the person", never a fabricated failure. Closure of a
		// never-answered deferred is the sender's declared deadline (expiry
		// reaper). Declared in describe below as the default row.
	}
}

// humanDescribe is the human cell's actor.describe self-answer catalog.
func humanDescribe(id string) introspect.Describe {
	return introspect.Describe{
		ActorID:     id,
		Description: "human subject — occupant off-process; the log is the inbox",
		Types: map[string]introspect.TypeMeta{
			TypeHumanMessage: {Description: "immediate: delivered to the human's inbox, answered completed on receipt"},
			TypeHumanApprove: {Description: "deferred: left open until the person answers via the door (resolve)"},
			// D9 default-deferred declaration: unrecognised types are accepted
			// deferred — the log is the inbox, a human is never unreachable.
			"*": {Description: "default-deferred: any unrecognised type is accepted and left open for the person (the log is the inbox)"},
		},
	}
}

// humanDriverDeps is the frame interpreter's injected read-only face (五步核查经
// Deps 注入, sysactor Deps 形): the from-log request lookup + open check + the
// cancel-hint reach (Hooks.Canceller = Home.CancelRequest, factory-captured).
// No capability of its own — every write goes through the cell's own Sys verbs.
type humanDriverDeps struct {
	self       actor.ActorID
	requests   requestLookup
	openCheck  func(ctx context.Context, receiver actor.ActorID, reqID message.ID) (bool, error)
	cancelHint func(target actor.ActorID, requestID message.ID)
}

// requestLookup is the from-log recovery seam (cs.Requests satisfies it).
type requestLookup interface {
	FindByID(ctx context.Context, id message.ID) (*message.Envelope, bool, error)
}

// interpretFrames is the frame interpreter goroutine (S1 纪律照 kimi): it
// consumes upstream frame jobs from the slot and answers each with a
// receipt-or-error frame. stop (closed by the Proc on serve-loop exit) is the
// 解阻/join edge.
func interpretFrames(sys actorbase.Sys, slot *subjectgate.Slot, deps humanDriverDeps, frames <-chan subjectgate.Job, stop <-chan struct{}) {
	for {
		select {
		case job := <-frames:
			job.Reply(subjectgate.FrameResult{Frame: interpretFrame(sys, slot, deps, job.Frame)})
		case <-stop:
			return
		}
	}
}

// interpretFrame drives one upstream business frame onto the cell's own caps and
// returns the receipt (or error) frame. binding_gen is stamped from the slot's
// current layer-2 generation. attach/detach/presence are gateway control frames
// (handled north of the cell) — they are unexpected here.
func interpretFrame(sys actorbase.Sys, slot *subjectgate.Slot, deps humanDriverDeps, f subjectgate.Frame) subjectgate.Frame {
	gen := slot.BindingGen()
	errFrame := func(code, detail string) subjectgate.Frame {
		fr, _ := subjectgate.NewFrame(subjectgate.FrameError, gen, f.Ref, subjectgate.ErrorPayload{
			Frame: string(f.Type), Code: code, Detail: detail,
		})
		return fr
	}
	receipt := func(load any) subjectgate.Frame {
		fr, _ := subjectgate.NewFrame(subjectgate.FrameReceipt, gen, f.Ref, load)
		return fr
	}

	switch f.Type {
	case subjectgate.FrameSubmit:
		return interpretSubmit(sys, f, receipt, errFrame)
	case subjectgate.FrameResolve:
		return interpretResolve(sys, deps, f, receipt, errFrame)
	case subjectgate.FrameCancel:
		return interpretCancel(sys, deps, f, receipt, errFrame)
	case subjectgate.FrameAfter:
		return interpretAfter(sys, f, receipt, errFrame)
	case subjectgate.FrameCancelTimer:
		return interpretCancelTimer(sys, f, receipt, errFrame)
	case subjectgate.FrameResource:
		return interpretResource(sys, f, receipt, errFrame)
	default:
		return errFrame(subjectgate.CodeBadPayload, "unexpected frame_type for a subject driver: "+string(f.Type))
	}
}

type frameBuild func(load any) subjectgate.Frame
type frameErr func(code, detail string) subjectgate.Frame

func interpretSubmit(sys actorbase.Sys, f subjectgate.Frame, receipt frameBuild, errFrame frameErr) subjectgate.Frame {
	var p subjectgate.SubmitPayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrame(subjectgate.CodeBadPayload, err.Error())
	}
	kind := message.Kind(p.Kind)
	if kind == "" {
		kind = message.KindRequest
	}
	id := message.ID(p.ID)
	if id == "" {
		id = message.ID(uuid.NewString())
	}
	aud := make(message.Audience, 0, len(p.Audience))
	for _, a := range p.Audience {
		aud = append(aud, actor.ActorID(a))
	}
	msgID, seq, err := sys.SubmitEnvelope(behavior.SubjectWriteSpec{
		ID:         id,
		Type:       p.MsgType,
		Kind:       kind,
		Payload:    p.Payload,
		Audience:   aud,
		Visibility: message.Visibility(p.Visibility),
		ParentID:   message.ID(p.ParentID),
	})
	if err != nil {
		return mapVerbErr(err, errFrame)
	}
	return receipt(subjectgate.SubmitReceipt{MessageID: string(msgID), Seq: seq})
}

func interpretResolve(sys actorbase.Sys, deps humanDriverDeps, f subjectgate.Frame, receipt frameBuild, errFrame frameErr) subjectgate.Frame {
	var p subjectgate.ResolvePayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrame(subjectgate.CodeBadPayload, err.Error())
	}
	// decision闭集 BEFORE any log work — payload.decision becomes permanent truth.
	if p.Decision != "approved" && p.Decision != "rejected" {
		return errFrame(subjectgate.CodeInvalidDecision, "decision must be approved or rejected")
	}
	ctx := context.Background()
	reqID := message.ID(p.ReqID)
	req, ok, err := deps.requests.FindByID(ctx, reqID)
	if err != nil {
		return errFrame(subjectgate.CodeUnavailable, err.Error())
	}
	if !ok || req == nil {
		return errFrame(subjectgate.CodeRequestNotFound, "no such request")
	}
	if !req.Audience.Contains(deps.self) {
		return errFrame(subjectgate.CodeNotInAudience, "request not addressed to this subject")
	}
	open, err := deps.openCheck(ctx, deps.self, reqID)
	if err != nil {
		return errFrame(subjectgate.CodeUnavailable, err.Error())
	}
	if !open {
		return errFrame(subjectgate.CodeAlreadyClosed, "request already closed")
	}
	merged := map[string]any{}
	if len(p.Payload) > 0 {
		if derr := jsondepth.Bounded(p.Payload); derr != nil {
			return errFrame(subjectgate.CodeBadPayload, derr.Error())
		}
		if uerr := json.Unmarshal(p.Payload, &merged); uerr != nil {
			return errFrame(subjectgate.CodeBadPayload, uerr.Error())
		}
		if merged == nil { // JSON null → no payload (guard against nil-map panic).
			merged = map[string]any{}
		}
	}
	merged["decision"] = p.Decision
	raw, _ := json.Marshal(merged)
	if _, err := sys.RespondEnvelope(req, behavior.ResponseSpec{Status: message.StatusCompleted, Payload: raw}); err != nil {
		return mapVerbErr(err, errFrame)
	}
	return receipt(subjectgate.ResolveReceipt{ReqID: p.ReqID})
}

func interpretCancel(sys actorbase.Sys, deps humanDriverDeps, f subjectgate.Frame, receipt frameBuild, errFrame frameErr) subjectgate.Frame {
	var p subjectgate.CancelPayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrame(subjectgate.CodeBadPayload, err.Error())
	}
	ctx := context.Background()
	reqID := message.ID(p.ReqID)
	req, ok, err := deps.requests.FindByID(ctx, reqID)
	if err != nil {
		return errFrame(subjectgate.CodeUnavailable, err.Error())
	}
	if !ok || req == nil {
		return errFrame(subjectgate.CodeRequestNotFound, "no such request")
	}
	if req.Sender.ID != deps.self {
		return errFrame(subjectgate.CodeUnauthorizedSender, "only the sender may cancel")
	}
	var receiver actor.ActorID
	if len(req.Audience) > 0 {
		receiver = req.Audience[0]
	}
	open, err := deps.openCheck(ctx, receiver, reqID)
	if err != nil {
		return errFrame(subjectgate.CodeUnavailable, err.Error())
	}
	if !open {
		return errFrame(subjectgate.CodeAlreadyClosed, "request already closed")
	}
	cancelPayload, _ := json.Marshal(map[string]any{
		"error_code": string(message.TerminalUnansweredTimeout),
		"detail":     "cancelled by sender",
		"cancelled":  true,
	})
	if _, err := sys.RespondEnvelope(req, behavior.ResponseSpec{
		Status:  message.StatusFailed,
		Reason:  string(message.TerminalUnansweredTimeout),
		Payload: cancelPayload,
	}); err != nil {
		return mapVerbErr(err, errFrame)
	}
	// best-effort打断 hint (后送) — truth is already closed above.
	if receiver != "" && deps.cancelHint != nil {
		deps.cancelHint(receiver, reqID)
	}
	return receipt(subjectgate.CancelReceipt{ReqID: p.ReqID})
}

func interpretAfter(sys actorbase.Sys, f subjectgate.Frame, receipt frameBuild, errFrame frameErr) subjectgate.Frame {
	var p subjectgate.AfterPayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrame(subjectgate.CodeBadPayload, err.Error())
	}
	// Input bounds (期12 v0.4, migrated from the app edge to the driver — the error
	// vocabulary is the driver's): the schedule engine treats a past FireAt as "fire
	// now", so a non-positive / overflow duration would be a legal immediate trigger.
	// Refuse it as bad_payload (裁决8 平面词). No upper cap (abuse hardening is the
	// engine-level anti-storm axis, deferred).
	if p.DurationMs <= 0 || p.DurationMs > math.MaxInt64/int64(time.Millisecond) {
		return errFrame(subjectgate.CodeBadPayload, "duration_ms must be a positive millisecond count")
	}
	if p.MsgType == "" {
		return errFrame(subjectgate.CodeBadPayload, "msg_type required")
	}
	id, err := sys.AfterIdentity(durationMs(p.DurationMs), p.MsgType, p.Payload)
	if err != nil {
		return mapVerbErr(err, errFrame)
	}
	return receipt(subjectgate.AfterReceipt{TimerID: string(id)})
}

func interpretCancelTimer(sys actorbase.Sys, f subjectgate.Frame, receipt frameBuild, errFrame frameErr) subjectgate.Frame {
	var p subjectgate.CancelTimerPayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrame(subjectgate.CodeBadPayload, err.Error())
	}
	if err := sys.CancelTimerIdentity(scheduleTimerID(p.TimerID)); err != nil {
		return mapVerbErr(err, errFrame)
	}
	return receipt(subjectgate.CancelTimerReceipt{TimerID: p.TimerID})
}

func interpretResource(sys actorbase.Sys, f subjectgate.Frame, receipt frameBuild, errFrame frameErr) subjectgate.Frame {
	var p subjectgate.ResourcePayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrame(subjectgate.CodeBadPayload, err.Error())
	}
	rh := sys.ResourceIdentity()
	rid := resource.ResourceID(p.ResourceID)
	switch p.Op {
	case subjectgate.ResCreate:
		out, err := rh.Create(rid, p.Args)
		return resourceOutcomeFrame(out, err, receipt, errFrame)
	case subjectgate.ResRead:
		out, err := rh.Read(rid)
		return resourceOutcomeFrame(out, err, receipt, errFrame)
	case subjectgate.ResWrite:
		out, err := rh.Write(rid, p.Args)
		return resourceOutcomeFrame(out, err, receipt, errFrame)
	case subjectgate.ResDelete:
		out, err := rh.Delete(rid)
		return resourceOutcomeFrame(out, err, receipt, errFrame)
	case subjectgate.ResStat:
		st, err := rh.Stat(rid)
		if err != nil {
			return mapVerbErr(err, errFrame)
		}
		// exists = the resource resolved & is visible to this subject (a
		// not-found / denied verdict rides Reject, not a Go error — §3.9').
		meta, _ := json.Marshal(st.Meta)
		return receipt(subjectgate.ResourceStat{Exists: st.Reject == "", Meta: meta})
	case subjectgate.ResList:
		page, err := rh.List(listQueryOf(p.Query))
		if err != nil {
			return mapVerbErr(err, errFrame)
		}
		items := make([]json.RawMessage, 0, len(page.Entries))
		for _, it := range page.Entries {
			raw, _ := json.Marshal(it)
			items = append(items, raw)
		}
		return receipt(subjectgate.ResourcePage{Items: items, Next: page.Next})
	case subjectgate.ResShareActor:
		out, err := rh.ShareActor(rid, actor.ActorID(p.Target), operationsOf(p.Ops))
		return resourceOutcomeFrame(out, err, receipt, errFrame)
	case subjectgate.ResShareMembers:
		out, err := rh.ShareMembers(rid, operationsOf(p.Ops))
		return resourceOutcomeFrame(out, err, receipt, errFrame)
	default:
		return errFrame(subjectgate.CodeBadPayload, "unknown resource op: "+string(p.Op))
	}
}

func operationsOf(ops []string) []access.Operation {
	out := make([]access.Operation, 0, len(ops))
	for _, o := range ops {
		out = append(out, access.Operation(o))
	}
	return out
}

// isRequestOpen reports whether reqID is still an open request addressed to
// receiver — the truth-derived open-status check the frame interpreter's
// from-log five steps use ("仍 open"). A closed (terminal-answered) or unknown
// request is not open. (Relocated here from the removed HumanHandle door — the
// cell's own driver deps are its only consumer now.)
func (h *Home) isRequestOpen(ctx context.Context, receiver actor.ActorID, reqID message.ID) (bool, error) {
	if receiver == "" {
		return false, nil
	}
	rows, err := h.cs.Query.OpenRequestsForActor(ctx, receiver)
	if err != nil {
		return false, err
	}
	for _, r := range rows {
		if r.Envelope.ID == reqID {
			return true, nil
		}
	}
	return false, nil
}
