package humancell

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/lib/jsondepth"
	"github.com/wanpengxie/atoll/platform/subjectgate"
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
	// typeHumanMessage is IMMEDIATE: a message to the human's inbox, answered
	// completed on receipt (durable delivery to the log IS the answer).
	// Unexported (platform-topology 批 T5b 裁决9②): not part of humancell's
	// five-name wiring seam (Deps/RequestLookup/InterpretFrames/HumanServe/
	// WirePresenceSelfReport) — the request-type literal is a private detail
	// of this package's own dispatch table, not a word a caller needs.
	typeHumanMessage = "human.message"
	// typeHumanApprove is DEFERRED: left OPEN until the person answers via the
	// door (the resolve frame). Closure is the sender's caller-scoped timer.
	// Unexported for the same reason as typeHumanMessage above.
	typeHumanApprove = "human.approve"
)

// WirePresenceSelfReport wires the cell's device-presence self-report against its
// slot (装配链 step③④; design §5.4). It observes edges by THIS incarnation's token
// so an old cell's摘除 (its token) can never unregister this one. The CURRENT value
// (if any) arrives as RegisterObserver's FIRST callback投递, under the slot lock and
// in-order with every subsequent edge (出生握手, §3.2 六轮 P0-2) — the cell no longer
// self-reads Snapshot (whose out-of-lock return value could逆序 behind a newer edge).
// Only positive edges are self-reported: a revocation (Live=false) is the容器 owner's
// 证词账清洁 (Forget/ForgetEpoch/epoch teardown, S4), NOT the cell's to retract via
// PublishObs. Returns the token the caller defers RemoveObserver on. (Extracted from
// runHumanCell for churn testing — gateway S6; behavior identical.)
func WirePresenceSelfReport(sys actorbase.Sys, slot *subjectgate.Slot) string {
	token := uuid.NewString()
	slot.RegisterObserver(token, func(u subjectgate.PresenceUpdate) {
		if u.Live {
			publishPresence(sys, u.Level)
		}
	})
	return token
}

func publishPresence(sys actorbase.Sys, level subjectgate.Level) {
	_ = sys.PublishObs(introspect.ObsDevicePresence, introspect.MarshalDevicePresence(level == subjectgate.LevelOnline))
}

// HumanServe is the human cell's mailbox serve loop: delivered requests route
// through the three-choice type table. Returning on a Recv error is the
// cooperative termination contract (spec §1.6).
func HumanServe(sys actorbase.Sys) error {
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
	case typeHumanMessage:
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
			typeHumanMessage: {Description: "immediate: delivered to the human's inbox, answered completed on receipt"},
			typeHumanApprove: {Description: "deferred: left open until the person answers via the door (resolve)"},
			// D9 default-deferred declaration: unrecognised types are accepted
			// deferred — the log is the inbox, a human is never unreachable.
			"*": {Description: "default-deferred: any unrecognised type is accepted and left open for the person (the log is the inbox)"},
		},
	}
}

// Deps is the frame interpreter's injected read-only face (五步核查经
// Deps 注入, sysactor Deps 形): the from-log request lookup + open check + the
// cancel-hint reach (Hooks.Canceller = Home.CancelRequest, factory-captured).
// No capability of its own — every write goes through the cell's own Sys verbs.
type Deps struct {
	Self       actor.ActorID
	Requests   RequestLookup
	OpenCheck  func(ctx context.Context, receiver actor.ActorID, reqID message.ID) (bool, error)
	CancelHint func(target actor.ActorID, requestID message.ID)
}

// RequestLookup is the from-log recovery seam (cs.Requests satisfies it).
type RequestLookup interface {
	FindByID(ctx context.Context, id message.ID) (*message.Envelope, bool, error)
}

// InterpretFrames is the frame interpreter goroutine (S1 纪律照 kimi): it
// consumes upstream frame jobs from the slot and answers each with a
// receipt-or-error frame. stop (closed by the Proc on serve-loop exit) is the
// 解阻/join edge. (slot is retained in the wiring seam signature though the
// interpreter no longer reaches into it — the client-visible binding-generation
// axis it re-verified against was整删, 连接模型勘误期.)
func InterpretFrames(sys actorbase.Sys, slot *subjectgate.Slot, deps Deps, frames <-chan subjectgate.Job, stop <-chan struct{}) {
	for {
		select {
		case job := <-frames:
			job.Reply(subjectgate.FrameResult{Frame: interpretFrame(sys, deps, job.Frame)})
		case <-stop:
			return
		}
	}
}

// interpretFrame drives one upstream business frame onto the cell's own caps and
// returns the receipt (or error) frame. attach is a gateway control frame (handled
// north of the cell) — it is unexpected here. The frame's channel_id (business
// frames carry a required one, 连接模型勘误期 v2) is the gateway's concern — this
// body only消费 the payload's action fields, never校验 channel归属.
//
// (The绑定世代提交守卫 was整删 with the client-visible binding axis, 连接模型勘误期 §3.3-c
// A案: there is no client generation to re-verify at the commit point — the write
// verb's落账 runs directly. Revocation is server-internal — the read pump's per-batch
// eligibility recheck + lease upper bound, north of this queue.)
func interpretFrame(sys actorbase.Sys, deps Deps, f subjectgate.Frame) subjectgate.Frame {
	switch f.Type {
	case subjectgate.FrameSubmit:
		return interpretSubmit(sys, f)
	case subjectgate.FrameResolve:
		return interpretResolve(sys, deps, f)
	case subjectgate.FrameCancel:
		return interpretCancel(sys, deps, f)
	case subjectgate.FrameAfter:
		return interpretAfter(sys, f)
	case subjectgate.FrameCancelTimer:
		return interpretCancelTimer(sys, f)
	case subjectgate.FrameResource:
		return interpretResource(sys, f)
	default:
		return errFrame(f, subjectgate.CodeBadPayload, "unexpected frame_type for a subject driver: "+string(f.Type))
	}
}

// errFrame builds an error frame for f (裁决8 平面词).
func errFrame(f subjectgate.Frame, code, detail string) subjectgate.Frame {
	fr, _ := subjectgate.NewFrame(subjectgate.FrameError, f.Ref, subjectgate.ErrorPayload{
		Frame: string(f.Type), Code: code, Detail: detail,
	})
	return fr
}

func receipt(f subjectgate.Frame, load any) subjectgate.Frame {
	fr, _ := subjectgate.NewFrame(subjectgate.FrameReceipt, f.Ref, load)
	return fr
}

// mapVerbErrFrame folds a Sys-verb error into an error frame for f.
func mapVerbErrFrame(err error, f subjectgate.Frame) subjectgate.Frame {
	return mapVerbErr(err, func(code, detail string) subjectgate.Frame {
		return errFrame(f, code, detail)
	})
}

type frameBuild func(load any) subjectgate.Frame
type frameErr func(code, detail string) subjectgate.Frame

func interpretSubmit(sys actorbase.Sys, f subjectgate.Frame) subjectgate.Frame {
	var p subjectgate.SubmitPayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrame(f, subjectgate.CodeBadPayload, err.Error())
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
	spec := behavior.SubjectWriteSpec{
		ID:         id,
		Type:       p.MsgType,
		Kind:       kind,
		Payload:    p.Payload,
		Audience:   aud,
		Visibility: message.Visibility(p.Visibility),
		ParentID:   message.ID(p.ParentID),
		ExpiresAt:  p.ExpiresAt, // additive透传 (v0.4.1); nil → harness default TTL
	}
	msgID, seq, err := sys.SubmitEnvelope(spec)
	if err != nil {
		return mapVerbErrFrame(err, f)
	}
	return receipt(f, subjectgate.SubmitReceipt{MessageID: string(msgID), Seq: seq})
}

func interpretResolve(sys actorbase.Sys, deps Deps, f subjectgate.Frame) subjectgate.Frame {
	prepErr := func(code, detail string) subjectgate.Frame { return errFrame(f, code, detail) }
	var p subjectgate.ResolvePayload
	if err := f.DecodePayload(&p); err != nil {
		return prepErr(subjectgate.CodeBadPayload, err.Error())
	}
	// decision闭集 BEFORE any log work — payload.decision becomes permanent truth.
	if p.Decision != "approved" && p.Decision != "rejected" {
		return prepErr(subjectgate.CodeInvalidDecision, "decision must be approved or rejected")
	}
	ctx := context.Background()
	reqID := message.ID(p.ReqID)
	req, ok, err := deps.Requests.FindByID(ctx, reqID)
	if err != nil {
		return prepErr(subjectgate.CodeUnavailable, err.Error())
	}
	if !ok || req == nil {
		return prepErr(subjectgate.CodeRequestNotFound, "no such request")
	}
	if !req.Audience.Contains(deps.Self) {
		return prepErr(subjectgate.CodeNotInAudience, "request not addressed to this subject")
	}
	open, err := deps.OpenCheck(ctx, deps.Self, reqID)
	if err != nil {
		return prepErr(subjectgate.CodeUnavailable, err.Error())
	}
	if !open {
		return prepErr(subjectgate.CodeAlreadyClosed, "request already closed")
	}
	merged := map[string]any{}
	if len(p.Payload) > 0 {
		if derr := jsondepth.Bounded(p.Payload); derr != nil {
			return prepErr(subjectgate.CodeBadPayload, derr.Error())
		}
		if uerr := json.Unmarshal(p.Payload, &merged); uerr != nil {
			return prepErr(subjectgate.CodeBadPayload, uerr.Error())
		}
		if merged == nil { // JSON null → no payload (guard against nil-map panic).
			merged = map[string]any{}
		}
	}
	merged["decision"] = p.Decision
	raw, _ := json.Marshal(merged)
	if _, err := sys.RespondEnvelope(req, behavior.ResponseSpec{Status: message.StatusCompleted, Payload: raw}); err != nil {
		return mapVerbErrFrame(err, f)
	}
	return receipt(f, subjectgate.ResolveReceipt{ReqID: p.ReqID})
}

func interpretCancel(sys actorbase.Sys, deps Deps, f subjectgate.Frame) subjectgate.Frame {
	prepErr := func(code, detail string) subjectgate.Frame { return errFrame(f, code, detail) }
	var p subjectgate.CancelPayload
	if err := f.DecodePayload(&p); err != nil {
		return prepErr(subjectgate.CodeBadPayload, err.Error())
	}
	ctx := context.Background()
	reqID := message.ID(p.ReqID)
	req, ok, err := deps.Requests.FindByID(ctx, reqID)
	if err != nil {
		return prepErr(subjectgate.CodeUnavailable, err.Error())
	}
	if !ok || req == nil {
		return prepErr(subjectgate.CodeRequestNotFound, "no such request")
	}
	if req.Sender.ID != deps.Self {
		return prepErr(subjectgate.CodeUnauthorizedSender, "only the sender may cancel")
	}
	var receiver actor.ActorID
	if len(req.Audience) > 0 {
		receiver = req.Audience[0]
	}
	open, err := deps.OpenCheck(ctx, receiver, reqID)
	if err != nil {
		return prepErr(subjectgate.CodeUnavailable, err.Error())
	}
	if !open {
		return prepErr(subjectgate.CodeAlreadyClosed, "request already closed")
	}
	cancelPayload, _ := json.Marshal(map[string]any{
		"error_code": string(message.TerminalUnansweredTimeout),
		"detail":     "cancelled by sender",
		"cancelled":  true,
	})
	out := func() subjectgate.Frame {
		if _, err := sys.RespondEnvelope(req, behavior.ResponseSpec{
			Status:  message.StatusFailed,
			Reason:  string(message.TerminalUnansweredTimeout),
			Payload: cancelPayload,
		}); err != nil {
			return mapVerbErrFrame(err, f)
		}
		return receipt(f, subjectgate.CancelReceipt{ReqID: p.ReqID})
	}()
	// best-effort打断 hint (后送) — fired ONLY once truth is actually closed (a
	// receipt); a verb-errored self-close sends no hint.
	if out.Type == subjectgate.FrameReceipt && receiver != "" && deps.CancelHint != nil {
		deps.CancelHint(receiver, reqID)
	}
	return out
}

func interpretAfter(sys actorbase.Sys, f subjectgate.Frame) subjectgate.Frame {
	prepErr := func(code, detail string) subjectgate.Frame { return errFrame(f, code, detail) }
	var p subjectgate.AfterPayload
	if err := f.DecodePayload(&p); err != nil {
		return prepErr(subjectgate.CodeBadPayload, err.Error())
	}
	// Input bounds (期12 v0.4, migrated from the app edge to the driver — the error
	// vocabulary is the driver's): the schedule engine treats a past FireAt as "fire
	// now", so a non-positive / overflow duration would be a legal immediate trigger.
	// Refuse it as bad_payload (裁决8 平面词). No upper cap (abuse hardening is the
	// engine-level anti-storm axis, deferred).
	if p.DurationMs <= 0 || p.DurationMs > math.MaxInt64/int64(time.Millisecond) {
		return prepErr(subjectgate.CodeBadPayload, "duration_ms must be a positive millisecond count")
	}
	if p.MsgType == "" {
		return prepErr(subjectgate.CodeBadPayload, "msg_type required")
	}
	id, err := sys.AfterIdentity(durationMs(p.DurationMs), p.MsgType, p.Payload)
	if err != nil {
		return mapVerbErrFrame(err, f)
	}
	return receipt(f, subjectgate.AfterReceipt{TimerID: string(id)})
}

func interpretCancelTimer(sys actorbase.Sys, f subjectgate.Frame) subjectgate.Frame {
	var p subjectgate.CancelTimerPayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrame(f, subjectgate.CodeBadPayload, err.Error())
	}
	if err := sys.CancelTimerIdentity(scheduleTimerID(p.TimerID)); err != nil {
		return mapVerbErrFrame(err, f)
	}
	return receipt(f, subjectgate.CancelTimerReceipt{TimerID: p.TimerID})
}

func interpretResource(sys actorbase.Sys, f subjectgate.Frame) subjectgate.Frame {
	var p subjectgate.ResourcePayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrame(f, subjectgate.CodeBadPayload, err.Error())
	}
	rh := sys.ResourceIdentity()
	rid := resource.ResourceID(p.ResourceID)
	switch p.Op {
	// --- write ops ---
	case subjectgate.ResCreate:
		out, err := rh.Create(rid, p.Args)
		return resourceOutcomeFrameFor(f, out, err)
	case subjectgate.ResWrite:
		out, err := rh.Write(rid, p.Args)
		return resourceOutcomeFrameFor(f, out, err)
	case subjectgate.ResDelete:
		out, err := rh.Delete(rid)
		return resourceOutcomeFrameFor(f, out, err)
	case subjectgate.ResShareActor:
		out, err := rh.ShareActor(rid, actor.ActorID(p.Target), operationsOf(p.Ops))
		return resourceOutcomeFrameFor(f, out, err)
	case subjectgate.ResShareMembers:
		out, err := rh.ShareMembers(rid, operationsOf(p.Ops))
		return resourceOutcomeFrameFor(f, out, err)
	// --- pure-read ops ---
	case subjectgate.ResRead:
		out, err := rh.Read(rid)
		return resourceOutcomeFrameFor(f, out, err)
	case subjectgate.ResStat:
		st, err := rh.Stat(rid)
		if err != nil {
			return mapVerbErrFrame(err, f)
		}
		// exists = the resource resolved & is visible to this subject (a
		// not-found / denied verdict rides Reject, not a Go error — §3.9').
		meta, _ := json.Marshal(st.Meta)
		return receipt(f, subjectgate.ResourceStat{Exists: st.Reject == "", Meta: meta})
	case subjectgate.ResList:
		page, err := rh.List(listQueryOf(p.Query))
		if err != nil {
			return mapVerbErrFrame(err, f)
		}
		items := make([]json.RawMessage, 0, len(page.Entries))
		for _, it := range page.Entries {
			raw, _ := json.Marshal(it)
			items = append(items, raw)
		}
		return receipt(f, subjectgate.ResourcePage{Items: items, Next: page.Next})
	default:
		return errFrame(f, subjectgate.CodeBadPayload, "unknown resource op: "+string(p.Op))
	}
}

func operationsOf(ops []string) []access.Operation {
	out := make([]access.Operation, 0, len(ops))
	for _, o := range ops {
		out = append(out, access.Operation(o))
	}
	return out
}
