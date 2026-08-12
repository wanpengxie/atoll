package humancell

import (
	"bytes"
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
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/schedule"
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

// Deps is the frame interpreter's injected read-only face. No capability of
// its own: every write goes through the cell's own Sys verbs.
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
// north of the cell) — it is unexpected here. For file resources the gateway
// derives the channel from the address; for kv it uses the required channel_id.
// This body consumes only the payload's action fields after that routing decision.
//
// (The绑定世代提交守卫 was整删 with the client-visible binding axis, 连接模型勘误期 §3.3-c
// A案: there is no client generation to re-verify at the commit point — the write
// verb's落账 runs directly. Revocation is server-internal — the read pump's per-batch
// eligibility recheck + lease upper bound, north of this queue.)
func interpretFrame(sys actorbase.Sys, deps Deps, f subjectgate.Frame) subjectgate.Frame {
	switch f.Type {
	case subjectgate.FrameSubmit:
		return interpretSubmit(sys, deps, f)
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
	return subjectgate.NewErrorFrame(f.Ref, string(f.Type), code, detail)
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

func interpretSubmit(sys actorbase.Sys, deps Deps, f subjectgate.Frame) subjectgate.Frame {
	var p subjectgate.SubmitPayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrame(f, subjectgate.CodeBadPayload, err.Error())
	}
	fingerprint, err := submitFingerprint(p)
	if err != nil {
		return errFrame(f, subjectgate.CodeBadPayload, "invalid submit payload: "+err.Error())
	}
	kind := message.Kind(p.Kind)
	if kind == "" {
		kind = message.KindRequest
	}
	// The kind whitelist is the INTERPRETER's now, not a verb's. Post writes
	// only requests and Emit only events, so no Sys verb can be handed a
	// kind=response any more — but `kind` here is still a client-supplied
	// string, and a subject hand-writing a response would forge closure around
	// the from-log five-step authorization the resolve frame exists for. An
	// unknown/refused kind is a permanently malformed frame, so it answers
	// bad_payload rather than the retryable unavailable the deleted verb-level
	// check used to fold into.
	if kind != message.KindRequest && kind != message.KindEvent {
		return errFrame(f, subjectgate.CodeBadPayload, "kind must be request or event; got "+string(kind))
	}
	id := message.ID(p.ID)
	if id == "" {
		id = message.ID(uuid.NewString())
	}
	aud := make(message.Audience, 0, len(p.Audience))
	for _, a := range p.Audience {
		aud = append(aud, actor.ActorID(a))
	}
	if kind == message.KindRequest && len(aud) == 0 {
		return errFrame(f, subjectgate.CodeRoutingUnavailable, "request must name at least one recipient")
	}
	// Two kinds, two verbs — the dispatch the deleted SubjectWriteSpec used to
	// hide behind one call. An event carries no deadline (nothing waits on it),
	// which is why ExpiresAt rides only the request arm.
	var msgID message.ID
	if kind == message.KindEvent {
		msgID, err = sys.Emit(behavior.EventSpec{
			ID:                id,
			Type:              p.MsgType,
			Payload:           p.Payload,
			Audience:          aud,
			Visibility:        message.Visibility(p.Visibility),
			ParentID:          message.ID(p.ParentID),
			ClientFingerprint: fingerprint,
		})
	} else {
		// Post, not Call/Submit: the person is not waiting on this goroutine,
		// so there is no caller obligation to register — and an absent
		// ExpiresAt must stay absent so the substrate stamps its own long TTL
		// (additive透传 v0.4.1), never a short caller-side default.
		msgID, err = sys.Post(behavior.RequestSpec{
			ID:                id,
			Type:              p.MsgType,
			Payload:           p.Payload,
			Audience:          aud,
			Visibility:        message.Visibility(p.Visibility),
			ParentID:          message.ID(p.ParentID),
			ExpiresAt:         p.ExpiresAt,
			ClientFingerprint: fingerprint,
		})
	}
	if err != nil {
		return mapVerbErrFrame(err, f)
	}
	return receipt(f, subjectgate.SubmitReceipt{MessageID: string(msgID)})
}

// submitFingerprint freezes exactly the client-owned semantic surface before
// harness normalization. Frame ref and message id are deliberately absent;
// channel_id is the other half of the store-scoped (channel_id,id) key. JSON
// is digested through RFC-8785/JCS so object key order and numeric spelling do
// not manufacture conflicts.
func submitFingerprint(p subjectgate.SubmitPayload) (string, error) {
	kind := p.Kind
	if kind == "" {
		kind = string(message.KindRequest)
	}
	visibility := p.Visibility
	if visibility == "" {
		visibility = string(message.VisibilityPublic)
	}
	var payload any = map[string]any{}
	if len(p.Payload) != 0 {
		dec := json.NewDecoder(bytes.NewReader(p.Payload))
		dec.UseNumber()
		if err := dec.Decode(&payload); err != nil {
			return "", err
		}
	}
	semantic := map[string]any{
		"msg_type": p.MsgType, "kind": kind, "payload": payload,
		"visibility": visibility, "parent_id": p.ParentID,
	}
	// A missing request audience is completed by the human membrane's live
	// routing policy and therefore is not client fingerprint material. A missing
	// event audience is the event's canonical pure-log shape.
	if len(p.Audience) != 0 {
		semantic["audience"] = p.Audience
	}
	if p.ExpiresAt != nil {
		semantic["expires_at_ms"] = *p.ExpiresAt
	}
	return channel.Digest(semantic)
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
	// The person holds no mailbox handle — the frame carried a bare req_id and
	// the envelope came back from the LOG, so the write's authority is the log
	// (actorbase.OriginLog). ctx is sys.Life(): the cell outlives the person
	// going offline, and a log-origin Msg promises no request scope anyway.
	//
	// Reply takes a Go VALUE and marshals it exactly once. Handing it the
	// already-marshalled []byte would make that second marshal encode the bytes
	// as a base64 JSON string — the decision would silently vanish from truth.
	// Pinned by TestResolvePayloadIsMarshalledExactlyOnce.
	msg := actorbase.NewMsg(actorbase.OriginLog, sys.Life(), *req)
	if _, err := sys.Reply(msg, merged); err != nil {
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
	// Same log-origin handle as resolve (see interpretResolve). Fail derives the
	// terminal reason from WHO is writing: this identity sent the request, so
	// the engine picks the caller's own word (unanswered_timeout) and stamps
	// cancelled:true into the payload itself. The three-field literal this
	// function used to hand-write is gone — one act, one producer.
	msg := actorbase.NewMsg(actorbase.OriginLog, sys.Life(), *req)
	out := func() subjectgate.Frame {
		if _, err := sys.Fail(msg, string(message.TerminalUnansweredTimeout), "cancelled by caller"); err != nil {
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
	// Input bounds (期12 v0.4, migrated from the old edge to the driver — the error
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
	// Durable home: a person's reminder must survive a Scheduler restart, which
	// is what the home parameter names (durability, not lifetime). p.Payload is
	// json.RawMessage, which After stores byte for byte — the person's composed
	// bytes reach the fired timer unrewritten, no []byte→base64 (the trap §7.1
	// names for resolve) and no whitespace compaction either.
	id, err := sys.After(durationMs(p.DurationMs), p.MsgType, p.Payload, schedule.TimerHomeDurable)
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
	if err := sys.CancelTimer(scheduleTimerID(p.TimerID)); err != nil {
		return mapVerbErrFrame(err, f)
	}
	return receipt(f, subjectgate.CancelTimerReceipt{TimerID: p.TimerID})
}

func interpretResource(sys actorbase.Sys, f subjectgate.Frame) subjectgate.Frame {
	var p subjectgate.ResourcePayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrame(f, subjectgate.CodeBadPayload, err.Error())
	}
	rh := sys.Resource()
	rid := resource.ResourceID(p.ResourceID)
	switch p.Op {
	// --- write ops ---
	case subjectgate.ResCreate:
		if p.Address != "" {
			out, err := rh.CreateFileDecided(resource.ResourceID(p.Address), p.WithContent)
			return resourceOutcomeFrameFor(f, out, err)
		}
		out, err := rh.Create(rid, p.Args)
		return resourceOutcomeFrameFor(f, out, err)
	case subjectgate.ResWrite:
		out, err := rh.Write(rid, p.Args)
		return resourceOutcomeFrameFor(f, out, err)
	case subjectgate.ResDelete:
		out, err := rh.Delete(rid)
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
