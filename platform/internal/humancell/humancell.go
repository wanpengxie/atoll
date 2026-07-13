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
// slot (装配链 step③④; design §5.4). It reads the current slot snapshot (step④) and
// self-reports its level — nothing published (unknown) → say nothing (fold 无行 =
// unknown 诚实默认) — then observes edges by THIS incarnation's token so an old
// cell's摘除 (its token) can never unregister this one. Only positive edges are
// self-reported: a revocation (Live=false) is the容器 owner's证词账清洁 (Forget/epoch
// teardown, S4), NOT the cell's to retract via PublishObs. Returns the token the
// caller defers RemoveObserver on. (Extracted from runHumanCell for churn testing —
// gateway S6; behavior identical.)
func WirePresenceSelfReport(sys actorbase.Sys, slot *subjectgate.Slot) string {
	token := uuid.NewString()
	if level, _, _, present := slot.Snapshot(); present {
		publishPresence(sys, level)
	}
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
// 解阻/join edge.
func InterpretFrames(sys actorbase.Sys, slot *subjectgate.Slot, deps Deps, frames <-chan subjectgate.Job, stop <-chan struct{}) {
	for {
		select {
		case job := <-frames:
			job.Reply(subjectgate.FrameResult{Frame: interpretFrame(sys, slot, deps, job.Frame, job.BindingGen)})
		case <-stop:
			return
		}
	}
}

// interpretFrame drives one upstream business frame onto the cell's own caps and
// returns the receipt (or error) frame. attach/detach/presence are gateway control
// frames (handled north of the cell) — they are unexpected here.
//
// carriedGen is the绑定世代 Deliver was invoked with, carried WITH the job. This is
// the真线性化点 of the双向世代 gate (design §5.4 / 修复批五轮): the gateway's upstream
// 初验 and Deliver's enqueue-time check are BOTH pre-queue — a rebind (seal→新臂→
// SetBinding) can land while the job sits in the帧递交端 queue, so the authoritative
// re-verification happens HERE, on the interpreter's commit path.
//
// 守卫圈收窄至单写 (修复批六轮 P1): each verb does ALL of its decode / 五步核查 DB reads /
// 资格查询 / pure-read resource ops / cancel 后送 hint OUTSIDE the绑定世代提交守卫. The guard
// (slot.WithBindingGuard, a shared RLock) wraps ONLY the动词's SINGLE truth-mutating
// call (see commitWrite), so a slow五步核查 or an unbounded DB query never freezes SetBinding
// — only the actual落账 does (one pen write, same exposure级 as any pen writer). The
// recheck↔落账 atomicity is preserved (WithBindingGuard rechecks + commits under the same
// RLock, 六轮 FIXED 本体): a rebind serializes either before this commit (recheck sees the
// new gen → refused, the frame never lands in the successor binding) or after it (SetBinding
// waits on the exclusive Lock only for the one write's duration). DeliverAnyGen (trusted
// platform-internal shim, no gateway binding behind it) skips the gen comparison but still
// commits under the guard.
func interpretFrame(sys actorbase.Sys, slot *subjectgate.Slot, deps Deps, f subjectgate.Frame, carriedGen int64) subjectgate.Frame {
	switch f.Type {
	case subjectgate.FrameSubmit:
		return interpretSubmit(sys, slot, f, carriedGen)
	case subjectgate.FrameResolve:
		return interpretResolve(sys, slot, deps, f, carriedGen)
	case subjectgate.FrameCancel:
		return interpretCancel(sys, slot, deps, f, carriedGen)
	case subjectgate.FrameAfter:
		return interpretAfter(sys, slot, f, carriedGen)
	case subjectgate.FrameCancelTimer:
		return interpretCancelTimer(sys, slot, f, carriedGen)
	case subjectgate.FrameResource:
		return interpretResource(sys, slot, f, carriedGen)
	default:
		return errFrameGen(slot.BindingGen(), f, subjectgate.CodeBadPayload, "unexpected frame_type for a subject driver: "+string(f.Type))
	}
}

// commitWrite runs a write verb's SINGLE truth-mutating call inside the绑定世代提交守卫's
// narrow window: WithBindingGuard rechecks carriedGen against the slot's current层2世代
// under the shared RLock and runs build ONLY if the gen still matches, so no rebind
// (SetBinding, exclusive Lock) can insert between the recheck and this one write
// (复核↔落账 atomicity, 六轮 FIXED 本体). build receives the guard's gen and returns the
// receipt / verb-error frame stamped with it. A rebind that advanced the gen first →
// build never runs, stale_binding is returned. This one call is the ONLY thing under the
// guard — all decode / 五步核查 / 资格查询 / 纯读 stay OUTSIDE it (P1: 守卫圈收窄至单写).
func commitWrite(slot *subjectgate.Slot, carriedGen int64, f subjectgate.Frame, build func(gen int64) subjectgate.Frame) subjectgate.Frame {
	var out subjectgate.Frame
	gen, ok := slot.WithBindingGuard(carriedGen, func(gen int64) {
		out = build(gen)
	})
	if !ok {
		// 复核不符: a rebind advanced the绑定世代 before this frame could commit. gen is
		// the current (post-rebind) generation read under the guard. The frame NEVER lands.
		return staleBindingFrame(gen, f)
	}
	return out
}

// freshReadGen is the守卫外 advisory gen compare for PURE-READ resource verbs
// (read/stat/list). A read never mutates truth, so a rebind landing right after this
// compare exposes only a stale READ (advisory) — never a write into a superseded
// binding — so these verbs stay OUT of the绑定世代提交守卫 entirely (P1: 守卫圈收窄至单写).
// DeliverAnyGen is exempt. ok=false → an advisory stale_binding refusal.
func freshReadGen(slot *subjectgate.Slot, carriedGen int64) (gen int64, ok bool) {
	gen = slot.BindingGen()
	if carriedGen != subjectgate.DeliverAnyGen && carriedGen != gen {
		return gen, false
	}
	return gen, true
}

func staleBindingFrame(gen int64, f subjectgate.Frame) subjectgate.Frame {
	fr, _ := subjectgate.NewFrame(subjectgate.FrameError, gen, f.Ref, subjectgate.ErrorPayload{
		Frame: string(f.Type), Code: subjectgate.CodeStaleBinding, Detail: "binding superseded before commit (rebound)",
	})
	return fr
}

// errFrameGen builds an error frame stamped with gen. Prep-time (守卫外) errors read the
// slot's current gen — the exact value is advisory since an error never lands in truth.
func errFrameGen(gen int64, f subjectgate.Frame, code, detail string) subjectgate.Frame {
	fr, _ := subjectgate.NewFrame(subjectgate.FrameError, gen, f.Ref, subjectgate.ErrorPayload{
		Frame: string(f.Type), Code: code, Detail: detail,
	})
	return fr
}

func receiptGen(gen int64, f subjectgate.Frame, load any) subjectgate.Frame {
	fr, _ := subjectgate.NewFrame(subjectgate.FrameReceipt, gen, f.Ref, load)
	return fr
}

// mapVerbErrGen folds a Sys-verb error into an error frame stamped with the guard's gen.
func mapVerbErrGen(err error, gen int64, f subjectgate.Frame) subjectgate.Frame {
	return mapVerbErr(err, func(code, detail string) subjectgate.Frame {
		return errFrameGen(gen, f, code, detail)
	})
}

type frameBuild func(load any) subjectgate.Frame
type frameErr func(code, detail string) subjectgate.Frame

func interpretSubmit(sys actorbase.Sys, slot *subjectgate.Slot, f subjectgate.Frame, carriedGen int64) subjectgate.Frame {
	var p subjectgate.SubmitPayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrameGen(slot.BindingGen(), f, subjectgate.CodeBadPayload, err.Error())
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
	// 单写落账 under the guard; the spec assembly above is守卫外.
	return commitWrite(slot, carriedGen, f, func(gen int64) subjectgate.Frame {
		msgID, seq, err := sys.SubmitEnvelope(spec)
		if err != nil {
			return mapVerbErrGen(err, gen, f)
		}
		return receiptGen(gen, f, subjectgate.SubmitReceipt{MessageID: string(msgID), Seq: seq})
	})
}

func interpretResolve(sys actorbase.Sys, slot *subjectgate.Slot, deps Deps, f subjectgate.Frame, carriedGen int64) subjectgate.Frame {
	prepErr := func(code, detail string) subjectgate.Frame { return errFrameGen(slot.BindingGen(), f, code, detail) }
	var p subjectgate.ResolvePayload
	if err := f.DecodePayload(&p); err != nil {
		return prepErr(subjectgate.CodeBadPayload, err.Error())
	}
	// decision闭集 BEFORE any log work — payload.decision becomes permanent truth.
	if p.Decision != "approved" && p.Decision != "rejected" {
		return prepErr(subjectgate.CodeInvalidDecision, "decision must be approved or rejected")
	}
	// 五步核查 (all reads) run OUTSIDE the guard (P1: 守卫圈收窄至单写) — a slow/unbounded
	// FindByID or openCheck must never freeze SetBinding.
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
	// 单写落账 under the guard.
	return commitWrite(slot, carriedGen, f, func(gen int64) subjectgate.Frame {
		if _, err := sys.RespondEnvelope(req, behavior.ResponseSpec{Status: message.StatusCompleted, Payload: raw}); err != nil {
			return mapVerbErrGen(err, gen, f)
		}
		return receiptGen(gen, f, subjectgate.ResolveReceipt{ReqID: p.ReqID})
	})
}

func interpretCancel(sys actorbase.Sys, slot *subjectgate.Slot, deps Deps, f subjectgate.Frame, carriedGen int64) subjectgate.Frame {
	prepErr := func(code, detail string) subjectgate.Frame { return errFrameGen(slot.BindingGen(), f, code, detail) }
	var p subjectgate.CancelPayload
	if err := f.DecodePayload(&p); err != nil {
		return prepErr(subjectgate.CodeBadPayload, err.Error())
	}
	// 核查 (all reads) OUTSIDE the guard (P1).
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
	// 单写自闭落账 under the guard.
	out := commitWrite(slot, carriedGen, f, func(gen int64) subjectgate.Frame {
		if _, err := sys.RespondEnvelope(req, behavior.ResponseSpec{
			Status:  message.StatusFailed,
			Reason:  string(message.TerminalUnansweredTimeout),
			Payload: cancelPayload,
		}); err != nil {
			return mapVerbErrGen(err, gen, f)
		}
		return receiptGen(gen, f, subjectgate.CancelReceipt{ReqID: p.ReqID})
	})
	// best-effort打断 hint (后送) OUTSIDE the guard — fired ONLY once truth is actually
	// closed (a receipt); a stale/refused or verb-errored commit sends no hint.
	if out.Type == subjectgate.FrameReceipt && receiver != "" && deps.CancelHint != nil {
		deps.CancelHint(receiver, reqID)
	}
	return out
}

func interpretAfter(sys actorbase.Sys, slot *subjectgate.Slot, f subjectgate.Frame, carriedGen int64) subjectgate.Frame {
	prepErr := func(code, detail string) subjectgate.Frame { return errFrameGen(slot.BindingGen(), f, code, detail) }
	var p subjectgate.AfterPayload
	if err := f.DecodePayload(&p); err != nil {
		return prepErr(subjectgate.CodeBadPayload, err.Error())
	}
	// Input bounds (期12 v0.4, migrated from the app edge to the driver — the error
	// vocabulary is the driver's): the schedule engine treats a past FireAt as "fire
	// now", so a non-positive / overflow duration would be a legal immediate trigger.
	// Refuse it as bad_payload (裁决8 平面词). No upper cap (abuse hardening is the
	// engine-level anti-storm axis, deferred). These bounds are守卫外 (P1).
	if p.DurationMs <= 0 || p.DurationMs > math.MaxInt64/int64(time.Millisecond) {
		return prepErr(subjectgate.CodeBadPayload, "duration_ms must be a positive millisecond count")
	}
	if p.MsgType == "" {
		return prepErr(subjectgate.CodeBadPayload, "msg_type required")
	}
	// 单写落账 under the guard.
	return commitWrite(slot, carriedGen, f, func(gen int64) subjectgate.Frame {
		id, err := sys.AfterIdentity(durationMs(p.DurationMs), p.MsgType, p.Payload)
		if err != nil {
			return mapVerbErrGen(err, gen, f)
		}
		return receiptGen(gen, f, subjectgate.AfterReceipt{TimerID: string(id)})
	})
}

func interpretCancelTimer(sys actorbase.Sys, slot *subjectgate.Slot, f subjectgate.Frame, carriedGen int64) subjectgate.Frame {
	var p subjectgate.CancelTimerPayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrameGen(slot.BindingGen(), f, subjectgate.CodeBadPayload, err.Error())
	}
	// 单写落账 under the guard.
	return commitWrite(slot, carriedGen, f, func(gen int64) subjectgate.Frame {
		if err := sys.CancelTimerIdentity(scheduleTimerID(p.TimerID)); err != nil {
			return mapVerbErrGen(err, gen, f)
		}
		return receiptGen(gen, f, subjectgate.CancelTimerReceipt{TimerID: p.TimerID})
	})
}

func interpretResource(sys actorbase.Sys, slot *subjectgate.Slot, f subjectgate.Frame, carriedGen int64) subjectgate.Frame {
	var p subjectgate.ResourcePayload
	if err := f.DecodePayload(&p); err != nil {
		return errFrameGen(slot.BindingGen(), f, subjectgate.CodeBadPayload, err.Error())
	}
	rh := sys.ResourceIdentity()
	rid := resource.ResourceID(p.ResourceID)
	switch p.Op {
	// --- write ops: the SINGLE truth-mutating call under the绑定世代提交守卫 (P1) ---
	case subjectgate.ResCreate:
		return commitWrite(slot, carriedGen, f, func(gen int64) subjectgate.Frame {
			out, err := rh.Create(rid, p.Args)
			return resourceOutcomeFrameGen(gen, f, out, err)
		})
	case subjectgate.ResWrite:
		return commitWrite(slot, carriedGen, f, func(gen int64) subjectgate.Frame {
			out, err := rh.Write(rid, p.Args)
			return resourceOutcomeFrameGen(gen, f, out, err)
		})
	case subjectgate.ResDelete:
		return commitWrite(slot, carriedGen, f, func(gen int64) subjectgate.Frame {
			out, err := rh.Delete(rid)
			return resourceOutcomeFrameGen(gen, f, out, err)
		})
	case subjectgate.ResShareActor:
		return commitWrite(slot, carriedGen, f, func(gen int64) subjectgate.Frame {
			out, err := rh.ShareActor(rid, actor.ActorID(p.Target), operationsOf(p.Ops))
			return resourceOutcomeFrameGen(gen, f, out, err)
		})
	case subjectgate.ResShareMembers:
		return commitWrite(slot, carriedGen, f, func(gen int64) subjectgate.Frame {
			out, err := rh.ShareMembers(rid, operationsOf(p.Ops))
			return resourceOutcomeFrameGen(gen, f, out, err)
		})
	// --- pure-read ops: NO guard, only a守卫外 advisory gen compare (read不改真相, P1) ---
	case subjectgate.ResRead:
		gen, ok := freshReadGen(slot, carriedGen)
		if !ok {
			return staleBindingFrame(gen, f)
		}
		out, err := rh.Read(rid)
		return resourceOutcomeFrameGen(gen, f, out, err)
	case subjectgate.ResStat:
		gen, ok := freshReadGen(slot, carriedGen)
		if !ok {
			return staleBindingFrame(gen, f)
		}
		st, err := rh.Stat(rid)
		if err != nil {
			return mapVerbErrGen(err, gen, f)
		}
		// exists = the resource resolved & is visible to this subject (a
		// not-found / denied verdict rides Reject, not a Go error — §3.9').
		meta, _ := json.Marshal(st.Meta)
		return receiptGen(gen, f, subjectgate.ResourceStat{Exists: st.Reject == "", Meta: meta})
	case subjectgate.ResList:
		gen, ok := freshReadGen(slot, carriedGen)
		if !ok {
			return staleBindingFrame(gen, f)
		}
		page, err := rh.List(listQueryOf(p.Query))
		if err != nil {
			return mapVerbErrGen(err, gen, f)
		}
		items := make([]json.RawMessage, 0, len(page.Entries))
		for _, it := range page.Entries {
			raw, _ := json.Marshal(it)
			items = append(items, raw)
		}
		return receiptGen(gen, f, subjectgate.ResourcePage{Items: items, Next: page.Next})
	default:
		return errFrameGen(slot.BindingGen(), f, subjectgate.CodeBadPayload, "unknown resource op: "+string(p.Op))
	}
}

func operationsOf(ops []string) []access.Operation {
	out := make([]access.Operation, 0, len(ops))
	for _, o := range ops {
		out = append(out, access.Operation(o))
	}
	return out
}
