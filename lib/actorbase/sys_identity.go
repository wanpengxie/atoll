package actorbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// sys_identity.go implements the identity-dimension Sys verbs (gateway 期 S1):
// four write/schedule verbs + one resource handle whose lifecycle promise is
//系于 IDENTITY (the log is truth — D1), not this incarnation's serve
// projection. They are the off-process subject's drive face relocated onto Sys
// as first-class verbs (design-v2 §5.2.3 A 案): ANY Proc may call them, and the
// human driver is only the first动词级 consumer — the same path closes agent's
// latent cross-incarnation gap (answering a log-open request after a re-cast).
//
// The gateway 期 S5 tore out the former actorrt.OccupantDriver seam (drive.go):
// these Sys methods are the permanent home of that logic. The shared gate
// helpers below (driveReady/driveCtx/driveWriteKindAllowed) and the typed
// WriteRejected carrier moved here with them.

// errOccupantNotReady is the engine-side occupant gate sentinel: an
// identity-verb call landed before Start finished wiring lifeCtx (go-live
// precedes impl.Start) or after Stop began draining. The subjectgate frame
// interpreter maps it (with the live-membrane sentinels) to the retryable
// unavailable code — actorbase cannot name a platform error itself.
var errOccupantNotReady = errors.New("actorbase: occupant not running")

// WriteRejected is the typed harness-reject carrier: Reason/Detail cross the
// verb boundary typed so the subjectgate interpreter can surface the reason as
// the flat error-frame code (裁决8) without parsing strings. It is an error
// value, not a sentinel — match with errors.As.
type WriteRejected struct {
	Reason string
	Detail string
}

func (w *WriteRejected) Error() string {
	return fmt.Sprintf("actorbase: write rejected: %s (%s)", w.Reason, w.Detail)
}

// driveCtx is the ctx every identity verb runs under: the engine's lifeCtx with
// its CANCELLATION detached (裁决 4 WithoutCancel). A verb call runs on the
// caller's goroutine — the cell tearing down mid-call must not abort a write the
// live membrane would still accept, and must never leak a raw "context
// canceled" through the boundary (the membrane sentinels are the honest WHEN
// verdict; ctx cancellation is the cell's own lifecycle). The occupant gate at
// every entry guarantees lifeCtx is non-nil here.
func (e *engine) driveCtx() context.Context {
	return context.WithoutCancel(e.lifeCtx)
}

// driveReady is the occupant-ready gate every identity verb runs first:
// actorrt's go-live publication precedes impl.Start, and
// lifeCtx is only assigned inside Start — a verb call in that window would
// pen.Write(nil)/WithoutCancel(nil) panic and race the lifeCtx write. The
// atomic occupant load doubles as the happens-before edge for reading lifeCtx
// (Start assigns lifeCtx BEFORE Store(occupantRunning)). Starting/Draining/Dead
// all refuse.
func (e *engine) driveReady() error {
	if occupantState(e.occupant.Load()) != occupantRunning {
		return errOccupantNotReady
	}
	return nil
}

// driveWriteKindAllowed pins SubmitEnvelope's kind whitelist: request/event
// only. A response MUST go through RespondEnvelope (from-log five-step
// authorization) — a subject hand-writing kind=response would forge closure
// around that enforcement.
func driveWriteKindAllowed(k message.Kind) bool {
	return k == message.KindRequest || k == message.KindEvent
}

// SubmitEnvelope commits a full off-process-subject envelope. It receives a
// SPEC, not a finished envelope: the engine builds and validates it (kind
// whitelist request/event, visibility empty→public with EXPLICIT system/unknown
// rejected, request audience required — drive.go:88-108 逐字). Returns
// (message id, harness seq, err). NO call-ledger register, NO author#2 arm: the
// subject is not waiting on this goroutine, and the deadline's closure is the
// substrate reaper's obligation (harness stamps a default TTL when none is
// declared, so nothing dangles).
func (e *engine) SubmitEnvelope(spec behavior.SubjectWriteSpec) (message.ID, int64, error) {
	if err := e.driveReady(); err != nil {
		return "", 0, err
	}
	if !driveWriteKindAllowed(spec.Kind) {
		return "", 0, fmt.Errorf("actorbase: submit envelope kind must be request or event; got %q", spec.Kind)
	}
	// Visibility: empty normalises to public (the harness's own contract — every
	// existing frame omits it); the whitelist rejects only an EXPLICIT
	// system/unknown value (a subject writing visibility=system would dodge
	// future read-side enforcement, 主题A A3).
	vis := spec.Visibility
	if vis == "" {
		vis = message.VisibilityPublic
	}
	if vis != message.VisibilityPublic && vis != message.VisibilityPrivate {
		return "", 0, fmt.Errorf("actorbase: submit envelope visibility must be public or private; got %q", spec.Visibility)
	}
	spec.Visibility = vis
	env, err := behavior.BuildSubjectWrite(e.clockFn, spec)
	if err != nil {
		return "", 0, err
	}
	out, err := e.pen.Write(e.driveCtx(), env)
	if err != nil {
		return "", 0, err
	}
	if !out.Accepted() {
		return "", 0, &WriteRejected{Reason: string(out.RejectReason), Detail: out.RejectDetail}
	}
	return out.MessageID, out.Seq, nil
}

// RespondEnvelope answers a request the caller holds in hand — recovered from
// the log, possibly by an incarnation that never Recv'd it (cross-incarnation
// response: response authority系于 identity, D1). A nil req is a defended error.
// The serve ledger closes IFF this incarnation admitted the request (its own
// double-answer guard); no entry = zero account action — the ledger is a
// per-incarnation projection, the log holds the truth.
func (e *engine) RespondEnvelope(req *message.Envelope, spec behavior.ResponseSpec) (message.ID, error) {
	if err := e.driveReady(); err != nil {
		return "", err
	}
	if req == nil {
		return "", fmt.Errorf("actorbase: respond envelope request required")
	}
	id, err := behavior.Respond(e.driveCtx(), e.pen, e.clockFn, req, spec)
	if err != nil {
		return "", err
	}
	e.serve.close(req.ID)
	return id, nil
}

// AfterIdentity arms an IDENTITY-bound durable timer (schedule.BindIdentity —
// an off-process subject's reminder is a promise that outlives incarnations).
// The Bind value is the ONE difference from Sys.After's BindIncarnation; same
// schedule engine, same WithoutCancel ctx. payload is json.RawMessage carried
// VERBATIM (裁决 6: never []byte→base64 through a marshal — a fired timer's
// recipient parses the same bytes the subject wrote).
func (e *engine) AfterIdentity(d time.Duration, msgType string, payload json.RawMessage) (schedule.TimerID, error) {
	if err := e.driveReady(); err != nil {
		return "", err
	}
	if e.sched == nil {
		return "", ErrUnsupported
	}
	return e.sched.Schedule(e.driveCtx(), schedule.ScheduleReq{
		Bind:    schedule.BindIdentity,
		FireAt:  e.clockFn().Add(d).UnixMilli(),
		Type:    msgType,
		Payload: payload,
	})
}

// CancelTimerIdentity cancels an identity-bound timer by id (ack-less race
// semantics as Sys.CancelTimer).
func (e *engine) CancelTimerIdentity(id schedule.TimerID) error {
	if err := e.driveReady(); err != nil {
		return err
	}
	if e.sched == nil {
		return ErrUnsupported
	}
	return e.sched.Cancel(e.driveCtx(), id)
}

// ResourceIdentity is Sys.Resource()'s WithoutCancel variant (裁决 4): the SAME
// access membrane (e.access, the cell's liveResourceAccess), driven under a ctx
// whose cancellation is detached from this incarnation's teardown — enforcement
// (membrane WHEN + door R) is one path with zero subject bypass.
func (e *engine) ResourceIdentity() ResourceHandle {
	return resourceAdapter{h: e.access, ctx: e.driveCtx}
}
