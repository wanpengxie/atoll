package actorbase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// drive.go implements actorrt.OccupantDriver on the engine — the off-process
// subject drive seam (期12 S2, 缝家族第四条). Every verb drives the engine's
// OWN welded caps (pen/sched/access minted only at buildCaps — P2 能力取用不
// 现铸): a drive call is the metatool-JobTable geometry (cross-goroutine
// synchronous call onto self-locking ledgers, never the mailbox).
//
// The engine's existing Sys verbs are untouched (红线6): Drive* are PARALLEL
// verbs for the off-process subject, not a re-plumb of submit/Emit/After —
// DriveWrite carries the full envelope surface (kind/visibility/parent/ID/
// deadline), enters NO call ledger and arms NO author#2 timer (deadline
// closure is the substrate expiry reaper's obligation, 义务归位 D3);
// DriveAfter binds BindIdentity (a subject's reminder is a promise that
// outlives incarnations) where Sys.After stays BindIncarnation.

// ErrOccupantNotReady is the engine-side occupant gate sentinel: a Drive*
// call landed before Start finished wiring lifeCtx (go-live precedes
// impl.Start) or after Stop began draining. The platform door maps it (with
// the three live-membrane sentinels) to its public ErrCellUnavailable —
// actorbase cannot name that error itself (it must not import platform).
var ErrOccupantNotReady = errors.New("actorbase: occupant not running")

// WriteRejected is DriveWrite's typed harness-reject carrier (期12 v0.4
// P0-1): Reason/Detail must cross the actorrt seam typed so the platform
// door can wrap them into its public WriteRejectedError without parsing
// strings. It is an error value, not a sentinel — match with errors.As.
type WriteRejected struct {
	Reason string
	Detail string
}

func (w *WriteRejected) Error() string {
	return fmt.Sprintf("actorbase: write rejected: %s (%s)", w.Reason, w.Detail)
}

var _ actorrt.OccupantDriver = (*engine)(nil)

// driveReady is the occupant-ready gate every Drive* entry runs first (期12
// S1 P0): actorrt's go-live (embodiments entry + live=true) precedes
// impl.Start, and lifeCtx is only assigned inside Start — a drive call in
// that window would pen.Write(nil)/WithoutCancel(nil) panic and race the
// lifeCtx write. The atomic occupant load doubles as the happens-before edge
// for reading lifeCtx (Start assigns lifeCtx BEFORE Store(occupantRunning)).
// Starting/Draining/Dead all refuse.
func (e *engine) driveReady() error {
	if occupantState(e.occupant.Load()) != occupantRunning {
		return ErrOccupantNotReady
	}
	return nil
}

// driveWriteKindAllowed pins DriveWrite's kind whitelist: request/event only.
// A response MUST go through DriveRespond (the door's from-log five-step
// authorization) — a subject hand-writing kind=response would forge closure
// around that enforcement.
func driveWriteKindAllowed(k message.Kind) bool {
	return k == message.KindRequest || k == message.KindEvent
}

func (e *engine) DriveWrite(spec actorrt.DriveWrite) (message.ID, int64, error) {
	if err := e.driveReady(); err != nil {
		return "", 0, err
	}
	if !driveWriteKindAllowed(spec.Kind) {
		return "", 0, fmt.Errorf("actorbase: drive write kind must be request or event; got %q", spec.Kind)
	}
	// Visibility: empty normalises to public (the harness's own contract,
	// step_normalize — every existing frame omits it); the whitelist rejects
	// only an EXPLICIT system/unknown value (a subject writing
	// visibility=system would dodge future read-side enforcement, 主题A A3).
	vis := spec.Visibility
	if vis == "" {
		vis = message.VisibilityPublic
	}
	if vis != message.VisibilityPublic && vis != message.VisibilityPrivate {
		return "", 0, fmt.Errorf("actorbase: drive write visibility must be public or private; got %q", spec.Visibility)
	}
	if spec.Kind == message.KindRequest && len(spec.Audience) == 0 {
		return "", 0, fmt.Errorf("actorbase: drive write request audience required")
	}
	env, err := behavior.BuildSubjectWrite(e.clockFn, behavior.SubjectWriteSpec{
		ID:         spec.ID,
		Type:       spec.Type,
		Kind:       spec.Kind,
		Payload:    spec.Payload,
		Audience:   message.Audience(spec.Audience),
		Visibility: vis,
		ParentID:   spec.ParentID,
		ExpiresAt:  spec.ExpiresAt,
	})
	if err != nil {
		return "", 0, err
	}
	// Straight pen path: NO call-ledger register, NO author#2 arm, no
	// ErrSelfCall — the subject is not waiting on this goroutine, and the
	// deadline's closure guarantee is the substrate reaper (harness stamps a
	// default TTL when none is declared, so nothing dangles).
	out, err := e.pen.Write(e.lifeCtx, env)
	if err != nil {
		return "", 0, err
	}
	if !out.Accepted() {
		return "", 0, &WriteRejected{Reason: string(out.RejectReason), Detail: out.RejectDetail}
	}
	return out.MessageID, out.Seq, nil
}

func (e *engine) DriveRespond(req *message.Envelope, spec actorrt.DriveRespond) (message.ID, error) {
	if err := e.driveReady(); err != nil {
		return "", err
	}
	if req == nil {
		return "", fmt.Errorf("actorbase: drive respond request required")
	}
	// WithoutCancel: the response is authored on the door's goroutine — a
	// concurrently-draining lifeCtx must not abort a write the live membrane
	// would still accept (the membrane, not the ctx, is the WHEN gate).
	id, err := behavior.Respond(context.WithoutCancel(e.lifeCtx), e.pen, e.clockFn, req, behavior.ResponseSpec{
		Status:  spec.Status,
		Reason:  spec.Reason,
		Payload: spec.Payload,
	})
	if err != nil {
		return "", err
	}
	// Serve-ledger close: an entry exists when this incarnation admitted the
	// request (prevents its own double-answer); no entry = zero action — the
	// ledger is a per-incarnation projection, the log holds the truth.
	e.serve.close(req.ID)
	return id, nil
}

func (e *engine) DriveAfter(d time.Duration, msgType string, payload []byte) (string, error) {
	if err := e.driveReady(); err != nil {
		return "", err
	}
	if e.sched == nil {
		return "", ErrUnsupported
	}
	// Same schedule engine as Sys.After — the ONE difference is the Bind
	// value (D7): an off-process subject's reminder is an identity-level
	// promise (survives restarts/deploys); Sys.After's incarnation bind is
	// the in-process self-wakeup. No subject-specific abuse limits here —
	// engine-wide hardening is the anti-storm axis, deferred.
	id, err := e.sched.Schedule(e.lifeCtx, schedule.ScheduleReq{
		Bind:    schedule.BindIdentity,
		FireAt:  e.clockFn().Add(d).UnixMilli(),
		Type:    msgType,
		Payload: payload,
	})
	if err != nil {
		return "", err
	}
	return string(id), nil
}

func (e *engine) DriveCancelTimer(id string) error {
	if err := e.driveReady(); err != nil {
		return err
	}
	if e.sched == nil {
		return ErrUnsupported
	}
	return e.sched.Cancel(e.lifeCtx, schedule.TimerID(id))
}

// driveResource returns the SAME adapter Sys.Resource() hands a Proc —
// literally the same e.access (the cell's liveResourceAccess membrane), so
// enforcement (membrane WHEN + door R) is one path with zero subject bypass.
func (e *engine) driveResource() resourceAdapter {
	return resourceAdapter{h: e.access, ctx: e.life}
}

func (e *engine) DriveResourceCreate(id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	if err := e.driveReady(); err != nil {
		return accessdoor.Outcome{}, err
	}
	return e.driveResource().Create(id, args)
}

func (e *engine) DriveResourceRead(id resource.ResourceID) (accessdoor.Outcome, error) {
	if err := e.driveReady(); err != nil {
		return accessdoor.Outcome{}, err
	}
	return e.driveResource().Read(id)
}

func (e *engine) DriveResourceWrite(id resource.ResourceID, args []byte) (accessdoor.Outcome, error) {
	if err := e.driveReady(); err != nil {
		return accessdoor.Outcome{}, err
	}
	return e.driveResource().Write(id, args)
}

func (e *engine) DriveResourceDelete(id resource.ResourceID) (accessdoor.Outcome, error) {
	if err := e.driveReady(); err != nil {
		return accessdoor.Outcome{}, err
	}
	return e.driveResource().Delete(id)
}

func (e *engine) DriveResourceStat(id resource.ResourceID) (accessdoor.StatResult, error) {
	if err := e.driveReady(); err != nil {
		return accessdoor.StatResult{}, err
	}
	return e.driveResource().Stat(id)
}

func (e *engine) DriveResourceList(q accessdoor.ListQuery) (accessdoor.ListPage, error) {
	if err := e.driveReady(); err != nil {
		return accessdoor.ListPage{}, err
	}
	return e.driveResource().List(q)
}

func (e *engine) DriveResourceShareActor(id resource.ResourceID, target actor.ActorID, ops []access.Operation) (accessdoor.Outcome, error) {
	if err := e.driveReady(); err != nil {
		return accessdoor.Outcome{}, err
	}
	return e.driveResource().ShareActor(id, target, ops)
}

func (e *engine) DriveResourceShareMembers(id resource.ResourceID, ops []access.Operation) (accessdoor.Outcome, error) {
	if err := e.driveReady(); err != nil {
		return accessdoor.Outcome{}, err
	}
	return e.driveResource().ShareMembers(id, ops)
}
