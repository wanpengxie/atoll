package sysactor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// The time axis has always been a CAPABILITY every actor holds (actorbase's
// After/CancelTimer, welded to the holder — any Go body can set its own alarm).
// What it lacked was a WORD: a callable ability needs an actor identity and an
// endpoint, not merely a handler, and a per-actor handle is by construction
// neither. Hence these four verbs live on the channel's system actor.
//
// The system actor is also the ONLY place they could live. ScheduleReq is
// structurally self-targeted (no target field; the author is welded at Mint),
// so an ordinary actor answering "set a timer for my caller" would schedule its
// OWN alarm. Only a kernel resident may mint a handle against another
// coordinate — through the same documented seam the remote ingress uses
// (`MintAuthority(IdentityAuthorityFor(id))`), where the coordinate is
// authenticated by the endpoint and never chosen by the caller. That is why the
// port below takes a subject: the door authenticates it, the composition root
// mints for it, and nothing in a payload can forge it.

// TimerSet is one resolved alarm request: an absolute instant, the message the
// alarm will say, and which storage home keeps it. The relative→absolute
// conversion happens at the door, so the port speaks only in instants.
type TimerSet struct {
	FireAt  int64
	Type    string
	Payload json.RawMessage
	Home    string
}

// TimerHandle names a freshly-armed alarm.
type TimerHandle struct {
	ID     string
	FireAt int64
}

// TimerInfo is one pending alarm as a list read answers it. Payload is absent
// by design: a list read must not become a content read.
type TimerInfo struct {
	ID        string
	Home      string
	FireAt    int64
	Type      string
	CreatedAt int64
}

// TimerPort executes one already-authorised alarm action for a subject the gate
// has authenticated. It is the INJECTION-POINT CONTRACT, the same shape as
// OperateExecutor: the gate does permission + routing, the injected port mints
// the subject's schedule handle and acts. nil port = the injection point is
// unfilled and the timer words do not synthesize (the caller's own closure
// reaps the request).
type TimerPort interface {
	Set(ctx context.Context, subject actor.ActorID, req TimerSet) (TimerHandle, error)
	Cancel(ctx context.Context, subject actor.ActorID, id string) (existed bool, err error)
	List(ctx context.Context, subject actor.ActorID) ([]TimerInfo, error)
}

const (
	timerHomeDurable = "durable"
	timerHomeMemory  = "memory"
)

type timerSetPayload struct {
	DurationMs int64           `json:"duration_ms,omitempty"`
	FireAt     int64           `json:"fire_at,omitempty"`
	MsgType    string          `json:"msg_type,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Home       string          `json:"home,omitempty"`
	Subject    string          `json:"subject,omitempty"`
}

type timerIDPayload struct {
	TimerID string `json:"timer_id,omitempty"`
	Subject string `json:"subject,omitempty"`
}

type timerResetPayload struct {
	TimerID    string `json:"timer_id,omitempty"`
	DurationMs int64  `json:"duration_ms,omitempty"`
	FireAt     int64  `json:"fire_at,omitempty"`
	Subject    string `json:"subject,omitempty"`
}

type timerListPayload struct {
	Subject string `json:"subject,omitempty"`
}

// handleTimer is the gate for all four verbs: authorise the sender, resolve the
// subject, hand the rest to the injected port.
func (s *SystemActor) handleTimer(sys actorbase.Sys, msg actorbase.Msg) {
	if s.timers == nil {
		return
	}
	caller := actorbase.EffectiveCaller(msg)
	authed, err := s.callerIsAuthorized(msg, caller)
	if err != nil {
		_, _ = sys.Fail(msg, "internal_error", err.Error())
		return
	}
	if !authed {
		_, _ = sys.Fail(msg, unauthorizedSenderCode, fmt.Sprintf("%q is not an active member of this channel, so it may not use the timer words; check the roster with system.member.list", caller.Actor))
		return
	}
	switch msg.Type {
	case message.TypeSystemTimerSet:
		s.timerSet(sys, msg)
	case message.TypeSystemTimerCancel:
		s.timerCancel(sys, msg)
	case message.TypeSystemTimerReset:
		s.timerReset(sys, msg)
	case message.TypeSystemTimerList:
		s.timerList(sys, msg)
	}
}

// timerSubject decides WHOSE alarm this is. The default is msg.Sender.ID — the
// harness-welded LOCAL identity, deliberately not EffectiveCaller: a
// cross-membrane request's effective caller lives in another channel and has no
// coordinate here to mint against (the same Initiator/Caller split the membrane
// control path already draws).
//
// Naming someone else is a read-only privilege. Listing another member's alarms
// is allowed — a channel is one permission boundary and a pending intent is not
// a secret within it — while SETTING one would be the power to make another
// actor wake up and work, which is a different grant than "may use timers" and
// is refused here until it is decided on its own terms.
func (s *SystemActor) timerSubject(msg actorbase.Msg, declared string, write bool) (actor.ActorID, *OperateError) {
	self := msg.Sender.ID
	if declared == "" || actor.ActorID(declared) == self {
		return self, nil
	}
	if write {
		return "", &OperateError{Code: "forbidden", Detail: "a timer may only be set, reset or cancelled for yourself; subject names another member"}
	}
	subject := actor.ActorID(declared)
	if s.authority == nil {
		return "", &OperateError{Code: "internal_error", Detail: "membership authority unavailable"}
	}
	active, err := s.authority.IsActive(msg.Ctx(), subject)
	if err != nil {
		return "", &OperateError{Code: "internal_error", Detail: err.Error()}
	}
	if !active {
		return "", &OperateError{Code: "actor_not_in_channel", Detail: fmt.Sprintf("%q is not an active member of this channel", declared)}
	}
	return subject, nil
}

// timerFireAt resolves the two accepted ways to say WHEN into one instant.
// Exactly one must be given: accepting both would leave the substrate to decide
// which the author meant.
func (s *SystemActor) timerFireAt(durationMs, fireAt int64) (int64, *OperateError) {
	switch {
	case durationMs != 0 && fireAt != 0:
		return 0, &OperateError{Code: "bad_payload", Detail: "give duration_ms or fire_at, not both"}
	case durationMs != 0:
		if durationMs < 0 || durationMs > math.MaxInt64/int64(time.Millisecond) {
			return 0, &OperateError{Code: "bad_payload", Detail: "duration_ms must be a positive millisecond count"}
		}
		return s.clock().Add(time.Duration(durationMs) * time.Millisecond).UnixMilli(), nil
	case fireAt > 0:
		return fireAt, nil
	default:
		return 0, &OperateError{Code: "bad_payload", Detail: "duration_ms or fire_at required"}
	}
}

func (s *SystemActor) timerSet(sys actorbase.Sys, msg actorbase.Msg) {
	var p timerSetPayload
	if err := actorbase.DecodeStrict(msg.Payload, &p); err != nil {
		_, _ = sys.Fail(msg, "bad_payload", err.Error())
		return
	}
	subject, opErr := s.timerSubject(msg, p.Subject, true)
	if opErr != nil {
		_, _ = sys.Fail(msg, opErr.Code, opErr.Detail)
		return
	}
	fireAt, opErr := s.timerFireAt(p.DurationMs, p.FireAt)
	if opErr != nil {
		_, _ = sys.Fail(msg, opErr.Code, opErr.Detail)
		return
	}
	if p.MsgType == "" {
		_, _ = sys.Fail(msg, "bad_payload", "msg_type required")
		return
	}
	home := p.Home
	switch home {
	case "":
		home = timerHomeDurable
	case timerHomeDurable, timerHomeMemory:
	default:
		_, _ = sys.Fail(msg, "bad_payload", fmt.Sprintf("home must be %q or %q", timerHomeDurable, timerHomeMemory))
		return
	}
	handle, err := s.timers.Set(msg.Ctx(), subject, TimerSet{
		FireAt: fireAt, Type: p.MsgType, Payload: p.Payload, Home: home,
	})
	if err != nil {
		s.failTimer(sys, msg, err)
		return
	}
	_, _ = sys.Reply(msg, map[string]any{
		"timer_id": handle.ID, "fire_at": handle.FireAt, "subject": string(subject),
	})
}

func (s *SystemActor) timerCancel(sys actorbase.Sys, msg actorbase.Msg) {
	var p timerIDPayload
	if err := actorbase.DecodeStrict(msg.Payload, &p); err != nil {
		_, _ = sys.Fail(msg, "bad_payload", err.Error())
		return
	}
	if p.TimerID == "" {
		_, _ = sys.Fail(msg, "bad_payload", "timer_id required")
		return
	}
	subject, opErr := s.timerSubject(msg, p.Subject, true)
	if opErr != nil {
		_, _ = sys.Fail(msg, opErr.Code, opErr.Detail)
		return
	}
	existed, err := s.timers.Cancel(msg.Ctx(), subject, p.TimerID)
	if err != nil {
		s.failTimer(sys, msg, err)
		return
	}
	// existed=false covers "already fired", "never existed" and "belongs to
	// someone else" alike — the store's own non-leaking verdict, passed through
	// rather than re-interpreted here.
	_, _ = sys.Reply(msg, map[string]any{"timer_id": p.TimerID, "existed": existed})
}

// timerReset is cancel + set. The store has no update, and inventing one would
// entangle "move an alarm" with "a fired alarm is not retractable"; returning a
// NEW id and naming the replaced one is the honest shape. A vanished id fails
// the whole word rather than silently becoming "armed a new one".
func (s *SystemActor) timerReset(sys actorbase.Sys, msg actorbase.Msg) {
	var p timerResetPayload
	if err := actorbase.DecodeStrict(msg.Payload, &p); err != nil {
		_, _ = sys.Fail(msg, "bad_payload", err.Error())
		return
	}
	if p.TimerID == "" {
		_, _ = sys.Fail(msg, "bad_payload", "timer_id required")
		return
	}
	subject, opErr := s.timerSubject(msg, p.Subject, true)
	if opErr != nil {
		_, _ = sys.Fail(msg, opErr.Code, opErr.Detail)
		return
	}
	fireAt, opErr := s.timerFireAt(p.DurationMs, p.FireAt)
	if opErr != nil {
		_, _ = sys.Fail(msg, opErr.Code, opErr.Detail)
		return
	}
	// The pending row carries what the alarm will SAY; reset moves only when it
	// says it, so the type and home are read back rather than re-supplied.
	pending, err := s.timers.List(msg.Ctx(), subject)
	if err != nil {
		s.failTimer(sys, msg, err)
		return
	}
	var found *TimerInfo
	for i := range pending {
		if pending[i].ID == p.TimerID {
			found = &pending[i]
			break
		}
	}
	if found == nil {
		_, _ = sys.Fail(msg, "timer_gone", fmt.Sprintf("timer %q is not pending for %q — it may have fired, been cancelled, or never existed", p.TimerID, subject))
		return
	}
	existed, err := s.timers.Cancel(msg.Ctx(), subject, p.TimerID)
	if err != nil {
		s.failTimer(sys, msg, err)
		return
	}
	if !existed {
		_, _ = sys.Fail(msg, "timer_gone", fmt.Sprintf("timer %q fired or was cancelled while being reset", p.TimerID))
		return
	}
	handle, err := s.timers.Set(msg.Ctx(), subject, TimerSet{
		FireAt: fireAt, Type: found.Type, Home: found.Home,
	})
	if err != nil {
		s.failTimer(sys, msg, err)
		return
	}
	_, _ = sys.Reply(msg, map[string]any{
		"timer_id": handle.ID, "fire_at": handle.FireAt,
		"replaced": p.TimerID, "subject": string(subject),
	})
}

func (s *SystemActor) timerList(sys actorbase.Sys, msg actorbase.Msg) {
	var p timerListPayload
	if err := actorbase.DecodeStrictEmpty(msg.Payload, &p); err != nil {
		_, _ = sys.Fail(msg, "bad_payload", err.Error())
		return
	}
	subject, opErr := s.timerSubject(msg, p.Subject, false)
	if opErr != nil {
		_, _ = sys.Fail(msg, opErr.Code, opErr.Detail)
		return
	}
	timers, err := s.timers.List(msg.Ctx(), subject)
	if err != nil {
		s.failTimer(sys, msg, err)
		return
	}
	rows := make([]map[string]any, 0, len(timers))
	for _, t := range timers {
		row := map[string]any{
			"timer_id": t.ID, "fire_at": t.FireAt, "msg_type": t.Type, "home": t.Home,
		}
		if t.CreatedAt > 0 {
			row["created_at"] = t.CreatedAt
		}
		rows = append(rows, row)
	}
	_, _ = sys.Reply(msg, map[string]any{"subject": string(subject), "timers": rows})
}

// failTimer maps a port error onto the request's failed terminal, honouring an
// *OperateError's chosen code the same way the operate gate does.
func (s *SystemActor) failTimer(sys actorbase.Sys, msg actorbase.Msg, err error) {
	var oe *OperateError
	if errors.As(err, &oe) {
		s.logger.Info("sysactor.timer.refused", "type", msg.Type, "sender", string(msg.Sender.ID), "code", oe.Code)
		_, _ = sys.Fail(msg, oe.Code, oe.Detail)
		return
	}
	_, _ = sys.Fail(msg, "internal_error", err.Error())
}
