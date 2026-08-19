package sysactor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// unauthorizedSenderCode belongs to the sysactor transport gate. It is not an
// OperationErrorCode: rejected senders create no value-operation account.
const unauthorizedSenderCode = "unauthorized_sender"

// The channel operate face — the in-gate control plane (owner 2026-07-05 拍
// NP-1=c). Channel-scoped control actions (remove/restart/set-default/introduce
// a composition member) enter as a member's request (audience=[system]) rather
// than an out-of-band HTTP call to a Home face, so the whole action — request +
// terminal reply — lands in the log and is replayable, and the permission
// verdict has ONE authority (this gate).
//
// 防 ioctl 法条 (owner 2026-07-05 过堂): this verb table MUST NOT grow linearly.
// The three types are noun-CRUD on the channel composition (remove_actor=delete a
// composition row /
// introduce_actor=the add-OR-update UPSERT of a composition row, incl. its config
// field (改配置门, K2=a/S8: config is an existing FIELD on the noun, so 改配置 is
// CRUD-Update — NOT a new verb) / restart_actor=祈使残渣 foldable into an
// update-generation field). A new controllable state is a new field/row on the
// noun, NEVER a new hand-rolled verb (Slack's 400+ RPC = the reverse anti-pattern;
// Linux's closed verb set + open file-name noun = the pattern; ioctl = the escape
// hatch this law forbids).
const (
	TypeMemberCreate  = message.TypeSystemMemberCreate
	TypeMemberAdmit   = message.TypeSystemMemberAdmit
	TypeMemberDelete  = message.TypeSystemMemberDelete
	TypeMemberRestart = message.TypeSystemMemberRestart
)

// OperateRequest is the decoded delivery an OperateExecutor acts on: the gate
// has already authorised it (sender is an active member) and supplies the
// channel scope + the operator's id; the raw payload is left for the executor to
// decode (the payload schema is the executor's concern — the gate is kind-blind
// and payload-blind, it only routes the closed verb set and enforces membership).
type OperateRequest struct {
	ChannelID channel.ID
	Caller    harness.Caller
	Anchor    string
	Payload   json.RawMessage
}

// OperateError is an executor's typed failure carrying the {error_code, detail}
// shape sys.Fail speaks (behavior.Fail's terminal payload). An executor returns
// it to control the code the caller sees; any other error maps to internal_error.
type OperateError struct {
	Code   string
	Detail string
}

func (e *OperateError) Error() string { return e.Code + ": " + e.Detail }

// OperateExecutor executes one channel-scoped control action the gate has already
// authorised. It is the INJECTION-POINT CONTRACT (the process assembly fills it; the
// gate does permission + transport, the executor does the intent write + Home-face
// call). Each method returns a reply value on success or an error (an *OperateError
// to pick the code, else mapped to internal_error). nil executor = the injection
// point is unfilled; the gate does not synthesize (caller's closure reaps it).
type OperateExecutor interface {
	Execute(ctx context.Context, operation string, req OperateRequest) (any, error)
}

// handleOperate is the gate: permission (NP-2=a — sender is an active member of
// the unified authority, storage-home blind, so an agent member may be
// delegated channel management) then route to the injected executor, mapping
// its decision to Reply/Fail. Rejections are noise, not truth: they terminate
// as the request's failed reply and never touch the operation ledger — the
// cheapest deny point, so a rejected sender cannot grow any durable account by
// repeating garbage. Unfilled executor = no synthesis (same posture as an
// unrouted type).
func (s *SystemActor) handleOperate(sys actorbase.Sys, msg actorbase.Msg) {
	if s.operate == nil {
		return
	}
	caller := actorbase.EffectiveCaller(msg)
	authed, err := s.callerIsAuthorized(msg, caller)
	if err != nil {
		_, _ = sys.Fail(msg, "internal_error", err.Error())
		return
	}
	if !authed {
		s.logger.Info("sysactor.operate.refused", "type", msg.Type,
			"sender", string(caller.Actor), "code", unauthorizedSenderCode)
		_, _ = sys.Fail(msg, unauthorizedSenderCode, fmt.Sprintf("%q is not an active member of this channel, so it may not use the channel control words; check the roster with system.member.list", caller.Actor))
		return
	}
	req := OperateRequest{ChannelID: msg.ChannelID, Caller: caller, Anchor: string(msg.ID), Payload: msg.Payload}
	result, err := s.operate.Execute(msg.Ctx(), msg.Type, req)
	if err != nil {
		var oe *OperateError
		if errors.As(err, &oe) {
			// The refusal already lands in the channel log as this request's
			// failed terminal (replayable truth); this line is the OPS-side trace
			// — a storm of refused control actions must be visible in the server
			// log too, not only inside per-channel sqlite.
			s.logger.Info("sysactor.operate.refused", "type", msg.Type,
				"sender", string(msg.Sender.ID), "code", oe.Code)
			_, _ = sys.Fail(msg, oe.Code, oe.Detail)
			return
		}
		_, _ = sys.Fail(msg, "internal_error", err.Error())
		return
	}
	_, _ = sys.Reply(msg, result)
}

// senderIsActiveMember is the gate's permission predicate (NP-2=a) over the
// unified active-identity authority. Physical identity storage is unobservable
// here. An authority error is surfaced (internal_error), not silently read as
// unauthorized. The
// window between this check and the value commit is the system's standard
// in-flight tolerance (same doctrine as message delivery vs incarnation).
func (s *SystemActor) callerIsAuthorized(msg actorbase.Msg, caller harness.Caller) (bool, error) {
	if caller.Channel == channelspec.C0ChannelID {
		return true, nil
	}
	if caller.Channel != msg.ChannelID {
		return false, nil
	}
	if s.authority == nil {
		return false, nil
	}
	return s.authority.IsActive(msg.Ctx(), caller.Actor)
}
