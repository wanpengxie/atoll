package sysactor

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// The channel operate face — the in-gate control plane (owner 2026-07-05 拍
// NP-1=c). Channel-scoped control actions (remove/restart/set-default/introduce
// a composition member) enter as a member's request (audience=[system]) rather
// than an out-of-band HTTP call to a Home face, so the whole action — request +
// terminal reply — lands in the log and is replayable, and the permission
// verdict has ONE authority (this gate).
//
// 防 ioctl 法条 (owner 2026-07-05 过堂): this verb table MUST NOT grow linearly.
// The four types are noun-CRUD on the channel composition (remove_actor=delete a
// composition row / set_default_agent=update a config field / introduce_actor=
// the add half / restart_actor=祈使残渣 foldable into an update-generation field).
// A new controllable state is a new field/row on the noun, NEVER a new hand-rolled
// verb (Slack's 400+ RPC = the reverse anti-pattern; Linux's closed verb set +
// open file-name noun = the pattern; ioctl = the escape hatch this law forbids).
const (
	TypeIntroduceActor  = "channel.introduce_actor"
	TypeRemoveActor     = "channel.remove_actor"
	TypeRestartActor    = "channel.restart_actor"
	TypeSetDefaultAgent = "channel.set_default_agent"
)

// OperateRequest is the decoded delivery an OperateExecutor acts on: the gate
// has already authorised it (sender is an active member) and supplies the
// channel scope + the operator's id; the raw payload is left for the executor to
// decode (the payload schema is the executor's concern — the gate is kind-blind
// and payload-blind, it only routes the closed verb set and enforces membership).
type OperateRequest struct {
	ChannelID channel.ID
	Sender    actor.ActorID
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
// authorised. It is the INJECTION-POINT CONTRACT (the app assembly fills it; the
// gate does permission + transport, the executor does the intent write + Home-face
// call). Each method returns a reply value on success or an error (an *OperateError
// to pick the code, else mapped to internal_error). nil executor = the injection
// point is unfilled; the gate does not synthesize (caller's closure reaps it).
type OperateExecutor interface {
	Introduce(ctx context.Context, req OperateRequest) (any, error)
	Remove(ctx context.Context, req OperateRequest) (any, error)
	Restart(ctx context.Context, req OperateRequest) (any, error)
	SetDefaultAgent(ctx context.Context, req OperateRequest) (any, error)
}

// handleOperate is the gate: permission (NP-2=a — sender is an active member,
// kind-blind, so an agent member may be delegated channel management) then route
// to the injected executor, mapping its decision to Reply/Fail. Unfilled
// executor = no synthesis (same posture as an unrouted type).
func (s *SystemActor) handleOperate(sys actorbase.Sys, msg actorbase.Msg) {
	if s.operate == nil {
		return
	}
	authed, err := s.senderIsActiveMember(msg)
	if err != nil {
		_, _ = sys.Fail(msg, "internal_error", err.Error())
		return
	}
	if !authed {
		_, _ = sys.Fail(msg, "unauthorized_sender", "sender is not an active channel member")
		return
	}
	req := OperateRequest{ChannelID: msg.ChannelID, Sender: msg.Sender.ID, Payload: msg.Payload}
	var result any
	switch msg.Type {
	case TypeIntroduceActor:
		result, err = s.operate.Introduce(msg.Ctx(), req)
	case TypeRemoveActor:
		result, err = s.operate.Remove(msg.Ctx(), req)
	case TypeRestartActor:
		result, err = s.operate.Restart(msg.Ctx(), req)
	case TypeSetDefaultAgent:
		result, err = s.operate.SetDefaultAgent(msg.Ctx(), req)
	default:
		return
	}
	if err != nil {
		var oe *OperateError
		if errors.As(err, &oe) {
			_, _ = sys.Fail(msg, oe.Code, oe.Detail)
			return
		}
		_, _ = sys.Fail(msg, "internal_error", err.Error())
		return
	}
	_, _ = sys.Reply(msg, result)
}

// senderIsActiveMember is the gate's permission predicate (NP-2=a): the request
// sender must be an active membership row of this channel. kind-blind (an agent
// member is a legitimate delegate). A registry error is surfaced (internal_error),
// not silently read as unauthorized.
func (s *SystemActor) senderIsActiveMember(msg actorbase.Msg) (bool, error) {
	if s.registry == nil {
		return false, nil
	}
	rec, ok, err := s.registry.Lookup(msg.Ctx(), msg.Sender.ID)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return rec.IsActive(), nil
}
