package remoteingress

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

var (
	// ErrInvalidInput is the assembly fail-fast: an ingress missing a door
	// would fail at the first frame instead of at construction.
	ErrInvalidInput = errors.New("remoteingress: invalid construction input")

	// ErrInvalidRequest is a structurally unacceptable request — a malformed
	// frame that never reaches a verdict. It is not a verdict word: a denial
	// speaks the organ's own vocabulary (actorctl.ErrInactive /
	// ErrStaleAttempt), identical to the local path's.
	ErrInvalidRequest = errors.New("remoteingress: invalid request")
)

// RemoteIngress is the ONE closed interface an authenticated remote endpoint
// calls. The endpoint supplies the coordinate (id, and where the arm is
// run-level, the attempt key) from its own authenticated stream — never from a
// frame — and the payload from the frame.
//
// The permission matrix is welded into the arms and cannot be re-decided by a
// caller: pen A/G, channel resource access A/G, state A, schedule A (its
// signature carries no key at all), Fork/EndSelf A/G inside the Controller's
// own write door.
type RemoteIngress interface {
	Emit(context.Context, actor.ActorID, actorhost.AttemptKey, *message.Envelope) (harness.WriteResult, error)
	Access(context.Context, actor.ActorID, actorhost.AttemptKey, AccessRequest) (AccessResponse, error)
	Schedule(context.Context, actor.ActorID, ScheduleRequest) (ScheduleResponse, error)
	Fork(context.Context, actor.ActorID, actorhost.AttemptKey, ForkRequest) (actor.ActorID, error)
	EndSelf(context.Context, actor.ActorID, actorhost.AttemptKey, actorcaps.EndSelfRequest) error
}

// Controller is the value ledger's face: the narrow remote admission questions
// and the two typed self-lifecycle commands. Fork/EndSelf go through the same
// completed command face the local Lifecycle arm uses, so a remote self-command
// settles exactly like a local one, tail included.
type Controller interface {
	AdmitRun(actor.ActorID, actorhost.AttemptKey) (actorctl.RunAdmission, error)
	RunAuthorityFor(actor.ActorID, actorhost.AttemptKey) actorctl.RunAuthority
	IdentityAuthorityFor(actor.ActorID) actorctl.IdentityAuthority
	Fork(context.Context, actorctl.ForkRequest) (actorctl.ForkResult, error)
	End(context.Context, actorctl.EndRequest) (actorctl.EndResult, error)
}

// The four organ doors. Each is the organ's OWN authority-shaped mint face —
// building one of these shells costs about what passing the arguments costs,
// and the shell drives the organ's single admitted execution chain. That is
// the whole point: the remote path executes the local path's code.
type (
	PenDoor interface {
		MintAuthority(capauth.Authority, actor.Kind) harness.Pen
	}
	ResourceDoor interface {
		MintAuthority(capauth.Authority) accessdoor.ResourceAccessHandle
	}
	StateDoor interface {
		StateIngress(context.Context, capauth.Authority, accessdoor.StateOp) (accessdoor.Outcome, error)
	}
	ScheduleDoor interface {
		MintAuthority(capauth.Authority) schedule.ScheduleHandle
	}
)

// AccessKind selects which door verb a request drives.
type AccessKind string

const (
	AccessInvoke AccessKind = "invoke"
	AccessCreate AccessKind = "create"
	AccessStat   AccessKind = "stat"
	AccessList   AccessKind = "list"
)

// AccessScope splits the two loci an Invoke can address: the channel-scoped
// resource tree (A/G) and the actor-scoped state branch (A). It is meaningful
// on Invoke alone — Create/Stat/List are structurally channel-scoped.
type AccessScope string

const (
	ScopeChannel AccessScope = "channel"
	ScopeState   AccessScope = "state"
)

// AccessRequest is one decoded access operation. It carries no caller field:
// the caller is the endpoint's authenticated coordinate, welded by the arm, so
// there is structurally nothing to self-report.
type AccessRequest struct {
	Kind  AccessKind
	Scope AccessScope

	Operation access.Operation
	Resource  resource.ResourceID
	Args      []byte
	Grant     *access.Grant

	Spec    accessdoor.CreateSpec
	Initial []byte

	List accessdoor.ListQuery
}

// AccessResponse carries whichever product the driven verb returns.
type AccessResponse struct {
	Outcome accessdoor.Outcome
	Stat    accessdoor.StatResult
	List    accessdoor.ListPage
}

// ScheduleMethod names which ScheduleHandle verb a request drives.
type ScheduleMethod string

const (
	ScheduleSet    ScheduleMethod = "schedule"
	ScheduleCancel ScheduleMethod = "cancel"
	ScheduleAck    ScheduleMethod = "ack"
)

type ScheduleRequest struct {
	Method ScheduleMethod
	Req    schedule.ScheduleReq
	ID     schedule.TimerID
}

type ScheduleResponse struct {
	ID schedule.TimerID
}

// ForkRequest is the remote self-fork operand. RequestID is the caller's own
// idempotency coordinate, the one command that has one.
type ForkRequest struct {
	RequestID message.ID
	Spec      actorcaps.ForkSpec
}

type ingress struct {
	controller Controller
	pen        PenDoor
	access     ResourceDoor
	state      StateDoor
	schedule   ScheduleDoor
}

// New builds one Channel's ingress. Its parameters are the Controller and the
// four organ doors — nothing else: no channel id, no actor, no state of its
// own. One instance serves every remote body of that Channel and, holding its
// Channel's own doors, cannot be used across Channels.
func New(
	controller Controller,
	pen PenDoor,
	access ResourceDoor,
	state StateDoor,
	scheduleDoor ScheduleDoor,
) (RemoteIngress, error) {
	if controller == nil || pen == nil || access == nil ||
		state == nil || scheduleDoor == nil {
		return nil, ErrInvalidInput
	}
	return &ingress{
		controller: controller, pen: pen, access: access,
		state: state, schedule: scheduleDoor,
	}, nil
}

// Emit is the pen arm (A/G). AdmitRun is one ledger read lock: it settles the
// verdict AND yields Kind — the message-protocol field the pen welds — from a
// single snapshot, because stitching "current?" and "which kind?" out of two
// snapshots is a race by construction. The welded shell then drives the very
// same chain a local body's pen drives, re-admitting on the write.
func (i *ingress) Emit(
	ctx context.Context,
	id actor.ActorID,
	attempt actorhost.AttemptKey,
	env *message.Envelope,
) (harness.WriteResult, error) {
	admission, err := i.controller.AdmitRun(id, attempt)
	if err != nil {
		return harness.WriteResult{}, err
	}
	return i.pen.MintAuthority(admission.Run, admission.Kind).Write(ctx, env)
}

// Access is the resource arm. Channel-scoped work is A/G — acting as the
// current term; the actor-scoped state branch is A — an identity's own
// belongings, which survive a change of term. The state branch enters the state
// organ's per-call door, which routes to the record's backing itself.
func (i *ingress) Access(
	ctx context.Context,
	id actor.ActorID,
	attempt actorhost.AttemptKey,
	request AccessRequest,
) (AccessResponse, error) {
	if request.Kind == AccessInvoke && request.Scope == ScopeState {
		outcome, err := i.state.StateIngress(
			ctx,
			i.controller.IdentityAuthorityFor(id),
			accessdoor.StateOp{
				Operation: request.Operation, Resource: request.Resource,
				Args: request.Args, Grant: request.Grant,
			},
		)
		return AccessResponse{Outcome: outcome}, err
	}

	handle := i.access.MintAuthority(i.controller.RunAuthorityFor(id, attempt))
	switch request.Kind {
	case AccessInvoke:
		if request.Scope != "" && request.Scope != ScopeChannel {
			return AccessResponse{}, ErrInvalidRequest
		}
		outcome, err := handle.Invoke(
			ctx, request.Operation, request.Resource, request.Args, request.Grant,
		)
		return AccessResponse{Outcome: outcome}, err
	case AccessCreate:
		outcome, err := handle.Create(
			ctx, request.Resource, request.Spec, request.Initial,
		)
		return AccessResponse{Outcome: outcome}, err
	case AccessStat:
		result, err := handle.Stat(ctx, request.Resource)
		return AccessResponse{Stat: result}, err
	case AccessList:
		page, err := handle.List(ctx, request.List)
		return AccessResponse{List: page}, err
	default:
		return AccessResponse{}, ErrInvalidRequest
	}
}

// Schedule is the time arm (A). A timer belongs to the identity, not to the
// term, so the signature carries no attempt key — there is nothing to compare
// even if a caller wanted to.
func (i *ingress) Schedule(
	ctx context.Context,
	id actor.ActorID,
	request ScheduleRequest,
) (ScheduleResponse, error) {
	handle := i.schedule.MintAuthority(i.controller.IdentityAuthorityFor(id))
	switch request.Method {
	case ScheduleSet:
		timer, err := handle.Schedule(ctx, request.Req)
		return ScheduleResponse{ID: timer}, err
	case ScheduleCancel:
		return ScheduleResponse{}, handle.Cancel(ctx, request.ID)
	case ScheduleAck:
		return ScheduleResponse{}, handle.Ack(ctx, request.ID)
	default:
		return ScheduleResponse{}, ErrInvalidRequest
	}
}

// Fork and EndSelf are the Controller's typed commands, unchanged: the caller
// coordinate rides the request and the A/G verdict happens inside the ledger
// lock, where the change itself settles.
func (i *ingress) Fork(
	ctx context.Context,
	id actor.ActorID,
	attempt actorhost.AttemptKey,
	request ForkRequest,
) (actor.ActorID, error) {
	result, err := i.controller.Fork(ctx, actorctl.ForkRequest{
		CallerActorID: id,
		CallerAttempt: attempt,
		RequestID:     request.RequestID,
		Spec:          request.Spec,
	})
	return result.ChildActorID, err
}

func (i *ingress) EndSelf(
	ctx context.Context,
	id actor.ActorID,
	attempt actorhost.AttemptKey,
	request actorcaps.EndSelfRequest,
) error {
	_, err := i.controller.End(ctx, actorctl.EndRequest{
		CallerActorID: id,
		CallerAttempt: attempt,
		Target:        id,
		Reason:        request.Reason,
	})
	return err
}

var _ RemoteIngress = (*ingress)(nil)
