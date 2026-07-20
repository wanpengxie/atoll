package storespec

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// SysOpSource is part of the admission facts, never inferred from a wire name.
// Member anchors and realm refs are encoded into disjoint correlation domains
// by OpEntry before they enter this store contract.
type SysOpSource string

const (
	SysOpSourceSystem SysOpSource = "system"
	SysOpSourceMember SysOpSource = "member"
)

type SysOpMeta struct {
	Anchor        string
	RequestDigest string
	Source        SysOpSource
	Sender        actor.ActorID
	// DecisiveError carries a completed pre-admission verdict whose facts were
	// obtained through a typed requirement port. Retryable failures never enter
	// the store and therefore leave no event pair.
	DecisiveError *channel.OperationError
}

type PostCommitEffects struct {
	Poke       bool
	Despawn    []actor.ActorID
	KickDaemon *DaemonID
	Principals []string
}

type DaemonID string

type CompletedView struct {
	Operation     string
	RequestDigest string
	Result        json.RawMessage
	ErrorCode     channel.OperationErrorCode
	ErrorDetail   string
}

type AdmitTx struct {
	SysOpMeta
	Principal string
}

type AdmitResult struct {
	ActorID actor.ActorID     `json:"actor_id"`
	Created bool              `json:"created"`
	Effects PostCommitEffects `json:"-"`
}

type IntroduceTx struct {
	SysOpMeta
	DeclID             string
	InitiatorPrincipal string
	OwnerPrincipal     string
	Visibility         string
	Kind               actor.Kind
	Rendered           channel.RenderedSnapshot
}

type IntroduceResult struct {
	ActorID actor.ActorID     `json:"actor_id"`
	Created bool              `json:"created"`
	Effects PostCommitEffects `json:"-"`
}

type AttachTx struct {
	SysOpMeta
	DaemonID DaemonID
}

type DetachTx = AttachTx

type BindingResult struct {
	Bound            bool              `json:"bound"`
	ClearedInstances []actor.ActorID   `json:"cleared_instances,omitempty"`
	Effects          PostCommitEffects `json:"-"`
}

type AttachResult = BindingResult
type DetachResult = BindingResult

type ApplyTx struct {
	SysOpMeta
	DeclID             string
	Rendered           channel.RenderedSnapshot
	Authority          channel.ApplyAuthority
	InitiatorPrincipal string
	OwnerPrincipal     string
	Visibility         string
}

type ApplyResult struct {
	Status  channel.ApplyStatus `json:"status"`
	Version int64               `json:"version,omitempty"`
	Effects PostCommitEffects   `json:"-"`
}

type RevokeDeclTx struct {
	SysOpMeta
	DeclID string
}

type RevokeDaemonTx struct {
	SysOpMeta
	DaemonID DaemonID
}

type RevokeResult struct {
	PerInstance []channel.InstanceOutcome `json:"per_instance"`
	Effects     PostCommitEffects         `json:"-"`
}

// SysOpAdmission is the only channel-store port allowed to atomically combine
// a sysop event pair with structural truth. It deliberately exposes neither a
// transaction nor a callback escape hatch.
type SysOpAdmission interface {
	LookupCompleted(context.Context, string, string) (CompletedView, bool, error)
	Admit(context.Context, AdmitTx) (AdmitResult, error)
	Introduce(context.Context, IntroduceTx) (IntroduceResult, error)
	AttachDaemon(context.Context, AttachTx) (AttachResult, error)
	DetachDaemon(context.Context, DetachTx) (DetachResult, error)
	ApplyDeclVersion(context.Context, ApplyTx) (ApplyResult, error)
	RevokeDeclTargets(context.Context, RevokeDeclTx) (RevokeResult, error)
	RevokeDaemon(context.Context, RevokeDaemonTx) (RevokeResult, error)
}

type DaemonBindingReader interface {
	IsBound(context.Context, DaemonID) (bool, error)
	ListBound(context.Context) ([]DaemonID, error)
}
