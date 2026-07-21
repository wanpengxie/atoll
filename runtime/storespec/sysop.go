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
	InitiatorActorID   actor.ActorID
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

type DetachTx struct {
	SysOpMeta
	DaemonID   DaemonID
	DurableIDs []actor.ActorID
	AllIDs     []actor.ActorID
	Envelopes  []CascadeEnvelope
}

type BindingResult struct {
	Bound            bool              `json:"bound"`
	ClearedInstances []actor.ActorID   `json:"cleared_instances,omitempty"`
	Effects          PostCommitEffects `json:"-"`
}

type AttachResult = BindingResult
type DetachResult = BindingResult

type DeclarationSyncStatus string

const (
	DeclarationAbsent  DeclarationSyncStatus = "absent"
	DeclarationEqual   DeclarationSyncStatus = "equal"
	DeclarationApplied DeclarationSyncStatus = "applied"
)

// DeclarationSyncTx is the Home-private declaration publication input. Realm
// supplies only class/config; placement and idle are re-read from channel truth
// by the store in the same transaction that compares and advances the version.
type DeclarationSyncTx struct {
	SysOpMeta
	ActorID actor.ActorID
	DeclID  string
	Class   string
	Config  json.RawMessage
}

type DeclarationSyncResult struct {
	Status  DeclarationSyncStatus `json:"status"`
	Version int64                 `json:"version,omitempty"`
	Effects PostCommitEffects     `json:"-"`
}

type DeclarationSyncStore interface {
	ApplyResolvedDeclaration(context.Context, DeclarationSyncTx) (DeclarationSyncResult, error)
}

// RemoveTx removes a composition member and its sponsor closure. The closure
// (durable rows to deregister + the whole-tree ended-event set) is computed by
// the sole holder of the run-world session authority (Home) and handed in; the
// store commits the anchor+event pair together with the cascade value rows in
// one transaction. See RemoveActor for why the closure cannot be re-derived
// from durable rows alone.
type RemoveTx struct {
	SysOpMeta
	Target           actor.ActorID
	InitiatorActorID actor.ActorID
	Reason           string
	DurableIDs       []actor.ActorID
	Envelopes        []CascadeEnvelope
}

// RemoveResult carries no PostCommitEffects: the member-remove session teardown
// (including run-world state/grants that no PostCommitEffects can name) is driven
// from Home's plan under the same Fork-race locks, not replayed from the store.
type RemoveResult struct {
	Removed []actor.ActorID `json:"removed"`
}

// RestartTx restarts one composition member's live incarnation. The word is an
// incarnation-axis operation: the transaction commits ONLY the event pair —
// identity truth (actor_registry) is never touched — and the bounce itself is
// a post-commit advisory effect (liveness retire with restart intent +
// despawn/poke). A crashed effect means the whole process died, which killed
// every incarnation anyway; reboot re-embodies from identity truth.
type RestartTx struct {
	SysOpMeta
	Target actor.ActorID
}

type RestartResult struct {
	Effects PostCommitEffects `json:"-"`
}

// SetDefaultTx writes the channel default-agent routing field. An empty target
// clears it. The value row is committed in the same transaction as the event
// pair.
type SetDefaultTx struct {
	SysOpMeta
	Target actor.ActorID
}

type SetDefaultResult struct {
	Effects PostCommitEffects `json:"-"`
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
	RemoveActor(context.Context, RemoveTx) (RemoveResult, error)
	RestartActor(context.Context, RestartTx) (RestartResult, error)
	SetDefaultAgent(context.Context, SetDefaultTx) (SetDefaultResult, error)
}

type DaemonBindingReader interface {
	IsBound(context.Context, DaemonID) (bool, error)
	ListBound(context.Context) ([]DaemonID, error)
}
