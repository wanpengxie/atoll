package actorctl

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var (
	ErrBootstrapping   = errors.New("actorctl: bootstrapping")
	ErrClosed          = errors.New("actorctl: closed")
	ErrChannelClosing  = errors.New("actorctl: channel closing")
	ErrInactive        = errors.New("actorctl: actor inactive")
	ErrStaleAttempt    = errors.New("actorctl: stale attempt")
	ErrReservedSystem  = errors.New("actorctl: system actor has a separate lifecycle")
	ErrInvalidKernel   = errors.New("actorctl: invalid system kernel")
	ErrAlreadyStarted  = errors.New("actorctl: already started")
	ErrInvalidMutation = errors.New("actorctl: invalid mutation")
	ErrForkInvalid     = errors.New("actorctl: invalid fork")
)

type ControllerPhase uint8

const (
	Bootstrapping ControllerPhase = iota
	Running
	Closed
)

type Origin uint8

const (
	OriginDurable Origin = iota + 1
	OriginRunWorld
)

// ActorDefinition is the immutable managed identity/config projection.
type ActorDefinition struct {
	Kind         actor.Kind
	Principal    string
	Role         storespec.ActorRole
	Sponsor      actor.ActorID
	Origin       Origin
	SourceDeclID string
	CreatedAt    int64
	// DefinitionVersion is durable declaration metadata. It is not an
	// incarnation fence: collaboration authority remains ActorID-active.
	DefinitionVersion int64
	Placement         storespec.Placement
	Execution         actorhost.ExecutionSpec
}

type DesiredKind uint8

const (
	DesiredDormant DesiredKind = iota
	DesiredRun
)

type DesiredState struct {
	Kind       DesiredKind
	AttemptKey actorhost.AttemptKey
}

type ActiveActor struct {
	Definition ActorDefinition
	Desired    DesiredState
}

// StoredActor is the authoritative value a typed Store operation returns for
// Controller publication.
type StoredActor struct {
	Row    storespec.ActorControlRow
	Origin Origin
}

type ActorCommit[T any] struct {
	Actor   StoredActor
	Result  T
	Effects storespec.PostCommitEffects
}

type ValueCommit[T any] struct {
	Result  T
	Effects storespec.PostCommitEffects
}

func definitionFromStored(stored StoredActor) (ActorDefinition, error) {
	row := stored.Row
	if row.ID == "" || row.ID == actor.SystemActorID {
		return ActorDefinition{}, ErrInvalidMutation
	}
	if _, ok := actor.ParseKind(string(row.Kind)); !ok || row.Kind == actor.KindSystem {
		return ActorDefinition{}, ErrInvalidMutation
	}
	if err := row.Placement.Validate(); err != nil {
		return ActorDefinition{}, err
	}
	origin := stored.Origin
	if origin != OriginDurable && origin != OriginRunWorld {
		return ActorDefinition{}, ErrInvalidMutation
	}
	return ActorDefinition{
		Kind:              row.Kind,
		Principal:         row.Principal,
		Role:              row.Role,
		Sponsor:           row.Sponsor,
		Origin:            origin,
		SourceDeclID:      row.SourceDeclID,
		CreatedAt:         row.CreatedAt,
		DefinitionVersion: row.CurrentDeclVersion,
		Placement:         row.Placement,
		Execution: actorhost.ExecutionSpec{
			Kind:        row.Kind,
			Class:       row.Class,
			Config:      append(json.RawMessage(nil), row.Config...),
			IdleTimeout: row.TIdle,
		},
	}, nil
}

func rowFromActive(id actor.ActorID, value ActiveActor) storespec.ActorControlRow {
	def := value.Definition
	binding := actor.Binding("")
	if def.Placement.Kind == storespec.PlacementDaemon {
		binding = actor.BindingRuntimeInboundViaRelay
	}
	return storespec.ActorControlRow{
		ID:                 id,
		Kind:               def.Kind,
		Principal:          def.Principal,
		Role:               def.Role,
		Sponsor:            def.Sponsor,
		Binding:            binding,
		CreatedAt:          def.CreatedAt,
		CurrentDeclVersion: def.DefinitionVersion,
		Class:              def.Execution.Class,
		Config:             append(json.RawMessage(nil), def.Execution.Config...),
		TIdle:              def.Execution.IdleTimeout,
		Placement:          def.Placement,
		SourceDeclID:       def.SourceDeclID,
	}
}

// BootstrapStore supplies only durable active rows. Run-world children are
// intentionally not recovered after restart.
type BootstrapStore interface {
	ListDeclaredActive(context.Context) ([]storespec.ActorControlRow, error)
}

type ForkCommitRequest struct {
	CallerActorID actor.ActorID
	RequestID     message.ID
	ChildActorID  actor.ActorID
	Spec          actorcaps.ForkSpec
	Placement     storespec.Placement
}

type ForkCommitResult struct {
	ChildActorID actor.ActorID
	Actor        StoredActor
	Effects      storespec.PostCommitEffects
}

type RestartRequest struct {
	ActorID       actor.ActorID
	CallerActorID actor.ActorID
	RequestID     message.ID
}

type DeclarationChange struct {
	ActorID   actor.ActorID
	Class     string
	Config    json.RawMessage
	RequestID message.ID
}

// MemberOperation identifies a command that entered through the collaboration
// plane. It carries the existing RequestID and request payload only; lifecycle
// execution identity never enters this value.
type MemberOperation struct {
	RequestID message.ID
	Sender    actor.ActorID
	Payload   json.RawMessage
}

type AdmitRequest struct {
	Ref       string
	Principal string
	Role      storespec.ActorRole
}

type AdmitResult = channel.AdmitResult

type IntroduceRequest struct {
	Ref              string
	DeclID           string
	InitiatorActorID actor.ActorID
	Member           *MemberOperation
}

type IntroduceResult = channel.IntroduceResult

type RemoveRequest struct {
	Ref              string
	Target           actor.ActorID
	InitiatorActorID actor.ActorID
	Member           *MemberOperation
}

type RemoveResult = channel.RemoveResult
type AttachDaemonRequest = channel.DaemonRequest
type AttachDaemonResult = channel.BindingResult
type DetachDaemonRequest = channel.DaemonRequest
type DetachDaemonResult = channel.BindingResult

type EndRequest struct {
	CallerActorID actor.ActorID
	CallerAttempt actorhost.AttemptKey
	Target        actor.ActorID
	Reason        string
}

type EndResult struct {
	Ended []actor.ActorID
}

type TerminalKind uint8

const (
	TerminalEnd TerminalKind = iota + 1
	TerminalRemove
	TerminalDetachDaemon
)

type TerminalCommand struct {
	Kind   TerminalKind
	End    EndRequest
	Remove RemoveRequest
	Detach DetachDaemonRequest
}

type TerminalPlan struct {
	IDs    []actor.ActorID
	Opaque any
}

type TerminalResult struct {
	Ended  []actor.ActorID
	Remove RemoveResult
	Detach DetachDaemonResult
}

// Store is the typed persistence port. Each mutation returns authoritative
// committed values; Controller never accepts a callback transaction escape.
type Store interface {
	BootstrapStore
	LookupActive(context.Context, actor.ActorID) (StoredActor, bool, error)
	Admit(context.Context, AdmitRequest) (ActorCommit[AdmitResult], error)
	Introduce(context.Context, IntroduceRequest) (ActorCommit[IntroduceResult], error)
	LookupFork(context.Context, actor.ActorID, message.ID) (actor.ActorID, bool, error)
	CommitFork(context.Context, ForkCommitRequest) (ForkCommitResult, error)
	Restart(context.Context, RestartRequest) (ActorCommit[struct{}], error)
	ApplyDeclaration(context.Context, DeclarationChange) (ActorCommit[struct{}], error)
	AttachDaemon(context.Context, AttachDaemonRequest) (ValueCommit[AttachDaemonResult], error)
	ResolveTerminal(context.Context, TerminalCommand, []storespec.ActorControlRow) (TerminalPlan, error)
	CommitTerminal(context.Context, TerminalCommand, TerminalPlan) (ValueCommit[TerminalResult], error)
}

type Commands interface {
	Admit(context.Context, AdmitRequest) (AdmitResult, error)
	Introduce(context.Context, IntroduceRequest) (IntroduceResult, error)
	Fork(context.Context, ForkRequest) (ForkResult, error)
	Restart(context.Context, RestartRequest) error
	ApplyDeclaration(context.Context, DeclarationChange) error
	AttachDaemon(context.Context, AttachDaemonRequest) (AttachDaemonResult, error)
	End(context.Context, EndRequest) (EndResult, error)
	Remove(context.Context, RemoveRequest) (RemoveResult, error)
	DetachDaemon(context.Context, DetachDaemonRequest) (DetachDaemonResult, error)
}

type ForkRequest struct {
	CallerActorID actor.ActorID
	CallerAttempt actorhost.AttemptKey
	RequestID     message.ID
	Spec          actorcaps.ForkSpec
}

type ForkResult struct {
	ChildActorID actor.ActorID
}

type Effects interface {
	WakeDomain(actorhost.ExecutionDomain)
	PlanPoke(actorhost.ExecutionDomain)
	ApplyPostCommit(storespec.PostCommitEffects)
	RunActorBorn(actor.ActorID) error
	RunActorsEnded([]actor.ActorID)
	Fatal(error)
}

type nopEffects struct{}

func (nopEffects) WakeDomain(actorhost.ExecutionDomain) {}
func (nopEffects) PlanPoke(actorhost.ExecutionDomain)   {}
func (nopEffects) ApplyPostCommit(storespec.PostCommitEffects) {
}
func (nopEffects) RunActorBorn(actor.ActorID) error { return nil }
func (nopEffects) RunActorsEnded([]actor.ActorID)   {}
func (nopEffects) Fatal(error)                      {}

// ManagedBodyBuilder is the composition callback for a Server-hosted managed
// body. ChannelActors mints the welded lifecycle capability before invoking
// it; callers can assemble business caps but cannot mint another handle.
type ManagedBodyBuilder func(
	actorhost.BodyBuildInput,
	actorcaps.LifecycleHandle,
) actorrt.Actor

type Config struct {
	Store            Store
	Effects          Effects
	ServerDomain     actorhost.ExecutionDomain
	ServerHost       actorhost.Config
	BuildManagedBody ManagedBodyBuilder
	Now              func() time.Time
}
