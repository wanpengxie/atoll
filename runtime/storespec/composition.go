package storespec

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
)

type Placement string

const (
	PlacementServer Placement = "server"
	PlacementDaemon Placement = "daemon"
)

var ErrCompositionNotFound = errors.New("storespec: composition instance not found")

// CompositionRecord is one channel-local instance intent. World declaration
// data deliberately does not ride this type; DeclID is the join key resolved by
// the app-owned world resolver at the Home assembly seam.
type CompositionRecord struct {
	InstanceID  actor.ActorID
	DeclID      string
	Principal   string
	Class       string
	ConfigJSON  string
	Placement   Placement
	DesiredHost string
	IsDefault   bool
	Epoch       int64
}

type CompositionIntroduce struct {
	DeclID      string
	Principal   string
	Class       string
	ConfigJSON  *string // nil preserves an existing value / inserts SQL NULL
	Placement   Placement
	DesiredHost string
	MakeDefault bool
	Kind        actor.Kind
	At          int64
}

// ComputeDeclaration is one daemon-reported embodiment. It is untrusted input:
// ApplyComputeDeclaration compares every metadata field with channel truth.
type ComputeDeclaration struct {
	ActorID actor.ActorID
	Kind    actor.Kind
	Binding actor.Binding
	Epoch   int64
}

type DeclarationPortAction uint8

const (
	DeclarationPortNone DeclarationPortAction = iota
	// DeclarationPortTakeLink conditionally takes the incarnation owned by the
	// declaring link. It covers stale/index-only entries without touching a
	// successor link.
	DeclarationPortTakeLink
	// DeclarationPortTakeAny conditionally takes the Home-indexed remote
	// incarnation, irrespective of which link generation owns it.
	DeclarationPortTakeAny
	// DeclarationPortTakeCurrent conditionally takes Runtime.CurrentIncarnation;
	// this is the local-cell half of replacing an active Host="" body.
	DeclarationPortTakeCurrent
)

type DeclarationDecision struct {
	ActorID    actor.ActorID
	Kind       actor.Kind
	Binding    actor.Binding
	Epoch      int64
	Allow      bool
	Rejected   bool
	PortAction DeclarationPortAction
}

type ComputeDeclarationInput struct {
	DaemonID   string
	Declared   []ComputeDeclaration
	IndexedIDs []actor.ActorID
	At         int64
}

type ComputeDeclarationResult struct {
	Decisions []DeclarationDecision
}

type CompositionReader interface {
	LookupComposition(context.Context, actor.ActorID) (CompositionRecord, bool, error)
	LookupCompositionPrincipal(context.Context, string) (CompositionRecord, bool, error)
	ListComposition(context.Context) ([]CompositionRecord, error)
	DefaultComposition(context.Context) (actor.ActorID, bool, error)
}

// CompositionControlPlane owns the operations whose composition and registry
// effects must share one channel transaction.
type CompositionControlPlane interface {
	CompositionReader
	// IntroduceComposition atomically advances restart_epoch when it changes an
	// existing row's config. configChanged reports whether that combined state
	// transition occurred; callers must not issue a second restart mutation.
	IntroduceComposition(context.Context, CompositionIntroduce) (record CompositionRecord, created, configChanged bool, err error)
	RemoveComposition(context.Context, actor.ActorID, int64) (removed bool, err error)
	RestartComposition(context.Context, actor.ActorID) (newEpoch int64, err error)
	ApplyRestartComposition(context.Context, int64, actor.ActorID, int64) (newEpoch int64, applied bool, err error)
	SetDefaultComposition(context.Context, actor.ActorID) error
	RevokeDaemonTarget(context.Context, string) ([]actor.ActorID, error)
	// ApplyComputeDeclaration is the complete channel-local declaration
	// decision function. beforeWrite runs inside the transaction after the full
	// decision set is known and before any Host/deregister write. A callback
	// failure rolls the transaction back; a later commit failure leaves only a
	// safe fail-closed body removal and never publishes an allow snapshot.
	ApplyComputeDeclaration(context.Context, ComputeDeclarationInput, func([]DeclarationDecision) error) (ComputeDeclarationResult, error)
}
