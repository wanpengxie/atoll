package actorctl

import (
	"errors"
	"slices"

	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var (
	ErrBootstrapping   = errors.New("actorctl: bootstrapping")
	ErrClosed          = errors.New("actorctl: closed")
	ErrChannelClosing  = errors.New("actorctl: channel closing")
	ErrInactive        = errors.New("actorctl: actor inactive")
	ErrStaleAttempt    = errors.New("actorctl: stale attempt")
	ErrAlreadyStarted  = errors.New("actorctl: already started")
	ErrInvalidMutation = errors.New("actorctl: invalid mutation")
	ErrForkInvalid     = errors.New("actorctl: invalid fork")
	ErrForkConflict    = errors.New("actorctl: fork request replayed with a different payload")
)

type ControllerPhase uint8

const (
	Bootstrapping ControllerPhase = iota
	Running
	Closed
)

// managedActor is the whole Controller value: one record plus its current
// logical run authorization. Nothing else — no storage home, no definition
// version, no incarnation, no presence.
type managedActor struct {
	Record  storespec.ActorRecord
	Attempt actorhost.AttemptKey
}

// AdmitRequest is the pre-resolved human admission. Policy (who may admit, who
// counts as channel owner) settles at the Platform door before the command is
// issued; the command carries facts only.
type AdmitRequest struct {
	Principal string
}

type AdmitResult = channel.AdmitResult

// IntroduceRequest is the pre-resolved declaration admission. The declaration
// was fetched, its visibility judged and its placement host chosen at the
// Platform door; the command carries only mechanical facts.
type IntroduceRequest struct {
	DeclID     string
	Kind       actor.Kind
	Definition storespec.ActorDefinition
	Placement  storespec.Placement
}

// DeclarationChange is content-triggered: an equal definition is a no-op. It
// carries no RequestID — the receipt dedup axis is gone.
type DeclarationChange struct {
	ActorID    actor.ActorID
	Definition storespec.ActorDefinition
}

// RestartRequest is a pure value command (mint a new AttemptKey, republish). It
// has no store verb and no RequestID: restart is edge-triggered, never
// idempotent — each successful call is one new term.
type RestartRequest struct {
	ActorID actor.ActorID
}

type RemoveRequest struct {
	Target           actor.ActorID
	InitiatorActorID actor.ActorID
}

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
)

type TerminalCommand struct {
	Kind   TerminalKind
	End    EndRequest
	Remove RemoveRequest
}

type TerminalResult struct {
	Ended  []actor.ActorID
	Remove channel.RemoveResult
}

// ForkRequest is the only command carrying a RequestID: a child id is freshly
// minted, so the ledger itself cannot answer "did I already do this".
type ForkRequest struct {
	CallerActorID actor.ActorID
	CallerAttempt actorhost.AttemptKey
	RequestID     message.ID
	Spec          actorcaps.ForkSpec
}

type ForkResult struct {
	ChildActorID actor.ActorID
}

// ReconcileHints is derived from an already committed transition. It carries no
// definition, promises no delivery, and a duplicate poke is legal. Peers is a
// canonically ordered deduplicated set: the same committed change always
// projects the same hint value, whatever order the change enumerated its
// records in.
type ReconcileHints struct {
	Server bool
	Peers  []actorhost.ExecutionDomain
}

func (h *ReconcileHints) add(placement storespec.Placement) {
	if placement.Kind != storespec.PlacementDaemon {
		h.Server = true
		return
	}
	peer := actorhost.ExecutionDomain(placement.Host)
	for _, existing := range h.Peers {
		if existing == peer {
			return
		}
	}
	h.Peers = append(h.Peers, peer)
	slices.Sort(h.Peers)
}

// Transition is the committed change set of one Controller command. Platform
// owns every cross-organ tail these facts imply.
type Transition[T any] struct {
	Result     T
	Ended      []actor.ActorID
	EndedFacts []EndedFact
	Reconcile  ReconcileHints
}

// EndedFact preserves the identity facts held at the terminal commit point.
// Terminal is the only place they are still available without a post-commit
// lookup: the record is removed from the active ledger immediately afterwards.
type EndedFact struct {
	ID           actor.ActorID
	Kind         actor.Kind
	Principal    string
	SourceDeclID string
}

// DeclaredInstance is the declaration reconcile loop's question shape: "what
// does the pull loop compare against". Its only consumer is the Platform
// declaration pull loop; it is not a whole-record face resurrected.
type DeclaredInstance struct {
	ID           actor.ActorID
	Kind         actor.Kind
	SourceDeclID string
	Definition   storespec.ActorDefinition
}
