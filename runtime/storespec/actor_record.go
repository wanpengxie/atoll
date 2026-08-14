package storespec

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
)

var (
	ErrActorNotFound = errors.New("storespec: actor not found")
	// ErrNoDeclaration refuses a definition change aimed at a record that has
	// no declaration source (a human admission): there is no declaration to
	// change. An operation verdict, not a species branch.
	ErrNoDeclaration = errors.New("storespec: record has no declaration to change")
)

// PlacementKind is the closed placement vocabulary. Placement itself is a
// tagged value so an invalid server/host or daemon/no-host combination cannot
// cross an authority boundary unnoticed.
type PlacementKind string

const (
	PlacementServer PlacementKind = "server"
	PlacementDaemon PlacementKind = "daemon"
)

type Placement struct {
	Kind PlacementKind
	Host string
}

var ErrInvalidPlacement = errors.New("storespec: invalid placement")

func NewServerPlacement() Placement { return Placement{Kind: PlacementServer} }

func NewDaemonPlacement(host string) (Placement, error) {
	if host == "" {
		return Placement{}, ErrInvalidPlacement
	}
	return Placement{Kind: PlacementDaemon, Host: host}, nil
}

func (p Placement) Validate() error {
	switch p.Kind {
	case PlacementServer:
		if p.Host != "" {
			return ErrInvalidPlacement
		}
	case PlacementDaemon:
		if p.Host == "" {
			return ErrInvalidPlacement
		}
	default:
		return ErrInvalidPlacement
	}
	return nil
}

// ActorDefinition is the mutable half of one actor record: what it runs right
// now. The registry keeps only the current value — a definition change
// overwrites it, because the record answers "what is it", never "what happened".
type ActorDefinition struct {
	Class  string
	Config json.RawMessage
}

func (d ActorDefinition) Clone() ActorDefinition {
	d.Config = append(json.RawMessage(nil), d.Config...)
	return d
}

func (d ActorDefinition) Equal(other ActorDefinition) bool {
	return d.Class == other.Class && string(d.Config) == string(other.Config)
}

// ActorRecord is the storage-neutral standard actor record: who it is and what
// it is. It is deliberately NOT a SQL row — the same value shape is produced by
// the durable registry and by the process entry table.
//
// It carries no storage home, no transport binding, no authority role, no
// lineage edge, no definition
// version, no AttemptKey, no Incarnation, no presence and no belonging
// (state/timer/grant/resource) location.
type ActorRecord struct {
	ID        actor.ActorID
	Kind      actor.Kind
	Principal string

	SourceDeclID string
	CreatedAt    int64

	Definition ActorDefinition
	Placement  Placement
}

// Clone is the mandatory value-handoff copy: ActorRecord carries a
// json.RawMessage, so every store↔Controller↔Host handoff deep-copies.
func (r ActorRecord) Clone() ActorRecord {
	r.Definition = r.Definition.Clone()
	return r
}

// ActorDraft is the insert payload for one declaration-class birth. It carries
// no id: a birth id is minted inside the registry transaction that inserts the
// row, and the authoritative record comes back out. There is deliberately no
// way to ask for a particular one — an id chosen outside the transaction is an
// id that was decided before anyone checked, and it would also sit outside the
// naming the mint derives, free to occupy a value some later mint would produce.
type ActorDraft struct {
	Kind         actor.Kind
	Principal    string
	SourceDeclID string
	CreatedAt    int64
	Definition   ActorDefinition
	Placement    Placement
}

func (d ActorDraft) Clone() ActorDraft {
	d.Definition = d.Definition.Clone()
	return d
}

// IdentityAdmission is one immutable ActorID-level collaboration snapshot.
// It carries no AttemptKey, Incarnation or physical identity storage home.
type IdentityAdmission struct {
	ID   actor.ActorID
	Kind actor.Kind
}

func (a IdentityAdmission) Valid() bool {
	_, validKind := actor.ParseKind(string(a.Kind))
	return a.ID != "" && validKind
}

// ActorFacts is the narrow identity-fact projection: who is behind one actor
// and what kind it is. It is the question-shaped answer used by request-side
// authorization and by door policy (owner derivation) — never a whole record,
// never a general authority.
type ActorFacts struct {
	Kind         actor.Kind
	Principal    string
	SourceDeclID string
}

// ActiveIdentity answers "who is a member right now". It deliberately carries
// no definition, no principal and no placement: presence and connection-slot
// sweeps ask membership, nothing else. SourceDeclID is admitted because it is
// an identity fact rather than a definition — which declaration produced this
// member — and the roster already holds it; withholding it only forces readers
// to fetch per member what this call already had in hand.
type ActiveIdentity struct {
	ID           actor.ActorID
	Kind         actor.Kind
	SourceDeclID string
}

// ActorFactsAuthority answers the narrow identity-fact question for one id.
type ActorFactsAuthority interface {
	ActorFacts(context.Context, actor.ActorID) (ActorFacts, bool, error)
}

// IdentityRoster answers "who is here right now" as identities only.
type IdentityRoster interface {
	ActiveIdentities() ([]ActiveIdentity, error)
}

// PrincipalIdentity answers the inverse of ActorFacts' principal: given a login
// principal, which member is it. Question-shaped by construction — it returns an
// id, never a record — and it is the ONLY principal-axis read outside the store,
// so a login can be turned into a member without anyone holding the registry.
//
// A principal is a human-only fact (the registry refuses a non-human one), so
// the answer needs no kind to disambiguate. An empty principal is not a query
// and never resolves: every non-human carries "" and must not be reachable by
// asking for nothing.
type PrincipalIdentity interface {
	ResolvePrincipal(principal string) (actor.ActorID, bool, error)
}

// DeclaredInstanceReader answers "which actors did this declaration produce".
// It returns ids alone: the definition axis belongs to the declaration pull
// loop's own projection and never leaks into the business membrane.
type DeclaredInstanceReader interface {
	DeclaredInstances(string) ([]actor.ActorID, error)
}

// IdentityPresence answers only irreversible collaboration membership.
type IdentityPresence interface {
	IsActive(context.Context, actor.ActorID) (bool, error)
}

// CollaborationAuthority owns the one-shot ActorID-level admission used at
// remote ingress and delayed-product fire boundaries.
type CollaborationAuthority interface {
	AdmitIdentity(context.Context, actor.ActorID) (IdentityAdmission, bool, error)
}

// ResourceActorFacts is the narrow resource-policy projection — exactly what
// the resource door consumes, nothing more. It deliberately exposes neither
// ActorRecord nor raw Placement; the owner-derivation basis stays behind the
// Platform door that fills Owner.
type ResourceActorFacts struct {
	Active               bool
	Owner                bool
	PreferredStorageHost string
}

type ResourceActorAuthority interface {
	ResourceActorFacts(context.Context, actor.ActorID) (ResourceActorFacts, error)
}

// ChannelAuthority is the Platform composition face. Individual Runtime
// organs receive only the narrow interface they actually consume.
type ChannelAuthority interface {
	ActorFactsAuthority
	IdentityRoster
	DeclaredInstanceReader
	IdentityPresence
	CollaborationAuthority
	ResourceActorAuthority
}

// ActorRegistryStore is the durable half of the actor record store. It is the
// only durable actor-record surface: it touches actor_registry and nothing
// else — never messages, state, timers, grants or resources.
type ActorRegistryStore interface {
	LookupActive(context.Context, actor.ActorID) (ActorRecord, bool, error)
	ListActive(context.Context) ([]ActorRecord, error)
	// Insert commits one add-or-update introduction as a single transaction:
	// semantic-key lookup/update or id mint/insert live on one bed.
	Insert(context.Context, ActorDraft) (ActorRecord, error)
	// UpdateDefinition overwrites the current definition of an active row.
	UpdateDefinition(context.Context, actor.ActorID, ActorDefinition) (ActorRecord, error)
	// Deregister is the monotonic termination latch; it is idempotent and
	// silently skips ids that carry no durable row.
	Deregister(context.Context, []actor.ActorID, int64) error
}
