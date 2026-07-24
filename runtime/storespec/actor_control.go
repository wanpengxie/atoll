package storespec

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/wanpengxie/atoll/protocol/actor"
)

var (
	ErrActorNotFound         = errors.New("storespec: actor not found")
	ErrMemberInactive        = errors.New("storespec: member missing or ended")
	ErrChannelOwnerProtected = errors.New("storespec: channel owner is protected")
)

// ActorRole is the closed authority role carried by durable control rows.
// RoleNone is the ordinary membership state; RoleOwner is the unique channel
// root and is valid only for a durable human sponsored by the system actor.
type ActorRole string

const (
	RoleNone  ActorRole = ""
	RoleOwner ActorRole = "owner"
)

func ParseActorRole(raw string) (ActorRole, bool) {
	switch ActorRole(raw) {
	case RoleNone, RoleOwner:
		return ActorRole(raw), true
	default:
		return RoleNone, false
	}
}

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

// ActorControlRow is an immutable-by-contract snapshot returned by the Home
// authority. Implementations must clone Config on publication and lookup.
type ActorControlRow struct {
	ID                 actor.ActorID
	Kind               actor.Kind
	Principal          string
	Role               ActorRole
	Binding            actor.Binding
	CreatedAt          int64
	CurrentDeclVersion int64
	Sponsor            actor.ActorID
	Class              string
	Config             json.RawMessage
	Placement          Placement
	SourceDeclID       string
}

type ActorWorld uint8

const (
	WorldDurable ActorWorld = iota + 1
	WorldRun
)

type AuthorStamp struct {
	ID           actor.ActorID
	BirthVersion int64
}

type AuthorVerdict uint8

const (
	AuthorOK AuthorVerdict = iota + 1
	AuthorNotMember
	AuthorVersionStale
)

// ActorAuthority is the sole active-identity authority used by every effect
// membrane. Durable history is deliberately a separate contract below.
type ActorAuthority interface {
	LookupActive(context.Context, actor.ActorID) (ActorControlRow, bool, error)
	ListActive(context.Context) ([]ActorControlRow, error)
	WorldOf(context.Context, actor.ActorID) (ActorWorld, bool, error)
	CheckAuthor(context.Context, AuthorStamp) (AuthorVerdict, error)
}

type DurableHistory interface {
	ExistsEver(context.Context, actor.ActorID) (bool, error)
}

type AdmitBundle struct {
	ID           actor.ActorID
	Kind         actor.Kind
	Principal    string
	Role         ActorRole
	Binding      actor.Binding
	Class        string
	Config       json.RawMessage
	Placement    Placement
	SourceDeclID string
	CreatedAt    int64
}

type DeclAdmissionResult struct {
	ID      actor.ActorID
	Created bool
}

type DeclAdmissionStore interface {
	AdmitDeclared(context.Context, AdmitBundle) (DeclAdmissionResult, error)
}

// DeclaredControlReader is the durable boot read face. It returns the
// joined registry+current-declaration row, never a forked identity.
type DeclaredControlReader interface {
	LookupDeclaredActive(context.Context, actor.ActorID) (ActorControlRow, bool, error)
	ListDeclaredActive(context.Context) ([]ActorControlRow, error)
}

type CascadeBundle struct {
	IDs       []actor.ActorID
	EndedAt   int64
	Envelopes []CascadeEnvelope
}

// CascadeEnvelope keeps the store contract independent of harness internals
// while retaining a strongly typed control-plane operation.
type CascadeEnvelope struct {
	Target  actor.ActorID
	Reason  string
	EndedBy actor.ActorID
}

type CascadeResult struct {
	Ended   []actor.ActorID
	Already []actor.ActorID
}

type CascadeStore interface {
	EndCascade(context.Context, CascadeBundle) (CascadeResult, error)
}
