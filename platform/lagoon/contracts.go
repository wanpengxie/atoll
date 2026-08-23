// Package lagoon owns the channel-zero registry contract and the registrar
// actor. Business words deliberately live here rather than in protocol: they
// are the vocabulary of one built-in actor, not substrate vocabulary.
package lagoon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon/regspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type Word string

const (
	WordChannelCreate         Word = message.TypeSystemChannelCreate
	WordChannelDelete         Word = message.TypeSystemChannelDelete
	WordPrincipalCreate       Word = message.TypeSystemPrincipalCreate
	WordPrincipalLogin        Word = message.TypeSystemPrincipalLogin
	WordPrincipalDelete       Word = message.TypeSystemPrincipalDelete
	WordCredentialSet         Word = message.TypeSystemCredentialSet
	WordActorTemplateCreate   Word = message.TypeSystemActorTemplateCreate
	WordActorTemplateSet      Word = message.TypeSystemActorTemplateSet
	WordActorTemplateDelete   Word = message.TypeSystemActorTemplateDelete
	WordActorOverlaySet       Word = message.TypeSystemActorOverlaySet
	WordActorOverlayDelete    Word = message.TypeSystemActorOverlayDelete
	WordChannelTemplateCreate Word = message.TypeSystemChannelTemplateCreate
	WordChannelTemplateSet    Word = message.TypeSystemChannelTemplateSet
	WordChannelTemplateDelete Word = message.TypeSystemChannelTemplateDelete
	WordChannelSet            Word = message.TypeSystemChannelSet
	WordDeviceCreate          Word = message.TypeSystemDeviceCreate
	WordDeviceDelete          Word = message.TypeSystemDeviceDelete
	WordDeviceAttach          Word = message.TypeSystemDeviceAttach
	WordDeviceDetach          Word = message.TypeSystemDeviceDetach

	WordChannelList         Word = message.TypeSystemChannelList
	WordChannelGet          Word = message.TypeSystemChannelGet
	WordPrincipalList       Word = message.TypeSystemPrincipalList
	WordActorTemplateList   Word = message.TypeSystemActorTemplateList
	WordActorTemplateGet    Word = message.TypeSystemActorTemplateGet
	WordChannelTemplateList Word = message.TypeSystemChannelTemplateList
	WordChannelTemplateGet  Word = message.TypeSystemChannelTemplateGet
	WordDeviceList          Word = message.TypeSystemDeviceList
	WordPrincipalGet        Word = message.TypeSystemPrincipalGet
	WordClassList           Word = message.TypeSystemClassList
)

var WriteWords = [...]Word{
	WordChannelCreate, WordChannelDelete,
	WordPrincipalCreate, WordPrincipalDelete, WordCredentialSet,
	WordActorTemplateCreate, WordActorTemplateSet, WordActorTemplateDelete,
	WordActorOverlaySet, WordActorOverlayDelete,
	WordChannelTemplateCreate, WordChannelTemplateSet, WordChannelTemplateDelete, WordChannelSet,
	WordDeviceCreate, WordDeviceDelete, WordDeviceAttach, WordDeviceDetach,
}

var ReadWords = [...]Word{
	WordChannelList, WordChannelGet, WordPrincipalList,
	WordActorTemplateList, WordActorTemplateGet, WordDeviceList, WordPrincipalGet, WordPrincipalLogin,
	WordChannelTemplateList, WordChannelTemplateGet, WordClassList,
}

// LobbyWords is everything c0 exposes to the lobby: the two doors an
// unauthenticated guest may knock on. The lobby is outside the trust domain,
// so c0's svcactor neither advertises nor dispatches any other endpoint to
// it, and the registrar accepts these two words from the lobby only.
var LobbyWords = [...]Word{WordPrincipalCreate, WordPrincipalLogin}

func LobbyWord(word Word) bool {
	for _, candidate := range LobbyWords {
		if word == candidate {
			return true
		}
	}
	return false
}

// The *DeclID constants key the system declarations; the *Seed constants name
// the members they seat. They read alike here only because a system seat has
// nothing to hide behind an opaque key — the two are separate namespaces, and
// the declaration row's name must agree with the seed constant so that a seat
// is named the same whether it came from a recipe or from member.create.
const (
	PeerActorClass  = "peeractor"
	SvcActorDeclID  = "svcactor"
	SvcActorSeed    = "svcactor"
	SvcActorClass   = "svcactor"
	ClassRegistrar  = "registrar"
	RegistrarDeclID = "registrar"
	RegistrarSeed   = "registrar"
)

func StableBootstrapDeclID(owner, role string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("atoll:bootstrap:"+owner+":"+role)).String()
}

func HomeCodexDeclID(owner string) string { return StableBootstrapDeclID(owner, "home-codex") }

type ErrorCode string

const (
	CodeInvalidArgs        ErrorCode = "invalid_args"
	CodeNotFound           ErrorCode = "not_found"
	CodeConflictExists     ErrorCode = "conflict_exists"
	CodePermissionDenied   ErrorCode = "permission_denied"
	CodeInvalidCredentials ErrorCode = "invalid_credentials"
	CodeReserved           ErrorCode = "reserved"
	CodeResultUnknown      ErrorCode = "result_unknown"
)

type Error struct {
	Code   ErrorCode `json:"code"`
	Detail string    `json:"detail,omitempty"`
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Detail
}

// CredentialRow is intentionally private. CredentialReply is the only public
// credential shape and structurally has no secret_hash field.
type CredentialReply struct {
	PrincipalID string                   `json:"principal_id"`
	Kind        string                   `json:"kind"`
	Status      regspec.CredentialStatus `json:"status"`
	RotatedAt   int64                    `json:"rotated_at,omitempty"`
}

type Confirmation struct {
	Word     Word   `json:"word"`
	TargetID string `json:"target_id"`
	Status   string `json:"status"`
}

type GenesisSpec struct {
	ChannelID          channel.ID             `json:"channel_id"`
	Type               string                 `json:"type"`
	OwnerPrincipal     string                 `json:"owner_principal"`
	CreatedAt          int64                  `json:"created_at"`
	ParentID           channel.ID             `json:"parent_id,omitempty"`
	InitiatorPrincipal string                 `json:"initiator_principal,omitempty"`
	Humans             []GenesisHuman         `json:"genesis_humans,omitempty"`
	Declarations       []GenesisDeclaration   `json:"genesis_declarations"`
	Profile            regspec.ChannelProfile `json:"profile"`
}

// Seed is the birth name the seated member is called by. It sits beside the
// rendered snapshot rather than inside it: the snapshot's digest drives
// reconciliation, and renaming a declaration must not restart the actor it
// already seated.
type GenesisDeclaration struct {
	DeclID        string                       `json:"decl_id"`
	Seed          string                       `json:"seed"`
	Kind          actor.Kind                   `json:"kind"`
	Principal     string                       `json:"principal,omitempty"`
	SourceActorID actor.ActorID                `json:"source_actor_id,omitempty"`
	Rendered      channelspec.RenderedSnapshot `json:"rendered_snapshot"`
}

// GenesisHuman is one explicit human seat in a channel's immutable creation
// plan. Ownership never implies this row: a channel may be owned by an agent
// principal, and only the resolved initial-seat plan creates members.
type GenesisHuman struct {
	Principal     string        `json:"principal"`
	SourceActorID actor.ActorID `json:"source_actor_id,omitempty"`
}

// ChannelCreateIntent is the public create shape. Actor ids are meaningful
// only inside the source channel, so the source sysactor resolves them before
// this request is allowed to cross into c0.
type ChannelCreateIntent struct {
	Name            string               `json:"name"`
	Recipe          regspec.TemplateBody `json:"recipe"`
	InitialActorIDs []actor.ActorID      `json:"initial_actor_ids"`
}

// InitialSeatIntent is a platform-authored snapshot of one active source
// member. Callers cannot assert these fields: the source sysactor derives them
// from its Controller-owned ActorFacts authority.
type InitialSeatIntent struct {
	SourceActorID actor.ActorID `json:"source_actor_id"`
	Kind          actor.Kind    `json:"kind"`
	Principal     string        `json:"principal,omitempty"`
	DeclID        string        `json:"decl_id,omitempty"`
}

// ResolvedChannelCreate is the internal create shape accepted by Registrar.
// It is carried only after the source-channel gate has resolved every id.
type ResolvedChannelCreate struct {
	Name         string               `json:"name"`
	Recipe       regspec.TemplateBody `json:"recipe"`
	InitialSeats []InitialSeatIntent  `json:"initial_seats"`
}
type ChannelRetire struct {
	ChannelID channel.ID `json:"channel_id"`
}
type PrincipalRegister struct {
	ID          string `json:"id,omitempty"`
	Email       string `json:"email"`
	SecretHash  string `json:"secret_hash"`
	DisplayName string `json:"display_name,omitempty"`
}
type PrincipalLogin struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}
type PrincipalLoginReply struct {
	PrincipalID string `json:"id"`
}
type PrincipalRetire struct {
	PrincipalID string `json:"principal_id"`
}
type CredentialSet struct {
	PrincipalID string `json:"principal_id"`
	SecretHash  string `json:"secret_hash"`
}
type DeclRegister struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Class       string          `json:"class"`
	Config      json.RawMessage `json:"config,omitempty"`
	Visibility  string          `json:"visibility"`
	Singleton   bool            `json:"singleton"`
}
type DeclEdit struct {
	ID          string          `json:"id"`
	Name        *string         `json:"name,omitempty"`
	Description *string         `json:"description,omitempty"`
	Class       *string         `json:"class,omitempty"`
	Config      json.RawMessage `json:"config,omitempty"`
	Visibility  *string         `json:"visibility,omitempty"`
	Singleton   *bool           `json:"singleton,omitempty"`
}
type DeclRevoke struct {
	ID string `json:"id"`
}
type OverlaySet struct {
	DeclID    string          `json:"decl_id"`
	ChannelID channel.ID      `json:"channel_id"`
	Config    json.RawMessage `json:"config"`
}
type OverlayClear struct {
	DeclID    string     `json:"decl_id"`
	ChannelID channel.ID `json:"channel_id"`
}
type DeviceMint struct {
	Name string `json:"name"`
}
type DeviceClaim struct {
	DeviceID string `json:"device_id"`
	Name     string `json:"name"`
}
type DeviceRetire struct {
	DeviceID string `json:"device_id"`
}
type DeviceBinding struct {
	DeviceID  string     `json:"device_id"`
	ChannelID channel.ID `json:"channel_id"`
}
type ChannelList struct {
	ParentID *channel.ID `json:"parent_id,omitempty"`
}
type ChannelGet struct {
	ChannelID channel.ID `json:"channel_id"`
}
type ChannelCandidates struct {
	ChannelID channel.ID `json:"channel_id"`
}

type ChannelTemplateRegister struct {
	ID          string               `json:"id"`
	Name        string               `json:"name"`
	Description string               `json:"description,omitempty"`
	Visibility  string               `json:"visibility"`
	Body        regspec.TemplateBody `json:"body"`
}

type ChannelTemplateEdit struct {
	ID          string                `json:"id"`
	Name        *string               `json:"name,omitempty"`
	Description *string               `json:"description,omitempty"`
	Visibility  *string               `json:"visibility,omitempty"`
	Body        *regspec.TemplateBody `json:"body,omitempty"`
}

type ChannelTemplateRevoke struct {
	ID string `json:"id"`
}
type ChannelTemplateGet struct {
	ID string `json:"id"`
}

type ChannelProfileSet struct {
	ChannelID   channel.ID `json:"channel_id"`
	Description string     `json:"description"`
	Serving     *int       `json:"serving"`
}

type ChannelDescribe struct {
	Channel   string     `json:"channel,omitempty"`
	ChannelID channel.ID `json:"channel_id,omitempty"`
}

type Reply struct {
	Value json.RawMessage `json:"value,omitempty"`
}

func (r Reply) ValidValue() error {
	value := bytes.TrimSpace(r.Value)
	if len(value) == 0 {
		return errors.New("lagoon: reply has no value")
	}
	if bytes.Equal(value, []byte("null")) {
		return errors.New("lagoon: reply value is null")
	}
	var raw json.RawMessage
	if err := json.Unmarshal(value, &raw); err != nil {
		return fmt.Errorf("lagoon: invalid reply value: %w", err)
	}
	return nil
}

func (r Reply) DecodeValue(out any) error {
	if err := r.ValidValue(); err != nil {
		return err
	}
	if err := json.Unmarshal(r.Value, out); err != nil {
		return fmt.Errorf("lagoon: decode reply value: %w", err)
	}
	return nil
}

type SystemGenesisResolver interface {
	SystemGenesis(context.Context) (GenesisSpec, bool, error)
}

type ClassCatalog interface {
	ValidateConfig(class string, config json.RawMessage) error
	ResolveConfig(class string, config json.RawMessage) (json.RawMessage, error)
	ClassConfigSchema(class string) (json.RawMessage, bool)
	ClassDefaultConfig(class string) (json.RawMessage, bool)
	Classes() []string
	LookupClassKind(class string) (actor.Kind, bool)
	LookupClassPlacement(class string) (channelspec.PlacementKind, bool)
}

type SourceActorFactsResolver interface {
	ActorFacts(context.Context, channel.ID, actor.ActorID) (channelspec.ActorFacts, bool, error)
}

type Clock func() time.Time
