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
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type Word string

const (
	WordChannelCreate     Word = "channel.create"
	WordChannelRetire     Word = "channel.retire"
	WordPrincipalRegister Word = "principal.register"
	WordPrincipalRetire   Word = "principal.retire"
	WordCredentialSet     Word = "credential.set"
	WordDeclRegister      Word = "decl.register"
	WordDeclEdit          Word = "decl.edit"
	WordDeclRevoke        Word = "decl.revoke"
	WordOverlaySet        Word = "overlay.set"
	WordOverlayClear      Word = "overlay.clear"
	WordDeviceMint        Word = "device.mint"
	WordDeviceClaim       Word = "device.claim"
	WordDeviceRetire      Word = "device.retire"
	WordDeviceAttach      Word = "device.attach"
	WordDeviceDetach      Word = "device.detach"

	WordChannelList       Word = "channel.list"
	WordChannelGet        Word = "channel.get"
	WordChannelCandidates Word = "channel.candidates"
	WordDeclList          Word = "decl.list"
	WordDeviceList        Word = "device.list"
	WordPrincipalMe       Word = "principal.me"
)

var WriteWords = [...]Word{
	WordChannelCreate, WordChannelRetire,
	WordPrincipalRegister, WordPrincipalRetire, WordCredentialSet,
	WordDeclRegister, WordDeclEdit, WordDeclRevoke,
	WordOverlaySet, WordOverlayClear,
	WordDeviceMint, WordDeviceClaim, WordDeviceRetire, WordDeviceAttach, WordDeviceDetach,
}

var ReadWords = [...]Word{
	WordChannelList, WordChannelGet, WordChannelCandidates,
	WordDeclList, WordDeviceList, WordPrincipalMe,
}

const (
	SpaceToolDeclID = "space-tool"
	SpaceToolClass  = "space-tool"
	RegistrarClass  = "atoll-internal:registrar"
	// RegistrarSeatDeclID is an installation detail, not a well-known public
	// identity. Its stable source key lets channel genesis rebuild the seat.
	RegistrarSeatDeclID = "atoll-internal:registrar-seat"
)

func StableBootstrapDeclID(owner, role string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("atoll:bootstrap:"+owner+":"+role)).String()
}

func HomeCodexDeclID(owner string) string { return StableBootstrapDeclID(owner, "home-codex") }

type ErrorCode string

const (
	CodeInvalidArgs      ErrorCode = "invalid_args"
	CodeNotFound         ErrorCode = "not_found"
	CodeConflictExists   ErrorCode = "conflict_exists"
	CodePermissionDenied ErrorCode = "permission_denied"
	CodeReserved         ErrorCode = "reserved"
	CodeResultUnknown    ErrorCode = "result_unknown"
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

type SourceRef struct {
	ChannelID channel.ID `json:"channel_id"`
	RequestID string     `json:"request_id"`
}

func (r SourceRef) String() string { return string(r.ChannelID) + ":" + r.RequestID }

type ChannelStatus string

const (
	ChannelPresent ChannelStatus = "present"
	ChannelRetired ChannelStatus = "retired"
)

type PrincipalStatus string

const (
	PrincipalPresent PrincipalStatus = "present"
	PrincipalRetired PrincipalStatus = "retired"
)

type DeclStatus string

const (
	DeclPresent DeclStatus = "present"
	DeclRevoked DeclStatus = "revoked"
)

type DeviceStatus string

const (
	DevicePresent DeviceStatus = "present"
	DeviceRetired DeviceStatus = "retired"
)

type CredentialStatus string

const (
	CredentialActive  CredentialStatus = "active"
	CredentialRetired CredentialStatus = "retired"
)

type ChannelRow struct {
	ID             channel.ID      `json:"id"`
	ParentID       channel.ID      `json:"parent_id,omitempty"`
	Name           string          `json:"name"`
	Type           string          `json:"type"`
	Status         ChannelStatus   `json:"status"`
	OwnerPrincipal string          `json:"owner_principal"`
	Spec           json.RawMessage `json:"spec"`
	CreatedAt      int64           `json:"created_at"`
}

type PrincipalRow struct {
	ID          string          `json:"id"`
	Kind        actor.Kind      `json:"kind"`
	Email       string          `json:"email,omitempty"`
	DisplayName string          `json:"display_name,omitempty"`
	Status      PrincipalStatus `json:"status"`
	CreatedAt   int64           `json:"created_at"`
}

// CredentialRow is intentionally private. CredentialReply is the only public
// credential shape and structurally has no secret_hash field.
type CredentialReply struct {
	PrincipalID string           `json:"principal_id"`
	Kind        string           `json:"kind"`
	Status      CredentialStatus `json:"status"`
	RotatedAt   int64            `json:"rotated_at,omitempty"`
}

type DeclRow struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Owner        string          `json:"owner"`
	DefaultClass string          `json:"default_class"`
	Config       json.RawMessage `json:"config,omitempty"`
	Status       DeclStatus      `json:"status"`
	Visibility   string          `json:"visibility"`
	CreatedAt    int64           `json:"created_at"`
	UpdatedAt    int64           `json:"updated_at"`
}

type OverlayRow struct {
	DeclID    string          `json:"decl_id"`
	ChannelID channel.ID      `json:"channel_id"`
	Config    json.RawMessage `json:"config,omitempty"`
	UpdatedAt int64           `json:"updated_at"`
}

type DeviceRow struct {
	ID             string       `json:"id"`
	OwnerPrincipal string       `json:"owner_principal"`
	Name           string       `json:"name"`
	Key            string       `json:"key"`
	Status         DeviceStatus `json:"status"`
	CreatedAt      int64        `json:"created_at"`
}

type BindingRow struct {
	ChannelID  channel.ID `json:"channel_id"`
	DeviceID   string     `json:"device_id"`
	AttachedAt int64      `json:"attached_at"`
}

type Confirmation struct {
	Word     Word   `json:"word"`
	TargetID string `json:"target_id"`
	Status   string `json:"status"`
}

type GenesisSpec struct {
	ChannelID          channel.ID           `json:"channel_id"`
	Type               string               `json:"type"`
	OwnerPrincipal     string               `json:"owner_principal"`
	CreatedAt          int64                `json:"created_at"`
	ParentID           channel.ID           `json:"parent_id,omitempty"`
	InitiatorPrincipal string               `json:"initiator_principal,omitempty"`
	Declarations       []GenesisDeclaration `json:"genesis_declarations"`
}

type GenesisDeclaration struct {
	DeclID   string                       `json:"decl_id"`
	Kind     actor.Kind                   `json:"kind"`
	Rendered channelspec.RenderedSnapshot `json:"rendered_snapshot"`
}

type ChannelCreate struct {
	Name   string     `json:"name"`
	Parent channel.ID `json:"parent,omitempty"`
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
type PrincipalRetire struct {
	PrincipalID string `json:"principal_id"`
}
type CredentialSet struct {
	PrincipalID string `json:"principal_id"`
	SecretHash  string `json:"secret_hash"`
}
type DeclRegister struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Class      string          `json:"class"`
	Config     json.RawMessage `json:"config,omitempty"`
	Visibility string          `json:"visibility"`
}
type DeclEdit struct {
	ID         string          `json:"id"`
	Name       *string         `json:"name,omitempty"`
	Class      *string         `json:"class,omitempty"`
	Config     json.RawMessage `json:"config,omitempty"`
	Visibility *string         `json:"visibility,omitempty"`
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

type Reply struct {
	Word   Word            `json:"word"`
	Value  json.RawMessage `json:"value,omitempty"`
	Source SourceRef       `json:"source,omitempty"`
}

func (r Reply) DecodeValue(out any) error {
	if len(r.Value) == 0 {
		return errors.New("lagoon: reply has no value")
	}
	if string(bytes.TrimSpace(r.Value)) == "null" {
		return errors.New("lagoon: reply value is null")
	}
	if err := json.Unmarshal(r.Value, out); err != nil {
		return fmt.Errorf("lagoon: decode reply value: %w", err)
	}
	return nil
}

type SubmitIn struct {
	Source    channel.ID
	Sender    actor.ActorID
	RequestID string
	Word      Word
	Payload   any
}

type Submitter interface {
	Submit(context.Context, SubmitIn) (Reply, error)
	SubmitApplication(context.Context, Word, any) (Reply, error)
}

// SpaceOps mirrors the fourteen authenticated registrar write words. Anonymous
// principal.register is intentionally absent and enters via SubmitApplication.
type SpaceOps interface {
	CreateChannel(context.Context, ChannelCreate) (ChannelRow, error)
	RetireChannel(context.Context, ChannelRetire) (ChannelRow, error)
	RetirePrincipal(context.Context, PrincipalRetire) (PrincipalRow, error)
	SetCredential(context.Context, CredentialSet) (CredentialReply, error)
	RegisterDecl(context.Context, DeclRegister) (DeclRow, error)
	EditDecl(context.Context, DeclEdit) (DeclRow, error)
	RevokeDecl(context.Context, DeclRevoke) (DeclRow, error)
	SetOverlay(context.Context, OverlaySet) (OverlayRow, error)
	ClearOverlay(context.Context, OverlayClear) (Confirmation, error)
	MintDevice(context.Context, DeviceMint) (DeviceRow, error)
	ClaimDevice(context.Context, DeviceClaim) (DeviceRow, error)
	RetireDevice(context.Context, DeviceRetire) (DeviceRow, error)
	AttachDevice(context.Context, DeviceBinding) (BindingRow, error)
	DetachDevice(context.Context, DeviceBinding) (Confirmation, error)
}

type SpaceQueries interface {
	ListChannels(context.Context, ChannelList) ([]ChannelRow, error)
	GetChannel(context.Context, ChannelGet) (ChannelRow, error)
	ListCandidates(context.Context, ChannelCandidates) ([]PrincipalRow, error)
	ListDecls(context.Context) ([]DeclRow, error)
	ListDevices(context.Context) ([]DeviceRow, error)
	Me(context.Context) (PrincipalRow, error)
}

type SpaceOpsBinder interface {
	Bind(SubmitIn) (SpaceOps, SpaceQueries)
}

type ActorFactsResolver interface {
	ActorFacts(context.Context, actor.ActorID) (channelspec.ActorFacts, bool, error)
}

type SystemGenesisResolver interface {
	SystemGenesis(context.Context) (GenesisSpec, bool, error)
}

type ClassCatalog interface {
	ValidateConfig(class string, config json.RawMessage) error
	LookupClassKind(class string) (actor.Kind, bool)
}

type SourceActorFactsResolver interface {
	ActorFacts(context.Context, channel.ID, actor.ActorID) (channelspec.ActorFacts, bool, error)
}

type Clock func() time.Time
