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
)

type Word string

const (
	WordChannelCreate           Word = "channel.create"
	WordChannelRetire           Word = "channel.retire"
	WordPrincipalRegister       Word = "principal.register"
	WordPrincipalRetire         Word = "principal.retire"
	WordCredentialSet           Word = "credential.set"
	WordDeclRegister            Word = "actor.template.register"
	WordDeclEdit                Word = "actor.template.edit"
	WordDeclRevoke              Word = "actor.template.revoke"
	WordOverlaySet              Word = "actor.overlay.set"
	WordOverlayClear            Word = "actor.overlay.clear"
	WordChannelTemplateRegister Word = "channel.template.register"
	WordChannelTemplateEdit     Word = "channel.template.edit"
	WordChannelTemplateRevoke   Word = "channel.template.revoke"
	WordChannelProfileSet       Word = "channel.profile.set"
	WordDeviceMint              Word = "device.mint"
	WordDeviceClaim             Word = "device.claim"
	WordDeviceRetire            Word = "device.retire"
	WordDeviceAttach            Word = "device.attach"
	WordDeviceDetach            Word = "device.detach"

	WordChannelList         Word = "channel.list"
	WordChannelGet          Word = "channel.get"
	WordChannelCandidates   Word = "channel.candidates"
	WordDeclList            Word = "actor.template.list"
	WordChannelTemplateList Word = "channel.template.list"
	WordChannelTemplateGet  Word = "channel.template.get"
	WordChannelDescribe     Word = "channel.describe"
	WordDeviceList          Word = "device.list"
	WordPrincipalMe         Word = "principal.me"
)

var WriteWords = [...]Word{
	WordChannelCreate, WordChannelRetire,
	WordPrincipalRegister, WordPrincipalRetire, WordCredentialSet,
	WordDeclRegister, WordDeclEdit, WordDeclRevoke,
	WordOverlaySet, WordOverlayClear,
	WordChannelTemplateRegister, WordChannelTemplateEdit, WordChannelTemplateRevoke, WordChannelProfileSet,
	WordDeviceMint, WordDeviceClaim, WordDeviceRetire, WordDeviceAttach, WordDeviceDetach,
}

var ReadWords = [...]Word{
	WordChannelList, WordChannelGet, WordChannelCandidates,
	WordDeclList, WordDeviceList, WordPrincipalMe,
	WordChannelTemplateList, WordChannelTemplateGet, WordChannelDescribe,
}

const (
	CoreActorDeclID     = "coreactor"
	PeerActorClass      = "peeractor"
	PeerActorDeclPrefix = "peer:"
	SvcActorDeclID      = "atoll-internal:svcactor"
	SvcActorClass       = "svcactor"
	RegistrarClass      = "atoll-internal:registrar"
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
	Declarations       []GenesisDeclaration   `json:"genesis_declarations"`
	Profile            regspec.ChannelProfile `json:"profile"`
}

type GenesisDeclaration struct {
	DeclID   string                       `json:"decl_id"`
	Kind     actor.Kind                   `json:"kind"`
	Rendered channelspec.RenderedSnapshot `json:"rendered_snapshot"`
}

type ChannelCreate struct {
	Name      string                `json:"name"`
	Template  string                `json:"template,omitempty"`
	Overrides *regspec.TemplateBody `json:"overrides,omitempty"`
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
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Class       string          `json:"class"`
	Config      json.RawMessage `json:"config,omitempty"`
	Visibility  string          `json:"visibility"`
}
type DeclEdit struct {
	ID          string          `json:"id"`
	Name        *string         `json:"name,omitempty"`
	Description *string         `json:"description,omitempty"`
	Class       *string         `json:"class,omitempty"`
	Config      json.RawMessage `json:"config,omitempty"`
	Visibility  *string         `json:"visibility,omitempty"`
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
	ChannelID   channel.ID                      `json:"channel_id"`
	Description string                          `json:"description"`
	Serving     *int                            `json:"serving"`
	Endpoints   map[string]regspec.EndpointSpec `json:"endpoints"`
}

type ChannelDescribe struct {
	Channel   string     `json:"channel,omitempty"`
	ChannelID channel.ID `json:"channel_id,omitempty"`
}

type Reply struct {
	Word   Word            `json:"word"`
	Value  json.RawMessage `json:"value,omitempty"`
	Source SourceRef       `json:"source,omitempty"`
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
	LookupClassKind(class string) (actor.Kind, bool)
	LookupClassPlacement(class string) (channel.PlacementKind, bool)
}

type SourceActorFactsResolver interface {
	ActorFacts(context.Context, channel.ID, actor.ActorID) (channelspec.ActorFacts, bool, error)
}

type ChannelInstancesResolver interface {
	DeclaredInstances(context.Context, channel.ID, string) ([]actor.ActorID, error)
}

type ChannelServiceResolver interface {
	WaitChannelService(context.Context, channel.ID) error
}

type Clock func() time.Time
