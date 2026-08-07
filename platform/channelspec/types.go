package channelspec

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

var (
	// ErrDefaultAgentUnavailable distinguishes a corrupt/unreadable authoritative
	// routing fold from the ordinary Unset state returned as found=false.
	ErrDefaultAgentUnavailable = errors.New("channel: default agent unavailable")
	ErrDeclarationNotFound     = errors.New("channel: declaration not found")
	ErrDigestMismatch          = errors.New("channel: snapshot digest mismatch")
)

type ActorFacts struct {
	Principal string     `json:"principal"`
	Kind      actor.Kind `json:"kind"`
	Active    bool       `json:"active"`
}

// HumanRosterEntry is one row of the channel's human membership roster — the
// entitlement projection's whole vocabulary. It is a business-membrane value:
// no kind axis (every entry is human by construction), no definition, no
// placement, no storage home.
type HumanRosterEntry struct {
	ActorID   actor.ActorID `json:"actor_id"`
	Principal string        `json:"principal"`
}

type DeclarationFacts struct {
	OwnerPrincipal string
	Visibility     string
	Class          string
	Config         json.RawMessage
}

// RenderedSnapshot is the complete, already-resolved declaration value accepted
// by a channel. Channel storage has no global/local merge semantics.
type RenderedSnapshot struct {
	Class     string            `json:"class"`
	Config    json.RawMessage   `json:"config,omitempty"`
	Placement channel.Placement `json:"placement"`
	Digest    string            `json:"digest"`
}

func (s RenderedSnapshot) Validate() error {
	if strings.TrimSpace(s.Class) == "" {
		return channel.ErrInvalidRequest
	}
	if err := s.Placement.Validate(); err != nil {
		return err
	}
	want, err := s.ContentDigest()
	if err != nil {
		return err
	}
	if s.Digest != want {
		return ErrDigestMismatch
	}
	return nil
}

// ContentDigest covers the rendered value only. Digest is deliberately
// excluded so equal values can be detected across local declaration versions.
func (s RenderedSnapshot) ContentDigest() (string, error) {
	payload := struct {
		Class     string            `json:"class"`
		Config    json.RawMessage   `json:"config,omitempty"`
		Placement channel.Placement `json:"placement"`
	}{s.Class, s.Config, s.Placement}
	return channel.Digest(payload)
}

// Seal computes and installs the content digest.
func (s RenderedSnapshot) Seal() (RenderedSnapshot, error) {
	digest, err := s.ContentDigest()
	if err != nil {
		return RenderedSnapshot{}, err
	}
	s.Digest = digest
	return s, nil
}

type OperationErrorCode string

const (
	ErrCodeBadPayload           OperationErrorCode = "bad_payload"
	ErrCodeChannelUnavailable   OperationErrorCode = "channel_unavailable"
	ErrCodeInvalidDesiredHost   OperationErrorCode = "invalid_desired_host"
	ErrCodeDeclNotFound         OperationErrorCode = "decl_not_found"
	ErrCodeForbidden            OperationErrorCode = "forbidden"
	ErrCodeUnknownClass         OperationErrorCode = "unknown_class"
	ErrCodeProtectedActor       OperationErrorCode = "protected_actor"
	ErrCodeNotAcceptedSource    OperationErrorCode = "not_accepted_source"
	ErrCodeMemberInactive       OperationErrorCode = "member_inactive"
	ErrCodeAuthorityUnavailable OperationErrorCode = "authority_unavailable"
)

var operationErrorCodes = [...]OperationErrorCode{
	ErrCodeBadPayload, ErrCodeChannelUnavailable, ErrCodeInvalidDesiredHost,
	ErrCodeDeclNotFound, ErrCodeForbidden,
	ErrCodeUnknownClass, ErrCodeProtectedActor,
	ErrCodeNotAcceptedSource, ErrCodeMemberInactive, ErrCodeAuthorityUnavailable,
}

type OperationError struct {
	Code      OperationErrorCode
	Detail    string
	Retryable bool
}

func (e *OperationError) Error() string {
	if e.Detail == "" {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Detail
}

type AdmitRequest struct {
	Ref       string `json:"ref"`
	Principal string `json:"principal"`
}

type IntroduceRequest struct {
	Ref              string        `json:"ref"`
	DeclID           string        `json:"decl_id"`
	InitiatorActorID actor.ActorID `json:"initiator_actor_id"`
}

type RemoveRequest struct {
	Ref              string        `json:"ref"`
	Target           actor.ActorID `json:"target"`
	InitiatorActorID actor.ActorID `json:"initiator_actor_id"`
}

type DaemonRequest struct {
	Ref      string `json:"ref"`
	DaemonID string `json:"daemon_id"`
}

type BindingResult struct {
	Bound            bool            `json:"bound"`
	ClearedInstances []actor.ActorID `json:"cleared_instances,omitempty"`
}

type RelationKind string

const (
	RelationJoined          RelationKind = "joined"
	RelationLeft            RelationKind = "left"
	RelationIntroduced      RelationKind = "introduced"
	RelationInstanceRemoved RelationKind = "instance_removed"
	RelationBound           RelationKind = "bound"
	RelationUnbound         RelationKind = "unbound"
	RelationGone            RelationKind = "gone"
)

// RelationDelta is the complete fact emitted at the membrane commit boundary.
// Reset marks the first item of a full-channel snapshot; following positive
// deltas are the complete replacement set for that channel.
type RelationDelta struct {
	Kind      RelationKind  `json:"kind,omitempty"`
	ChannelID channel.ID    `json:"channel_id"`
	Principal string        `json:"principal,omitempty"`
	ActorID   actor.ActorID `json:"actor_id,omitempty"`
	DeclID    string        `json:"decl_id,omitempty"`
	DaemonID  string        `json:"daemon_id,omitempty"`
	Reset     bool          `json:"reset,omitempty"`
}
