package channelspec

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

var (
	ErrDeclarationNotFound = errors.New("channel: declaration not found")
	ErrDigestMismatch      = errors.New("channel: snapshot digest mismatch")
	ErrTargetNotServing    = errors.New("channel: target is not serving")
)

type ActorFacts struct {
	Principal    string     `json:"principal"`
	SourceDeclID string     `json:"source_decl_id,omitempty"`
	Kind         actor.Kind `json:"kind"`
	Active       bool       `json:"active"`
}

type ChannelDesiredFacts struct {
	Present  bool
	ParentID channel.ID
}

// ResolveActorPrincipal applies the platform's single attribution rule: an
// actor's explicit principal wins; otherwise its source declaration's owner is
// the principal. The lookup is intentionally supplied by the caller because
// channel homes and the c0 registrar reach declaration truth through different
// read ports.
func ResolveActorPrincipal(
	ctx context.Context,
	facts ActorFacts,
	lookupOwner func(context.Context, string) (owner string, found bool, err error),
) (string, error) {
	if facts.Principal != "" {
		return facts.Principal, nil
	}
	if facts.SourceDeclID == "" || lookupOwner == nil {
		return "", nil
	}
	owner, found, err := lookupOwner(ctx, facts.SourceDeclID)
	if err != nil || !found {
		return "", err
	}
	return owner, nil
}

// HumanRosterEntry is one row of the channel's human membership roster — the
// entitlement projection's whole vocabulary. It is a business-membrane value:
// no kind axis (every entry is human by construction), no definition, no
// placement, no storage home.
type HumanRosterEntry struct {
	ActorID   actor.ActorID `json:"actor_id"`
	Principal string        `json:"principal"`
}

// ObsRosterRow is the business membrane's complete actor-roster projection.
// It deliberately contains neither a presence snapshot nor a runtime record.
type ObsRosterRow struct {
	ID          actor.ActorID `json:"id"`
	Kind        actor.Kind    `json:"kind"`
	DeclID      string        `json:"decl_id,omitempty"`
	Name        string        `json:"name,omitempty"`
	Description string        `json:"description,omitempty"`
	Bound       bool          `json:"-"`
	Device      DeviceState   `json:"-"`
}

type DeviceStateKind string

const (
	DeviceKnown     DeviceStateKind = "known"
	DeviceAbsent    DeviceStateKind = "absent"
	DeviceStale     DeviceStateKind = "stale"
	DeviceMalformed DeviceStateKind = "malformed"
)

type DeviceState struct {
	Kind       DeviceStateKind
	Online     bool
	ReceivedAt int64
}

type DeclarationFacts struct {
	OwnerPrincipal string
	Name           string
	Description    string
	Visibility     string
	Class          string
	Config         json.RawMessage
	Singleton      bool
}

// RenderedSnapshot is the complete, already-resolved declaration value accepted
// by a channel. Channel storage has no global/local merge semantics.
type RenderedSnapshot struct {
	Class     string          `json:"class"`
	Config    json.RawMessage `json:"config,omitempty"`
	Placement Placement       `json:"placement"`
	Singleton bool            `json:"singleton,omitempty"`
	Digest    string          `json:"digest"`
}

func (s RenderedSnapshot) Validate() error {
	if strings.TrimSpace(s.Class) == "" {
		return ErrInvalidRequest
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
		Class     string          `json:"class"`
		Config    json.RawMessage `json:"config,omitempty"`
		Placement Placement       `json:"placement"`
		Singleton bool            `json:"singleton,omitempty"`
	}{s.Class, s.Config, s.Placement, s.Singleton}
	return Digest(payload)
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
	ErrCodeConflictExists       OperationErrorCode = "conflict_exists"
)

var operationErrorCodes = [...]OperationErrorCode{
	ErrCodeBadPayload, ErrCodeChannelUnavailable, ErrCodeInvalidDesiredHost,
	ErrCodeDeclNotFound, ErrCodeForbidden,
	ErrCodeUnknownClass, ErrCodeProtectedActor,
	ErrCodeNotAcceptedSource, ErrCodeMemberInactive, ErrCodeAuthorityUnavailable,
	ErrCodeConflictExists,
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
