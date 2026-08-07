// Package contract is the machine-readable Coral shell contract. It owns the
// REST surface; the websocket envelope remains owned by platform/subjectgate
// (the link half of the contract — the schema generator here aggregates both
// halves into one signed golden schema).
//
// Layer wall: app-side only. drivers and protocol must never import this
// package; drivers receive the contract version as a plain string injected by
// the assembly root (cmd/server).
package contract

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Version is the server contract version exposed by both /api/meta and the
// websocket attach receipt. Envelope versioning is separately owned by
// subjectgate.FrameVersion.
const Version = "1.0"

// AgentMessagePayload documents the day-1 conventions for messages addressed
// to agents. The generated schema remains open: content fields are provider
// vocabulary, and text is only a common optional convention.
type AgentMessagePayload struct {
	ExpectedTurnID string `json:"expected_turn_id,omitempty"`
	Text           string `json:"text,omitempty"`
}

// ErrorCode is the closed, append-only vocabulary clients branch on.
type ErrorCode string

const (
	CodeBadPayload            ErrorCode = "bad_payload"
	CodeInvalidRequest        ErrorCode = "invalid_request"
	CodeNotAuthenticated      ErrorCode = "not_authenticated"
	CodeInvalidCredentials    ErrorCode = "invalid_credentials"
	CodeForbidden             ErrorCode = "forbidden"
	CodeNotFound              ErrorCode = "not_found"
	CodeChannelNotFound       ErrorCode = "channel_not_found"
	CodeDaemonNotFound        ErrorCode = "daemon_not_found"
	CodeDeclNotFound          ErrorCode = "decl_not_found"
	CodeResourceNotFound      ErrorCode = "resource_not_found"
	CodeAlreadyExists         ErrorCode = "already_exists"
	CodeConflict              ErrorCode = "conflict"
	CodeParentNotPresent      ErrorCode = "parent_not_present"
	CodeNowMember             ErrorCode = "now_member"
	CodeConfigInvalid         ErrorCode = "config_invalid"
	CodeUnknownClass          ErrorCode = "unknown_class"
	CodeInvalidDesiredHost    ErrorCode = "invalid_desired_host"
	CodeProtectedActor        ErrorCode = "protected_actor"
	CodeNotAcceptedSource     ErrorCode = "not_accepted_source"
	CodeMemberInactive        ErrorCode = "member_inactive"
	CodeChannelUnavailable    ErrorCode = "channel_unavailable"
	CodeCapabilityUnavailable ErrorCode = "capability_unavailable"
	CodeAuthorityUnavailable  ErrorCode = "authority_unavailable"
	CodeRealmUnavailable      ErrorCode = "realm_unavailable"
	CodeRoutingUnavailable    ErrorCode = "routing_unavailable"
	CodeUnavailable           ErrorCode = "unavailable"
	CodeResultUnknown         ErrorCode = "result_unknown"
	CodeInternal              ErrorCode = "internal_error"
)

var errorCodes = [...]ErrorCode{
	CodeBadPayload, CodeInvalidRequest, CodeNotAuthenticated,
	CodeInvalidCredentials, CodeForbidden, CodeNotFound, CodeChannelNotFound,
	CodeDaemonNotFound, CodeDeclNotFound, CodeResourceNotFound,
	CodeAlreadyExists, CodeConflict, CodeParentNotPresent, CodeNowMember,
	CodeConfigInvalid, CodeUnknownClass, CodeChannelUnavailable,
	CodeInvalidDesiredHost, CodeProtectedActor, CodeNotAcceptedSource,
	CodeMemberInactive, CodeCapabilityUnavailable, CodeAuthorityUnavailable,
	CodeRealmUnavailable, CodeRoutingUnavailable,
	CodeUnavailable, CodeResultUnknown, CodeInternal,
}

// ErrorCodes returns a copy of the closed vocabulary in stable order.
func ErrorCodes() []ErrorCode {
	out := make([]ErrorCode, len(errorCodes))
	copy(out, errorCodes[:])
	return out
}

// NormalizeErrorCode is the producer-side closed-set gate. Internal error
// vocabularies must be explicitly added to this contract before they can cross
// the REST boundary; an unregistered value degrades to internal_error.
func NormalizeErrorCode(code ErrorCode) ErrorCode {
	for _, known := range errorCodes {
		if code == known {
			return code
		}
	}
	return CodeInternal
}

// Error is the only REST error response shape. Message is presentation text;
// clients branch only on Code. Details is present from day one and may carry
// structured context. WillRetry states that the engine will retry by itself.
type Error struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	Details   any       `json:"details,omitempty"`
	WillRetry *bool     `json:"will_retry,omitempty"`
}

// Meta is returned by GET /api/meta.
type Meta struct {
	ContractVersion string `json:"contract_version"`
}

// Request DTOs are shared by handlers and schema generation, keeping a single
// source for every REST write payload.
type RegisterRequest struct {
	Email       string `json:"email"`
	Password    string `json:"password"`
	DisplayName string `json:"display_name,omitempty"`
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type CreateChannelRequest struct {
	Name     string  `json:"name"`
	Type     string  `json:"type,omitempty"`
	ParentID *string `json:"parent_id,omitempty"`
}

type CreateDaemonRequest struct {
	Name string `json:"name"`
}

type AttachDaemonRequest struct {
	DaemonID string `json:"daemon_id"`
}

type IntroduceActorRequest struct {
	DeclID string `json:"decl_id"`
}

type DeclarationOverlayRequest struct {
	Config json.RawMessage `json:"config"`
}

type DeclarationCreateRequest struct {
	Name       string          `json:"name"`
	Class      string          `json:"class,omitempty"`
	Config     json.RawMessage `json:"config,omitempty"`
	Visibility string          `json:"visibility,omitempty"`
}

type DeclarationUpdateRequest struct {
	Name       *string         `json:"name,omitempty"`
	Class      *string         `json:"class,omitempty"`
	Config     json.RawMessage `json:"config,omitempty"`
	Visibility *string         `json:"visibility,omitempty"`
}

// DecodeRequest applies the fail-closed REST-write rule: unknown fields,
// multiple JSON values, and malformed JSON are all rejected.
func DecodeRequest(r io.Reader, out any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("decode request: multiple JSON values")
		}
		return fmt.Errorf("decode request trailer: %w", err)
	}
	return nil
}
