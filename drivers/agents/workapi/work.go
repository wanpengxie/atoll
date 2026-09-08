package workapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	TypeAsk       = "agent.ask"
	TypeStatus    = "agent.status"
	TypeResult    = "agent.result"
	TypeSteer     = "agent.steer"
	TypeInterrupt = "agent.interrupt"
)

const (
	// CapabilityWorkProtocolV1 means that the actor implements stable work
	// addressing, receipt delivery, status/result lookup, and targeted control.
	CapabilityWorkProtocolV1 = "work_protocol_v1"
	// CapabilityMultiWork is independent from the protocol: an implementation
	// may speak the work protocol while admitting only one open work at a time.
	CapabilityMultiWork = "multi_work"
	// CapabilityWorkTextSteer means agent.steer accepts a work_id plus new text.
	CapabilityWorkTextSteer = "work_text_steer"
	// CapabilityTargetedInterrupt means agent.interrupt can stop one work_id
	// without stopping sibling work.
	CapabilityTargetedInterrupt = "targeted_interrupt"
	// CapabilityAgentWideInterrupt means the empty interrupt form is available
	// in addition to the targeted form.
	CapabilityAgentWideInterrupt = "agent_wide_interrupt"
	// CapabilityBranchFromWork means agent.ask related_work_id starts from the
	// addressed work's latest durable context checkpoint.
	CapabilityBranchFromWork = "branch_from_work"
)

// WorkID identifies one accepted Agent commitment. It is stable across actor
// incarnations and opaque to callers; it is not a request id or a turn id.
type WorkID string

func (id WorkID) String() string { return string(id) }

// Delivery states how the creating agent.ask request is closed.
type Delivery string

const (
	DeliveryWait    Delivery = "wait"
	DeliveryReceipt Delivery = "receipt"
)

func ParseDelivery(raw string) (Delivery, bool) {
	d := Delivery(raw)
	return d, d == DeliveryWait || d == DeliveryReceipt
}

// WorkState is deliberately coarse. Stage carries the open work's current
// presentation without turning every scheduler phase into protocol state.
type WorkState string

const (
	WorkOpen   WorkState = "open"
	WorkClosed WorkState = "closed"
)

func ParseWorkState(raw string) (WorkState, bool) {
	s := WorkState(raw)
	return s, s == WorkOpen || s == WorkClosed
}

type Outcome string

const (
	OutcomeAnswered  Outcome = "answered"
	OutcomeCompleted Outcome = "completed"
	OutcomeCancelled Outcome = "cancelled"
	OutcomeFailed    Outcome = "failed"
)

func ParseOutcome(raw string) (Outcome, bool) {
	o := Outcome(raw)
	switch o {
	case OutcomeAnswered, OutcomeCompleted, OutcomeCancelled, OutcomeFailed:
		return o, true
	default:
		return "", false
	}
}

// Origin identifies the UI surface from which the sentence was authored. It
// belongs to agent.ask, not to controls or authorization.
type Origin struct {
	Session string `json:"session"`
	Label   string `json:"label,omitempty"`
}

// AskRequest is the work-aware agent.ask payload after the generic request
// envelope's body has been unwrapped.
type AskRequest struct {
	Text          string            `json:"text"`
	Attachments   []json.RawMessage `json:"attachments,omitempty"`
	Origin        *Origin           `json:"origin,omitempty"`
	Delivery      Delivery          `json:"delivery,omitempty"`
	SubmissionKey string            `json:"submission_key,omitempty"`
	RelatedWorkID WorkID            `json:"related_work_id,omitempty"`
}

// SteerRequest keeps target's established meaning: target is a buffered source
// request to insert. WorkID is the destination work. Text, target, and all are
// mutually exclusive; All is Agent-wide and therefore has no destination.
type SteerRequest struct {
	WorkID         WorkID `json:"work_id,omitempty"`
	Text           string `json:"text,omitempty"`
	ExpectedTurnID string `json:"expected_turn_id,omitempty"`
	Target         string `json:"target,omitempty"`
	All            bool   `json:"all,omitempty"`
	OperationKey   string `json:"operation_key,omitempty"`
}

type InterruptRequest struct {
	WorkID       WorkID `json:"work_id,omitempty"`
	OperationKey string `json:"operation_key,omitempty"`
}

// StatusRequest selects one work by a stable caller-visible correlation. An
// empty selector lists visible works. Cursor/Limit apply only to that list.
// The receiver's local request id is intentionally not a public selector: a
// caller in another channel does not know the new id minted by the membrane.
type StatusRequest struct {
	WorkID        WorkID `json:"work_id,omitempty"`
	SubmissionKey string `json:"submission_key,omitempty"`
	Cursor        string `json:"cursor,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

type ResultRequest struct {
	WorkID WorkID `json:"work_id"`
}

type Receipt struct {
	Status      string    `json:"status"`
	Disposition string    `json:"disposition"`
	WorkID      WorkID    `json:"work_id"`
	WorkState   WorkState `json:"work_state"`
}

type Work struct {
	WorkID         WorkID      `json:"work_id"`
	State          WorkState   `json:"state"`
	Stage          string      `json:"stage,omitempty"`
	Outcome        Outcome     `json:"outcome,omitempty"`
	SourceRequest  string      `json:"source_request_id,omitempty"`
	SubmissionKey  string      `json:"submission_key,omitempty"`
	RelatedWorkID  WorkID      `json:"related_work_id,omitempty"`
	ExecutionState string      `json:"execution_state,omitempty"`
	Inputs         []WorkInput `json:"inputs,omitempty"`
	CreatedAt      int64       `json:"created_at,omitempty"`
	UpdatedAt      int64       `json:"updated_at,omitempty"`
}

// WorkInput is the public reconciliation record for one accepted input. Text
// and attachment bodies are intentionally not repeated in status pages.
type WorkInput struct {
	InputID     string `json:"input_id"`
	Seq         int64  `json:"seq"`
	Disposition string `json:"disposition"`
}

// NextAction is an executable follow-up suggested by the Agent. Callers should
// prefer these actor-authored requests over reconstructing control payloads
// from UI-local request ids.
type NextAction struct {
	Word    string          `json:"word"`
	Label   string          `json:"label,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

type StatusResponse struct {
	Work       *Work        `json:"work,omitempty"`
	Works      []Work       `json:"works,omitempty"`
	NextCursor string       `json:"next_cursor,omitempty"`
	Guidance   string       `json:"guidance,omitempty"`
	Next       []NextAction `json:"next,omitempty"`
}

type ResultResponse struct {
	WorkID   WorkID          `json:"work_id"`
	State    WorkState       `json:"state"`
	Outcome  Outcome         `json:"outcome,omitempty"`
	Result   json.RawMessage `json:"result,omitempty"`
	Guidance string          `json:"guidance,omitempty"`
	Next     []NextAction    `json:"next,omitempty"`
}

var (
	ErrMalformedPayload = errors.New("agent: malformed payload")
	ErrUnknownField     = errors.New("agent: payload contains an unknown field")
	ErrInvalidPayload   = errors.New("agent: invalid payload")
)

func DecodeAsk(raw []byte) (AskRequest, error) {
	var v AskRequest
	if err := decodeClosed(raw, &v); err != nil {
		return AskRequest{}, err
	}
	if strings.TrimSpace(v.Text) == "" {
		return AskRequest{}, invalid("ask.text is required")
	}
	if v.Delivery == "" {
		v.Delivery = DeliveryWait
	} else if _, ok := ParseDelivery(string(v.Delivery)); !ok {
		return AskRequest{}, invalid("ask.delivery must be wait or receipt")
	}
	if v.Delivery == DeliveryReceipt && strings.TrimSpace(v.SubmissionKey) == "" {
		return AskRequest{}, invalid("ask.submission_key is required for receipt delivery")
	}
	if v.Origin != nil && strings.TrimSpace(v.Origin.Session) == "" {
		return AskRequest{}, invalid("ask.origin.session is required when origin is present")
	}
	if err := validToken("ask.submission_key", v.SubmissionKey, 256); err != nil {
		return AskRequest{}, err
	}
	if err := validID("ask.related_work_id", v.RelatedWorkID); err != nil {
		return AskRequest{}, err
	}
	return v, nil
}

func DecodeSteer(raw []byte) (SteerRequest, error) {
	var v SteerRequest
	if err := decodeClosed(raw, &v); err != nil {
		return SteerRequest{}, err
	}
	forms := 0
	if strings.TrimSpace(v.Text) != "" {
		forms++
	}
	if strings.TrimSpace(v.Target) != "" {
		forms++
	}
	if v.All {
		forms++
	}
	if forms != 1 {
		return SteerRequest{}, invalid("steer requires exactly one of text, target, or all=true")
	}
	if v.All {
		if v.WorkID != "" || v.ExpectedTurnID != "" {
			return SteerRequest{}, invalid("steer all=true is Agent-wide and cannot select a work or turn")
		}
	} else if err := requiredID("steer.work_id", v.WorkID); err != nil {
		return SteerRequest{}, err
	}
	if v.ExpectedTurnID != "" && strings.TrimSpace(v.Text) == "" {
		return SteerRequest{}, invalid("steer.expected_turn_id is valid only with text")
	}
	if err := validToken("steer.operation_key", v.OperationKey, 256); err != nil {
		return SteerRequest{}, err
	}
	return v, nil
}

func DecodeInterrupt(raw []byte) (InterruptRequest, error) {
	var v InterruptRequest
	if err := decodeClosed(raw, &v); err != nil {
		return InterruptRequest{}, err
	}
	if err := validID("interrupt.work_id", v.WorkID); err != nil {
		return InterruptRequest{}, err
	}
	if err := validToken("interrupt.operation_key", v.OperationKey, 256); err != nil {
		return InterruptRequest{}, err
	}
	return v, nil
}

func DecodeStatus(raw []byte) (StatusRequest, error) {
	var v StatusRequest
	if err := decodeClosed(raw, &v); err != nil {
		return StatusRequest{}, err
	}
	selectors := 0
	if v.WorkID != "" {
		selectors++
	}
	if strings.TrimSpace(v.SubmissionKey) != "" {
		selectors++
	}
	if selectors > 1 {
		return StatusRequest{}, invalid("status accepts at most one selector")
	}
	if err := validID("status.work_id", v.WorkID); err != nil {
		return StatusRequest{}, err
	}
	if err := validToken("status.submission_key", v.SubmissionKey, 256); err != nil {
		return StatusRequest{}, err
	}
	if selectors > 0 && (v.Cursor != "" || v.Limit != 0) {
		return StatusRequest{}, invalid("status cursor and limit are valid only when listing works")
	}
	if v.Limit < 0 || v.Limit > 100 {
		return StatusRequest{}, invalid("status.limit must be between 1 and 100 when present")
	}
	if selectors == 0 && v.Limit == 0 {
		v.Limit = 20
	}
	return v, nil
}

func DecodeResult(raw []byte) (ResultRequest, error) {
	var v ResultRequest
	if err := decodeClosed(raw, &v); err != nil {
		return ResultRequest{}, err
	}
	if err := requiredID("result.work_id", v.WorkID); err != nil {
		return ResultRequest{}, err
	}
	return v, nil
}

func decodeClosed(raw []byte, out any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte(`{}`)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return fmt.Errorf("%w: %v", ErrUnknownField, err)
		}
		return fmt.Errorf("%w: %v", ErrMalformedPayload, err)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%w: multiple JSON values", ErrMalformedPayload)
		}
		return fmt.Errorf("%w: %v", ErrMalformedPayload, err)
	}
	return nil
}

func requiredID(name string, value WorkID) error {
	if value == "" {
		return invalid("%s is required", name)
	}
	return validID(name, value)
}

func validID(name string, value WorkID) error {
	if value == "" {
		return nil
	}
	return validToken(name, string(value), 256)
}

func validToken(name, value string, max int) error {
	if value == "" {
		return nil
	}
	if strings.TrimSpace(value) != value || len(value) > max {
		return invalid("%s must be a trimmed string of at most %d bytes", name, max)
	}
	for _, r := range value {
		if r < 0x21 || r == 0x7f {
			return invalid("%s contains whitespace or control characters", name)
		}
	}
	return nil
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidPayload, fmt.Sprintf(format, args...))
}
