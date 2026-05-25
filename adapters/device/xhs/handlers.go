package xhs

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// buildCommand transforms one inbound kind=request envelope into a
// wire-form Command. Routing is envelope-level: Module.Handle sends to
// the adapter actor route selected by envelope.audience, and payload is
// treated as domain data only.
//
// Errors are surfaced as plain Go errors; the caller (Module.Handle)
// decides whether they map to a synchronous Respond / failed terminal.
func buildCommand(env *message.Envelope) (Command, error) {
	if env == nil {
		return Command{}, errors.New("xhs.buildCommand: envelope is nil")
	}
	if env.Type == "" {
		return Command{}, errors.New("xhs.buildCommand: envelope.type is empty")
	}
	if !strings.HasPrefix(env.Type, "xhs.") {
		return Command{}, fmt.Errorf("xhs.buildCommand: type %q lacks xhs. prefix", env.Type)
	}
	if env.ID == "" {
		return Command{}, errors.New("xhs.buildCommand: envelope.id is empty")
	}

	// Decode payload as JSON object so the extension receives domain
	// params in the same shape the caller supplied.
	var payload map[string]any
	if len(env.Payload) > 0 {
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			return Command{}, fmt.Errorf("xhs.buildCommand: payload decode: %w", err)
		}
	} else {
		payload = map[string]any{}
	}

	params := make(map[string]any, len(payload))
	for k, v := range payload {
		params[k] = v
	}

	cmd := Command{
		Type:          CommandWireType,
		CorrelationID: env.ID.String(),
		Cmd:           strings.TrimPrefix(env.Type, "xhs."),
		Params:        params,
	}
	return cmd, nil
}

// parseCallback decodes a `device_transit.send` payload (impl-layer2
// §5.3.1 inbound — device → adapter) into a Callback struct. The wire
// JSON is whatever the extension produced — the framework de-dupes
// orphans before invoking OnExternalCallback so the adapter can assume
// one canonical decode-and-act flow.
func parseCallback(raw []byte) (Callback, error) {
	var cb Callback
	if len(raw) == 0 {
		return cb, errors.New("xhs.parseCallback: empty payload")
	}
	if err := json.Unmarshal(raw, &cb); err != nil {
		return cb, fmt.Errorf("xhs.parseCallback: decode: %w", err)
	}
	cb.CorrelationID = strings.TrimSpace(cb.CorrelationID)
	if cb.CorrelationID == "" {
		return cb, errors.New("xhs.parseCallback: missing correlation_id")
	}
	return cb, nil
}

// callbackOutcome is the closed set of statuses the adapter recognises
// on the inbound callback path. Legacy synonyms (success / completed /
// failure) collapse to the canonical pair so downstream logic stays
// tidy.
type callbackOutcome string

const (
	outcomeOK      callbackOutcome = "ok"
	outcomeError   callbackOutcome = "error"
	outcomeUnknown callbackOutcome = "unknown"
)

// normaliseStatus maps the wire status string to the closed outcome
// set. Unknown / blank values resolve to outcomeUnknown so the adapter
// emits a failed terminal with receiver_internal_error and preserves
// callback_status_unknown in payload.error_code.
func normaliseStatus(raw string) callbackOutcome {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "ok", "completed", "success":
		return outcomeOK
	case "error", "failed", "failure":
		return outcomeError
	default:
		return outcomeUnknown
	}
}

// buildRespondPayload constructs the response payload for ctx.Respond
// per the recovered request type. Applies the per-type allow-list
// (resultAllowListFor / errorAllowListFor) at the adapter boundary so
// cross-type stowaway keys do not leak into caller-visible responses
// (R4-FIX-A).
//
// Returns (payload, status, terminal reason). The caller wraps them
// into adapter.RespondOptions.
func buildRespondPayload(cb Callback, requestType string) (json.RawMessage, string, string, error) {
	payload := map[string]any{}

	// device_id flows back only when the response schema declares it
	// (xhs.publish today). R4-FIX-A regression guard: do not unconditionally
	// fold cb.DeviceID for non-publish types.
	if cb.DeviceID != "" && requestType == TypePublish {
		payload["device_id"] = cb.DeviceID
	}

	status := "completed"
	reason := ""
	switch normaliseStatus(cb.Status) {
	case outcomeOK:
		copyAllowedKeys(payload, cb.Result, resultAllowListFor(requestType))
	case outcomeError:
		status = "failed"
		reason = string(message.TerminalReceiverInternalError)
		payload["error_code"] = errorReason(cb.ErrorObj)
		copyAllowedKeys(payload, cb.ErrorObj, errorAllowListFor(requestType))
	default:
		status = "failed"
		reason = string(message.TerminalReceiverInternalError)
		payload["error_code"] = "callback_status_unknown"
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, "", "", fmt.Errorf("xhs.buildRespondPayload: marshal: %w", err)
	}
	return body, status, reason, nil
}

// errorReason picks a human-readable reason string from the callback
// error object. Falls back to "callback_failed" when neither `reason`
// nor `code` are set.
func errorReason(e map[string]any) string {
	if e == nil {
		return "callback_failed"
	}
	if v, ok := e["reason"].(string); ok && strings.TrimSpace(v) != "" {
		return v
	}
	if v, ok := e["code"].(string); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return "callback_failed"
}

// copyAllowedKeys copies entries from src to dst whose key is present
// in allow. Nil src / nil allow are no-ops. dst is assumed non-nil.
func copyAllowedKeys(dst, src map[string]any, allow map[string]struct{}) {
	if src == nil || allow == nil {
		return
	}
	for k, v := range src {
		if _, ok := allow[k]; ok {
			dst[k] = v
		}
	}
}
