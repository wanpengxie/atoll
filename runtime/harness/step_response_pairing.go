package harness

import (
	"context"
	"encoding/json"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepResponsePairing implements L1 §10.2 step 8 — The One Law /
// terminal-uniqueness contract. Applies only to kind=response.
//
// Concretely:
//
//   - response.parent_id must point to an existing kind=request message
//     (response_parent_invalid otherwise).
//   - is_terminal is computed per type_registry.terminal_convention:
//     core types collapse to single-response semantics; business types
//     read payload.status (or single-response, when set).
//   - same-parent_id duplicate is enforced at engine append (the
//     store's UNIQUE constraint maps to terminal_duplicate). This step
//     does NOT pre-scan store; the store's unique-index plus the
//     classifyAppendErr mapping in runtime/store handles concurrency
//     correctly per L2 §1.4.1 invariant.
//
// We DO perform a non-authoritative early check (FindByID parent) so
// the harness can return response_parent_invalid before any sqlite
// transaction starts — saves a roundtrip on obviously wrong responses.
type stepResponsePairing struct {
	deps Deps
}

func newStepResponsePairing(d Deps) khar.Step { return &stepResponsePairing{deps: d} }

func (s *stepResponsePairing) ID() khar.StepID { return khar.StepResponsePairing }

func (s *stepResponsePairing) Run(ctx context.Context, env *message.Envelope) (khar.Outcome, error) {
	if env.Kind != message.KindResponse {
		return khar.Outcome{}, nil
	}

	// Parent existence + kind check.
	parent, ok, err := s.deps.Log.FindByID(ctx, s.deps.ChannelID, env.ParentID)
	if err != nil {
		return khar.Outcome{}, err
	}
	if !ok {
		return khar.Outcome{
			RejectReason: message.HarnessResponseParentInvalid,
			Detail:       "parent_id not found: " + env.ParentID,
		}, nil
	}
	if parent.Kind != message.KindRequest {
		return khar.Outcome{
			RejectReason: message.HarnessResponseParentInvalid,
			Detail:       "parent_id is not kind=request: " + env.ParentID,
		}, nil
	}

	// Compute is_terminal.
	if _, isCore := CoreTypeTable[env.Type]; isCore {
		env.IsTerminal = true
		return khar.Outcome{}, nil
	}
	if s.deps.TypeRegistry == nil {
		// Defensive — without type_registry we cannot decide terminal
		// convention; default to payload_status which still surfaces
		// completed/failed as terminal.
		env.IsTerminal = payloadStatusTerminal(env.Payload)
		return khar.Outcome{}, nil
	}
	view, ok, err := s.deps.TypeRegistry.Lookup(ctx, env.Type)
	if err != nil {
		return khar.Outcome{}, err
	}
	if !ok || view.TerminalConvention == "" || view.TerminalConvention == "payload_status" {
		env.IsTerminal = payloadStatusTerminal(env.Payload)
	} else {
		env.IsTerminal = true
	}
	return khar.Outcome{}, nil
}

// payloadStatusTerminal returns true when payload.status is one of
// {"completed","failed"} per L1 §10.2.
func payloadStatusTerminal(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(payload, &doc); err != nil {
		return false
	}
	raw, ok := doc["status"]
	if !ok {
		return false
	}
	var status string
	if err := json.Unmarshal(raw, &status); err != nil {
		return false
	}
	return status == "completed" || status == "failed"
}
