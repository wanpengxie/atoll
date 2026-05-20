package harness

import (
	"context"
	"encoding/json"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepEnvelopeShape implements proto-layer1 §2.2 — envelope shape
// validation. It covers seven kinds of wire-level guards (round 3
// cluster F insertion):
//
//  1. content fields present (proto-layer0 §1.1)
//  2. envelope.channel_id == caller channel context
//  3. kind ∈ {event, request, response}
//  4. visibility (when non-empty) ∈ {public, private} — Step Normalize
//     fills the default when caller leaves it empty.
//  5. audience cardinality + wildcard form (proto-layer0 §2.3)
//  6. visibility=private + audience=['*'] semantic contradiction
//  7. unknown top-level field fail-closed reject (proto-layer0 §7.3) —
//     only enforced when the caller supplies the original raw JSON via
//     CtxWithRawEnvelope; in-process Go struct callers naturally
//     cannot drift on field set because the struct fixes the schema at
//     compile time.
//
// The step runs after CallerAuth and before SenderConsistent/Dedupe/
// Normalize, so downstream stages never see malformed envelopes and
// dedupe never wastes a canonical_hash compute on a will-reject row.
type stepEnvelopeShape struct{}

func newStepEnvelopeShape(_ Deps) khar.Step { return &stepEnvelopeShape{} }

func (s *stepEnvelopeShape) ID() khar.StepID { return khar.StepEnvelopeShape }

func (s *stepEnvelopeShape) Run(ctx context.Context, env *message.Envelope) (khar.Outcome, error) {
	// (1) content fields present — proto-layer0 §1.1.
	switch {
	case env.ID == "":
		return rejectFieldMissing("envelope.id required"), nil
	case env.ChannelID == "":
		return rejectFieldMissing("envelope.channel_id required"), nil
	case env.Kind == "":
		return rejectFieldMissing("envelope.kind required"), nil
	case env.Type == "":
		return rejectFieldMissing("envelope.type required"), nil
	case env.Sender.ID == "":
		return rejectFieldMissing("envelope.sender required"), nil
	case env.Audience == nil:
		return rejectFieldMissing("envelope.audience required"), nil
	case env.TS == 0:
		return rejectFieldMissing("envelope.ts required"), nil
	}

	// (2) channel_id consistency vs caller context.
	caller := CallerFromCtx(ctx)
	if caller.ChannelID != "" && env.ChannelID != caller.ChannelID {
		return khar.Outcome{
			RejectReason: message.HarnessChannelMismatch,
			Detail:       "envelope.channel_id does not match caller channel context",
		}, nil
	}

	// (3) kind closed set — proto-layer0 §2.1.
	switch env.Kind {
	case message.KindEvent, message.KindRequest, message.KindResponse:
	default:
		return khar.Outcome{
			RejectReason: message.HarnessKindInvalid,
			Detail:       "envelope.kind not in {event, request, response}",
		}, nil
	}

	// (4) visibility closed set — proto-layer0 §2.4.
	// Empty visibility is legal here (Step Normalize defaults to public).
	if env.Visibility != "" &&
		env.Visibility != message.VisibilityPublic &&
		env.Visibility != message.VisibilityPrivate {
		return khar.Outcome{
			RejectReason: message.HarnessVisibilityInvalid,
			Detail:       "envelope.visibility not in {public, private}",
		}, nil
	}

	// (5) audience cardinality + wildcard form — proto-layer0 §2.3.
	if len(env.Audience) == 0 {
		return khar.Outcome{
			RejectReason: message.HarnessAudienceEmpty,
			Detail:       "envelope.audience empty",
		}, nil
	}
	hasWildcard := false
	for _, a := range env.Audience {
		if a == message.AudienceWildcard {
			hasWildcard = true
			break
		}
	}
	if hasWildcard && len(env.Audience) > 1 {
		return khar.Outcome{
			RejectReason: message.HarnessAudienceMixedWildcard,
			Detail:       "envelope.audience contains '*' mixed with concrete actors",
		}, nil
	}
	if env.Kind == message.KindRequest {
		// request must address either ['*'] or exactly one concrete actor
		// (the broader Step 5/7 layer narrows further; here we only fail
		// the structural case of multi-element-no-wildcard).
		if !hasWildcard && len(env.Audience) != 1 {
			return khar.Outcome{
				RejectReason: message.HarnessRequestAudienceInvalid,
				Detail:       "kind=request requires audience=[<concrete-actor>] or ['*']",
			}, nil
		}
	}
	if env.Kind == message.KindResponse {
		if len(env.Audience) != 1 {
			return khar.Outcome{
				RejectReason: message.HarnessResponseAudienceInvalid,
				Detail:       "kind=response requires audience cardinality 1",
			}, nil
		}
	}

	// (6) visibility ↔ audience consistency — proto-layer1 §4.1.3.
	if env.Visibility == message.VisibilityPrivate && hasWildcard {
		return khar.Outcome{
			RejectReason: message.HarnessVisibilityAudienceInvalid,
			Detail:       "visibility=private with audience=['*'] is semantically inconsistent",
		}, nil
	}

	// (7) response.parent_id non-null — One Law extra-strong constraint.
	if env.Kind == message.KindResponse && env.ParentID == "" {
		return khar.Outcome{
			RejectReason: message.HarnessResponseMissingParentID,
			Detail:       "kind=response requires non-empty parent_id",
		}, nil
	}

	// (8) unknown top-level field fail-closed — proto-layer0 §7.3.
	// Enforced only when the caller plumbed the raw envelope JSON via
	// CtxWithRawEnvelope. Struct-based Go callers cannot drift on field
	// set (the envelope struct fixes the schema at compile time) so the
	// raw-JSON plumb is opt-in and skipped when absent.
	if raw := RawEnvelopeFromCtx(ctx); len(raw) > 0 {
		if out, err := checkUnknownTopLevelFields(raw); err != nil {
			return khar.Outcome{}, err
		} else if !out.Continue() {
			return out, nil
		}
	}

	return khar.Outcome{}, nil
}

// rejectFieldMissing returns the proto-layer1 §2.2 reject for missing
// content fields.
func rejectFieldMissing(detail string) khar.Outcome {
	return khar.Outcome{
		RejectReason: message.HarnessEnvelopeFieldMissing,
		Detail:       detail,
	}
}

// allowedTopLevelEnvelopeKey reports whether key is in the union of
// proto-layer0 §1.1 content fields ∪ §1.2 delivery metadata ∪ §1.3
// store-derived columns.
func allowedTopLevelEnvelopeKey(key string) bool {
	switch key {
	// L0 §1.1 content fields. sender is nested; sender.{kind,id,name}
	// flatten into the same top-level "sender" key.
	case "id", "ts", "ts_received", "channel_id", "sender", "kind", "type",
		"payload", "parent_id", "correlation_id", "doc_refs",
		"visibility", "audience", "not_before", "expires_at":
		return true
	// L0 §1.2 delivery metadata.
	case "delivered_at", "delivery_failed_at", "last_error", "attempts":
		return true
	// L0 §1.3 store-derived columns.
	case "is_terminal", "seq":
		return true
	}
	return false
}

// checkUnknownTopLevelFields decodes raw JSON enough to enumerate
// top-level keys and rejects when any unknown key is present.
func checkUnknownTopLevelFields(raw []byte) (khar.Outcome, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		// Caller plumbed malformed JSON; this is a programming error
		// rather than a protocol reject — surface as error.
		return khar.Outcome{}, err
	}
	for k := range top {
		if !allowedTopLevelEnvelopeKey(k) {
			return khar.Outcome{
				RejectReason: message.HarnessEnvelopeUnknownField,
				Detail:       "envelope top-level field not in spec: " + k,
			}, nil
		}
	}
	return khar.Outcome{}, nil
}
