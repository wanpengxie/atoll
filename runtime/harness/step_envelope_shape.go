package harness

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/protocol/message"
)

// stepEnvelopeShape implements proto-layer1 §2.2 — envelope shape
// validation. It covers seven kinds of wire-level guards (round 3
// cluster F insertion):
//
//  1. content fields present (proto-layer0 §1.1)
//  2. payload wellformedness — non-empty payload must be valid JSON and not
//     the null literal (L0 §2.2: payload={} legal, payload=null not; empty
//     stays legal here — Step Normalize fills the {} default)
//  3. envelope.channel_id == the harness-bound channel (unconditional)
//  4. kind ∈ {event, request, response}
//  5. visibility (when non-empty) ∈ {public, private, system} — Step
//     Normalize fills the default when caller leaves it empty.
//  6. audience cardinality + wildcard ban (proto-layer0 §2.3)
//  7. response.parent_id non-null
//
// (The proto-layer0 §7.3 unknown-top-level-field fail-closed reject is NOT a
// harness step: it rides the Envelope type — message.Envelope.UnmarshalJSON
// rejects out-of-set keys at every wire decode, so by the time a decoded
// envelope reaches this chain the field set is already closed; in-process Go
// struct callers cannot drift on field set at all.)
//
// The step runs after CallerAuth and before SenderConsistent/Normalize, so
// downstream stages never see a malformed envelope.
type stepEnvelopeShape struct{ deps Deps }

func newStepEnvelopeShape(d Deps) step { return &stepEnvelopeShape{deps: d} }

func (s *stepEnvelopeShape) ID() stepID { return StepEnvelopeShape }

func (s *stepEnvelopeShape) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
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
	case env.TS == 0:
		return rejectFieldMissing("envelope.ts required"), nil
	}

	// (2) payload wellformedness — L0 §2.2: payload={} legal, payload=null
	// not. Truth is append-only, so a malformed payload admitted here is a
	// protocol-illegal row FOREVER (and an in-process json.RawMessage("{bad")
	// would additionally break every later delivery marshal of the committed
	// row). Empty payload is legal at this step — Step Normalize fills the {}
	// default; the guard covers only what a caller actually supplied.
	if len(env.Payload) > 0 {
		if !json.Valid(env.Payload) {
			return outcome{
				RejectReason: HarnessPayloadInvalid,
				Detail:       "envelope.payload is not valid JSON",
			}, nil
		}
		if string(bytes.TrimSpace(env.Payload)) == "null" {
			return outcome{
				RejectReason: HarnessPayloadInvalid,
				Detail:       "envelope.payload=null is not legal (L0 §2.2); omit payload or send {}",
			}, nil
		}
	}

	// (3) channel_id pinned to the harness-bound channel — UNCONDITIONAL.
	// This harness IS the single writer of deps.ChannelID's log, so a row
	// whose channel_id names a different channel is truth corruption (a
	// channel-A log holding a row that claims channel B). The guard is
	// structural and must NOT depend on the caller-context being plumbed —
	// substrate truth integrity cannot be left to downstream behaviour.
	// (StepCallerAuth separately checks the caller was authenticated for this
	// channel — an ACL concern, distinct from this content-vs-binding guard.)
	if env.ChannelID != s.deps.ChannelID {
		return outcome{
			RejectReason: HarnessChannelMismatch,
			Detail:       "envelope.channel_id does not match the harness-bound channel",
		}, nil
	}

	// (4) kind closed set — proto-layer0 §2.1.
	switch env.Kind {
	case message.KindEvent, message.KindRequest, message.KindResponse:
	default:
		return outcome{
			RejectReason: HarnessKindInvalid,
			Detail:       "envelope.kind not in {event, request, response}",
		}, nil
	}

	// (5) visibility closed set — proto-layer0 §2.4.
	// Empty visibility is legal here (Step Normalize defaults to public).
	if env.Visibility != "" &&
		env.Visibility != message.VisibilityPublic &&
		env.Visibility != message.VisibilityPrivate &&
		env.Visibility != message.VisibilitySystem {
		return outcome{
			RejectReason: HarnessVisibilityInvalid,
			Detail:       "envelope.visibility not in {public, private, system}",
		}, nil
	}

	// (6) audience wildcard ban — proto-layer0 §2.3 (pure format, no
	// channel truth). Wildcard "*" was removed from the audience closed
	// set (owner reframed addressing as Erlang-style explicit `pid !
	// msg`); every audience entry MUST be a literal actor_id.
	//
	// Audience EMPTINESS and request/response cardinality are NOT
	// validated here: empty/cardinality checks all live in
	// StepKindAndAudience (a single validation centre). The substrate does
	// not resolve a default audience — the caller must supply a named
	// audience; an empty one is rejected at the Kind+Audience step.
	for _, id := range env.Audience {
		if string(id) == "*" {
			return outcome{
				RejectReason: HarnessAudienceWildcardForbidden,
				Detail:       `envelope.audience wildcard "*" is not allowed; enumerate explicit actor ids`,
			}, nil
		}
	}

	// (7) response.parent_id non-null — One Law extra-strong constraint.
	if env.Kind == message.KindResponse && env.ParentID == "" {
		return outcome{
			RejectReason: HarnessResponseMissingParent,
			Detail:       "kind=response requires non-empty parent_id",
		}, nil
	}

	return outcome{}, nil
}

// rejectFieldMissing returns the proto-layer1 §2.2 reject for missing
// content fields.
func rejectFieldMissing(detail string) outcome {
	return outcome{
		RejectReason: HarnessEnvelopeFieldMissing,
		Detail:       detail,
	}
}

