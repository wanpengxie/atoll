package coagent

import (
	"fmt"
	"strings"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// buildBaseEnvelope assembles the envelope fields shared across all
// three subcommands. Per-subcommand callers then post-process
// (audience resolution, type defaults, payload-text fallback, etc.).
//
// Responsibilities:
//
//   - id      → cfg.NewID() when caller omitted
//   - ts      → cfg.Clock() unix millis
//   - channel_id → --channel-id || tc.ChannelID
//   - sender.id   → --sender-id || tc.SelfID
//   - sender.kind → tc.SenderKind (informational; harness force-writes from registry)
//   - kind    → fixed per-subcommand
//   - type / payload / parent / doc_refs / not_before / expires_at /
//     correlation_id / visibility → from parsed flags (no defaults
//     applied here; per-subcommand callers / harness normalize fill
//     them later)
//
// SendOptions are also populated here so subcommand callers don't
// re-thread the trigger context / fencing fields manually.
//
// The function purposefully does NOT validate channel-id or sender.id
// — that's harness Step 2 / Step 3's job; surfacing the rejects via
// the binding gives one consistent error path.
func buildBaseEnvelope(
	cfg Config,
	cf *commonFlags,
	tc TurnCtx,
	kind v4types.Kind,
) (*v4types.Envelope, SendOptions, error) {
	now := cfg.Clock()

	id := cfg.NewID()
	ts := now.UnixMilli()

	channelID := cf.ChannelID
	if channelID == "" {
		channelID = tc.ChannelID
	}

	senderID := cf.SenderID
	if senderID == "" {
		senderID = tc.SelfID
	}

	senderKind := v4types.SenderKind(tc.SenderKind)
	// Don't validate senderKind here; harness step 3 force-writes from
	// registry. We pass it through SendOptions.DeclaredSenderKind so
	// the harness rejects sender_kind_mismatch when caller explicitly
	// declared the wrong value.

	visibility, verr := resolveVisibility(cf)
	if verr != nil {
		return nil, SendOptions{}, verr
	}

	payload, perr := parsePayload(cf.Payload)
	if perr != nil {
		return nil, SendOptions{}, perr
	}

	docRefs, derr := parseDocRefs(cf.DocRefs)
	if derr != nil {
		return nil, SendOptions{}, derr
	}

	notBefore, nbErr := parseTime(cf.NotBefore, now)
	if nbErr != nil {
		return nil, SendOptions{}, fmt.Errorf("--not-before: %w", nbErr)
	}
	expiresAt, exErr := parseTime(cf.ExpiresAt, now)
	if exErr != nil {
		return nil, SendOptions{}, fmt.Errorf("--expires-at: %w", exErr)
	}

	env := &v4types.Envelope{
		ID:         id,
		TS:         ts,
		ChannelID:  channelID,
		Sender:     v4types.Sender{Kind: senderKind, ID: senderID},
		Kind:       kind,
		Type:       cf.Type,
		Payload:    payload,
		ParentID:   cf.Parent,
		Visibility: visibility,
		Audience:   parseAudience(cf.Audience),
		DocRefs:    docRefs,
		NotBefore:  notBefore,
		ExpiresAt:  expiresAt,
	}

	// --correlation-id handling per L2 §3.3.1:
	//   - "new"      → CLI generates a UUID and stores it as the
	//                  ExplicitCorrelationID (binding propagates it as
	//                  the harness 2nd-tier fallback)
	//   - "<value>"  → CLI writes the value directly into
	//                  envelope.correlation_id (joins an existing chain)
	//   - absent     → trigger-ctx propagation (1st-tier fallback)
	opts := SendOptions{
		DeclaredSenderKind:   senderKind,
		FencingToken:         tc.FencingToken,
		TriggerCorrelationID: tc.TriggerCorrelationID,
	}
	switch strings.TrimSpace(cf.CorrelationID) {
	case "":
		// no override → harness picks up TriggerCorrelationID first
	case "new":
		// CLI generates fresh UUID; store on SendOptions so harness
		// normalize picks it up as 2nd-tier fallback (envelope.id-based
		// 3rd-tier is the harness's own escape valve).
		opts.ExplicitCorrelationID = cfg.NewID()
	default:
		env.CorrelationID = cf.CorrelationID
	}

	return env, opts, nil
}
