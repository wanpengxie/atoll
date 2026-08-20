package behavior

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/protocol/message"
)

// call.go is the CALL face's request builder (closure author#2's write-side
// vocabulary). The former actor-private Caller closure manager (per-request
// AfterFunc timers) was拆删 in 期12 S6: its only consumers migrated to the
// actorbase engine's own callLedger (the caller-alive fast path) and the
// substrate expiry reaper (义务归位 — the durable deadline guarantee), so the
// helper survived only as an unconsumed twin of machinery that already
// exists. What remains here: RequestSpec/BuildRequest (the ONE home of the
// kind=request envelope literal) and ParseFinalStatus (the ledger's terminal
// classifier). Closure model (期12 义务归位): the declared ExpiresAt built
// here is a durable contract with the substrate — the expiry reaper is its
// guaranteed enforcer; a live caller's engine (callLedger) is the fast-path
// observer of the same fact.

// RequestSpec is the caller-supplied shape of a kind=request envelope —
// the call-face mirror of serve's ResponseSpec.
type RequestSpec struct {
	// ID is optional: empty = a fresh uuid. Callers with their own id scheme
	// (e.g. deterministic per-worker ids) override it.
	ID         message.ID
	Type       string // required
	Payload    json.RawMessage
	Audience   message.Audience // required
	Visibility message.Visibility
	// Cause is REQUIRED: why this request exists. See the Cause type — the
	// parent and correlation fields it replaced were optional, and optional is
	// how "nobody said" became "there is none" on every request written to
	// serve another one.
	Cause message.Cause
	// ExpiresAt is the request's declared deadline — durable truth the
	// substrate reaper enforces (a live caller's own timer merely races it).
	ExpiresAt *int64
	// ClientFingerprint is shell-ingress persistence metadata. Builders never
	// copy it into the protocol envelope; actorbase carries it only on the
	// harness write context.
	ClientFingerprint string
}

// BuildRequest assembles a kind=request envelope — the ONE home for request
// construction defaults, mirroring serve's BuildResponseFromRequest. Bindings
// stamp transport-edge fields (TSReceived) after build; this builder never
// writes.
//
// Sender / ChannelID are left ZERO: identity is substrate-injected by the pen
// at write time (sealed-pen). The calling actor's id is welded onto the pen, so
// the builder neither knows nor fills it.
func BuildRequest(
	clock func() time.Time,
	spec RequestSpec,
) (*message.Envelope, error) {
	if strings.TrimSpace(spec.Type) == "" {
		return nil, fmt.Errorf("behavior: BuildRequest type required")
	}
	if len(spec.Audience) == 0 {
		return nil, fmt.Errorf("behavior: BuildRequest audience required")
	}
	if !spec.Cause.Stated() {
		return nil, fmt.Errorf("behavior: BuildRequest cause required: say message.From(<the message this is written to serve>), or message.Root() when this errand starts here")
	}
	id := spec.ID
	if id == "" {
		id = message.ID(uuid.NewString())
	}
	parentID, correlationID := spec.Cause.Resolve(id)
	return &message.Envelope{
		ID:            id,
		TS:            clock().UnixMilli(),
		Kind:          message.KindRequest,
		Type:          strings.TrimSpace(spec.Type),
		Audience:      spec.Audience,
		Payload:       spec.Payload,
		Visibility:    spec.Visibility,
		ParentID:      parentID,
		CorrelationID: correlationID,
		ExpiresAt:     spec.ExpiresAt,
	}, nil
}

// ParseFinalStatus extracts payload.status (defensively: empty/malformed
// payloads read as "") and reports whether it is a Layer 1 final
// (completed/failed). 期12 S6 inlined the former ParseResponseStatus helper —
// the ledger_call consumer is this function's only remaining caller chain.
func ParseFinalStatus(raw []byte) (string, bool) {
	var status string
	if len(raw) > 0 {
		var obj struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(raw, &obj); err == nil {
			status = strings.TrimSpace(obj.Status)
		}
	}
	return status, message.IsFinalStatus(status)
}
