package message

// Kind is the v4 message ADT classifier (event / request / response).
//
// Once a message is written, kind is immutable.
type Kind string

// Kind enum — closed set.
const (
	KindEvent    Kind = "event"
	KindRequest  Kind = "request"
	KindResponse Kind = "response"
)

// allKinds backs ParseKind. UNEXPORTED: the closed-set contract is the
// ParseKind predicate, not a mutable enumeration slice.
var allKinds = []Kind{KindEvent, KindRequest, KindResponse}

// String returns the wire form.
func (k Kind) String() string { return string(k) }

// ParseKind resolves a canonical wire-form message-kind string against the
// closed set. Deserialization (wire / DB) MUST go through ParseKind rather than
// a bare message.Kind(string) cast so an out-of-set value cannot enter the ADT.
func ParseKind(raw string) (Kind, bool) {
	for _, k := range allKinds {
		if string(k) == raw {
			return k, true
		}
	}
	return "", false
}

// Visibility is the envelope `visibility` field — a 3-value closed set recording
// the sender's DECLARED intent for who in the channel should see this message.
// It lives in the envelope (not the payload) because who-may-see is a
// rule-managed ACL property of truth — the substrate's to enforce — not opaque
// data an actor interprets. Once written, visibility is immutable.
//
// Declared intent (what each value MEANS):
//   - public  — intended for every channel member.
//   - private — intended only for the sender + actors in audience.
//   - system  — protocol-internal metadata / intermediate output (agent.text
//     progress bubbles, placement notices, bootstrap events), intended to be
//     suppressed from the default UI view (still persisted as audit trail).
//
// ReadVisibleAfterSeq enforces this value before LIMIT: public is channel-wide,
// private is sender-or-audience, and system is absent from ordinary views.
// Delivery remains a separate question and follows explicit audience through
// ShouldDeliver, including for system requests.
type Visibility string

// Visibility enum — closed set.
const (
	VisibilityPublic  Visibility = "public"
	VisibilityPrivate Visibility = "private"
	VisibilitySystem  Visibility = "system"
)

// allVisibilities backs ParseVisibility. UNEXPORTED: the closed-set contract is
// the predicate, not a mutable enumeration slice.
var allVisibilities = []Visibility{VisibilityPublic, VisibilityPrivate, VisibilitySystem}

// String returns the wire form.
func (v Visibility) String() string { return string(v) }

// ParseVisibility resolves a canonical wire-form visibility string against the
// closed set. Deserialization MUST go through it rather than a bare cast.
func ParseVisibility(raw string) (Visibility, bool) {
	for _, v := range allVisibilities {
		if string(v) == raw {
			return v, true
		}
	}
	return "", false
}
