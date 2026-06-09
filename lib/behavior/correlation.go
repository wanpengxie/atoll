package behavior

import "github.com/wanpengxie/ActOS/protocol/message"

// CorrelationKey is an alias for message.ID used as the in-flight
// correlation anchor — it equals the request envelope.id (wire form:
// request_id).
type CorrelationKey message.ID

// String returns the wire form.
func (k CorrelationKey) String() string { return string(k) }

// There is no separate CorrelationEntry/State: the original request envelope
// (keyed by CorrelationKey) is the single source of truth (id / expires_at /
// correlation_id / parent_id all live on it). "pending" is presence in the
// cache; "done" is removal on the terminal write. A parallel entry duplicating
// those envelope fields would be redundant state.

// CorrelationID derives the correlation id for a meta-tool request from
// a trigger payload's fields.
func CorrelationID(triggerCorrelationID, envelopeCorrelationID, envelopeID message.ID) message.ID {
	if triggerCorrelationID != "" {
		return triggerCorrelationID
	}
	if envelopeCorrelationID != "" {
		return envelopeCorrelationID
	}
	return envelopeID
}
