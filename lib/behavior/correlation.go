package behavior

import "github.com/wanpengxie/ActOS/protocol/message"

// There is no separate CorrelationEntry/State: the original request envelope
// (keyed by its id) is the single source of truth (id / expires_at /
// correlation_id / parent_id all live on it). "pending" is presence in the
// cache; "done" is removal on the terminal write. A parallel entry duplicating
// those envelope fields would be redundant state. parent_id on a response is the
// request id GEOMETRICALLY (BuildResponseFromRequest reads request.ID), not a
// caller-supplied key that could disagree.

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
