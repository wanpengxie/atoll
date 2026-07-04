package behavior

import "github.com/wanpengxie/atoll/protocol/message"

// There is no separate CorrelationEntry/State: the original request envelope
// (keyed by its id) is the single source of truth (id / expires_at /
// correlation_id / parent_id all live on it). "pending" is presence in the
// cache; "done" is removal on the terminal write. A parallel entry duplicating
// those envelope fields would be redundant state. parent_id on a response is the
// request id GEOMETRICALLY (BuildResponseFromRequest reads request.ID), not a
// caller-supplied key that could disagree.

// CorrelationID derives the correlation id: chain wins when the caller has one
// pinned (e.g. a task chain that must survive a request envelope that carries
// none), otherwise rootID (the envelope's own correlation id, or its own id
// when that is empty).
func CorrelationID(chain, rootID message.ID) message.ID {
	if chain != "" {
		return chain
	}
	return rootID
}
