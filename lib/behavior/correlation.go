package behavior

import "github.com/wanpengxie/ActOS/kernel/message"

// CorrelationKey is the lookup key an adapter uses to find an in-flight
// request — it is the request envelope.id (the "request_id" wire form).
type CorrelationKey message.ID

// String returns the wire form.
func (k CorrelationKey) String() string { return string(k) }

// There is no separate CorrelationEntry/State: an adapter cell tracks its
// in-flight requests by caching the original request envelope (keyed by
// CorrelationKey) — that cached envelope is the single source of truth
// (id / expires_at / correlation_id / parent_id all live on it). "pending" is
// presence in the cache; "done" is removal on the terminal write. A parallel
// entry duplicating those envelope fields would be redundant state.
