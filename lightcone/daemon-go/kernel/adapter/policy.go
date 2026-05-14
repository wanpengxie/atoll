package adapter

import "time"

// ErrorPolicy is the v4 adapter framework F3 contract — adapter-side
// timeout / fallback policy.
//
// Each adapter declares:
//
//   - PendingDeadline: when the in-flight envelope's response is
//     considered "long-pending" and the long-pending scheduler stamps a
//     TerminalUnansweredTimeout / TerminalAdapterDefaultTimeout
//     response.
//   - ResponseFallback: optional pre-stamped response payload the
//     scheduler uses on timeout (per L1 §6.4).
//
// kernel/adapter only defines the contract; concrete policy lives in
// each adapter implementation.
type ErrorPolicy interface {
	PendingDeadline() time.Duration
	ResponseFallback() []byte
}
