package message

// TerminalFailureReason is the closed set of reasons stamped into a
// kind=response payload's `reason` field when status=failed. It is the
// closure ADT's failure vocabulary (INVARIANT-10), frozen at exactly 3
// values.
//
// Prior-art anchor (Erlang BEAM monitor/call): these are the substrate +
// caller produced DOWN-reasons of the request/response lifecycle, and they
// map 1:1 onto the closure three-author model:
//
//   - unanswered_timeout      — author #2: the request SENDER's own
//     blessed-call timer fired. Producer is the caller, NOT a global/system
//     scanner. Semantics = "I, the caller, am no longer waiting" (a
//     voluntary, caller-scoped abandonment, NOT an assertion that the
//     receiver is objectively silent). = Erlang gen_server:call `timeout`.
//     A receiver's genuine late final therefore simply arrives after the
//     request already closed and rejects as a terminal duplicate (surfacing
//     late answers for monitoring is a domain observability concern).
//   - receiver_unavailable    — author #3: the SUBSTRATE positively observed
//     the receiver's death (cell goroutine panic or relay/wire disconnect),
//     published it as the obs down edge, and a watcher materialised a
//     terminal. The substrate never guesses "slow"; it only materialises death
//     it observed. = Erlang monitor DOWN `noproc` / `noconnection`.
//   - receiver_internal_error — author #1 tail: an in-handler failure the
//     receiver itself reports. = the callee's own exit reason.
//
// NOTE: harness reject reasons (the write engine's errno) and install
// reasons (the install engine's errno) are NOT here — they are layer-1
// engine vocabulary that co-evolves with its engine and lives outside this
// package. reason→HTTP-status mapping (strerror) is a binding concern, also
// outside this package. The kernel owns only this frozen ADT closed set.
type TerminalFailureReason string

// TerminalFailureReason closed set (INVARIANT-10, frozen).
const (
	TerminalUnansweredTimeout     TerminalFailureReason = "unanswered_timeout"
	TerminalReceiverInternalError TerminalFailureReason = "receiver_internal_error"
	TerminalReceiverUnavailable   TerminalFailureReason = "receiver_unavailable"
)

// allTerminalFailureReasons backs IsValidTerminalFailureReason. UNEXPORTED:
// the closed-set contract is the predicate, not a mutable enumeration.
var allTerminalFailureReasons = []TerminalFailureReason{
	TerminalUnansweredTimeout,
	TerminalReceiverInternalError,
	TerminalReceiverUnavailable,
}

// String returns the wire form of r.
func (r TerminalFailureReason) String() string { return string(r) }

// IsValidTerminalFailureReason reports whether r is a member of the frozen
// closed set. This is the enforcement predicate — closure validation MUST use
// it rather than re-deriving membership from a borrowed enumeration slice.
func IsValidTerminalFailureReason(r TerminalFailureReason) bool {
	for _, x := range allTerminalFailureReasons {
		if x == r {
			return true
		}
	}
	return false
}
