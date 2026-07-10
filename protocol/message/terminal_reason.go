package message

// TerminalFailureReason is the closed set of reasons stamped into a
// kind=response payload's `reason` field when status=failed. It is the
// closure ADT's failure vocabulary (INVARIANT-10), frozen at exactly 3
// values.
//
// Prior-art anchor (Erlang BEAM monitor/call): these are the substrate +
// caller produced DOWN-reasons of the request/response lifecycle. Each word
// is welded to one durable FACT; who may author it = whoever durably holds
// that fact (期12 义务归位 — the harness's response-pairing gate enforces the
// resulting (author, word) matrix):
//
//   - unanswered_timeout      — FACT: the request reached its declared
//     deadline (ExpiresAt, stamped into truth at write time) with no final
//     answer. TWO legitimate observers of this one fact: the caller's own
//     fast-path timer / its own Cancel (author #2), and the SUBSTRATE's
//     expiry reaper — deadline closure is a substrate obligation (a caller
//     may crash, deregister, or be deliberately kept down; "every open
//     request eventually closes" is only guaranteeable by the substrate), so
//     the caller's timer is the latency optimisation of the same
//     enforcement, not the obligation's owner. Provenance (which observer
//     wrote it) rides sender + payload `closed_by`, NEVER a separate word —
//     one fact, one word. NOT an assertion that the receiver is objectively
//     silent. = Erlang gen_server:call `timeout`. A receiver's genuine late
//     final simply arrives after the request already closed and rejects as
//     a terminal duplicate (surfacing late answers is a domain
//     observability concern).
//   - receiver_unavailable    — FACT: the SUBSTRATE positively observed
//     the receiver's death (cell goroutine panic or relay/wire disconnect),
//     published it as the obs down edge, and a watcher materialised a
//     terminal. = Erlang monitor DOWN `noproc` / `noconnection`.
//   - receiver_internal_error — FACT: an in-handler failure the receiver
//     itself reports (author #1 tail). = the callee's own exit reason.
//
// The substrate-authored arms are narrowly fact-gated — the substrate only
// enforces facts written in truth (an observed death edge, a declared
// deadline); it never guesses "slow", never invents a deadline, and never
// revives an actor to close an account. The old generic "system terminal
// fallback" author (any reason, any parent) stays deleted.
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
