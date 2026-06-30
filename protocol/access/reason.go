package access

// FailureReason is the closed set of access failure verdicts, derived by exhausting the
// stages of one access invocation (resolve → authorize → execute → return) — §2.2's
// 施动 pipeline. Both ends (caller actor + 门后 executor, across the wire) must agree on
// it. The kernel owns only this frozen set; reason→HTTP is a binding concern. A structurally
// MALFORMED request is rejected at ENTRY (before resolve) as a protocol error — see the
// door's ingress shape check (§3.4, runtime — not a proto method) — so "malformed" is
// deliberately NOT a FailureReason.
type FailureReason string

const (
	// ResourceNotFound — RESOLVE stage: the name does not resolve to a driver/object (for
	// read/write/set/delete on an absent id). Door-authoritative. = ENOENT. (Distinct from a
	// resolved-but-empty read, which is found=false in the outcome — §3.5, not a failure.)
	ResourceNotFound FailureReason = "resource_not_found"

	// AlreadyExists — RESOLVE stage, the DUAL of ResourceNotFound: op=create on a name that
	// already resolves. Door-authoritative. = EEXIST / K8s 409. Makes create an atomic test-
	// and-set on existence (no silent re-create + controller grab). The resolve stage thus has
	// both verdicts: absent-when-expected-present (not_found) and present-when-expected-absent.
	AlreadyExists FailureReason = "already_exists"

	// AccessDenied — AUTHORIZE stage: the caller is not authorized — for object ops
	// (read/write/set/delete) R.allows(caller, resource, op) is false; for create the caller is
	// not a member of the container channel (§2.4, two loci). Day-1 the object check is the
	// by-identity R lookup; a presented-token path (§11) reuses the same verdict. Door-
	// authoritative. = EACCES.
	AccessDenied FailureReason = "access_denied"

	// DriverError — EXECUTE stage: the operation's executor failed performing it. The
	// executor is the driver for content/lifecycle ops (create/read/write/delete) and the
	// substrate authz manager for set (writing R). Executor-authored.
	DriverError FailureReason = "driver_error"

	// OutcomeUnknown — RETURN/transport stage: completion was not confirmed (mid-op
	// disconnect / timeout). The substrate NEVER fakes success (access stays first-class
	// async, outcome honestly surfaced — forward §12.9 red-line②). Only the cross-wire/
	// async path produces it (in-proc is synchronous).
	OutcomeUnknown FailureReason = "outcome_unknown"
)

// allFailureReasons backs IsValidFailureReason. UNEXPORTED: the closed-set contract is
// the predicate, not a mutable enumeration (an exported slice would let an importer
// rewrite the protocol closed set).
var allFailureReasons = []FailureReason{
	ResourceNotFound, AlreadyExists, AccessDenied, DriverError, OutcomeUnknown,
}

// String returns the wire form of r.
func (r FailureReason) String() string { return string(r) }

// IsValidFailureReason reports whether r is a member of the frozen closed set. This is the
// enforcement predicate — outcome validation MUST use it rather than re-deriving membership
// from a borrowed enumeration slice.
func IsValidFailureReason(r FailureReason) bool {
	for _, x := range allFailureReasons {
		if x == r {
			return true
		}
	}
	return false
}
