package accessdoor

import (
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
)

// ErrMalformed is the ingress protocol-error class: a structurally bad request
// rejected BEFORE resolve. It is a Go error, NOT a FailureReason — reason.go
// deliberately has no "malformed" value (a malformed request never reaches the
// resolve→authorize→execute pipeline the FailureReason set exhausts). Each
// concrete failure wraps it with a detail.
var ErrMalformed = errors.New("accessdoor: malformed invocation")

// ingress runs the shape-check cluster in order; any failure short-circuits to
// ErrMalformed (wrapping a detail). It is the structural layer only — the day-1
// {read,write} Ops narrowing is a SEPARATE authorization step (day1OpsOverreach),
// not folded in here: structure→error and overreach→verdict stay two distinct
// layers. The cluster's members are named pure functions so each rule is
// independently unit-testable, mirroring a harness step's testability without
// borrowing its machinery.
func ingress(op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) error {
	if err := checkOperation(op); err != nil {
		return err
	}
	if err := checkResourceID(id); err != nil {
		return err
	}
	if err := checkArgs(op, args); err != nil {
		return err
	}
	if err := checkGrant(op, grant); err != nil {
		return err
	}
	// checkGrant guarantees grant != nil exactly when op=set, so the deref is safe.
	if op == access.OpSet {
		if err := ValidateGrant(*grant); err != nil {
			return err
		}
	}
	return nil
}

// checkResourceID rejects an empty object name. The id is opaque to the kernel
// (never interpreted), but an invocation without an object is structurally not
// an access — rejecting it here keeps the store's own empty-id guard from being
// misclassified as a driver_error verdict.
func checkResourceID(id resource.ResourceID) error {
	if id == "" {
		return fmt.Errorf("%w: empty resource id", ErrMalformed)
	}
	return nil
}

// checkOperation gates op against the proto closed set (a value out of set means
// the wire/dispatch layer let a bare cast through).
func checkOperation(op access.Operation) error {
	if _, ok := access.ParseOperation(op.String()); !ok {
		return fmt.Errorf("%w: operation %q not in closed set", ErrMalformed, op)
	}
	return nil
}

// checkArgs enforces the op×Args rule (proto invocation.go): delete is by-id and
// set's operand is the typed Grant, so NEITHER carries Args (nil = no operand).
// create/write take Args as their operand (nil or present-but-empty both legal).
// read's Args (a selector) is day-1 IGNORED, not rejected: kv has no sub-selector
// (granularity is the driver's model), so the selector is meaningless here; when
// a selector driver lands, Driver.Read grows an args parameter additively — not a
// new op — and this check gains a read case then, not before.
func checkArgs(op access.Operation, args []byte) error {
	switch op {
	case access.OpDelete, access.OpSet:
		if args != nil {
			return fmt.Errorf("%w: op %q carries no Args operand", ErrMalformed, op)
		}
	}
	return nil
}

// checkGrant enforces the op×Grant rule: op=set ⟺ Grant present. set without a
// Grant, or any other op with one, is structurally malformed.
func checkGrant(op access.Operation, grant *access.Grant) error {
	if op == access.OpSet {
		if grant == nil {
			return fmt.Errorf("%w: op=set requires a Grant operand", ErrMalformed)
		}
		return nil
	}
	if grant != nil {
		return fmt.Errorf("%w: op %q carries no Grant operand", ErrMalformed, op)
	}
	return nil
}

// ValidateGrant is the runtime ingress function proto grant.go names ("enforced
// at the door's ingress step, runtime — not a proto method"). It checks ONLY the
// grant's STRUCTURE, never authorization (that is the decision tree's job):
//
//   - GranteeKind is in the closed set;
//   - GranteeKind=actor ⟺ Grantee non-empty; GranteeKind=members ⟺ Grantee empty;
//   - every op in Ops is an OBJECT op {read,write,set,delete} — create is a
//     container-locus verb, NEVER an R grant, so its presence is a structural
//     violation, not merely over-broad.
//
// Any failure → ErrMalformed (a protocol error, not a verdict). The day-1
// {read,write} narrowing is intentionally NOT here — that is an authorization
// verdict (day1OpsOverreach), kept out of the structural layer.
func ValidateGrant(g access.Grant) error {
	if _, ok := access.ParseGranteeKind(g.GranteeKind.String()); !ok {
		return fmt.Errorf("%w: grantee_kind %q not in closed set", ErrMalformed, g.GranteeKind)
	}
	switch g.GranteeKind {
	case access.GranteeActor:
		if g.Grantee == "" {
			return fmt.Errorf("%w: grantee_kind=actor requires a Grantee", ErrMalformed)
		}
	case access.GranteeMembers:
		if g.Grantee != "" {
			return fmt.Errorf("%w: grantee_kind=members forbids a Grantee", ErrMalformed)
		}
	}
	for _, op := range g.Ops {
		switch op {
		case access.OpRead, access.OpWrite, access.OpSet, access.OpDelete:
			// object op — structurally allowed
		default:
			return fmt.Errorf("%w: grant op %q is not an object op (create is container-locus, never an R grant)", ErrMalformed, op)
		}
	}
	return nil
}

// day1OpsOverreach is the post-ingress authorization step (NOT a structural
// check): on op=set, day-1 narrows the granted Ops to {read,write}. Granting
// set/delete is structurally legal (ValidateGrant lets it through) but delegates
// control, so day-1 it is OVERREACH → access_denied verdict — a REJECT, not a
// silent clamp. ok reports whether the step applies (op=set with a grant); over
// reports the overreach.
func day1OpsOverreach(op access.Operation, grant *access.Grant) (over bool, ok bool) {
	if op != access.OpSet || grant == nil {
		return false, false
	}
	for _, o := range grant.Ops {
		if o != access.OpRead && o != access.OpWrite {
			return true, true
		}
	}
	return false, true
}
