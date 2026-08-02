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
// ErrMalformed (wrapping a detail). It is the structural layer only.
// The cluster's members are named pure functions so each rule is
// independently unit-testable, mirroring a harness step's testability without
// borrowing its machinery.
func ingress(op access.Operation, id resource.ResourceID, args []byte) error {
	if err := checkOperation(op); err != nil {
		return err
	}
	if err := checkResourceID(id); err != nil {
		return err
	}
	return checkArgs(op, args)
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

// checkArgs enforces the op×Args rule (proto invocation.go): delete is by-id,
// so it carries no Args (nil = no operand).
// create/write take Args as their operand (nil or present-but-empty both legal).
// read's Args (a selector) is day-1 IGNORED, not rejected: kv has no sub-selector
// (granularity is the driver's model), so the selector is meaningless here; when
// a selector driver lands, Driver.Read grows an args parameter additively — not a
// new op — and this check gains a read case then, not before.
func checkArgs(op access.Operation, args []byte) error {
	if op == access.OpDelete && args != nil {
		return fmt.Errorf("%w: op %q carries no Args operand", ErrMalformed, op)
	}
	return nil
}
