package accessdoor

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// door is the bare invoker — sealed inside the package (New hands out only a
// Minter, mirroring harness never handing out the bare chain). It holds Deps and
// runs the decision tree; the welded caller arrives as a parameter, never read
// off the wire.
type door struct{ deps Deps }

// driver resolves a kind to its Driver, returning a Go error when none is
// registered. A missing driver is an ASSEMBLY DEFECT (New fail-fasts on the day-1
// kind, so a kind reaching here without a driver means a kind was added without
// its driver) — a Go error, never a driver_error verdict.
func (d *door) driver(kind resourcespec.ResourceKind) (resourcespec.Driver, error) {
	drv, ok := d.deps.Drivers[kind]
	if !ok {
		return nil, errors.New("accessdoor: no driver for kind " + string(kind))
	}
	return drv, nil
}

// invoke runs the decision tree for one welded caller. ingress has already run
// (boundHandle.Invoke → ingress → day1OpsOverreach → invoke), so op/args/grant
// are structurally valid here.
//
// Two error channels, deliberately distinct (opus-F4 / codex):
//   - a Go error is returned ONLY for an assembly defect (no driver for a kind),
//     infrastructure failure (store broken at resolve/authorize), or the
//     caller's OWN cancellation surfacing mid-EXECUTE (executeFailure — a
//     driver_error there would blame the driver for the caller's hand);
//   - every other failure inside the resolve→authorize→EXECUTE pipeline is a
//     verdict (Outcome.RejectReason, nil error), including an executor failure
//     (driver_error). Folding EXECUTE failures into Go errors would leave
//     driver_error unproducible — the bug v1 shipped.
func (d *door) invoke(ctx context.Context, caller actor.ActorID, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (Outcome, error) {
	meta, exists, err := d.deps.Registry.Resolve(ctx, id)
	if err != nil {
		return Outcome{}, err // store broken = infrastructure-level, Go error
	}

	if op == access.OpCreate {
		if exists {
			return Outcome{RejectReason: access.AlreadyExists}, nil
		}
		member, err := d.deps.Membership.IsMember(ctx, caller)
		if err != nil {
			return Outcome{}, err
		}
		if !member {
			return Outcome{RejectReason: access.AccessDenied}, nil
		}
		// kind is hardcoded to KindKV day-1 (single driver). With multiple drivers
		// the kind is set by the handle the caller holds (forward §12.1.5), welded
		// by the caps facade — never derived from the ResourceID (that would be the
		// kernel interpreting an opaque name).
		if err := d.deps.Registry.Create(ctx, id, resourcespec.KindKV, caller, args); err != nil {
			return createVerdict(ctx, err) // one collision vocabulary, two loci — shared mapping
		}
		return Outcome{}, nil
	}

	if !exists {
		return Outcome{RejectReason: access.ResourceNotFound}, nil
	}

	// ---- A8 two halves unioned: actor entry ∪ (members entry ∧ current member) ----
	allowed, err := d.deps.Registry.ActorAllows(ctx, caller, id, op)
	if err != nil {
		return Outcome{}, err
	}
	if !allowed {
		mAllow, err := d.deps.Registry.MembersAllow(ctx, id, op)
		if err != nil {
			return Outcome{}, err
		}
		if mAllow {
			isM, err := d.deps.Membership.IsMember(ctx, caller) // late-binding: resolved at check time
			if err != nil {
				return Outcome{}, err
			}
			allowed = isM
		}
	}
	if !allowed {
		return Outcome{RejectReason: access.AccessDenied}, nil
	}

	switch op {
	case access.OpRead:
		drv, err := d.driver(meta.Kind)
		if err != nil {
			return Outcome{}, err
		}
		val, found, rerr := drv.Read(ctx, id)
		if rerr != nil {
			return executeFailure(ctx, rerr)
		}
		return Outcome{Value: val, Found: found}, nil

	case access.OpWrite:
		drv, err := d.driver(meta.Kind)
		if err != nil {
			return Outcome{}, err
		}
		if werr := drv.Write(ctx, id, args); werr != nil {
			return executeFailure(ctx, werr)
		}
		return Outcome{}, nil

	case access.OpSet:
		// set's executor is the substrate authz manager (Registry), not a driver.
		// grant is non-nil and structurally valid (ingress), so the deref is safe.
		if serr := d.deps.Registry.SetGrant(ctx, id, *grant); serr != nil {
			return executeFailure(ctx, serr) // executor-authored (reason.go)
		}
		return Outcome{}, nil

	case access.OpDelete:
		drv, err := d.driver(meta.Kind)
		if err != nil {
			return Outcome{}, err
		}
		// bytes first, existence row last: a mid-flight failure leaves the resource
		// resolvable (retryable) or resolved-but-empty (legal) — never a row gone
		// with bytes stranded under someone's name.
		if derr := drv.Delete(ctx, id); derr != nil {
			return executeFailure(ctx, derr)
		}
		if derr := d.deps.Registry.Delete(ctx, id); derr != nil {
			return executeFailure(ctx, derr)
		}
		return Outcome{}, nil

	default:
		// Unreachable: ingress ParseOperation already gated op. Defensive.
		return Outcome{}, ErrMalformed
	}
}
