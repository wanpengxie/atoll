package accessdoor

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// ErrOpNotInScope is the CATEGORY-ERROR class: a well-formed op that
// does not exist in this handle's locus (op=set on an actor-scoped handle — there
// is no R to write, and that absence IS the scope law). It is a PROTOCOL error,
// NOT a verdict: there is no possible world in which it is authorized — even the
// owner, the sole full-rights party, is refused — so access_denied would lie
// about the law (it invites the caller to seek a grant that can never exist), and
// ErrMalformed would stretch "bad wire shape" (EINVAL) over "op×locus not
// applicable" (ENOTSUP). Three distinct caller reactions, three distinct signals:
// fix-your-request (ErrMalformed) vs seek-grant-or-give-up (access_denied) vs
// never-retry-never-seek-grant (ErrOpNotInScope).
var ErrOpNotInScope = errors.New("accessdoor: operation not in this handle's scope")

// boundStateHandle is an AccessHandle welded to ONE owner — the actor-scoped
// (collapsed) implementation. It is the access-plane twin of boundHandle: same
// AccessHandle contract, same door, same Outcome/verdict closed set — the caps
// facade / liveness wrapper / port arm treat both handles alike (structural
// polymorphism, zero branch). The difference is entirely the scope law's value at
// the degenerate point: the reachable set is structurally ≡ {owner}, so there is
// no R query, no membership check, no DriverTable routing — the owner is the
// namespace coordinate, welded at mint, never read off the wire.
type boundStateHandle struct {
	door  *door
	owner actor.ActorID
}

// Invoke runs the actor-scoped ingress (structure → ErrMalformed / set →
// ErrOpNotInScope), then the collapsed decision tree under the welded owner.
// There is no day1OpsOverreach step: that narrows an op=set grant, and set does
// not exist in this locus (ingressState rejects it before the tree).
func (h boundStateHandle) Invoke(ctx context.Context, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (Outcome, error) {
	if err := ingressState(op, id, args, grant); err != nil {
		return Outcome{}, err
	}
	return h.door.invokeActorScoped(ctx, h.owner, op, id, args)
}

// ingressState is the actor-scoped ingress — the four-step decision order,
// order pinned. It mirrors the channel-scoped ingress's structural cluster
// but diverges on set, which is a category error here rather than a grant-bearing
// op:
//
//	① closed set (mirror checkOperation): out of set = wire-shape fault → ErrMalformed;
//	② ResourceID non-empty (mirror checkResourceID): → ErrMalformed;
//	③ op=set → ErrOpNotInScope (in the closed set, but this locus has no R to
//	   write = category error; decided BEFORE the op×shape rule and regardless of
//	   grant shape, so a set carrying a bogus grant still reads as "not in scope",
//	   never ErrMalformed);
//	④ op×shape (the SAME named checks the channel-scoped ingress runs — one
//	   rule, one wording, no drift): with set already gone, checkGrant rejects a
//	   Grant on any remaining op, and checkArgs rejects Args on delete (by-id).
//	   Violation → ErrMalformed.
func ingressState(op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) error {
	if err := checkOperation(op); err != nil { // ①
		return err
	}
	if err := checkResourceID(id); err != nil { // ②
		return err
	}
	if op == access.OpSet { // ③
		return ErrOpNotInScope
	}
	if err := checkGrant(op, grant); err != nil { // ④ — set unreachable here, so this is the "no grant on this op" half
		return err
	}
	return checkArgs(op, args) // ④ — delete is by-id
}

// executeFailure maps an EXECUTE-stage failure to the right error channel —
// shared by BOTH trees: the caller's OWN cancellation (the request ctx
// expired or was cancelled) is not a resource-plane verdict — a driver_error
// there would blame the driver for the caller's hand — so it surfaces as the
// Go error it is. Everything else stays a driver_error verdict
// (executor-authored, reason.go), keeping the verdict producible. Day-1 draws
// NO finer infra-vs-driver line inside EXECUTE (on sqlite they are physically
// one failure); a finer split waits for real pain.
func executeFailure(ctx context.Context, err error) (Outcome, error) {
	if ctx.Err() != nil {
		return Outcome{}, err
	}
	return Outcome{RejectReason: access.DriverError}, nil
}

// createVerdict maps the atomic-create error surface — ONE mapping shared by
// both loci (one collision vocabulary): ErrAlreadyExists → already_exists;
// anything else follows the shared EXECUTE-failure split above. A second
// sentinel added to either store's Create lands here once and both trees agree.
func createVerdict(ctx context.Context, err error) (Outcome, error) {
	if errors.Is(err, resourcespec.ErrAlreadyExists) {
		return Outcome{RejectReason: access.AlreadyExists}, nil // race collision decided atomically
	}
	if errors.Is(err, resourcespec.ErrOwnerInactive) {
		return Outcome{RejectReason: access.OwnerInactive}, nil
	}
	return executeFailure(ctx, err)
}

// invokeActorScoped runs the collapsed decision tree for one welded owner — the
// degenerate evaluation of the same tree invoke runs: the authorization
// judgment degenerates to owner-only (guaranteed structurally by the welded
// handle), so NO membership check, NO R union, NO DriverTable routing.
// Each op's own row-hit IS its existence check — there is no Resolve step because
// there is no meta.Kind to route.
//
// Two error channels stay distinct, exactly as invoke's: a Go error is
// returned only for something reason.go has no verdict for; every failure
// inside the EXECUTE pipeline is a verdict (driver_error). Folding an executor
// failure into a Go error would leave driver_error unproducible.
func (d *door) invokeActorScoped(ctx context.Context, owner actor.ActorID, op access.Operation, id resource.ResourceID, args []byte) (Outcome, error) {
	switch op {
	case access.OpCreate:
		if err := d.deps.State.Create(ctx, owner, id, args); err != nil {
			return createVerdict(ctx, err) // one collision vocabulary, two loci — shared mapping
		}
		return Outcome{}, nil

	case access.OpRead:
		val, exists, err := d.deps.State.Read(ctx, owner, id)
		if err != nil {
			return executeFailure(ctx, err)
		}
		if !exists {
			return Outcome{RejectReason: access.ResourceNotFound}, nil
		}
		// Found is the resolved-but-empty signal, uniform with the channel-scoped
		// tree: an existing row with NULL bytes (val == nil) = Found:false;
		// empty non-nil bytes are a value.
		return Outcome{Value: val, Found: val != nil}, nil

	case access.OpWrite:
		exists, err := d.deps.State.Write(ctx, owner, id, args)
		if err != nil {
			return executeFailure(ctx, err)
		}
		if !exists {
			return Outcome{RejectReason: access.ResourceNotFound}, nil // birth is Create, not Write
		}
		return Outcome{}, nil

	case access.OpDelete:
		exists, err := d.deps.State.Delete(ctx, owner, id)
		if err != nil {
			return executeFailure(ctx, err)
		}
		if !exists {
			return Outcome{RejectReason: access.ResourceNotFound}, nil // repeated delete is honestly not-found
		}
		return Outcome{}, nil

	default:
		// Unreachable: ingressState gated op (set → ErrOpNotInScope; the others are
		// handled above). Defensive.
		return Outcome{}, ErrMalformed
	}
}
