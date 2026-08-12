package accessdoor

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// door is the bare invoker — sealed inside the package (New hands out only a
// Minter, mirroring harness never handing out the bare chain). It holds Deps and
// runs the decision tree; the welded caller arrives as a parameter, never read
// off the wire.
type door struct {
	deps         Deps
	resourceGate sync.Mutex
}

// resolveFileRoute computes OpRead/OpWrite(file)'s (and, via query.go's
// create, a with_content create's write) byte-access authorization product
// (期11 spec §3.4/§5 item 0). The product explicitly selects local redemption
// or remote exchange. In both cases the server-side ledger remains
// authoritative for mode, shape, coordinate and reservation.
//
// reservationID is "" for a plain OpRead/OpWrite (§3.5: no outbox involvement
// — OpWrite never fires Committed) and the just-reserved id for a with-content
// create's write route (§1.7).
func (d *door) resolveFileRoute(ctx context.Context, caller actor.ActorID, id resource.ResourceID, placementDaemonID, coord string, mode access.Operation, reservationID string, dir bool) (*FileRoute, error) {
	facts, err := d.deps.Authority.ResourceActorFacts(ctx, caller)
	if err != nil {
		return nil, err
	}
	if !facts.Active {
		return nil, ErrFileCapabilityUnavailable
	}
	address, err := resourcespec.ParseFileAddress(string(id))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	mount, err := d.choosePlacement(ctx, address)
	if err != nil {
		return nil, err
	}
	if mount.DaemonID != placementDaemonID {
		return nil, fmt.Errorf("accessdoor: address host %q resolves to daemon %q, resource is placed on %q", address.Host, mount.DaemonID, placementDaemonID)
	}
	if d.deps.TransferControl == nil {
		return nil, errors.New("accessdoor: file byte route not wired (Deps.TransferControl is nil)")
	}
	token, redeem, terr := d.deps.TransferControl.IssueTransfer(ctx, id, placementDaemonID, address.Host, facts.PreferredStorageHost, coord, mode, reservationID, dir)
	if terr != nil {
		return nil, terr
	}
	if token == "" || (redeem != FileRedeemLocal && redeem != FileRedeemRemote) {
		return nil, errors.New("accessdoor: transfer control minted an empty ticket")
	}
	return &FileRoute{Token: token, Mode: mode, ReservationID: reservationID, Redeem: redeem, Dir: dir}, nil
}

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
// (boundHandle.Invoke → ingress → invoke), so op/args
// are structurally valid here.
//
// Two error channels, deliberately distinct:
//   - a Go error is returned ONLY for an assembly defect (no driver for a kind),
//     infrastructure failure (store broken at resolve/authorize), or the
//     caller's OWN cancellation surfacing mid-EXECUTE (executeFailure — a
//     driver_error there would blame the driver for the caller's hand);
//   - every other failure inside the resolve→authorize→EXECUTE pipeline is a
//     verdict (Outcome.RejectReason, nil error), including an executor failure
//     (driver_error). Folding EXECUTE failures into Go errors would leave
//     driver_error unproducible — the bug v1 shipped.
func (d *door) invoke(ctx context.Context, caller actor.ActorID, op access.Operation, id resource.ResourceID, args []byte) (Outcome, error) {
	d.resourceGate.Lock()
	defer d.resourceGate.Unlock()
	meta, exists, err := d.deps.Registry.Resolve(ctx, id)
	if err != nil {
		return Outcome{}, err // store broken = infrastructure-level, Go error
	}

	// op=create no longer reaches this tree at all (期11 spec §3.1's "create
	// 单入口"): boundHandle.Invoke rejects a bare OpCreate before calling
	// invoke (ErrCreateViaInvoke), and the resource-face Create method (§3,
	// door.create in query.go) is now the SOLE create locus. This function's
	// switch below has no OpCreate case, so a caller reaching it with
	// op=create (only possible by bypassing boundHandle, e.g. a raw test)
	// falls through the switch's defensive default.

	if !exists {
		return Outcome{RejectReason: access.ResourceNotFound}, nil
	}
	facts, err := d.deps.Authority.ResourceActorFacts(ctx, caller)
	if err != nil {
		return Outcome{}, err
	}
	// ---- membrane-uniform authorization (PM-D1/PM-D3) ----
	// The membrane is one trust phase: read/write on any channel-scoped
	// resource is membership itself — active member ⇒ allowed, no per-object
	// relation is consulted (none exists). delete alone distinguishes the
	// creation fact: creator ∨ channel owner root (PM-D3, meta.CreatedBy is
	// the authorization predicate). effectiveOps (query.go) is the SAME
	// formula ranged over the op set — one formula, two loci.
	if !effectiveOps(caller, facts.Active, facts.Owner, meta.CreatedBy)[op] {
		return Outcome{RejectReason: access.AccessDenied}, nil
	}

	switch op {
	case access.OpRead:
		if meta.Kind == resourcespec.KindFile {
			// file bytes never ride Outcome.Value (§8.1 red line) — the execute
			// arm's kind branch (期11 spec §3.4) redirects file read/write to
			// the daemon-hosted byte path, NOT this door's
			// Driver dispatch (file has no Driver — Allocator/Streamer, a
			// structurally different shape, realize its bytes, §4). The
			// accepted outcome carries a FileRoute (§5), never bytes.
			route, rerr := d.resolveFileRoute(ctx, caller, id, meta.PlacementDaemonID, meta.PlacementCoord, access.OpRead, "", meta.Dir)
			if rerr != nil {
				return Outcome{}, rerr
			}
			return Outcome{Route: route}, nil
		}
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
		if meta.Kind == resourcespec.KindFile {
			// OpWrite on an already-existing row never touches the create-
			// outbox / Committed (§3.5: "不走create-outbox、不发Committed、
			//不碰Registry.Create") — reservationID stays "" so the daemon
			// side's write-completion (fsync+rename) fires no control RPC.
			route, rerr := d.resolveFileRoute(ctx, caller, id, meta.PlacementDaemonID, meta.PlacementCoord, access.OpWrite, "", meta.Dir)
			if rerr != nil {
				return Outcome{}, rerr
			}
			return Outcome{Route: route}, nil
		}
		drv, err := d.driver(meta.Kind)
		if err != nil {
			return Outcome{}, err
		}
		if werr := drv.Write(ctx, id, args); werr != nil {
			return executeFailure(ctx, werr)
		}
		return Outcome{}, nil

	case access.OpDelete:
		// Time-order is KIND-DEPENDENT (期11 spec §1 item 8 — the flip from
		// the universal "bytes first, existence row last" contract):
		if meta.Kind == resourcespec.KindFile {
			// file: ROW-FIRST-BYTES-LAST. Registry.Delete ALREADY runs this
			// as one transaction (read row → write tombstone → delete row,
			// built in S1) — there is no Driver call at all: file has
			// no Driver (its bytes are realized by the daemon-side
			// Allocator/Streamer, never a DriverTable entry), and the actual
			// byte collection is the daemon-side Reclaimer's ASYNC job (§4,
			// S4), confirmed via ReclaimAck — never this door's concern.
			if derr := d.deps.Registry.Delete(ctx, id); derr != nil {
				return executeFailure(ctx, derr)
			}
			if d.deps.Logger != nil {
				d.deps.Logger.Info("resource deleted", "id", string(id), "kind", string(meta.Kind), "tombstoned", true)
			}
			return Outcome{}, nil
		}
		// kv (and any future inline-byte kind): bytes first, existence row
		// last — a mid-flight failure leaves the resource resolvable
		// (retryable) or resolved-but-empty (legal), never a row gone with
		// bytes stranded under someone's name.
		drv, err := d.driver(meta.Kind)
		if err != nil {
			return Outcome{}, err
		}
		if derr := drv.Delete(ctx, id); derr != nil {
			return executeFailure(ctx, derr)
		}
		if derr := d.deps.Registry.Delete(ctx, id); derr != nil {
			return executeFailure(ctx, derr)
		}
		if d.deps.Logger != nil {
			d.deps.Logger.Info("resource deleted", "id", string(id), "kind", string(meta.Kind), "tombstoned", false)
		}
		return Outcome{}, nil

	default:
		// Unreachable: ingress ParseOperation already gated op. Defensive.
		return Outcome{}, ErrMalformed
	}
}
