package accessdoor

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
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
// (期11 spec §3.4/§5 item 0): same-daemon (caller's authoritative Placement.Host
// equals placementDaemonID) → Local; else → mint a lane Token via
// Deps.LaneControl. reservationID is "" for a plain OpRead/OpWrite (§3.5:
// no outbox involvement — OpWrite never fires Committed) and the
// just-reserved id for a with-content create's write route (§1.7).
func (d *door) resolveFileRoute(ctx context.Context, caller actor.ActorID, placementDaemonID, coord string, mode access.Operation, reservationID string, dir bool) (*FileRoute, error) {
	row, found, err := d.deps.Authority.LookupActive(ctx, caller)
	if err != nil {
		return nil, err
	}
	host := ""
	if found && row.Placement.Kind == storespec.PlacementDaemon {
		host = row.Placement.Host
	}
	// Same-daemon (Local) resolves coord itself via the daemon-side
	// control-RPC ResolveCoord step (platform/internal/link) — never a lane
	// byte-hop (§5 item 0's "同daemon→daemon本地os.Root句柄...zerocopy").
	// Cross-host (!Local) redeems the Token by opening a lane stream on its
	// own connection. Both branches mint the SAME Token through ONE
	// LaneControl.OpenTransfer call — ResolveCoord's sender-auth check
	// (target daemon only) needs somewhere to look coord up by regardless
	// of which redemption path the caller takes (see platform/internal/
	// link's doc for the full walk).
	local := found && host != "" && host == placementDaemonID
	if dir && !local {
		// A directory lease is a whole-tree os.Root capability confined to one
		// machine — it does NOT serialize onto the lane's single byte-pipe. A
		// cross-host dir workspace Open is deferred whole (债② federation, 丁12
		// scope: same-daemon only); reject honestly rather than mint a lane
		// route the redeem side could only mis-handle as a byte stream.
		return nil, errors.New("accessdoor: cross-host directory lease deferred (债② federation) — a dir workspace Open requires a same-daemon caller")
	}
	if d.deps.LaneControl == nil {
		return nil, errors.New("accessdoor: file byte route not wired (Deps.LaneControl is nil)")
	}
	token, terr := d.deps.LaneControl.OpenTransfer(ctx, placementDaemonID, host, coord, mode, reservationID)
	if terr != nil {
		return nil, terr
	}
	return &FileRoute{Local: local, Token: token, Mode: mode, ReservationID: reservationID, Dir: dir}, nil
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
// (boundHandle.Invoke → ingress → day1OpsOverreach → invoke), so op/args/grant
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
func (d *door) invoke(ctx context.Context, caller actor.ActorID, op access.Operation, id resource.ResourceID, args []byte, grant *access.Grant) (Outcome, error) {
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

	// ---- A8 two halves unioned: actor entry ∪ (members entry ∧ current member) ----
	allowed, err := d.deps.Registry.ActorAllows(ctx, caller, id, op)
	if err != nil {
		return Outcome{}, err
	}
	if !allowed {
		allowed, err = d.deps.Overlay.ActorAllows(ctx, caller, id, op)
		if err != nil {
			return Outcome{}, err
		}
	}
	if !allowed {
		mAllow, err := d.deps.Registry.MembersAllow(ctx, id, op)
		if err != nil {
			return Outcome{}, err
		}
		if mAllow {
			_, isM, err := d.deps.Authority.LookupActive(ctx, caller)
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
		if meta.Kind == resourcespec.KindFile {
			// file bytes never ride Outcome.Value (§8.1 red line) — the execute
			// arm's kind branch (期11 spec §3.4) redirects file read/write to
			// the daemon-hosted / lane-forwarded byte path, NOT this door's
			// Driver dispatch (file has no Driver — Allocator/Streamer, a
			// structurally different shape, realize its bytes, §4). The
			// accepted outcome carries a FileRoute (§5), never bytes.
			route, rerr := d.resolveFileRoute(ctx, caller, meta.PlacementDaemonID, meta.PlacementCoord, access.OpRead, "", meta.Dir)
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
			route, rerr := d.resolveFileRoute(ctx, caller, meta.PlacementDaemonID, meta.PlacementCoord, access.OpWrite, "", meta.Dir)
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

	case access.OpSet:
		// set's executor is the substrate authz manager (Registry), not a driver.
		// grant is non-nil and structurally valid (ingress), so the deref is safe.
		//
		// Authorization DECAY LAW (期11 spec §2 item 1): set(X, ops) additionally
		// requires ops ⊆ effectiveOps(caller) — the ESCALATION check, distinct
		// from the union-authorize step above (which only confirmed caller holds
		// SET-right on id at all, not that the payload ops stay within caller's
		// own reach). Without this, a set-right holder could grant a subject
		// (themselves or a colluding third party) MORE than they themselves
		// hold — self-escalation / collusion. Revoke (grant.Ops == ∅) is
		// trivially legal for ANY caller (∅ ⊆ any set) and is short-circuited
		// here rather than routed through effectiveOps: the empty loop below
		// already accepts it, but skipping the (up to 4×ActorAllows +
		// 4×MembersAllow + IsMember) computation entirely keeps revoke cheap
		// and — more importantly — keeps revoke from depending on that
		// computation SUCCEEDING (a Registry hiccup must never block a revoke
		// that is unconditionally legal on its face).
		if len(grant.Ops) > 0 {
			eff, everr := d.effectiveOps(ctx, caller, id)
			if everr != nil {
				return Outcome{}, everr
			}
			for _, op := range grant.Ops {
				if !eff[op] {
					return Outcome{RejectReason: access.AccessDenied}, nil
				}
			}
		}
		world, found, werr := d.deps.Authority.WorldOf(ctx, grant.Grantee)
		if werr != nil {
			return Outcome{}, werr
		}
		var serr error
		if grant.GranteeKind == access.GranteeActor && found && world == storespec.WorldRun {
			serr = d.deps.Overlay.SetGrant(ctx, id, *grant)
		} else {
			serr = d.deps.Registry.SetGrant(ctx, id, *grant)
		}
		if serr != nil {
			return executeFailure(ctx, serr) // executor-authored (reason.go)
		}
		return Outcome{}, nil

	case access.OpDelete:
		// Time-order is KIND-DEPENDENT (期11 spec §1 item 8 — the flip from
		// the universal "bytes first, existence row last" contract):
		if meta.Kind == resourcespec.KindFile {
			// file: ROW-FIRST-BYTES-LAST. Registry.Delete ALREADY runs this
			// as one transaction (read row → write tombstone → delete row +
			// grants, built in S1) — there is no Driver call at all: file has
			// no Driver (its bytes are realized by the daemon-side
			// Allocator/Streamer, never a DriverTable entry), and the actual
			// byte collection is the daemon-side Reclaimer's ASYNC job (§4,
			// S4), confirmed via ReclaimAck — never this door's concern.
			if derr := d.deps.Registry.Delete(ctx, id); derr != nil {
				return executeFailure(ctx, derr)
			}
			d.deps.Overlay.DeleteResource(id)
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
		d.deps.Overlay.DeleteResource(id)
		if d.deps.Logger != nil {
			d.deps.Logger.Info("resource deleted", "id", string(id), "kind", string(meta.Kind), "tombstoned", false)
		}
		return Outcome{}, nil

	default:
		// Unreachable: ingress ParseOperation already gated op. Defensive.
		return Outcome{}, ErrMalformed
	}
}
