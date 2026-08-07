package accessdoor

import (
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
)

// objectOps is the closed set of object verbs an existing channel-scoped
// resource answers to (create is container-locus and lives on the Create
// method). effectiveOps ranges its computation over exactly this set, in this
// FIXED order (opSetFromEffective's wire-stable enumeration depends on it).
var objectOps = []access.Operation{access.OpRead, access.OpWrite, access.OpDelete}

// effectiveOps is THE authorization formula of the channel-scoped access
// plane (PM-D1/PM-D3), a PURE function of membership facts + the creation
// fact — no registry round trip, because there is no per-object relation to
// consult:
//
//	read/write : active            (membrane-uniform — membership IS the right)
//	delete     : active ∧ (channel owner ∨ caller == createdBy)   (PM-D3)
//
// Every locus that judges object rights uses this one function — the invoke
// gate (door.go, asked for the one op the call carries), Stat's echoed ops
// and List's per-row projection (query.go). Computing it differently in even
// one locus would fork the model; the function's purity is what keeps the
// three loci structurally identical.
//
// An inactive caller (departed member, dead identity) holds NO ops — the
// membrane's trust phase covers exactly its current members plus the channel
// owner root (whose facts also carry Active).
func effectiveOps(caller actor.ActorID, active, owner bool, createdBy actor.ActorID) map[access.Operation]bool {
	eff := make(map[access.Operation]bool, len(objectOps))
	if !active {
		return eff
	}
	eff[access.OpRead] = true
	eff[access.OpWrite] = true
	eff[access.OpDelete] = owner || caller == createdBy
	return eff
}
