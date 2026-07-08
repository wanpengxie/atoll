package storagehost

import (
	"errors"
	"fmt"
	"os"
)

// provenanceAxisAllocated mirrors resourcespec.ProvenanceAxisAllocated's wire
// value as a plain string (this package sits outside the runtime tree and
// must not import resourcespec — the control-RPC wire, platform/internal/
// link.ReconcileTombstone, already carries the string form; see its doc).
const provenanceAxisAllocated = "axis-allocated"

// Reclaimer is §4.1's delete-side component: collects a tombstoned
// resource's bytes, branching on provenance (§4.1: "axis-allocated rm-rf;
// registered 只销户"). Day-1 every tombstone is axis-allocated (registration
// is deferred whole to coral), so the "registered" branch is a no-op that
// exists only so the closed set has a home the day it lands (mirrors
// resourcespec.ProvenanceRegistered's own doc).
type Reclaimer struct{}

// Reclaim removes coord's live bytes (idempotent: an already-gone entry —
// e.g. a repeat ReclaimAck's collection request, or an Alloc that never
// actually landed anything — is a clean no-op, never an error) for an
// axis-allocated tombstone; any other provenance value is a no-op by design
// (registered: never touched; an unrecognized value: treated the SAME as
// registered — fail-safe, never destroy bytes this daemon cannot positively
// confirm it owns the collection contract for).
func (Reclaimer) Reclaim(cr *channelRoot, coord, provenance string) error {
	if provenance != provenanceAxisAllocated {
		return nil
	}
	p, err := livePath(coord)
	if err != nil {
		return err
	}
	if err := cr.root.RemoveAll(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("storagehost: reclaim %q: %w", coord, err)
	}
	return nil
}
