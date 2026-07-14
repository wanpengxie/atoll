package resourcespec

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// CreateSpec is the CLIENT-DECLARED shape of one create call — the operand
// the resource face's Create method (§3) or the wire's dedicated Create arm
// (§3.3) decodes, distinct from CreateParams below (the door's OWN derived
// attributes for the same birth event).
//
// CARRIER LAW (red line, 期11 spec §1 item 1): CreateSpec NEVER rides proto
// access.Invocation (the drift-guard in protocol/access pins that struct
// closed) and NEVER rides opaque Args (a create request is a control-plane
// operand the door decodes, not a driver-opaque byte blob — the same
// envelope/payload split access.Grant draws for op=set). Its ONLY in-proc
// carrier is the resource face's Create method; its ONLY over-wire carrier
// is the link layer's accessRequest Create arm. access.Invocation.Args keeps
// carrying exactly what it always has: kv's inline value.
type CreateSpec struct {
	// Kind selects the storage driver, drawn from the closed set ValidKind
	// gates ({KindKV, KindFile}). The door's ingress (§3) validates
	// membership before Kind ever reaches the Registry — an out-of-set value
	// here is a malformed request, never a driver_error verdict.
	Kind ResourceKind

	// Dir marks a directory-shaped file create (a workspace directory
	// variant of KindFile, not a distinct ResourceKind). Meaningless for kv.
	// Dir creates carry no byte content.
	Dir bool

	// WithContent declares "this create carries a byte stream" — the
	// create-outbox's judge between the two landing timings (§1.5):
	// WithContent routes through a server reservation (ReserveCreate) and a
	// daemon Committed round trip (bytes land before the row becomes
	// visible, "no half-built window"); its absence is an immediate create
	// (empty file / directory / kv, whose only content is CreateParams.
	// Initial, never a byte stream). Dir && WithContent is a malformed
	// combination — a directory carries no content — the door's ingress
	// rejects outright, never silently resolved.
	WithContent bool
}

// PlacementKind names the door-back storage locus mechanism projected for a
// resource. It is not an independently persisted axis: with today's resource
// kinds, KindFile derives PlacementDaemonLocal and KindKV derives the empty
// value because the placement axis does not apply to inline bytes.
type PlacementKind string

// PlacementDaemonLocal is KindFile's current placement mechanism: the bytes
// live on the daemon named by ResourceMeta.PlacementDaemonID.
const PlacementDaemonLocal PlacementKind = "daemon-local"

var allPlacementKinds = []PlacementKind{PlacementDaemonLocal}

// ValidPlacementKind reports whether raw is a legal public projection. The
// empty value is valid for KindKV, where no external placement exists.
func ValidPlacementKind(raw PlacementKind) bool {
	if raw == "" {
		return true
	}
	for _, want := range allPlacementKinds {
		if raw == want {
			return true
		}
	}
	return false
}

// coordSeedBytes is the random seed width GenerateCoord hashes — 256 bits,
// matching the sha256 digest it feeds (design doc C1: "种子=random/
// salted-hash").
const coordSeedBytes = 32

// GenerateCoord mints one placement_coord value: the SERVER-side registry's
// opaque storage handle (期11 spec §1.6/design doc C1 — "server 侧 registry
// 生成(salted-hash,非rowid非ResourceID)"). It is a hash of random bytes, NEVER
// derived from the resource id (a ResourceID-derived coord would let a
// caller who already knows/guesses an id predict — or collide — another
// object's on-disk coordinate, the traversal-injection risk C1 names) and
// NEVER a sequential rowid (which would leak creation order). The result is
// opaque to every caller but the placement daemon named in
// ResourceMeta.PlacementDaemonID, which alone interprets it as a path
// segment (§4.6's "只存不解释" — this package never does, either). Called by
// the door (accessdoor, itself server/home-process code) once per file
// create, before ReserveCreate/Create ever sees the value.
func GenerateCoord() (string, error) {
	var seed [coordSeedBytes]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return "", fmt.Errorf("resourcespec: generate coord: %w", err)
	}
	sum := sha256.Sum256(seed[:])
	return hex.EncodeToString(sum[:]), nil
}
