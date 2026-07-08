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

// PlacementKind is the closed set of file-object storage placement
// mechanisms — the door-back LOCUS a driver's bytes live at, dual to
// ResourceKind's door-back BYTE FORMAT axis. kv carries no placement concept
// at all: its persisted placement_kind column is the empty string, which is
// NOT a member of this set — it is "the placement axis does not apply to
// this row" (ValidPlacementKind treats "" as legal precisely to express
// that non-membership, not to smuggle in a zero-value PlacementKind).
type PlacementKind string

// PlacementDaemonLocal is KindFile's day-1 (and so-far only) placement:
// bytes live on one daemon's physical disk, addressed by an opaque
// placement_coord that daemon's Streamer alone interprets. A cloud-backed
// placement value is a future substrate driver addition — additive to this
// set when a real driver demands it, never pre-reserved.
const PlacementDaemonLocal PlacementKind = "daemon-local"

var allPlacementKinds = []PlacementKind{PlacementDaemonLocal}

// ValidPlacementKind reports whether raw is a legal persisted placement_kind
// column value: either "" (kv's non-membership) or a member of the closed
// set. An unrecognized non-empty value is a fail-fast signal — a future
// driver's placement landed in the column ahead of the Go closed set that
// names it — never silently accepted (the same discipline schema.go's doc
// describes for sender_kind/actor_kind: Go-enforced closed sets fail loud on
// read, no DB CHECK).
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

// Provenance is the closed set of how a resource's placement came to be
// known to the registry. Day-1 every row (kv and file alike) is stamped
// ProvenanceAxisAllocated by the door at create time (期11 spec §1 item 1:
// "provenance 由门盖章，day-1 恒 axis-allocated") — ProvenanceRegistered
// names the future "adopt an externally-created object" form (the登记式
// create, deferred whole to coral, Q-D) and is declared here so its column
// value has a home the day it lands, not pre-wired to any behavior now.
type Provenance string

const (
	// ProvenanceAxisAllocated — the registry itself minted this resource's
	// placement via create-outbox's coord generation (§1.6): day-1's only
	// value, for every kind.
	ProvenanceAxisAllocated Provenance = "axis-allocated"

	// ProvenanceRegistered — an externally-created object was adopted into
	// the registry after the fact. Declared, UNUSED day-1 (registration is
	// deferred whole to coral): the Reclaimer (§4) branches on it once it
	// exists — axis-allocated bytes are collected on delete (rm -rf),
	// registered bytes are only unlinked from the registry, never touched on
	// disk.
	ProvenanceRegistered Provenance = "registered"
)

var allProvenances = []Provenance{ProvenanceAxisAllocated, ProvenanceRegistered}

// ValidProvenance reports whether raw is a member of the closed set. Unlike
// PlacementKind, Provenance has no "legal empty" case — every resources row
// is stamped with one of the two values at create time.
func ValidProvenance(raw Provenance) bool {
	for _, want := range allProvenances {
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
