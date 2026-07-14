package resourcespec

import "testing"

// ValidKind gates the day-1 closed set {KindKV, KindFile} — the ingress
// membership test the door layer (§3) will call before a CreateSpec.Kind
// ever reaches the Registry.
func TestValidKind(t *testing.T) {
	for _, k := range []ResourceKind{KindKV, KindFile} {
		if !ValidKind(k) {
			t.Errorf("ValidKind(%q) = false, want true", k)
		}
	}
	for _, k := range []ResourceKind{"", "secret", "cloud-file", "KV"} {
		if ValidKind(k) {
			t.Errorf("ValidKind(%q) = true, want false (out of set)", k)
		}
	}
}

func TestValidPlacementKind(t *testing.T) {
	// "" is legal — kv's non-membership, not an unknown value.
	if !ValidPlacementKind("") {
		t.Error(`ValidPlacementKind("") = false, want true (kv's non-membership)`)
	}
	if !ValidPlacementKind(PlacementDaemonLocal) {
		t.Errorf("ValidPlacementKind(%q) = false, want true", PlacementDaemonLocal)
	}
	for _, raw := range []PlacementKind{"cloud", "daemon-local ", "DAEMON-LOCAL"} {
		if ValidPlacementKind(raw) {
			t.Errorf("ValidPlacementKind(%q) = true, want false (out of set)", raw)
		}
	}
}

func TestValidProvenance(t *testing.T) {
	if !ValidProvenance(ProvenanceAxisAllocated) {
		t.Errorf("ValidProvenance(%q) = false, want true", ProvenanceAxisAllocated)
	}
	// Unlike PlacementKind, "" is NOT legal — every row is always stamped.
	for _, raw := range []Provenance{"", "registered", "adopted", "AXIS-ALLOCATED"} {
		if ValidProvenance(raw) {
			t.Errorf("ValidProvenance(%q) = true, want false (out of set)", raw)
		}
	}
}
