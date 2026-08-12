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
