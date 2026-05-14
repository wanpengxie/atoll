package ledger_test

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/ledger"
)

// TestStatusClosedSet — only reserved / committed per L2 §1.4.10.1.
func TestStatusClosedSet(t *testing.T) {
	if ledger.StatusReserved != "reserved" {
		t.Errorf("StatusReserved=%q want reserved", ledger.StatusReserved)
	}
	if ledger.StatusCommitted != "committed" {
		t.Errorf("StatusCommitted=%q want committed", ledger.StatusCommitted)
	}
}

// TestDeriveKeyDeterministic — same inputs → same hex SHA-256.
func TestDeriveKeyDeterministic(t *testing.T) {
	a, err := ledger.DeriveKey("turn-1", "act/post")
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	b, err := ledger.DeriveKey("turn-1", "act/post")
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	if a != b {
		t.Errorf("deterministic check: %s != %s", a, b)
	}
	if len(a.String()) != 64 {
		t.Errorf("hex len=%d want 64", len(a.String()))
	}
	// Hex must be lowercase per L2 §1.4.10.2.
	for _, c := range a.String() {
		if c >= 'A' && c <= 'F' {
			t.Errorf("DeriveKey returned uppercase hex: %s", a)
			break
		}
	}
}

// TestDeriveKeyTurnIDChangesHash — distinct turn_id → distinct key.
func TestDeriveKeyTurnIDChangesHash(t *testing.T) {
	a, _ := ledger.DeriveKey("turn-1", "act/post")
	b, _ := ledger.DeriveKey("turn-2", "act/post")
	if a == b {
		t.Errorf("DeriveKey collision on turn_id change: %s == %s", a, b)
	}
}

// TestDeriveKeySemanticActionChangesHash — distinct semantic_action_key
// → distinct key.
func TestDeriveKeySemanticActionChangesHash(t *testing.T) {
	a, _ := ledger.DeriveKey("turn-1", "act/post")
	b, _ := ledger.DeriveKey("turn-1", "act/delete")
	if a == b {
		t.Errorf("DeriveKey collision on action change: %s == %s", a, b)
	}
}

// TestDeriveKeyEscapesQuotes — JSON-special characters in inputs must
// escape cleanly; collisions with naive concatenation MUST NOT happen.
func TestDeriveKeyEscapesQuotes(t *testing.T) {
	a, _ := ledger.DeriveKey(`turn-"1"`, `act"x`)
	b, _ := ledger.DeriveKey(`turn-1`, `act"x`)
	if a == b {
		t.Errorf("DeriveKey did not escape quotes; collision %s", a)
	}
	c, _ := ledger.DeriveKey("turn\n1", "act")
	d, _ := ledger.DeriveKey("turn1", "act")
	if c == d {
		t.Errorf("DeriveKey did not escape newline; collision %s", c)
	}
}

// TestEntryZeroValue — zero Entry is a sentinel (no row).
func TestEntryZeroValue(t *testing.T) {
	var e ledger.Entry
	if e.Key != "" || e.Status != "" || e.CommittedAt != 0 {
		t.Errorf("zero Entry has surprising fields: %+v", e)
	}
}

// TestKeyString — wire-form round-trip.
func TestKeyString(t *testing.T) {
	k := ledger.Key("abc")
	if k.String() != "abc" {
		t.Errorf("Key.String()=%q want abc", k.String())
	}
}
