package lagoon

import (
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

func TestNameBoundaries(t *testing.T) {
	valid := []string{"a", strings.Repeat("a", 63), "a-1"}
	for _, name := range valid {
		if err := ValidateName(name); err != nil {
			t.Errorf("valid name %q: %v", name, err)
		}
	}
	invalid := []string{"", strings.Repeat("a", 64), "-a", "a-", "A", "a.b", "a_b", "a b"}
	for _, name := range invalid {
		if err := ValidateName(name); err == nil {
			t.Errorf("invalid name %q accepted", name)
		}
	}
}

// One law serves device names and channel labels alike, so a qualified channel
// name is exactly a dotted chain of that same word, rooted at c0.
func TestQualifiedChannelNameBoundaries(t *testing.T) {
	valid := []string{"c0", "c0.ops", "c0.proj-x.backend"}
	for _, name := range valid {
		if err := ValidateQualifiedName(name); err != nil {
			t.Errorf("valid qualified name %q: %v", name, err)
		}
	}
	invalid := []string{"", "ops", "c0.", ".c0", "c0..ops", "c0.OPS", "c0.-ops", "c0.ops-", "c1.ops"}
	for _, name := range invalid {
		if err := ValidateQualifiedName(name); err == nil {
			t.Errorf("invalid qualified name %q accepted", name)
		}
	}
	if joined, err := JoinName("c0.proj", "backend"); err != nil || joined != "c0.proj.backend" {
		t.Fatalf("JoinName = %q, %v", joined, err)
	}
	if _, err := JoinName("c0", "Bad"); err == nil {
		t.Fatal("JoinName accepted an invalid label")
	}
	if _, err := JoinName("nope", "ok"); err == nil {
		t.Fatal("JoinName accepted an unrooted parent")
	}
}

// The qualified name is also one directory name on every daemon holding the
// channel, so a chain of individually-legal labels still has to fit in one
// filesystem component. Without this bound a channel would be created
// successfully and then never be able to exist on disk.
func TestQualifiedChannelNameFitsOneDirectoryName(t *testing.T) {
	label := strings.Repeat("a", 63)
	name := "c0"
	for i := 0; i < 3; i++ {
		joined, err := JoinName(name, label)
		if err != nil {
			t.Fatalf("depth %d rejected at %d bytes: %v", i+1, len(name), err)
		}
		name = joined
	}
	if len(name) != 2+3*64 {
		t.Fatalf("three-deep name is %d bytes", len(name))
	}
	if _, err := JoinName(name, label); err == nil {
		t.Fatalf("accepted a %d-byte name, past the %d-byte directory limit", len(name)+1+len(label), maxQualifiedName)
	}
	// Exactly at the limit is fine; one byte past it is not. Both labels below
	// are legal on their own, so only the total length can separate them.
	if _, err := JoinName(name, strings.Repeat("b", maxQualifiedName-len(name)-1)); err != nil {
		t.Fatalf("rejected a name of exactly %d bytes: %v", maxQualifiedName, err)
	}
	if _, err := JoinName(name, strings.Repeat("b", maxQualifiedName-len(name))); err == nil {
		t.Fatal("accepted a name one byte past the limit")
	}
}

// The address layer parses shape, not spelling: a name that minting would
// reject still round-trips, because such a name can never reach an address in
// the first place. Keeping the alphabet out of the parser leaves exactly one
// place where names are judged.
func TestAddressLayerRoundTripsNamesThatMintingRejects(t *testing.T) {
	for _, name := range []string{"UPPER", "a.b"} {
		raw := "daemon://" + name + "/c0/x"
		addr, err := resourcespec.ParseFileAddress(raw)
		if err != nil {
			t.Fatalf("address layer rejected %q: %v", raw, err)
		}
		got, err := resourcespec.FormatFileAddress(addr)
		if err != nil || got != raw {
			t.Fatalf("address round trip = %q, %v; want %q", got, err, raw)
		}
		if err := ValidateName(name); err == nil {
			t.Fatalf("minting layer accepted invalid machine name %q", name)
		}
	}
}
