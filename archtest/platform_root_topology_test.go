package archtest

import (
	"os"
	"sort"
	"strings"
	"testing"
)

// platform-topology 批 T4 / spec §0 裁决 5 (P4): platform/doc.go's "Root-file
// topology" section names a closed three-way classification (channel host /
// compute host / membrane) for every non-test .go file directly under
// platform/. This is the enforcement anchor: it walks platform/'s own
// directory entries (NOT its sub-packages — platform/subjectgate and
// platform/internal/* are packages of their own, out of scope for a root-file
// classification) and requires every production .go file found there to be
// named in exactly one of the three doc.go lists below.
//
// The three lists are transcribed from doc.go by hand, not parsed out of the
// comment — doc.go is prose for humans, this is the machine-checked mirror of
// it. Drift between the two (a root file added/renamed without updating
// doc.go, or vice versa) turns this test red; that IS the tripwire.
var platformChannelHostFiles = map[string]bool{
	"home.go":             true,
	"open.go":             true,
	"reconcile.go":        true,
	"census.go":           true,
	"control.go":          true,
	"close.go":            true,
	"view.go":             true,
	"remove.go":           true,
	"expiry.go":           true,
	"scheduler.go":        true,
	"storagehost.go":      true,
	"humancell_wiring.go": true,
	"testing.go":          true,
}

var platformComputeHostFiles = map[string]bool{
	"compute.go":            true,
	"ring.go":               true,
	"compute_forwarders.go": true,
	"decl.go":               true,
}

var platformMembraneFiles = map[string]bool{
	"caps.go":          true,
	"sysanchorcaps.go": true,
	"actorfactory.go":  true,
	"spawnhandle.go":   true,
}

// TestPlatformRootTopologyClosedSet enforces platform/doc.go's root-file
// three-way classification as a closed set: every production (non-_test.go)
// .go file that sits DIRECTLY in platform/ (sub-directories are packages of
// their own and out of scope here) must appear in exactly one of the three
// class maps above. doc.go itself is the documentation file being mirrored,
// not a classified topology file, and is exempted by name.
func TestPlatformRootTopologyClosedSet(t *testing.T) {
	entries, err := os.ReadDir("../platform")
	if err != nil {
		t.Fatalf("read platform dir: %v", err)
	}

	// Cross-check: no file may be double-listed across classes (a copy/paste
	// drift between the three maps would silently satisfy "classified" while
	// contradicting doc.go's "exactly one" claim).
	allClasses := []map[string]bool{platformChannelHostFiles, platformComputeHostFiles, platformMembraneFiles}
	seen := map[string]int{}
	for _, class := range allClasses {
		for name := range class {
			seen[name]++
		}
	}
	for name, count := range seen {
		if count > 1 {
			t.Errorf("platform root-topology maps: %s listed in %d classes, doc.go promises a closed 3-way partition (exactly one)", name, count)
		}
	}

	var unclassified []string
	var stale []string
	present := map[string]bool{}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if name == "doc.go" {
			continue
		}
		present[name] = true
		if !platformChannelHostFiles[name] && !platformComputeHostFiles[name] && !platformMembraneFiles[name] {
			unclassified = append(unclassified, name)
		}
	}

	// Stale entries: a class map naming a file that no longer exists at
	// platform/ root (renamed/removed without updating this anchor + doc.go).
	for name := range seen {
		if !present[name] {
			stale = append(stale, name)
		}
	}

	if len(unclassified) > 0 {
		sort.Strings(unclassified)
		t.Errorf("platform root files not classified in any of channel-host/compute-host/membrane (doc.go's Root-file topology section + this test's three maps must both list them): %v", unclassified)
	}
	if len(stale) > 0 {
		sort.Strings(stale)
		t.Errorf("platform root-topology maps name files no longer present at platform/ root (stale entries, doc.go likely needs the same trim): %v", stale)
	}
}
