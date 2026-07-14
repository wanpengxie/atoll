package archtest

import (
	"os"
	"sort"
	"strings"
	"testing"
)

// platform-topology 批 T5b / spec §0 裁决 2 + 裁决 8: the root now purifies to
// the cross-host membrane's own shared word table — exactly four non-test
// files (doc.go / decl.go / actorfactory.go / plan.go). Both hosts (platform/home,
// platform/compute) moved out into their own packages in T5b; the T4-era
// three-way channel-host/compute-host/membrane classification is retired —
// there is nothing left at root to classify BY host, only the membrane
// itself. This is the enforcement anchor: any new non-test .go file appearing
// directly under platform/ (sub-packages are out of scope) turns this test
// red — the root is a closed set, not an open one a future file can join
// without a spec decision.
var platformRootClosedSet = map[string]bool{
	"doc.go":          true,
	"decl.go":         true,
	"actorfactory.go": true,
	"plan.go":         true,
}

// TestPlatformRootTopologyClosedSet enforces platform/doc.go's root-file
// closed set: every production (non-_test.go) .go file that sits DIRECTLY in
// platform/ (sub-directories are packages of their own and out of scope here)
// must be exactly platformRootClosedSet — no more, no less.
func TestPlatformRootTopologyClosedSet(t *testing.T) {
	entries, err := os.ReadDir("../platform")
	if err != nil {
		t.Fatalf("read platform dir: %v", err)
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
		present[name] = true
		if !platformRootClosedSet[name] {
			unclassified = append(unclassified, name)
		}
	}

	// Stale entries: the closed set naming a file that no longer exists at
	// platform/ root (renamed/removed without updating this anchor + doc.go).
	for name := range platformRootClosedSet {
		if !present[name] {
			stale = append(stale, name)
		}
	}

	if len(unclassified) > 0 {
		sort.Strings(unclassified)
		t.Errorf("platform root files outside the closed set {doc.go, decl.go, actorfactory.go, plan.go} (doc.go's Root-file topology section + this test's set must both list them): %v", unclassified)
	}
	if len(stale) > 0 {
		sort.Strings(stale)
		t.Errorf("platform root-topology closed set names files no longer present at platform/ root (stale entries, doc.go likely needs the same trim): %v", stale)
	}
}
