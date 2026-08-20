package message

import "testing"

// Silence is the failure this type exists to prevent. Before it, parent and
// correlation were two optional fields, and a caller with no way to state a
// cause left them zero — which the ledger read as "nothing caused this". That
// was wrong for every message written to serve another one, and it was wrong
// silently.
func TestTheZeroCauseIsSilenceAndNotARoot(t *testing.T) {
	if (Cause{}).Stated() {
		t.Fatal("the zero Cause reports itself as stated")
	}
	if (Cause{}).IsRoot() {
		t.Fatal("the zero Cause reports itself as a root; silence and 'there is none' must not be the same value")
	}
}

// CorrelationID picks chain when pinned, else falls back to rootID. (Migrated
// with the function itself out of lib/behavior, where it used to live.)
func TestCorrelationID(t *testing.T) {
	if got := CorrelationID("chain", "root"); got != "chain" {
		t.Fatalf("want pinned chain, got %q", got)
	}
	if got := CorrelationID("", "root"); got != "root" {
		t.Fatalf("want rootID fallback, got %q", got)
	}
}

func TestARootOwnsItsOwnTree(t *testing.T) {
	parent, correlation := Root().Resolve("mine")
	if parent != "" {
		t.Fatalf("a root has no parent, got %q", parent)
	}
	if correlation != "mine" {
		t.Fatalf("a root heads its own tree, got correlation %q", correlation)
	}
	if !Root().IsRoot() || !Root().Stated() {
		t.Fatal("Root() must report itself as a stated root")
	}
}

// The invariant the two separate fields used to let callers break: a child
// joins its parent's TREE — the parent's correlation — not a tree named after
// its parent. Deriving from the parent's id would start a fresh tree at every
// hop, so a four-hop errand would leave four unrelated trees.
func TestAChildJoinsTheTreeItsParentIsIn_NotATreeNamedAfterItsParent(t *testing.T) {
	root := Envelope{ID: "root-1", CorrelationID: "root-1"}
	childParent, childCorrelation := From(root).Resolve("child-1")
	if childParent != "root-1" || childCorrelation != "root-1" {
		t.Fatalf("child resolved to parent %q correlation %q", childParent, childCorrelation)
	}

	child := Envelope{ID: "child-1", CorrelationID: childCorrelation}
	grandParent, grandCorrelation := From(child).Resolve("grand-1")
	if grandParent != "child-1" {
		t.Fatalf("grandchild parent %q, want child-1", grandParent)
	}
	if grandCorrelation != "root-1" {
		t.Fatalf("every message in one errand carries the root's id: got %q", grandCorrelation)
	}
	if grandCorrelation == "child-1" {
		t.Fatal("correlation was taken from the parent's id instead of the parent's correlation")
	}
}

// A parent carrying no correlation of its own still names a usable tree: it is
// read as heading one.
func TestAParentWithNoCorrelationOfItsOwnStillNamesATree(t *testing.T) {
	_, correlation := From(Envelope{ID: "older-row"}).Resolve("child")
	if correlation != "older-row" {
		t.Fatalf("correlation %q, want the parent's own id", correlation)
	}
}

// Anchored is the restore door: work that outlives the envelope which caused it
// — an agent turn resumed after a restart — still knows its errand.
func TestAnchoredRebuildsACauseWhoseEnvelopeIsGone(t *testing.T) {
	parent, correlation := Anchored("the-request", "the-errand").Resolve("new")
	if parent != "the-request" || correlation != "the-errand" {
		t.Fatalf("anchored resolved to parent %q correlation %q", parent, correlation)
	}
	// An anchor with no parent is a broken anchor, not a root. It must read as
	// silence so builders refuse it rather than quietly self-rooting.
	if Anchored("", "the-errand").Stated() {
		t.Fatal("an anchor with no parent reported itself as stated")
	}
}

// The two fields spell four combinations and only two mean anything. These are
// the two that used to be writable: no constructor can produce them now.
func TestTheTwoNonsenseCombinationsCannotBeBuilt(t *testing.T) {
	causes := []Cause{
		Root(),
		From(Envelope{ID: "a", CorrelationID: "tree"}),
		From(Envelope{ID: "a"}),
		Anchored("a", "tree"),
	}
	for _, cause := range causes {
		parent, correlation := cause.Resolve("self")
		hasParent := parent != ""
		ownsTree := correlation == "self"
		if !hasParent && !ownsTree {
			t.Fatalf("built no parent yet a member of someone else's tree: parent %q correlation %q", parent, correlation)
		}
		if hasParent && ownsTree {
			t.Fatalf("built a parent yet heading a tree of its own: parent %q correlation %q", parent, correlation)
		}
		if correlation == "" {
			t.Fatal("correlation must never be empty: every message belongs to some errand")
		}
	}
}
