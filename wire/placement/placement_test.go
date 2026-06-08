package placement

import (
	"sort"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

func TestAssignLookup(t *testing.T) {
	r := New()
	r.Assign("a1", "c1")
	r.Assign("a2", "c1")
	r.Assign("a3", "c2")

	if cid, ok := r.Lookup("a1"); !ok || cid != "c1" {
		t.Fatalf("a1 = %q %v", cid, ok)
	}
	if cid, ok := r.Lookup("a3"); !ok || cid != "c2" {
		t.Fatalf("a3 = %q %v", cid, ok)
	}
	if _, ok := r.Lookup("missing"); ok {
		t.Fatal("missing should not be found")
	}
}

func TestAssignReassign(t *testing.T) {
	r := New()
	r.Assign("a1", "c1")
	r.Assign("a1", "c2")

	if cid, _ := r.Lookup("a1"); cid != "c2" {
		t.Fatalf("reassign: a1 = %q, want c2", cid)
	}
	if actors := r.ByCompute("c1"); len(actors) != 0 {
		t.Fatalf("old compute still has actors: %v", actors)
	}
}

func TestRemove(t *testing.T) {
	r := New()
	r.Assign("a1", "c1")
	r.Remove("a1")

	if _, ok := r.Lookup("a1"); ok {
		t.Fatal("a1 should be gone")
	}
	r.Remove("nonexistent")
}

func TestByCompute(t *testing.T) {
	r := New()
	r.Assign("a1", "c1")
	r.Assign("a2", "c1")
	r.Assign("a3", "c2")

	got := r.ByCompute("c1")
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if len(got) != 2 || got[0] != actor.ActorID("a1") || got[1] != actor.ActorID("a2") {
		t.Fatalf("ByCompute(c1) = %v", got)
	}
}

func TestRemoveCompute(t *testing.T) {
	r := New()
	r.Assign("a1", "c1")
	r.Assign("a2", "c1")
	r.Assign("a3", "c2")

	affected := r.RemoveCompute("c1")
	sort.Slice(affected, func(i, j int) bool { return affected[i] < affected[j] })
	if len(affected) != 2 || affected[0] != actor.ActorID("a1") {
		t.Fatalf("affected = %v", affected)
	}
	if _, ok := r.Lookup("a1"); ok {
		t.Fatal("a1 should be gone after RemoveCompute")
	}
	if _, ok := r.Lookup("a3"); !ok {
		t.Fatal("a3 on c2 should survive")
	}
}
