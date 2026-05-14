package channel_test

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/channel"
)

// TestIDStringWireForm — ID.String returns the raw string.
func TestIDStringWireForm(t *testing.T) {
	cases := []string{"", "ch-1", "channel:abc:0", "ch:文字"}
	for _, c := range cases {
		if got := channel.ID(c).String(); got != c {
			t.Errorf("channel.ID(%q).String()=%q want %q", c, got, c)
		}
	}
}

// TestRefLocalEmptyOrg — ref with empty OrgID is Local; non-empty is not.
func TestRefLocalEmptyOrg(t *testing.T) {
	loc := channel.Ref{ID: "ch-1"}
	if !loc.Local() {
		t.Error("ref with empty OrgID should be Local")
	}
	fed := channel.Ref{OrgID: "org-X", ID: "ch-1"}
	if fed.Local() {
		t.Error("ref with non-empty OrgID must NOT be Local")
	}
}

// TestRefValueSemantics — ref is a pure value type usable as a map key.
func TestRefValueSemantics(t *testing.T) {
	a := channel.Ref{OrgID: "org-A", ID: "ch-1"}
	b := channel.Ref{OrgID: "org-A", ID: "ch-1"}
	c := channel.Ref{OrgID: "org-A", ID: "ch-2"}
	if a != b {
		t.Error("equal-valued Ref should compare == ")
	}
	if a == c {
		t.Error("different-id Ref should compare != ")
	}
	m := map[channel.Ref]int{a: 1}
	if m[b] != 1 {
		t.Error("Ref must be usable as map key (value equality)")
	}
}
