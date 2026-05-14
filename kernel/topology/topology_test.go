package topology_test

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/topology"
)

// TestAllNodeKindsClosedSet — three values per m1.5-tickets §T10.
func TestAllNodeKindsClosedSet(t *testing.T) {
	want := map[topology.NodeKind]string{
		topology.NodeDaemon:     "daemon",
		topology.NodeServer:     "server",
		topology.NodePeerServer: "peer_server",
	}
	if len(topology.AllNodeKinds) != len(want) {
		t.Fatalf("AllNodeKinds len=%d want %d", len(topology.AllNodeKinds), len(want))
	}
	seen := map[topology.NodeKind]bool{}
	for _, k := range topology.AllNodeKinds {
		if seen[k] {
			t.Errorf("duplicate kind %v", k)
		}
		seen[k] = true
		if w, ok := want[k]; !ok || k.String() != w {
			t.Errorf("unexpected kind %v (string=%q)", k, k.String())
		}
	}
	for k := range want {
		if !seen[k] {
			t.Errorf("missing kind %v", k)
		}
	}
}

// TestNodeIDString — wire form round-trip.
func TestNodeIDString(t *testing.T) {
	if got := topology.NodeID("daemon-A").String(); got != "daemon-A" {
		t.Errorf("NodeID.String()=%q want daemon-A", got)
	}
}

// TestNodeIsLocalServer — kind=server triggers true; all others false.
func TestNodeIsLocalServer(t *testing.T) {
	cases := []struct {
		n    topology.Node
		want bool
	}{
		{topology.Node{Kind: topology.NodeServer, ID: "srv-1"}, true},
		{topology.Node{Kind: topology.NodeDaemon, ID: "d-1"}, false},
		{topology.Node{Kind: topology.NodePeerServer, ID: "peer-1"}, false},
		{topology.Node{}, false},
	}
	for _, c := range cases {
		if got := c.n.IsLocalServer(); got != c.want {
			t.Errorf("(%+v).IsLocalServer()=%v want %v", c.n, got, c.want)
		}
	}
}

// TestNodeValueSemantics — Node is a value type usable as map key.
func TestNodeValueSemantics(t *testing.T) {
	a := topology.Node{Kind: topology.NodeDaemon, ID: "d-1"}
	b := topology.Node{Kind: topology.NodeDaemon, ID: "d-1"}
	c := topology.Node{Kind: topology.NodeDaemon, ID: "d-2"}
	if a != b {
		t.Error("equal Node should compare ==")
	}
	if a == c {
		t.Error("different-id Node should compare !=")
	}
	m := map[topology.Node]int{a: 1}
	if m[b] != 1 {
		t.Error("Node must be usable as map key")
	}
}

// TestPeerIsZero — empty Peer is zero-value placeholder; populated is not.
func TestPeerIsZero(t *testing.T) {
	if !(topology.Peer{}).IsZero() {
		t.Error("zero-value Peer.IsZero()=false")
	}
	p := topology.Peer{
		Node:   topology.Node{Kind: topology.NodePeerServer, ID: "peer-1"},
		Origin: "https://peer-1.example",
	}
	if p.IsZero() {
		t.Error("populated Peer.IsZero()=true")
	}
}
