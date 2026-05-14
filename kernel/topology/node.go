package topology

// NodeKind enumerates the deployment-topology node kinds per
// .dalek/pm/m1.5-tickets.md §T10 — "kind in {daemon, server,
// peer-server}".
//
// Closed set:
//
//   - NodeDaemon — a coagent daemon process (per-user / per-device).
//   - NodeServer — the central server process (one in M1.5).
//   - NodePeerServer — another coagent server on a federation peer
//     (M2+ placeholder; no concrete implementation in M1.5).
type NodeKind string

// NodeKind closed set.
const (
	NodeDaemon     NodeKind = "daemon"
	NodeServer     NodeKind = "server"
	NodePeerServer NodeKind = "peer_server" // M2+ federation; no M1.5 callers
)

// AllNodeKinds lists every node kind in spec order. Used by tests /
// downstream switches to assert closed-set coverage.
var AllNodeKinds = []NodeKind{
	NodeDaemon,
	NodeServer,
	NodePeerServer,
}

// String returns the wire form.
func (k NodeKind) String() string { return string(k) }

// NodeID is the stable per-node identifier (daemon registration id,
// server config id, peer-server federation id). Opaque string at the
// topology layer.
type NodeID string

// String returns the wire form.
func (n NodeID) String() string { return string(n) }

// Node is a topology participant — `(kind, id)` per .dalek/pm/m1.5-tickets.md
// §T10. M1.5 demo topology only emits NodeDaemon + NodeServer rows;
// NodePeerServer is reserved for M2+ federation.
//
// Pure value type — safe to copy, compare with ==, use as map key.
type Node struct {
	Kind NodeKind
	ID   NodeID
}

// IsLocalServer reports whether the node is the in-process server.
// Convenience helper for routing decisions.
func (n Node) IsLocalServer() bool { return n.Kind == NodeServer }
