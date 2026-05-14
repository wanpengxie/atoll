// Package topology declares the v5 deployment-topology placeholder
// types per .dalek/pm/m1.5-tickets.md §T10. It exists so that future
// federation / multi-region / SaaS work can express "what kind of
// node we are talking to" without changing call sites that already
// speak in topology.Node / topology.Peer.
//
// M1.5 demo topology is fixed (one server + N daemons + 0 peers); no
// concrete logic depends on these types yet. Federation v1 (M2+) will
// populate peer.Peer rows and route control-plane control messages
// across them.
//
// Invariants:
//
//   - Pure types only. No goroutines, no IO, no state.
//   - kernel/topology depends on the standard library only — no other
//     kernel/* imports (keeps the package a true leaf).
//   - Adding a NodeKind value here MUST update every closed-set
//     switch in kernel/topology and downstream callers; new values
//     land via spec change, not ad-hoc additions.
package topology
