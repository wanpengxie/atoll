// Package host runs business actor cells on an attached compute (daemon). Each
// cell is an actorrt.Actor spawned into a local Runtime. The cell's writer is
// an UplinkWriter that satisfies harness.Writer by forwarding each write UP to
// the channel home (server) as an EmitFrame, blocking for the home's EmitAck
// (the authoritative WriteResult). The daemon holds NO local truth.
//
// Host observes cell death via the actorrt.PresenceWatcher push and reports it
// UP via homelink.SendDeath so the home can materialise receiver_unavailable
// for in-flight requests addressed to the dead actor.
//
// Depends on runtime/actorrt + runtime/harness (interface/types only) +
// platform/computebus. MUST NOT import server or runtime/internal/store.
package host
