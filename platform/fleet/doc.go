// Package fleet is the physical layer of the channel home: a WS-to-virtual-pipe
// multiplexer. For each daemon actor it creates a net.Pipe, performs a synthetic
// ipc handshake, and calls actorrt.Attach to register the actor as a port. The
// actorrt Deliverer routes envelopes transparently through the pipe; fleet relays
// ipc.KindDeliver frames back out over WS as computebus.DispatchFrame.
//
// Upward emits (FrameEmit) are written to truth via the injected harness.Writer
// directly on the WS path (not through the pipe). Death is pipe-close: the
// port sees EOF and fires the standard OnDown edge, identical to a local cell
// crash.
//
// Depends on: runtime (actorrt, ipc, harness, storespec) + wire (computebus).
// MUST NOT import: channelhost, lib, gateway, daemon.
package fleet
