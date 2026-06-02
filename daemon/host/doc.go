// Package host runs the business actor cells on an attached compute (v2: actors
// of kind tool/agent live here, not on the channel home). It maps an inbound
// computebus.DispatchFrame to actorrt.Runtime.Deliver (into the cell mailbox),
// sends a cell's emit up via homelink, and spawns worker subprocesses for agent
// actors (runtime/workerhost; compute↔worker stays runtime/ipc).
//
// It composes lib/adapterhost (Install → adapterActor cell) + lib/agentactor +
// lib/behavior futureHub (caller-side) + actorrt. The adapterActor's device
// callback is fed here via actorrt.Runtime.Ask (sync ack); lifecycle via Post;
// the caller-side futures Submit/Await/Watch are wired here from the channel
// futureHub.
//
// Port from: adapters/proxy/actors + cmd/daemon/adapter_wiring.go +
// channel_reconciler.go (the v1 composition root for hosting adapters). NO
// channel truth here (daemon is attached compute).
//
// Depends on runtime + lib + wire. MUST NOT import server.
package host
