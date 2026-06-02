// Package fleet manages the attached computes (daemons) bound to this server. It
// receives api-key attach (wire/computebus.AttachRequest), tracks actor→compute
// assignment + lease (wire/placement), dispatches envelopes DOWN to the hosting
// compute (computebus.DispatchFrame) and feeds EmitFrames UP into the channel
// harness. Presence lease reports flow through here into the channel sysactor.
//
// Port from: server/daemonbus + server/devicebus + server/catalog +
// server/placements (v1 view-cache/directory split → one fleet manager over the
// v2 home↔compute wire).
//
// Depends on runtime + lib + wire + internal. MUST NOT import daemon.
package fleet
