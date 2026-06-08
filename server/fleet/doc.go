// Package fleet manages the attached computes (daemons) bound to this server. It
// receives api-key attach (wire/computebus.AttachRequest), tracks actor->compute
// assignment via wire/placement.Registry, dispatches envelopes DOWN to the hosting
// compute (computebus.DispatchFrame), and feeds EmitFrames UP into the channel
// harness (truth). Death frames and disconnect trigger receiver_unavailable
// materialisation at the home.
//
// Depends on runtime + lib + wire. MUST NOT import daemon.
package fleet
