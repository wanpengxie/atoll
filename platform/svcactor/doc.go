// Package svcactor implements each channel's peer-facing service endpoint.
// Its durable private state is a service-agent id plus a map from accepted word
// to complete member id; ordinary channels have no system-word table entries.
//
// Dispatch is structural: replies and cancellation return to pending calls;
// requests from c0 may reach membrane words, c0 itself routes space words to
// the fixed registrar target, explicit endpoint words use the private table,
// and agent.ask uses the configured service agent. The actor preserves the
// peer frame's caller with CallFor and maps local progress and terminal state
// back into peer protocol frames.
package svcactor
