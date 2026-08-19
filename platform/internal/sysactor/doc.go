// Package sysactor implements the single control-plane door present in every
// channel. It is addressed by actor.SystemActorID and is assembled with
// platform-internal capabilities rather than inserted as a roster member.
//
// The door owns membrane operations and log queries. For space-level system
// requests it preserves EffectiveCaller: a non-c0 door sends a peer frame to
// c0, while the c0 door calls the fixed registrar target. It maps peer progress
// and terminal results back to the original local request and emits the closed
// system events for successful membership changes and inbound frames.
//
// Sysactor remains a thin policy and routing boundary. Durable membership,
// actor lifecycle, cross-channel transport, and registrar state stay behind
// injected capabilities owned by their respective platform components.
package sysactor
