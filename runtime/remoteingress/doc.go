// Package remoteingress is the runtime organ an authenticated remote endpoint
// enters through.
//
// It exists because "check" and "execute" must never be assembled by a
// composition layer. Before it, the link layer took an identity admission,
// minted a capability out of it and then invoked an organ — three moves in the
// wiring, which is exactly where a remote body's judgment silently drifts away
// from a local body's (the same call had to be written twice, and the remote
// copy verified less).
//
// The organ holds the invariant instead: every substrate operation an
// authenticated remote endpoint initiates completes ONE admission at the right
// precision — A/G for the pen and channel resources, A for state and schedule —
// and then enters the real organ door, the same door a local body's handle
// enters. There is no second implementation of any organ's logic here, and no
// raw-organ call anywhere.
//
// It is per Channel, stateless, and holds no actor: one instance is constructed
// beside the managed capability minter with the Controller and the four organ
// doors, and nothing else — no channel id, no per-actor input.
package remoteingress
