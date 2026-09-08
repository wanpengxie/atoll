// Package channelmember implements the symbiotic channel-as-member relation.
//
// A Seat is a real member of host channel H (kind=channel) whose body is
// another channel A. A Handle is an organ inside A. The process-local Hub is
// the dumb seam between those two organs: it transports calls but owns no
// identity, policy, ledger, or routing decision.
//
// This is deliberately independent of peeractor/svcactor. Those packages
// implement a reference to a remote channel's service front; this package
// implements membership, so the Seat's own Sys is the authority used in H.
package channelmember
