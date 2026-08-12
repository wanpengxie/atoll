// Package accessdoor is the channel access gate.
//
// File calls admit an active member, resolve the daemon name in the address,
// and mint a ticket only for a cross-machine or frontend byte leg. They never
// consult the kv registry for file existence: local open, stat, and readdir
// report the physical channel directory directly. Same-machine actor calls
// carry the logical path to the daemon-local opener without issuing a ticket.
//
// KV and actor state retain their existing registry-backed paths.
package accessdoor
