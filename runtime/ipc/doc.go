// Package ipc declares the port wire format — the frames exchanged over an
// out-of-process actor's connection (runtime/actorrt port presence; Erlang
// open_port model). It is the smallest possible shared surface so the host
// side and the remote actor side agree on frame kinds and payload schemas:
// handshake / handshake_ack / deliver / emit / down.
package ipc
