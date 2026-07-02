// Package ipc defines the port wire protocol — the length-prefixed JSON
// byte-stream contract between the substrate host side (runtime/actorrt port
// embodiment) and one out-of-process actor. It is the smallest shared surface so
// both ends agree on frame kinds and payload schemas:
// handshake / handshake_ack / deliver / control / emit / emit_ack / down.
//
// Model: one connection == one actor (the Erlang open_port model). The
// connection IS the actor's identity, so there is no per-frame actor id or
// channel id. (Multiplexing many actors over one link = Erlang distribution;
// an additive future, not pre-built here.)
//
// Security boundary = the connection, authenticated once at handshake
// (resolve credential → ActorID). Thereafter the point-to-point stream is
// trusted — no per-frame re-auth (TCP/TLS model). A zombie/reconnecting actor
// is handled by connect-in REPLACE: a new connection for the same ActorID stops
// and closes the old one (actorrt Spawn-replace). No fence frame, no per-frame
// token.
//
// The wire is medium-agnostic: Codec wraps io.Reader / io.Writer, so the same
// protocol runs over a local pipe (same-node out-of-proc) or a net.Conn
// (remote actor across a network boundary).
package ipc
