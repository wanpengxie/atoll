// Package link is the platform's cross-machine embodiment layer (redesign §3.2):
// ONE authenticated WS link carries N logical streams, each stream a
// (channel, actor) embodiment running the NATIVE port-wire protocol (runtime/ipc)
// with a real handshake — stream-on-link is the SAME contract as a local pipe
// (Erlang-distribution zero-translation discipline). Stream 0 is the link
// control plane (attach). The home side (accept.go) judges liveness via a
// per-link lease (Lease is a judgement role, not a package); the daemon side
// (dial.go) opens one stream per attached actor.
//
// frame.go owns only the mux framing: the bytes that slice one WS link into
// streams. It is pure mechanism — it never decodes the ipc/JSON payload it
// carries (zero-translation: a stream's bytes pass through as an opaque
// data frame).
package link
