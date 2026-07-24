// Package link is the platform's cross-machine physical-link layer:
// ONE authenticated WS link carries N logical yamux substreams, each stream a
// (channel, actor) stream running the native actor wire protocol (runtime/ipc)
// with a real handshake — stream-on-link is the SAME contract as a local pipe
// (Erlang-distribution zero-translation discipline). The link control plane
// (attach) rides its own dedicated substream. The home side (accept.go) judges
// liveness via a per-link lease (Lease is a judgement role, not a package); the
// daemon side (dial.go) opens one stream per attached actor. Beyond message
// relay, each per-actor stream also carries that actor's access (+ state) and
// schedule capability arms (KindAccess/KindSchedule FIFO round-trips) — a
// daemon-hosted cell's off-log and time-axis capability travels the SAME
// stream, not a separate channel (transport neutrality, see CellArms).
//
// linksession.go owns the mux: one top-level yamux.Session rides a wsByteStream
// (the raw WS connection adapted to a byte stream), and every substream opens
// with a self-describing streamHeader{Kind} so the accept loop can dispatch it
// to its plane (control / actor / lane) without relying on stream ordering. It
// is pure mechanism — it never decodes the ipc/JSON payload a substream
// carries (zero-translation: a stream's bytes pass through as opaque data).
package link
