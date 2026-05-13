// Package harness will host the daemon-side bindings of the v4 message-
// write harness (`POST /api/rpc/message.send` HTTP entrypoint + error
// mapping). The shared 9-step body lives in pkg/harness so that the
// in-worker bus binding can reuse it without import cycles.
//
// Both bindings are introduced by ticket T7
// (.dalek/pm/m1.3-tickets.md §T7).
package harness
