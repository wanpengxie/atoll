// Package harness will host the shared 9-step message-write harness
// (universal id dedupe / auth / kind+type+audience / schema / doc_refs /
// The One Law) introduced by ticket T7
// (.dalek/pm/m1.3-tickets.md §T7).
//
// Two bindings reuse this body:
//
//   - internal/harness: HTTP daemon_rpc binding (POST /api/rpc/message.send)
//   - pkg/harness (here, exposed): in_worker_bus binding the go-kimi
//     worker imports directly.
package harness
