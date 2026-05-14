// Package transit is the daemon-side daemonbus client + view-sync push
// engine.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T3 + L1 §8 (view sync)
// + L2 §9 (daemonbus mux frame) + L1 §11.7 (device transit).
//
// Files:
//
//   - client.go         — Transport interface + mock_bus implementation
//     (in-process chan). Real WS in T6.
//   - frame.go          — daemonbus Frame mux (FrameType dispatch).
//   - outbox.go         — pulls pending view_sync_outbox rows, pushes
//     in seq ASC order, retries on failure.
//   - cursor.go         — last_pushed_seq / last_acked_seq tracker.
//   - ack_handler.go    — viewsync.ack -> advance LastAckedSeq + GC
//     outbox.
//   - resync_server.go  — daemon-side ServeResync(since, until) closed
//     interval RPC.
//   - device_transit.go — device_transit.* frame router (adapter ↔
//     server ↔ device).
//   - control.go        — control.* frame demux to lifecycle /
//     workerhost.
package transit
