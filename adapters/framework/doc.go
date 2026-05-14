// Package framework provides the concrete M1.5 adapter framework that
// implements the contracts declared in kernel/adapter:
//
//   - F2 CorrelationTracker — request_id ↔ state lifecycle store.
//   - F3 ErrorPolicy        — timer-driven adapter_default_timeout terminal.
//   - F4 StateStore         — pluggable persistent state seam (in-memory default).
//   - F5 Respond            — terminal response builder + harness write.
//   - F6 Manager            — Install / Dispatch / OnExternalCallback / RunGC / Shutdown.
//   - F7 Observability      — Logger / Metrics / Tracer with noop defaults.
//   - F8 HTTP + Credentials — outbound HTTP helper + credential store + redact helper.
//
// The framework consumes kernel/adapter, kernel/message, kernel/actor,
// kernel/channel, and kernel/harness. It declares two private seams the
// daemon composition root (T3) wires up:
//
//   - RequestLookup   — F5 needs to look up the original request envelope
//     to derive correlation_id / sender / type for the response.
//   - TypeRegistry    — Install upserts handler_actor_id / handler_binding /
//     max_pending_ms rows for every Declaration.Type. The framework ships
//     an InMemory implementation for tests; T3 will wire a sqlite-backed
//     impl from runtime/store.
//
// Three binding kinds (L1 §11.7) flow through the same Manager:
//
//   - in_process        — Handle runs on the daemon goroutine; no transport.
//   - outbound_http     — Handle uses HTTPClient to reach an external API;
//     callbacks flow through Manager.OnExternalCallback.
//   - via_server_transit — Handle calls DeviceTransit.Send to push a
//     device_transit.send frame through the daemonbus; callbacks flow
//     through Manager.OnExternalCallback after server.devicebus delivers a
//     device_transit.recv frame.
//
// All public types are safe for concurrent use unless noted otherwise.
package framework
