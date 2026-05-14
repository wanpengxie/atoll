// Package adapter holds the v4/v5 adapter framework contract:
//
//   - BindingKind closed-set enum (3 classes; covers codex 警告 #15)
//   - Module / Manager interfaces (F1)
//   - Correlation Tracker interface (F2)
//   - Error Policy interface (F3)
//   - AdapterCtx + ctx.Respond contract (F5)
//   - DeviceTransit interface (covers codex 警告 #15 — adapter uses
//     this to send device frames; runtime/transit composes the impl)
//
// kernel/adapter is IO-free. Concrete adapter implementations
// (xhs, feishu, slack, …) live in adapters/ per T4; runtime composes
// them in cmd/daemon per T3.
package adapter

// BindingKind classifies how an adapter actor reaches its handler
// (L1 §11.x, upgraded by T1.2).
//
// Three closed values:
//
//   - InProcess        — go-kimi worker tool wrapper, daemon-rpc HTTP,
//     in-worker bus; everything that runs inside the daemon / worker
//     process boundary.
//   - OutboundHTTP     — feishu / slack / github / OAuth callback;
//     adapter dials out from the daemon.
//   - ViaServerTransit — device class; adapter mux-frames its
//     command through daemon → server → device session.
//
// Legacy enum values (`daemon_rpc`, `in_worker_bus`) are install-time
// compatibility aliases that map to InProcess per L1 §11.x.
type BindingKind string

// BindingKind closed set (per L1 §11.x).
const (
	BindingInProcess        BindingKind = "in_process"
	BindingOutboundHTTP     BindingKind = "outbound_http"
	BindingViaServerTransit BindingKind = "via_server_transit"
)
