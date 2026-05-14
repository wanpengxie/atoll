package adapter

import "context"

// DeviceTransit is the kernel-level interface adapters use to send
// device frames. The concrete WS-mux client lives in runtime/transit
// per T3 and is wired in cmd/daemon's composition root.
//
// Covers codex 警告 #15: adapter framework MUST NOT depend on a
// runtime-specific transit. By declaring the interface in kernel,
// adapter code stays runtime-agnostic; adapters/ → kernel only.
//
// kernel/adapter only defines the contract.
type DeviceTransit interface {
	// SendFrame mux-pushes a device_transit.send frame to server.
	// frameID is the caller-supplied idempotency key (uuid).
	SendFrame(ctx context.Context, frameID string, payload []byte) error
}
