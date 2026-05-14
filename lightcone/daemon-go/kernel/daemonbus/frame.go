// Package daemonbus holds the daemon↔server control-plane mux frame
// schema (T1.5 §1.5).
//
// daemonbus is the single multiplexed WS connection between each daemon
// and server. It carries:
//
//   - view sync push/ack frames (kernel/viewsync)
//   - resync request/response frames (kernel/viewsync)
//   - placement control frames (kernel/placement)
//   - device session bind/unbind/transit frames (L4 §2.6)
//
// daemonbus frames are NOT channel envelopes — they don't go through
// the Message-Write Harness and aren't logged to channel-local message
// store. They are the protocol that BACKS the v4 envelope plane.
//
// kernel/daemonbus is IO-free. Concrete WS mux client lives in
// runtime/transit per T3.
package daemonbus

import "encoding/json"

// FrameType is the daemonbus mux frame type discriminator.
type FrameType string

// FrameType closed set (T1.5).
const (
	FrameViewsyncPush      FrameType = "viewsync.push"
	FrameViewsyncAck       FrameType = "viewsync.ack"
	FrameViewsyncResync    FrameType = "viewsync.resync"
	FrameViewsyncResyncRes FrameType = "viewsync.resync.response"
	FrameControlCreate     FrameType = "control.create_channel"
	FrameControlCreateAck  FrameType = "control.create_channel.ack"
	FrameControlBindDev    FrameType = "control.bind_device_session"
	FrameControlUnbindDev  FrameType = "control.unbind_device_session"
	FrameDeviceTransitSend FrameType = "device_transit.send"
	FrameDeviceTransitRecv FrameType = "device_transit.recv"
	FrameDeviceTransitAck  FrameType = "device_transit.ack"
	FrameDeviceTransitErr  FrameType = "device_transit.error"
)

// Frame is the daemonbus mux envelope (per T1.5 spec).
//
// All daemonbus frames carry a stable FrameID (uuid) for caller-side
// idempotency + correlation. Payload is the frame-type-specific body
// (JSON-encoded sub-frame).
type Frame struct {
	Type    FrameType       `json:"type"`
	FrameID string          `json:"frame_id"`
	Payload json.RawMessage `json:"payload"`
}
