// Package daemonbus declares the daemonbus mux frame schema — the
// top-level wrapper that carries every control-plane / view-sync /
// device-transit frame between daemon and server (L2 §9 daemonbus mux
// frame spec). The frame_type closed set + shared header fields belong
// here; per-frame-type payloads live in kernel/viewsync, kernel/placement,
// and kernel/devicetransit.
package daemonbus

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/placement"
)

// FrameType is the M1.5 closed set of daemonbus frame_type values. Per
// L2 §9.1 there are three categories (viewsync.* / control.* /
// device_transit.*) — each value is a string that matches the JSON
// wire form 1:1.
type FrameType string

// FrameType closed set per L2 §9.1.
const (
	// View-sync frames (also exposed via kernel/viewsync.FrameType — keep
	// duplicated string values in sync; both packages reference L1 §8.3
	// + L2 §9.1 as the source of truth).
	FrameTypeViewsyncPush           FrameType = "viewsync.push"
	FrameTypeViewsyncAck            FrameType = "viewsync.ack"
	FrameTypeViewsyncResyncRequest  FrameType = "viewsync.resync_request"
	FrameTypeViewsyncResyncResponse FrameType = "viewsync.resync_response"

	// Control plane frames (placement / member sync / device session /
	// human caller) per L2 §9.1.
	FrameTypeControlCreateChannel          FrameType = "control.create_channel"
	FrameTypeControlCreateChannelAck       FrameType = "control.create_channel_ack"
	FrameTypeControlUnbindChannel          FrameType = "control.unbind_channel"
	FrameTypeControlUnbindChannelAck       FrameType = "control.unbind_channel_ack"
	FrameTypeControlHeartbeat              FrameType = "control.heartbeat"
	FrameTypeControlHeartbeatAck           FrameType = "control.heartbeat_ack"
	FrameTypeControlDaemonReclaim          FrameType = "control.daemon_reclaim"
	FrameTypeControlReclaimAccepted        FrameType = "control.reclaim_accepted"
	FrameTypeControlReclaimRejected        FrameType = "control.reclaim_rejected"
	FrameTypeControlRejectChannel          FrameType = "control.reject_channel"
	FrameTypeControlBindDeviceSession      FrameType = "control.bind_device_session"
	FrameTypeControlBindDeviceSessionAck   FrameType = "control.bind_device_session_ack"
	FrameTypeControlUnbindDeviceSession    FrameType = "control.unbind_device_session"
	FrameTypeControlUnbindDeviceSessionAck FrameType = "control.unbind_device_session_ack"
	FrameTypeControlUpdateMembers          FrameType = "control.update_members"
	FrameTypeControlUpdateMembersAck       FrameType = "control.update_members_ack"
	FrameTypeControlWriteMessage           FrameType = "control.write_message"
	FrameTypeControlWriteMessageAck        FrameType = "control.write_message_ack"

	// Device transit frames per L2 §9.1 + L4 §2.6.4.
	FrameTypeDeviceTransitSend  FrameType = "device_transit.send"
	FrameTypeDeviceTransitRecv  FrameType = "device_transit.recv"
	FrameTypeDeviceTransitAck   FrameType = "device_transit.ack"
	FrameTypeDeviceTransitError FrameType = "device_transit.error"
)

// AllFrameTypes lists every daemonbus frame_type in spec order — used
// by tests to assert the closed-set field-coverage.
var AllFrameTypes = []FrameType{
	// view-sync
	FrameTypeViewsyncPush,
	FrameTypeViewsyncAck,
	FrameTypeViewsyncResyncRequest,
	FrameTypeViewsyncResyncResponse,
	// control plane
	FrameTypeControlCreateChannel,
	FrameTypeControlCreateChannelAck,
	FrameTypeControlUnbindChannel,
	FrameTypeControlUnbindChannelAck,
	FrameTypeControlHeartbeat,
	FrameTypeControlHeartbeatAck,
	FrameTypeControlDaemonReclaim,
	FrameTypeControlReclaimAccepted,
	FrameTypeControlReclaimRejected,
	FrameTypeControlRejectChannel,
	FrameTypeControlBindDeviceSession,
	FrameTypeControlBindDeviceSessionAck,
	FrameTypeControlUnbindDeviceSession,
	FrameTypeControlUnbindDeviceSessionAck,
	FrameTypeControlUpdateMembers,
	FrameTypeControlUpdateMembersAck,
	FrameTypeControlWriteMessage,
	FrameTypeControlWriteMessageAck,
	// device transit
	FrameTypeDeviceTransitSend,
	FrameTypeDeviceTransitRecv,
	FrameTypeDeviceTransitAck,
	FrameTypeDeviceTransitError,
}

// Category groups frame types by their L2 §9.1 category — used by
// dispatch tables + tests.
type Category string

const (
	CategoryViewsync      Category = "viewsync"
	CategoryControl       Category = "control"
	CategoryDeviceTransit Category = "device_transit"
)

// CategoryOf returns the L2 §9.1 category for a frame_type. Returns
// empty string for unknown values.
func CategoryOf(ft FrameType) Category {
	switch ft {
	case FrameTypeViewsyncPush,
		FrameTypeViewsyncAck,
		FrameTypeViewsyncResyncRequest,
		FrameTypeViewsyncResyncResponse:
		return CategoryViewsync
	case FrameTypeDeviceTransitSend,
		FrameTypeDeviceTransitRecv,
		FrameTypeDeviceTransitAck,
		FrameTypeDeviceTransitError:
		return CategoryDeviceTransit
	}
	// Anything starting with "control." (the rest of the closed set).
	if len(ft) > len("control.") && ft[:len("control.")] == "control." {
		return CategoryControl
	}
	return ""
}

// String returns the wire form of the frame_type.
func (f FrameType) String() string { return string(f) }

// String returns the wire form of the category.
func (c Category) String() string { return string(c) }

// ConnectionEpoch is the daemon-bus connection epoch (incremented on
// every WS reconnect — L2 §9.4). Frames carry it so receivers can drop
// frames from a stale connection.
type ConnectionEpoch int64

// FrameID is the daemonbus mux frame identifier.
type FrameID string

// String returns the wire form.
func (f FrameID) String() string { return string(f) }

// Frame is the daemonbus mux wrapper carried over the WS connection
// (L2 §9.2). Every frame — viewsync.*, control.*, device_transit.* —
// rides inside this envelope.
//
// The Payload is a json.RawMessage so callers can decode it into the
// type-specific struct (kernel/viewsync.PushFrame, kernel/placement.
// CreateChannelRequest, etc.) without forcing kernel/daemonbus to
// import every payload package.
type Frame struct {
	FrameID               FrameID            `json:"frame_id"`
	FrameType             FrameType          `json:"frame_type"`
	DaemonID              placement.DaemonID `json:"daemon_id"`
	DaemonConnectionEpoch ConnectionEpoch    `json:"daemon_connection_epoch"`
	SentAt                int64              `json:"sent_at"`
	Payload               json.RawMessage    `json:"payload"`
}

// HeaderFields lists the 5 daemonbus mux header field names (excluding
// payload). Used by frame_test.go to assert the schema 1:1 with L2 §9.2.
var HeaderFields = []string{
	"frame_id",
	"frame_type",
	"daemon_id",
	"daemon_connection_epoch",
	"sent_at",
}
