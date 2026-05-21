// Package daemonbus declares the daemonbus mux frame schema — the
// top-level wrapper that carries every control-plane / view-sync /
// device-transit frame between daemon and server (impl-layer2 §1).
// The frame_type closed set + shared header fields belong
// here; per-frame-type payloads live in kernel/viewsync, kernel/placement,
// and kernel/devicetransit.
package daemonbus

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/placement"
)

// FrameType is the M1.5 closed set of daemonbus frame_type values. Per
// impl-layer2 §1.2 there are three categories (viewsync.* / control.* /
// device_transit.*) — each value is a string that matches the JSON
// wire form 1:1.
type FrameType string

// FrameType closed set per impl-layer2 §1.2.
const (
	// View-sync frames (also exposed via kernel/viewsync.FrameType — keep
	// duplicated string values in sync; both packages reference L1 §8.3
	// + impl-layer2 §1.2 as the source of truth).
	FrameTypeViewsyncPush           FrameType = "viewsync.push"
	FrameTypeViewsyncAck            FrameType = "viewsync.ack"
	FrameTypeViewsyncResyncRequest  FrameType = "viewsync.resync_request"
	FrameTypeViewsyncResyncResponse FrameType = "viewsync.resync_response"

	// Control plane frames (placement / member sync / device session /
	// human caller) per impl-layer2 §1.2.
	FrameTypeControlConnectionAccepted     FrameType = "control.connection_accepted"
	FrameTypeControlCreateChannel          FrameType = "control.create_channel"
	FrameTypeControlCreateChannelAck       FrameType = "control.create_channel_ack"
	FrameTypeControlUnbindChannel          FrameType = "control.unbind_channel"
	FrameTypeControlUnbindChannelAck       FrameType = "control.unbind_channel_ack"
	FrameTypeControlHeartbeat              FrameType = "control.heartbeat"
	FrameTypeControlHeartbeatAck           FrameType = "control.heartbeat_ack"
	FrameTypeControlHeldChannelsReport     FrameType = "control.held_channels_report"
	FrameTypeControlHeldChannelsAck        FrameType = "control.held_channels_ack"
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

	// Device transit frames per impl-layer2 §1.2 + L4 §2.6.4.
	FrameTypeDeviceTransitSend FrameType = "device_transit.send"
	FrameTypeDeviceTransitRecv FrameType = "device_transit.recv"
	FrameTypeDeviceTransitAck  FrameType = "device_transit.ack"
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
	FrameTypeControlConnectionAccepted,
	FrameTypeControlCreateChannel,
	FrameTypeControlCreateChannelAck,
	FrameTypeControlUnbindChannel,
	FrameTypeControlUnbindChannelAck,
	FrameTypeControlHeartbeat,
	FrameTypeControlHeartbeatAck,
	FrameTypeControlHeldChannelsReport,
	FrameTypeControlHeldChannelsAck,
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
}

// Category groups frame types by their impl-layer2 §1.2 category — used by
// dispatch tables + tests.
type Category string

const (
	CategoryViewsync      Category = "viewsync"
	CategoryControl       Category = "control"
	CategoryDeviceTransit Category = "device_transit"
)

// CategoryOf returns the impl-layer2 §1.2 category for a frame_type. Returns
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
		FrameTypeDeviceTransitAck:
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
// every accepted WS connection). Post-handshake frames carry it so receivers
// can drop frames from a stale connection before payload dispatch.
type ConnectionEpoch int64

// FrameID is the daemonbus mux frame identifier.
type FrameID string

// String returns the wire form.
func (f FrameID) String() string { return string(f) }

// Frame is the daemonbus mux wrapper carried over the WS connection
// (impl-layer2 §1.3). Every frame — viewsync.*, control.*,
// device_transit.* — rides inside this outer envelope.
//
// Spec-canonical outer envelope field set (impl-layer2 §1.3):
//
//	frame_kind            string     required — closed set (§1.2)
//	frame_id              string     required — uniqueness within connection
//	correlation_frame_id  string|null optional — pairs response back to its
//	                                  request frame's frame_id
//	channel_id            string|null optional — channel-scope frames
//	                                  populate it; connection-level frames
//	                                  (e.g. heartbeat) leave it empty
//	daemon_id            string     required post-handshake — source daemon
//	                                  on daemon→server frames, target daemon
//	                                  on server→daemon frames
//	daemon_connection_epoch int64    required post-handshake — server-assigned
//	                                  connection epoch; validate before
//	                                  payload dispatch
//	ts                    int64      required — sender emit time (ms epoch)
//	payload               object     required — family-specific schema
//
// DaemonID / DaemonConnectionEpoch are spec-canonical mux headers, not
// per-family payload identity. Payload fields with the same names are
// legacy / diagnostic only and must not override the outer envelope.
//
// The Payload is a json.RawMessage so callers can decode it into the
// type-specific struct (kernel/viewsync.PushFrame, kernel/placement.
// CreateChannelRequest, etc.) without forcing kernel/daemonbus to
// import every payload package.
type Frame struct {
	FrameKind             FrameType          `json:"frame_kind"`
	FrameID               FrameID            `json:"frame_id"`
	CorrelationFrameID    FrameID            `json:"correlation_frame_id,omitempty"`
	ChannelID             string             `json:"channel_id,omitempty"`
	Ts                    int64              `json:"ts"`
	Payload               json.RawMessage    `json:"payload"`
	DaemonID              placement.DaemonID `json:"daemon_id,omitempty"`
	DaemonConnectionEpoch ConnectionEpoch    `json:"daemon_connection_epoch,omitempty"`
}

// HeaderFields lists the impl-layer2 §1.3 outer envelope field names
// (excluding payload, in spec order). Used by frame_test.go to assert
// the schema 1:1 with spec.
var HeaderFields = []string{
	"frame_kind",
	"frame_id",
	"correlation_frame_id",
	"channel_id",
	"daemon_id",
	"daemon_connection_epoch",
	"ts",
}
