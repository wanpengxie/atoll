package daemonbus

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
)

// UserID is the server identity user identifier carried by human caller
// control messages.
type UserID string

// String returns the wire form.
func (u UserID) String() string { return string(u) }

// HumanCaller is the wire object the server attaches to
// control.write_message so the daemon can authenticate the human origin.
type HumanCaller struct {
	UserID        UserID        `json:"user_id"`
	MemberActorID actor.ActorID `json:"member_actor_id"`
	TS            int64         `json:"ts"`
	Nonce         string        `json:"nonce"`
	ServerToken   string        `json:"server_token"`
}

// WriteMessageBody is the daemonbus control.write_message payload.
type WriteMessageBody struct {
	FrameID         FrameID          `json:"frame_id"`
	ChannelID       channel.ID       `json:"channel_id"`
	HumanCaller     HumanCaller      `json:"human_caller"`
	EnvelopePartial message.Envelope `json:"envelope_partial"`
}

// WriteMessageAckBody is the daemon -> server control.write_message_ack
// payload. One ack is emitted per inbound control.write_message frame.
type WriteMessageAckBody struct {
	FrameID      FrameID    `json:"frame_id"`
	Accepted     bool       `json:"accepted"`
	MessageID    message.ID `json:"message_id,omitempty"`
	Seq          int64      `json:"seq,omitempty"`
	Deduped      bool       `json:"deduped,omitempty"`
	RejectReason string     `json:"reject_reason,omitempty"`
	RejectDetail string     `json:"reject_detail,omitempty"`
}

// DeviceSessionRejectReason is the impl-layer2 §5.6 closed reason set
// for bind/token/transit/unbind device-session rejects. Details stay
// free-form in the sibling Detail field.
type DeviceSessionRejectReason string

// String returns the wire form.
func (r DeviceSessionRejectReason) String() string { return string(r) }

// MuxRejectReason is the impl-layer2 §1.8 daemonbus mux reject reason
// closed set. These classify frame-envelope / connection-layer rejects,
// not application-level write_message or device-session rejects.
type MuxRejectReason string

// String returns the wire form.
func (r MuxRejectReason) String() string { return string(r) }

const (
	MuxRejectUnknownFrameKind           MuxRejectReason = "mux_unknown_frame_kind"
	MuxRejectUnknownFrameField          MuxRejectReason = "mux_unknown_frame_field"
	MuxRejectUnknownPayloadField        MuxRejectReason = "mux_unknown_payload_field"
	MuxRejectPayloadSchemaInvalid       MuxRejectReason = "mux_payload_schema_invalid"
	MuxRejectProtocolVersionUnsupported MuxRejectReason = "mux_protocol_version_unsupported"
	MuxRejectAuthFailed                 MuxRejectReason = "mux_auth_failed"
	MuxRejectDuplicateDaemon            MuxRejectReason = "mux_duplicate_daemon"
	MuxRejectChannelIDUnknown           MuxRejectReason = "mux_channel_id_unknown"
	MuxRejectOwnerEpochStale            MuxRejectReason = "mux_owner_epoch_stale"
	MuxRejectFrameTooLarge              MuxRejectReason = "mux_frame_too_large"
	MuxRejectIdleTimeout                MuxRejectReason = "mux_idle_timeout"
	MuxRejectInternalError              MuxRejectReason = "mux_internal_error"
)

// BindDeviceSessionBody is the daemonbus control.bind_device_session
// payload. The server emits it after allocating a device session row.
type BindDeviceSessionBody struct {
	FrameID          FrameID                       `json:"frame_id"`
	BindRequestID    string                        `json:"bind_request_id"`
	DeviceSessionID  devicetransit.DeviceSessionID `json:"device_session_id"`
	ChannelID        channel.ID                    `json:"channel_id"`
	AdapterActorID   actor.ActorID                 `json:"adapter_actor_id"`
	DeviceID         string                        `json:"device_id"`
	DeviceType       string                        `json:"device_type"`
	DaemonID         placement.DaemonID            `json:"daemon_id"`
	TokenFingerprint string                        `json:"token_fingerprint"`
	ExpiresAt        int64                         `json:"expires_at,omitempty"`
	BoundAt          int64                         `json:"bound_at,omitempty"`
	Metadata         map[string]string             `json:"metadata,omitempty"`
}

// DeviceSessionBindResult is the canonical bind/unbind ack result set.
type DeviceSessionBindResult string

const (
	DeviceSessionBindAccepted DeviceSessionBindResult = "accepted"
	DeviceSessionBindRejected DeviceSessionBindResult = "rejected"
)

// BindDeviceSessionAckBody is the daemon -> server reply for
// control.bind_device_session.
type BindDeviceSessionAckBody struct {
	FrameID         FrameID                       `json:"frame_id"`
	ChannelID       channel.ID                    `json:"channel_id"`
	BindRequestID   string                        `json:"bind_request_id"`
	Result          DeviceSessionBindResult       `json:"result"`
	DeviceSessionID devicetransit.DeviceSessionID `json:"device_session_id,omitempty"`
	Reason          DeviceSessionRejectReason     `json:"reason,omitempty"`
	Detail          string                        `json:"detail,omitempty"`
}

// UnbindDeviceSessionBody is the daemonbus control.unbind_device_session
// payload.
type UnbindDeviceSessionBody struct {
	FrameID         FrameID                       `json:"frame_id"`
	DeviceSessionID devicetransit.DeviceSessionID `json:"device_session_id"`
	ChannelID       channel.ID                    `json:"channel_id"`
	Reason          string                        `json:"reason,omitempty"`
}

// UnbindDeviceSessionAckBody is the daemon -> server reply for
// control.unbind_device_session.
type UnbindDeviceSessionAckBody struct {
	FrameID         FrameID                       `json:"frame_id"`
	ChannelID       channel.ID                    `json:"channel_id"`
	Result          DeviceSessionBindResult       `json:"result"`
	DeviceSessionID devicetransit.DeviceSessionID `json:"device_session_id,omitempty"`
	Reason          DeviceSessionRejectReason     `json:"reason,omitempty"`
	Detail          string                        `json:"detail,omitempty"`
}

// UnbindChannelReason is the server -> daemon trigger classification for
// control.unbind_channel per impl-layer2 §3.4.1.
type UnbindChannelReason string

const (
	UnbindChannelReasonUserUnbind     UnbindChannelReason = "user_unbind"
	UnbindChannelReasonAbandon        UnbindChannelReason = "abandon"
	UnbindChannelReasonReclaimPreStep UnbindChannelReason = "reclaim_pre_step"
)

// UnbindChannelResult is the daemon -> server result set for
// control.unbind_channel_ack.
type UnbindChannelResult string

const (
	UnbindChannelReleased UnbindChannelResult = "released"
	UnbindChannelRejected UnbindChannelResult = "rejected"
)

// UnbindChannelRejectReason is the daemon-side reject reason set for
// control.unbind_channel_ack.
type UnbindChannelRejectReason string

const (
	UnbindChannelRejectOwnerEpochStale UnbindChannelRejectReason = "unbind_owner_epoch_stale"
	UnbindChannelRejectAlreadyReleased UnbindChannelRejectReason = "unbind_already_released"
	UnbindChannelRejectInternalError   UnbindChannelRejectReason = "unbind_internal_error"
)

// UnbindChannelBody is the server -> daemon control.unbind_channel payload.
type UnbindChannelBody struct {
	ChannelID  channel.ID           `json:"channel_id"`
	OwnerEpoch placement.OwnerEpoch `json:"owner_epoch"`
	Reason     UnbindChannelReason  `json:"reason"`
}

// UnbindChannelAckBody is the daemon -> server control.unbind_channel_ack
// payload.
type UnbindChannelAckBody struct {
	ChannelID  channel.ID                `json:"channel_id"`
	OwnerEpoch placement.OwnerEpoch      `json:"owner_epoch"`
	Result     UnbindChannelResult       `json:"result"`
	Reason     UnbindChannelRejectReason `json:"reason,omitempty"`
}

const (
	DeviceSessionRejectBindChannelNotActive      DeviceSessionRejectReason = "bind_channel_not_active"
	DeviceSessionRejectBindAdapterNotPresent     DeviceSessionRejectReason = "bind_adapter_not_present"
	DeviceSessionRejectBindAdapterBindingInvalid DeviceSessionRejectReason = "bind_adapter_binding_invalid"
	DeviceSessionRejectBindDeviceTypeUnsupported DeviceSessionRejectReason = "bind_device_type_unsupported"
	DeviceSessionRejectBindCapacityExceeded      DeviceSessionRejectReason = "bind_capacity_exceeded"
	DeviceSessionRejectBindInternalError         DeviceSessionRejectReason = "bind_internal_error"

	DeviceSessionRejectTokenInvalid   DeviceSessionRejectReason = "device_token_invalid"
	DeviceSessionRejectTokenExpired   DeviceSessionRejectReason = "device_token_expired"
	DeviceSessionRejectSessionRevoked DeviceSessionRejectReason = "device_session_revoked"
	DeviceSessionRejectSessionUnknown DeviceSessionRejectReason = "device_session_unknown"

	DeviceSessionRejectTransitPayloadTooLarge  DeviceSessionRejectReason = "transit_payload_too_large"
	DeviceSessionRejectTransitRouteUnavailable DeviceSessionRejectReason = "transit_route_unavailable"
	DeviceSessionRejectTransitInternalError    DeviceSessionRejectReason = "transit_internal_error"

	DeviceSessionRejectUnbindSessionUnknown DeviceSessionRejectReason = "unbind_session_unknown"
	DeviceSessionRejectUnbindInternalError  DeviceSessionRejectReason = "unbind_internal_error"
)
