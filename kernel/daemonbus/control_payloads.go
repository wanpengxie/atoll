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

// BindRejectReason is the closed daemon-edge reason set for bind/unbind
// session acks. Details stay free-form in the sibling Detail field.
type BindRejectReason string

// String returns the wire form.
func (r BindRejectReason) String() string { return string(r) }

// BindDeviceSessionBody is the daemonbus control.bind_device_session
// payload. The server emits it after allocating a device session row.
type BindDeviceSessionBody struct {
	FrameID          FrameID                       `json:"frame_id"`
	SessionID        devicetransit.DeviceSessionID `json:"session_id"`
	ChannelID        channel.ID                    `json:"channel_id"`
	DeviceID         string                        `json:"device_id"`
	DeviceType       string                        `json:"device_type"`
	DaemonID         placement.DaemonID            `json:"daemon_id"`
	TokenFingerprint string                        `json:"token_fingerprint"`
	ExpiresAt        int64                         `json:"expires_at,omitempty"`
	BoundAt          int64                         `json:"bound_at,omitempty"`
}

// BindDeviceSessionAckBody is the daemon -> server reply for
// control.bind_device_session.
type BindDeviceSessionAckBody struct {
	FrameID   FrameID                       `json:"frame_id"`
	SessionID devicetransit.DeviceSessionID `json:"session_id,omitempty"`
	Accepted  bool                          `json:"accepted"`
	Reason    BindRejectReason              `json:"reason,omitempty"`
	Detail    string                        `json:"detail,omitempty"`
}

// UnbindDeviceSessionBody is the daemonbus control.unbind_device_session
// payload.
type UnbindDeviceSessionBody struct {
	FrameID   FrameID                       `json:"frame_id"`
	SessionID devicetransit.DeviceSessionID `json:"session_id"`
	ChannelID channel.ID                    `json:"channel_id,omitempty"`
	Reason    string                        `json:"reason,omitempty"`
}

// UnbindDeviceSessionAckBody is the daemon -> server reply for
// control.unbind_device_session.
type UnbindDeviceSessionAckBody struct {
	FrameID   FrameID                       `json:"frame_id"`
	SessionID devicetransit.DeviceSessionID `json:"session_id,omitempty"`
	Accepted  bool                          `json:"accepted"`
	Reason    BindRejectReason              `json:"reason,omitempty"`
	Detail    string                        `json:"detail,omitempty"`
}

const (
	// BindRejectReasonDecodeFailed indicates a bind/unbind payload could
	// not be decoded. Dispatchers surface this as a protocol error.
	BindRejectReasonDecodeFailed BindRejectReason = "decode_failed"

	// BindRejectReasonHandlerMissing is emitted when the daemon dispatcher
	// has no bind/unbind lifecycle handler wired.
	BindRejectReasonHandlerMissing BindRejectReason = "handler_missing"

	// BindRejectReasonSessionStoreUpsert indicates the daemon failed to
	// mirror a bind into its local device session store.
	BindRejectReasonSessionStoreUpsert BindRejectReason = "session_store_upsert"

	// BindRejectReasonSessionStoreDelete indicates the daemon failed to
	// remove a local device session mirror during unbind.
	BindRejectReasonSessionStoreDelete BindRejectReason = "session_store_delete"
)
