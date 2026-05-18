package daemonbus

import (
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
)

// HumanCaller is the wire object the server attaches to
// control.write_message so the daemon can authenticate the human origin.
type HumanCaller struct {
	UserID           string `json:"user_id"`
	ActorIDInChannel string `json:"actor_id_in_channel"`
	TS               int64  `json:"ts"`
	Nonce            string `json:"nonce"`
	ServerToken      string `json:"server_token"`
}

// WriteMessageBody is the daemonbus control.write_message payload.
type WriteMessageBody struct {
	FrameID         string           `json:"frame_id"`
	ChannelID       string           `json:"channel_id"`
	HumanCaller     HumanCaller      `json:"human_caller"`
	EnvelopePartial message.Envelope `json:"envelope_partial"`
}

// WriteMessageAckBody is the daemon -> server control.write_message_ack
// payload. One ack is emitted per inbound control.write_message frame.
type WriteMessageAckBody struct {
	FrameID      string `json:"frame_id"`
	Accepted     bool   `json:"accepted"`
	MessageID    string `json:"message_id,omitempty"`
	Seq          int64  `json:"seq,omitempty"`
	Deduped      bool   `json:"deduped,omitempty"`
	RejectReason string `json:"reject_reason,omitempty"`
	RejectDetail string `json:"reject_detail,omitempty"`
}

// BindDeviceSessionBody is the daemonbus control.bind_device_session
// payload. The server emits it after allocating a device session row.
type BindDeviceSessionBody struct {
	FrameID          string                  `json:"frame_id"`
	SessionID        adapter.DeviceSessionID `json:"session_id"`
	ChannelID        channel.ID              `json:"channel_id"`
	DeviceID         string                  `json:"device_id"`
	DeviceType       string                  `json:"device_type"`
	DaemonID         placement.DaemonID      `json:"daemon_id"`
	TokenFingerprint string                  `json:"token_fingerprint"`
	ExpiresAt        int64                   `json:"expires_at,omitempty"`
	BoundAt          int64                   `json:"bound_at,omitempty"`
}

// BindDeviceSessionAckBody is the daemon -> server reply for
// control.bind_device_session.
type BindDeviceSessionAckBody struct {
	FrameID   string                  `json:"frame_id"`
	SessionID adapter.DeviceSessionID `json:"session_id,omitempty"`
	Accepted  bool                    `json:"accepted"`
	Reason    string                  `json:"reason,omitempty"`
	Detail    string                  `json:"detail,omitempty"`
}

// UnbindDeviceSessionBody is the daemonbus control.unbind_device_session
// payload.
type UnbindDeviceSessionBody struct {
	FrameID   string                  `json:"frame_id"`
	SessionID adapter.DeviceSessionID `json:"session_id"`
	ChannelID channel.ID              `json:"channel_id,omitempty"`
	Reason    string                  `json:"reason,omitempty"`
}

// UnbindDeviceSessionAckBody is the daemon -> server reply for
// control.unbind_device_session.
type UnbindDeviceSessionAckBody struct {
	FrameID   string                  `json:"frame_id"`
	SessionID adapter.DeviceSessionID `json:"session_id,omitempty"`
	Accepted  bool                    `json:"accepted"`
	Reason    string                  `json:"reason,omitempty"`
	Detail    string                  `json:"detail,omitempty"`
}

const (
	// BindRejectReasonDecodeFailed indicates a bind/unbind payload could
	// not be decoded. Dispatchers surface this as a protocol error.
	BindRejectReasonDecodeFailed = "decode_failed"

	// BindRejectReasonHandlerMissing is emitted when the daemon dispatcher
	// has no bind/unbind lifecycle handler wired.
	BindRejectReasonHandlerMissing = "handler_missing"
)
