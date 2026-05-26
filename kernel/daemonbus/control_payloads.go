package daemonbus

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
)

// UserID is the server identity user identifier carried by human caller
// control messages.
type UserID string

// String returns the wire form.
func (u UserID) String() string { return string(u) }

// AckPayload is the daemonbus-carried viewsync.ack payload.
type AckPayload = viewsync.AckFrame

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

// UpdateMember carries one actor membership row from the server catalog to
// the daemon's channel-local actor_registry.
type UpdateMember struct {
	UserID        UserID          `json:"user_id"`
	MemberActorID actor.ActorID   `json:"member_actor_id"`
	Kind          actor.Kind      `json:"kind"`
	Binding       actor.Binding   `json:"actor_binding,omitempty"`
	Role          string          `json:"role,omitempty"`
	DisplayName   string          `json:"display_name,omitempty"`
	CapabilitySet json.RawMessage `json:"capability_set,omitempty"`
}

// UpdateMembersBody is the server -> daemon control.update_members payload.
// Adds register or reactivate actors; Removes soft-deregister active actors.
type UpdateMembersBody struct {
	FrameID   FrameID         `json:"frame_id"`
	ChannelID channel.ID      `json:"channel_id"`
	Adds      []UpdateMember  `json:"adds,omitempty"`
	Removes   []actor.ActorID `json:"removes,omitempty"`
}

// UpdateMembersAckBody is the daemon -> server reply for
// control.update_members. Accepted=false leaves the server catalog row
// durable; callers retry through the hook path.
type UpdateMembersAckBody struct {
	FrameID      FrameID    `json:"frame_id"`
	ChannelID    channel.ID `json:"channel_id"`
	Accepted     bool       `json:"accepted"`
	RejectReason string     `json:"reject_reason,omitempty"`
	RejectDetail string     `json:"reject_detail,omitempty"`
}

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
