package viewsync

import (
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// FrameType is the closed set of view-sync frame_type values that ride
// inside the daemonbus mux (L1 §8.3 + L2 §9.1). Each value matches the
// JSON wire form 1:1.
type FrameType string

// FrameType closed set per L1 §8.3.
const (
	FrameTypePush           FrameType = "viewsync.push"
	FrameTypeAck            FrameType = "viewsync.ack"
	FrameTypeResyncRequest  FrameType = "viewsync.resync_request"
	FrameTypeResyncResponse FrameType = "viewsync.resync_response"
)

// AllFrameTypes lists every viewsync frame_type in spec order.
var AllFrameTypes = []FrameType{
	FrameTypePush,
	FrameTypeAck,
	FrameTypeResyncRequest,
	FrameTypeResyncResponse,
}

// String returns the wire form.
func (f FrameType) String() string { return string(f) }

// PushFrame is the payload of `viewsync.push` (L1 §8.3 row 1).
//
// Carried inside daemonbus mux frame (L2 §9.2 wraps with daemon_id /
// daemon_connection_epoch / frame_id / sent_at — those header fields
// belong to kernel/daemonbus.Frame, not here).
type PushFrame struct {
	ChannelID channel.ID       `json:"channel_id"`
	Seq       Seq              `json:"seq"`
	MessageID string           `json:"message_id"`
	Envelope  message.Envelope `json:"envelope"`
}

// AckFrame is the payload of `viewsync.ack` (L1 §8.3 row 2).
//
// Carries only the contiguous water-mark cursor — NOT the max seq just
// received. See L1 §8.4 apply rule for the exact semantics.
type AckFrame struct {
	ChannelID       channel.ID      `json:"channel_id"`
	LastReceivedSeq LastReceivedSeq `json:"last_received_seq"`
}

// ResyncRequest is the payload of `viewsync.resync_request` (L1 §8.3
// row 3 — closed interval [SinceSeq, UntilSeq]).
type ResyncRequest struct {
	ChannelID channel.ID `json:"channel_id"`
	SinceSeq  Seq        `json:"since_seq"` // inclusive
	UntilSeq  Seq        `json:"until_seq"` // inclusive
}

// ResyncMessage is one entry inside a resync response payload — daemon
// returns the closed interval [SinceSeq, UntilSeq] in `Seq ASC` order
// (L1 §8.5).
type ResyncMessage struct {
	Seq       Seq              `json:"seq"`
	MessageID string           `json:"message_id"`
	Envelope  message.Envelope `json:"envelope"`
}

// ResyncResponse is the payload of `viewsync.resync_response` (L1 §8.3
// row 4). Echoes the requested interval bounds + the messages array.
type ResyncResponse struct {
	ChannelID channel.ID      `json:"channel_id"`
	SinceSeq  Seq             `json:"since_seq"`
	UntilSeq  Seq             `json:"until_seq"`
	Messages  []ResyncMessage `json:"messages"`
}
