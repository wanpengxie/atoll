package viewsync

import (
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
)

// PushFrame is the payload of `viewsync.push` (L1 §8.3 row 1).
//
// Carried inside daemonbus mux frame (L2 §9.2 wraps with daemon_id /
// daemon_connection_epoch / frame_id / sent_at — those header fields
// belong to kernel/daemonbus.Frame, not here).
type PushFrame struct {
	ChannelID    channel.ID             `json:"channel_id"`
	Seq          Seq                    `json:"seq"`
	MessageID    message.ID             `json:"message_id"`
	Envelope     message.Envelope       `json:"envelope"`
	OwnerEpoch   placement.OwnerEpoch   `json:"owner_epoch"`
	FencingToken placement.FencingToken `json:"fencing_token"`
}

// RejectReason is the viewsync ack reject reason closed set used by the
// daemonbus split-brain guard.
type RejectReason string

const (
	RejectReasonMuxOwnerEpochStale         RejectReason = "mux_owner_epoch_stale"
	RejectReasonViewsyncResyncBackpressure RejectReason = "viewsync_resync_backpressure"
)

// AckFrame is the payload of `viewsync.ack` (L1 §8.3 row 2).
//
// Carries only the contiguous water-mark cursor — NOT the max seq just
// received. See L1 §8.4 apply rule for the exact semantics.
type AckFrame struct {
	ChannelID       channel.ID      `json:"channel_id"`
	LastReceivedSeq LastReceivedSeq `json:"last_received_seq"`
	Accepted        bool            `json:"accepted"`
	RejectReason    RejectReason    `json:"reject_reason,omitempty"`
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
	MessageID message.ID       `json:"message_id"`
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
