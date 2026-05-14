package viewsync

import (
	"encoding/json"

	"github.com/coagent-ai/daemon-go/kernel/channel"
	"github.com/coagent-ai/daemon-go/kernel/log"
)

// PushFrame is the daemon→server view sync push frame (L1 §8.x).
//
// One frame == one envelope. server applies in seq order; gaps trigger
// Resync.
type PushFrame struct {
	Type         string            `json:"type"` // == "viewsync.push"
	ChannelID    channel.ChannelId `json:"channel_id"`
	Seq          log.Seq           `json:"seq"`
	MessageID    string            `json:"message_id"`
	Envelope     json.RawMessage   `json:"envelope"`
	DaemonID     string            `json:"daemon_id"`
	DaemonEpoch  int64             `json:"daemon_epoch"`
}

// AckFrame is the server→daemon view sync ack frame (L1 §8.x).
//
// LastReceivedSeq is the contiguous high watermark (NOT max-seq); gaps
// produce ack at the lower contiguous boundary while the missing range
// is fetched via Resync.
type AckFrame struct {
	Type            string            `json:"type"` // == "viewsync.ack"
	ChannelID       channel.ChannelId `json:"channel_id"`
	LastReceivedSeq log.Seq           `json:"last_received_seq"`
}

// ResyncRequest is the server→daemon RPC Resync request (closed interval
// [Since, Until] per L1 §8.x).
type ResyncRequest struct {
	ChannelID channel.ChannelId `json:"channel_id"`
	Since     log.Seq           `json:"since_seq"`
	Until     log.Seq           `json:"until_seq"`
}

// ResyncResponse is the daemon→server RPC Resync response. Frames MUST
// be ordered by Seq ASC; server applies via INSERT OR IGNORE against
// view_cache_messages.
type ResyncResponse struct {
	ChannelID channel.ChannelId `json:"channel_id"`
	Frames    []PushFrame       `json:"frames"`
}
