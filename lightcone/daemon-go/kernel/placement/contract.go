// Package placement holds the channel-placement state machine and the
// control-plane protocol types (control.create_channel / ack / …).
//
// Placement is a server-side concern (L2 new section, T1.x): server
// decides which daemon owns which channel. Daemon receives
// control.create_channel via daemonbus mux, bootstraps the channel
// locally, and ACKs.
//
// L1 is unaware of placement (L1 only sees channel_id, not "where" the
// channel lives).
//
// kernel/placement is IO-free. Concrete placement runtime lives in
// server/placements per T6.
package placement

import (
	"github.com/coagent-ai/daemon-go/kernel/channel"
)

// State is the placement state for a single channel (server side).
type State string

// State enum.
const (
	StateUnassigned State = "unassigned"
	StatePending    State = "pending"
	StateActive     State = "active"
	StateOrphan     State = "orphan"
	StateStale      State = "stale"
)

// CreateChannelFrame is the control-plane frame the server sends to a
// daemon to ask it to bootstrap (or adopt) a channel.
type CreateChannelFrame struct {
	FrameID       string            `json:"frame_id"`
	ChannelID     channel.ChannelId `json:"channel_id"`
	FencingToken  int64             `json:"fencing_token"`
	BootstrapJSON []byte            `json:"bootstrap_json"` // bootstrap_registry spec
}

// CreateChannelAck is the daemon's response to CreateChannelFrame.
type CreateChannelAck struct {
	FrameID   string            `json:"frame_id"`
	ChannelID channel.ChannelId `json:"channel_id"`
	Status    string            `json:"status"` // "created" | "rejected"
	Reason    string            `json:"reason,omitempty"`
}
