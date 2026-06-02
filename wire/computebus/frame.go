package computebus

import (
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// FrameType tags one wire frame on the home↔compute WS connection.
type FrameType string

const (
	FrameAttach      FrameType = "attach"
	FrameAttachReply FrameType = "attach_reply"
	FrameHeartbeat   FrameType = "heartbeat"
	FrameDispatch    FrameType = "dispatch"   // home → compute
	FrameEmit        FrameType = "emit"       // compute → home
	FrameEmitAck     FrameType = "emit_ack"   // home → compute (write result)
	FrameDeath       FrameType = "death"      // compute → home
)

// EmitAck is the home harness's verdict for a compute EmitFrame: the truth was
// written (or rejected), and the WriteResult flows back so the compute cell's
// Respond/EmitEvent observes the authoritative outcome.
type EmitAck struct {
	EmitID       string `json:"emit_id"`       // correlates ack to the EmitFrame
	MessageID    message.ID `json:"message_id"`
	RejectReason string `json:"reject_reason,omitempty"`
	Err          string `json:"err,omitempty"`
}

// Frame is the tagged WS envelope. Exactly one payload field is set per Type.
type Frame struct {
	Type     FrameType      `json:"type"`
	Attach   *AttachRequest `json:"attach,omitempty"`
	Reply    *AttachReply   `json:"reply,omitempty"`
	Beat     *Heartbeat     `json:"beat,omitempty"`
	Dispatch *DispatchFrame `json:"dispatch,omitempty"`
	Emit     *EmitFrame     `json:"emit,omitempty"`
	EmitID   string         `json:"emit_id,omitempty"` // set on Emit frames for ack correlation
	Ack      *EmitAck       `json:"ack,omitempty"`
	Death    *DeathFrame    `json:"death,omitempty"`
}

// Encode marshals a frame to a WS text message.
func Encode(f Frame) ([]byte, error) { return json.Marshal(f) }

// Decode unmarshals a WS text message into a frame.
func Decode(b []byte) (Frame, error) {
	var f Frame
	if err := json.Unmarshal(b, &f); err != nil {
		return Frame{}, fmt.Errorf("computebus: decode frame: %w", err)
	}
	return f, nil
}
