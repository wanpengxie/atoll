// Package peerproto defines the in-process L1 frame exchanged by peeractor and
// svcactor. It is data-only and deliberately has no dependency on either actor.
package peerproto

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

type Origin struct {
	Channel   channel.ID    `json:"channel"`
	Actor     actor.ActorID `json:"actor"`
	RequestID message.ID    `json:"request_id"`
}

type Request struct {
	Origin  Origin          `json:"origin"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type Failure struct {
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

type Result struct {
	Body json.RawMessage `json:"body,omitempty"`
	Fail *Failure        `json:"fail,omitempty"`
}
