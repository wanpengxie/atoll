package gateway

import (
	"encoding/json"
	"errors"

	"github.com/wanpengxie/atoll/lib/boundedjson"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
)

const (
	maxFeedFrameBytes          = 64 << 10
	feedPayloadProjectionBytes = 10 << 10
)

type boundedFeedResult struct {
	seq           int64
	frame         subjectgate.Frame
	encoded       []byte
	envelope      json.RawMessage
	projected     bool
	originalBytes int
}

// buildBoundedFeed is the one live/history encoder. The ledger remains the
// exact fact; only the browser transport receives a bounded payload view.
func buildBoundedFeed(ref string, ch channel.ID, seq int64, source string, generation uint64, value any) (boundedFeedResult, error) {
	envelope, err := json.Marshal(value)
	if err != nil {
		return boundedFeedResult{}, err
	}
	result := boundedFeedResult{seq: seq, envelope: envelope, originalBytes: len(envelope)}
	if frame, encoded, ok := feedFrameWithinLimit(ref, ch, seq, source, generation, envelope); ok {
		result.frame, result.encoded = frame, encoded
		return result, nil
	}

	var env message.Envelope
	if err := json.Unmarshal(envelope, &env); err != nil {
		return boundedFeedResult{}, errors.New("gateway: oversized feed row is not a message envelope")
	}
	projected, _, err := boundedjson.Project(env.Payload, feedPayloadProjectionBytes)
	if err != nil {
		return boundedFeedResult{}, err
	}
	env.Payload = projected
	envelope, err = json.Marshal(env)
	if err != nil {
		return boundedFeedResult{}, err
	}
	frame, encoded, ok := feedFrameWithinLimit(ref, ch, seq, source, generation, envelope)
	if !ok {
		return boundedFeedResult{}, errors.New("gateway: projected feed row exceeds 64 KiB transport limit")
	}
	result.frame, result.encoded, result.envelope, result.projected = frame, encoded, envelope, true
	return result, nil
}

func feedFrameWithinLimit(ref string, ch channel.ID, seq int64, source string, generation uint64, envelope json.RawMessage) (subjectgate.Frame, []byte, bool) {
	frame, err := subjectgate.NewFrame(subjectgate.FrameFeed, ref, subjectgate.FeedPayload{
		ChannelID: string(ch), Seq: seq, Envelope: envelope, Source: source, Generation: generation,
	})
	if err != nil {
		return subjectgate.Frame{}, nil, false
	}
	encoded, err := frame.Marshal()
	if err != nil || len(encoded) > maxFeedFrameBytes {
		return subjectgate.Frame{}, nil, false
	}
	return frame, encoded, true
}
