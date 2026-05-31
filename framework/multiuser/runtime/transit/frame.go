package transit

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/framework/multiuser/daemonbus"
	"github.com/wanpengxie/ActOS/framework/multiuser/placement"
)

// Encode wraps a payload (PushFrame / AckFrame / CreateChannelRequest /
// CreateChannelAck / device-transit frame) into a daemonbus.Frame with
// the supplied header fields. The payload is JSON-encoded.
func Encode(
	frameID string,
	frameType daemonbus.FrameType,
	daemonID string,
	epoch daemonbus.ConnectionEpoch,
	sentAt int64,
	payload any,
) (daemonbus.Frame, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return daemonbus.Frame{}, fmt.Errorf("transit: encode payload (%s): %w", frameType, err)
	}
	return daemonbus.Frame{
		FrameID:               daemonbus.FrameID(frameID),
		FrameKind:             frameType,
		DaemonID:              placement.DaemonID(daemonID),
		DaemonConnectionEpoch: epoch,
		Ts:                    sentAt,
		Payload:               raw,
	}, nil
}

// DecodePayload decodes a Frame.Payload into the typed struct pointed to
// by out. Returns an error when out is nil or the JSON is invalid.
func DecodePayload(frame daemonbus.Frame, out any) error {
	if out == nil {
		return errors.New("transit: DecodePayload nil out")
	}
	return json.Unmarshal(frame.Payload, out)
}
