package proxyfacade

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/lib/behavior"
)

// Device-lifecycle decoding for the relay facade. v1 lived in
// framework/devicetransit (deleted); v2 the proxyfacade owns its own lifecycle
// contract — the daemon localdevice host stamps these RuntimeEvents and the
// facade folds them into its volatile `live` signal. The payload schema is
// adapter-domain (the substrate treats RuntimeEvent.Payload as opaque).

const runtimeEventKindDeviceLifecycle behavior.RuntimeEventKind = "device.lifecycle"

type lifecycleEvent string

const (
	lifecycleConnected    lifecycleEvent = "connected"
	lifecycleDisconnected lifecycleEvent = "disconnected"
	lifecycleTokenExpired lifecycleEvent = "token_expired"
)

type lifecyclePayload struct {
	Event lifecycleEvent `json:"event"`
	Ts    int64          `json:"ts"`
}

func decodeLifecycle(raw json.RawMessage) (lifecyclePayload, error) {
	var p lifecyclePayload
	err := json.Unmarshal(raw, &p)
	return p, err
}
