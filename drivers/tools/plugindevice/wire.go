package plugindevice

import "encoding/json"

// wire.go holds the minimal device-facing frame primitives — the plugin-facing
// language, deliberately NOT the channel envelope and NOT any legacy
// device_transit frame family. Two frames, paired by correlation_id:
//
//	down (adapter → extension):  {correlation_id, cmd, params}
//	up   (extension → adapter):  {correlation_id, ok, result}        on success
//	                             {correlation_id, ok:false, error}   on failure
//
// correlation_id == the channel request envelope.ID, so the read loop can map a
// reply straight back to the in-flight request it closes.

// DownFrame is the command sent down to the plugin. Exported because it is a
// published protocol: a mock plugin in a test and the端侧 forwarder both speak it.
type DownFrame struct {
	CorrelationID string          `json:"correlation_id"`
	Cmd           string          `json:"cmd"`
	Params        json.RawMessage `json:"params"`
}

// UpFrame is the reply the plugin sends back up.
type UpFrame struct {
	CorrelationID string          `json:"correlation_id"`
	OK            bool            `json:"ok"`
	Result        json.RawMessage `json:"result,omitempty"`
	Error         *UpError        `json:"error,omitempty"`
}

// UpError carries a failed reply's machine code + human detail. code becomes
// the channel response's error_code (the adapter's closed error set passes
// through from the device verbatim — the device is the authority on its own
// failure taxonomy).
type UpError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
