package xhs

import "encoding/json"

// wire.go holds the minimal device-facing frame primitives. This is the
// adapter's PRIVATE language with one xhs browser extension — deliberately NOT
// the channel envelope and NOT any legacy device_transit frame family. Two
// frames, paired by correlation_id:
//
//	down (adapter → extension):  {correlation_id, cmd, params}
//	up   (extension → adapter):  {correlation_id, ok, result}        on success
//	                             {correlation_id, ok:false, error}   on failure
//
// correlation_id == the channel request envelope.ID, so the read loop can map a
// reply straight back to the in-flight request it closes.

// downFrame is the command sent down to the extension.
type downFrame struct {
	CorrelationID string          `json:"correlation_id"`
	Cmd           string          `json:"cmd"`
	Params        json.RawMessage `json:"params"`
}

// upFrame is the reply the extension sends back up.
type upFrame struct {
	CorrelationID string          `json:"correlation_id"`
	OK            bool            `json:"ok"`
	Result        json.RawMessage `json:"result,omitempty"`
	Error         *upError        `json:"error,omitempty"`
}

// upError carries a failed reply's machine code + human detail. code becomes
// the channel response's error_code (the adapter's closed error set passes
// through from the device verbatim — the device is the authority on its own
// failure taxonomy).
type upError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
