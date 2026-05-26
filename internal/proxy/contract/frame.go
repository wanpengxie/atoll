package contract

import "encoding/json"

// FrameType is the reserved proxy daemon v2 frame_type set.
//
// Business envelope-carrying frames leave frame_type empty. The v1 proxy
// daemon startup contract reserves only ready, heartbeat, and shutdown; failed
// ready handling is modeled by closing the WebSocket rather than by adding
// accepted/rejected frame types.
type FrameType string

const (
	// FrameTypeReady is the first frame a proxy daemon sends after a v2
	// WebSocket handshake. It declares host metadata and the actor ids hosted
	// by this daemon process.
	FrameTypeReady FrameType = "ready"

	// FrameTypeHeartbeat is the daemon-to-server keepalive frame.
	FrameTypeHeartbeat FrameType = "heartbeat"

	// FrameTypeShutdown is the server-to-daemon notification used before the
	// server closes a revoked daemon connection.
	FrameTypeShutdown FrameType = "shutdown"
)

const (
	// WSSubprotocolV2 is the WebSocket subprotocol offered by proxy daemon v2
	// clients and selected by servers that speak this contract.
	WSSubprotocolV2 = "coagent.device.v2"

	// WSPathV2 is the path used by proxy daemon v2 WebSocket clients. The
	// api-key is supplied through QueryParamApiKey, not through this path.
	WSPathV2 = "/devicebus/v2/connect"

	// QueryParamApiKey is the URL query key carrying the daemon api-key.
	QueryParamApiKey = "key"
)

// DeviceFrameV2 is the top-level proxy daemon v2 WebSocket frame.
//
// The same shape carries reserved control frames and actor envelope frames.
// Reserved control frames set FrameType. Envelope-carrying frames leave
// FrameType empty and use ActorID plus Payload for actor dispatch.
type DeviceFrameV2 struct {
	Direction     string          `json:"direction,omitempty"`
	FrameType     FrameType       `json:"frame_type,omitempty"`
	ActorID       string          `json:"actor_id,omitempty"`
	ChannelID     string          `json:"channel_id,omitempty"`
	RequestID     string          `json:"request_id,omitempty"`
	ParentID      string          `json:"parent_id,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	ExpiresAt     int64           `json:"expires_at,omitempty"`
	TransitSeq    int64           `json:"transit_seq,omitempty"`

	Hostname     string         `json:"hostname,omitempty"`
	HostLabel    string         `json:"host_label,omitempty"`
	Actors       []ReadyActorV2 `json:"actors,omitempty"`
	ProxyVersion string         `json:"proxy_version,omitempty"`
}

// ReadyActorV2 is one actor advertised inside a ready frame.
//
// CapabilitySet is intentionally opaque to the transport contract. Later
// implementation phases may project it into framework declarations through the
// actor envelope path, but this package does not interpret it.
type ReadyActorV2 struct {
	ActorID       string          `json:"actor_id"`
	CapabilitySet json.RawMessage `json:"capability_set,omitempty"`
}
