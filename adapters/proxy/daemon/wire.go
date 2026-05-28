package daemon

import "encoding/json"

type FrameType string

const (
	FrameTypeReady     FrameType = "ready"
	FrameTypeHeartbeat FrameType = "heartbeat"
	FrameTypeShutdown  FrameType = "shutdown"
	FrameTypeAck       FrameType = "ack"
)

const (
	WSSubprotocolV2  = "coagent.device.v2"
	WSPathV2         = "/devicebus/v2/connect"
	QueryParamAPIKey = "key"
)

type DeviceFrame struct {
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

	Hostname     string       `json:"hostname,omitempty"`
	HostLabel    string       `json:"host_label,omitempty"`
	Actors       []ReadyActor `json:"actors,omitempty"`
	ProxyVersion string       `json:"proxy_version,omitempty"`
}

type ReadyActor struct {
	ActorID       string          `json:"actor_id"`
	CapabilitySet json.RawMessage `json:"capability_set,omitempty"`
}
