package plugindevice

import (
	"encoding/json"
	"time"
)

// protocol.go is the seam between the shared transport and one plugin's actual
// language. It exists because "browser plugin adapter" turned out not to imply a
// single wire: xhs speaks a frame family this project defined, while kimi's
// extension speaks kimi-webbridge's, which has its own path, its own field
// names, a hello handshake and an application-level heartbeat. Both DIAL IN to a
// local WS endpoint and correlate a reply to its request — that shared shape is
// the transport, and everything below it is the Protocol.
//
// A Protocol never touches the connection: it turns bytes into a decision and
// decisions into bytes. The transport owns the socket, the in-flight table and
// the deadlines, so a new plugin family costs one file here and nothing else.

// InboundKind classifies one message arriving from the plugin.
type InboundKind int

const (
	// InboundIgnore is a frame with no consequence for the in-flight table —
	// a heartbeat reply, or anything this protocol does not model.
	InboundIgnore InboundKind = iota
	// InboundResult closes one in-flight request.
	InboundResult
	// InboundReply is a frame the transport must answer immediately, without
	// involving the actor: a handshake acknowledgement.
	InboundReply
)

// Inbound is one decoded message from the plugin.
type Inbound struct {
	Kind InboundKind

	// CorrelationID, OK, Result and the error pair describe an InboundResult.
	CorrelationID string
	OK            bool
	Result        json.RawMessage
	ErrCode       string
	ErrMsg        string

	// Reply is the frame to send straight back for an InboundReply.
	Reply []byte

	// Note, when set, is logged. A handshake worth acknowledging is worth
	// saying out loud, because "the plugin connected" and "the plugin is ready"
	// are different facts and only the second one means anything works.
	Note string
}

// Protocol is one plugin family's language.
type Protocol interface {
	// Path is the WS route the plugin dials. It is part of the plugin's
	// contract, not ours — the extension has it compiled in.
	Path() string
	// EncodeCall builds the frame that asks the plugin to run one command.
	EncodeCall(correlationID, cmd string, params json.RawMessage) ([]byte, error)
	// Decode classifies one frame from the plugin. An unparseable or unmodelled
	// frame must return InboundIgnore rather than an error: a plugin is entitled
	// to say things this adapter does not care about.
	Decode(raw []byte) Inbound
	// Heartbeat is the application-level keepalive this protocol expects the
	// server to send, if any. ok=false means the protocol has none, and the
	// transport falls back to WS-level ping on a routable bind.
	Heartbeat() (every time.Duration, frame []byte, ok bool)
}

// ─────────────────────────── atoll frames ───────────────────────────

// AtollProtocol is the frame family this project defined for its own plugins:
// {correlation_id, cmd, params} down, {correlation_id, ok, result|error} up, on
// /device, with no handshake. It is what xhs speaks.
type AtollProtocol struct{}

func (AtollProtocol) Path() string { return "/device" }

func (AtollProtocol) EncodeCall(correlationID, cmd string, params json.RawMessage) ([]byte, error) {
	return json.Marshal(DownFrame{CorrelationID: correlationID, Cmd: cmd, Params: params})
}

func (AtollProtocol) Decode(raw []byte) Inbound {
	var up UpFrame
	if err := json.Unmarshal(raw, &up); err != nil || up.CorrelationID == "" {
		return Inbound{Kind: InboundIgnore}
	}
	in := Inbound{Kind: InboundResult, CorrelationID: up.CorrelationID, OK: up.OK, Result: up.Result}
	if up.Error != nil {
		in.ErrCode, in.ErrMsg = up.Error.Code, up.Error.Message
	}
	return in
}

func (AtollProtocol) Heartbeat() (time.Duration, []byte, bool) { return 0, nil, false }

// ───────────────────────── kimi-webbridge frames ─────────────────────────

// webbridgeHeartbeat matches kimi-webbridge's own server: it pings every 15s and
// the extension answers {"type":"pong"}. This is an APPLICATION frame, not a WS
// control frame — the extension does not answer a protocol-level ping, so a
// transport that only sent those would look alive to itself and dead to nobody.
const webbridgeHeartbeat = 15 * time.Second

// WebbridgeProtocol is the language of the kimi-webbridge Chrome extension
// (npm: kimi-webbridge, MIT). Taken from that package's own server, which is the
// thing our adapter stands in for:
//
//	path      /ws
//	handshake extension → {"type":"hello","payload":{"extensionVersion":…}}
//	          server    → {"type":"hello_ack"}
//	call      server    → {"type":"tool_call","requestId":…,"payload":{"name":…,"args":…}}
//	result    extension → {"type":"tool_result","responseToRequestId":…,"payload":{"data":…}}
//	                   or {"type":"tool_result","responseToRequestId":…,"payload":{"error":…}}
//	heartbeat server    → {"type":"ping"} every 15s; extension → {"type":"pong"}
//
// The handshake is not decoration: until the extension has its hello_ack it does
// not consider itself ready, so an adapter that merely accepts the socket and
// says nothing looks connected and answers nothing.
type WebbridgeProtocol struct{}

func (WebbridgeProtocol) Path() string { return "/ws" }

type webbridgeCall struct {
	Type      string           `json:"type"`
	RequestID string           `json:"requestId"`
	Payload   webbridgeCallArg `json:"payload"`
}

type webbridgeCallArg struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
}

func (WebbridgeProtocol) EncodeCall(correlationID, cmd string, params json.RawMessage) ([]byte, error) {
	if len(params) == 0 {
		params = json.RawMessage("{}")
	}
	return json.Marshal(webbridgeCall{
		Type:      "tool_call",
		RequestID: correlationID,
		Payload:   webbridgeCallArg{Name: cmd, Args: params},
	})
}

type webbridgeInbound struct {
	Type                string `json:"type"`
	ResponseToRequestID string `json:"responseToRequestId"`
	Payload             struct {
		Data             json.RawMessage `json:"data"`
		Error            json.RawMessage `json:"error"`
		ExtensionVersion string          `json:"extensionVersion"`
	} `json:"payload"`
}

func (WebbridgeProtocol) Decode(raw []byte) Inbound {
	var msg webbridgeInbound
	if err := json.Unmarshal(raw, &msg); err != nil {
		return Inbound{Kind: InboundIgnore}
	}
	switch msg.Type {
	case "hello":
		version := msg.Payload.ExtensionVersion
		if version == "" {
			version = "unknown"
		}
		return Inbound{
			Kind:  InboundReply,
			Reply: []byte(`{"type":"hello_ack"}`),
			Note:  "extension version " + version,
		}
	case "tool_result":
		if msg.ResponseToRequestID == "" {
			return Inbound{Kind: InboundIgnore}
		}
		in := Inbound{Kind: InboundResult, CorrelationID: msg.ResponseToRequestID, OK: true, Result: msg.Payload.Data}
		if len(msg.Payload.Error) > 0 && string(msg.Payload.Error) != "null" {
			in.OK = false
			in.ErrCode = "device_error"
			// The extension's error is free-form: a string on the common path,
			// an object on some. Unquote a plain string so the channel gets the
			// sentence rather than the sentence inside quotes.
			var text string
			if err := json.Unmarshal(msg.Payload.Error, &text); err == nil {
				in.ErrMsg = text
			} else {
				in.ErrMsg = string(msg.Payload.Error)
			}
		}
		return in
	default:
		// pong, and anything a newer extension invents.
		return Inbound{Kind: InboundIgnore}
	}
}

func (WebbridgeProtocol) Heartbeat() (time.Duration, []byte, bool) {
	return webbridgeHeartbeat, []byte(`{"type":"ping"}`), true
}

// Ensure both dialects satisfy the seam at compile time.
var (
	_ Protocol = AtollProtocol{}
	_ Protocol = WebbridgeProtocol{}
)
