//go:build e2e

package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// MockExtensionOriginID is the synthetic chrome-extension origin the
// harness uses for /devicebus handshakes. Tests pass this through
// Options.DeviceAllowedOrigins so server-side checkOrigin accepts it.
const MockExtensionOriginID = "chrome-extension://e2e-test-extension-id-0000000000000000"

// DeviceFrame mirrors server/devicebus.DeviceFrame on the wire. We
// redeclare it here (rather than importing) so the harness module stays
// independent of devicebus internals and the wire shape is asserted
// from the test side. Drift between the two structs surfaces as
// MockExtension decode failures — which is exactly the regression guard
// we want.
type DeviceFrame struct {
	Direction       string          `json:"direction"`
	DeviceSessionID string          `json:"device_session_id"`
	ChannelID       string          `json:"channel_id"`
	RequestID       string          `json:"request_id,omitempty"`
	CorrelationID   string          `json:"correlation_id,omitempty"`
	Payload         json.RawMessage `json:"payload"`
	ExpiresAt       int64           `json:"expires_at,omitempty"`
}

// CommandPayload is the inner JSON the device extension sees in every
// direction=to_device frame. Field shape matches adapters/device/xhs/
// proto.go::Command — kept inline so the harness has zero deps on the
// xhs adapter.
type CommandPayload struct {
	Type          string         `json:"type"`
	CorrelationID string         `json:"correlation_id"`
	Cmd           string         `json:"cmd"`
	Params        map[string]any `json:"params,omitempty"`
	Session       map[string]any `json:"session,omitempty"`
}

// CallbackPayload mirrors adapters/device/xhs/proto.go::Callback (the
// inner JSON of direction=from_device frames the extension emits).
type CallbackPayload struct {
	CorrelationID string         `json:"correlation_id"`
	Status        string         `json:"status"` // "ok" | "error"
	Result        map[string]any `json:"result,omitempty"`
	Error         map[string]any `json:"error,omitempty"`
	DeviceID      string         `json:"device_id,omitempty"`
}

// MockExtension simulates the Chrome extension half of the v4 device
// transit binding. It opens a WS to the harness server's /devicebus
// endpoint, ack-friendly reads inbound `to_device` frames, and replies
// with a synthesised `from_device` Callback frame whose body is
// produced by an OnCommand handler. Strict enough to assert correlation
// ids round-trip; loose enough to let tests inject failure / success on
// a per-cmd basis.
type MockExtension struct {
	t         *testing.T
	conn      *websocket.Conn
	sessionID string
	channelID string
	deviceID  string

	mu       sync.Mutex
	closed   bool
	commands chan CommandPayload // observability: every dispatched cmd

	// onCommand routes one inbound CommandPayload to its synthesised
	// Callback. Default echoes status=ok with empty result. Tests
	// override to inject e.g. xhs.publish results.
	onCommand func(CommandPayload) CallbackPayload
}

// MockExtensionConfig wires a MockExtension before Connect. SessionID,
// Token, and the WS URL are required. ChannelID + DeviceID are stamped
// into outbound frames so the mock matches the server-issued device
// session row.
type MockExtensionConfig struct {
	WSURL     string
	SessionID string
	Token     string
	ChannelID string
	DeviceID  string

	// Origin overrides the default MockExtensionOriginID. Empty = default.
	Origin string

	// OnCommand lets the test inject the response body. Optional —
	// defaults to a generic {status: "ok", result: {}} reply.
	OnCommand func(CommandPayload) CallbackPayload
}

// NewMockExtension dials /devicebus and starts the read loop. The
// returned extension is registered with t.Cleanup so callers don't need
// to defer Close. Fatal on dial failure (the test stack already proved
// the server is up via waitHealthy, so any error here means the
// handshake itself is broken — surface it).
func NewMockExtension(t *testing.T, ctx context.Context, cfg MockExtensionConfig) *MockExtension {
	t.Helper()
	if cfg.SessionID == "" || cfg.Token == "" || cfg.WSURL == "" {
		t.Fatalf("harness: MockExtension config missing session_id/token/ws_url: %+v", cfg)
	}
	origin := cfg.Origin
	if origin == "" {
		origin = MockExtensionOriginID
	}
	header := http.Header{}
	header.Set("Origin", origin)
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second
	// impl-layer3 §6.5.1: device WS token rides Sec-WebSocket-Protocol
	// (`coagent.device.v1, token.<token>`). gorilla wires the offered
	// subprotocols into the handshake header when Subprotocols is set.
	dialer.Subprotocols = []string{"coagent.device.v1", "token." + cfg.Token}
	conn, resp, err := dialer.DialContext(ctx, cfg.WSURL, header)
	if err != nil {
		status := -1
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("harness: MockExtension dial %s (origin=%s status=%d): %v",
			cfg.WSURL, origin, status, err)
	}
	m := &MockExtension{
		t:         t,
		conn:      conn,
		sessionID: cfg.SessionID,
		channelID: cfg.ChannelID,
		deviceID:  cfg.DeviceID,
		commands:  make(chan CommandPayload, 16),
		onCommand: cfg.OnCommand,
	}
	if m.onCommand == nil {
		m.onCommand = defaultOnCommand
	}
	t.Cleanup(func() { _ = m.Close() })
	go m.readLoop(ctx)
	return m
}

// defaultOnCommand replies status=ok with no result body so unrelated
// frame types (xhs.cookie.sync etc.) don't trip an adapter timeout when
// a test forgot to register a handler.
func defaultOnCommand(cmd CommandPayload) CallbackPayload {
	return CallbackPayload{
		CorrelationID: cmd.CorrelationID,
		Status:        "ok",
		Result:        map[string]any{},
	}
}

// Close drops the WS. Idempotent.
func (m *MockExtension) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	conn := m.conn
	m.mu.Unlock()
	if conn != nil {
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "test-stop"),
			time.Now().Add(time.Second))
		_ = conn.Close()
	}
	return nil
}

// Commands returns a channel buffering every inbound CommandPayload the
// extension has dispatched. Tests use it to assert the daemon→device
// fan-out actually fired.
func (m *MockExtension) Commands() <-chan CommandPayload { return m.commands }

// SetOnCommand swaps the callback handler at runtime. Tests use it to
// rewire the mock after observing the first frame (e.g. fail then
// succeed). Safe to call across goroutines.
func (m *MockExtension) SetOnCommand(fn func(CommandPayload) CallbackPayload) {
	m.mu.Lock()
	m.onCommand = fn
	m.mu.Unlock()
}

func (m *MockExtension) readLoop(ctx context.Context) {
	for {
		_ = m.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, raw, err := m.conn.ReadMessage()
		if err != nil {
			return
		}
		var frame DeviceFrame
		if err := json.Unmarshal(raw, &frame); err != nil {
			// non-frame payloads are ignored — server only writes
			// well-formed DeviceFrame JSON.
			continue
		}
		if frame.Direction != "to_device" {
			continue
		}
		var cmd CommandPayload
		if err := json.Unmarshal(frame.Payload, &cmd); err != nil {
			// Reply with synthetic error frame so the daemon doesn't
			// time out on malformed-but-still-routable cases — the
			// adapter framework's correlation tracker pairs by
			// payload.correlation_id which we won't have, so use
			// frame.RequestID as a last resort.
			m.sendCallback(frame, CallbackPayload{
				CorrelationID: frame.RequestID,
				Status:        "error",
				Error: map[string]any{
					"code":    "invalid_payload",
					"message": err.Error(),
				},
			})
			continue
		}
		// Publish for observability (non-blocking; drop if test isn't
		// reading).
		select {
		case m.commands <- cmd:
		default:
		}

		m.mu.Lock()
		handler := m.onCommand
		m.mu.Unlock()
		body := handler(cmd)
		if body.CorrelationID == "" {
			body.CorrelationID = cmd.CorrelationID
		}
		m.sendCallback(frame, body)
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// sendCallback emits a from_device frame echoing the inbound id slots
// and carrying the supplied Callback body. Best-effort: write errors
// surface as test failure only if the loop later fails to receive the
// ack (the daemon-side correlation tracker will surface the timeout).
func (m *MockExtension) sendCallback(inbound DeviceFrame, body CallbackPayload) {
	if m.deviceID != "" && body.DeviceID == "" {
		body.DeviceID = m.deviceID
	}
	payload, _ := json.Marshal(body)
	out := DeviceFrame{
		Direction:       "from_device",
		DeviceSessionID: m.sessionID,
		ChannelID:       inbound.ChannelID,
		RequestID:       inbound.RequestID,
		CorrelationID:   inbound.CorrelationID,
		Payload:         payload,
	}
	if out.ChannelID == "" {
		out.ChannelID = m.channelID
	}
	if out.CorrelationID == "" {
		out.CorrelationID = body.CorrelationID
	}
	m.mu.Lock()
	conn := m.conn
	closed := m.closed
	m.mu.Unlock()
	if closed || conn == nil {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := conn.WriteJSON(out); err != nil {
		// Don't fail the test directly — callers assert on the
		// observable side effects (envelope appears in channel sqlite,
		// adapter timeout doesn't fire). A WriteJSON failure here only
		// matters if it cascades.
		_ = err
	}
}

// WaitForCommand blocks until at least one inbound CommandPayload of
// the named cmd kind arrives (e.g. "publish"), or timeout elapses.
// Returns the matched payload. Fatal on timeout — typical use is right
// after a POST that should trigger an xhs.publish device transit.
func (m *MockExtension) WaitForCommand(name string, timeout time.Duration) CommandPayload {
	m.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case cmd := <-m.commands:
			if cmd.Cmd == name {
				return cmd
			}
		case <-deadline:
			m.t.Fatalf("harness: MockExtension never received cmd=%q within %s",
				name, timeout)
			return CommandPayload{}
		}
	}
}

// Sprintf is a tiny helper exported so tests can include the mock's id
// in failure messages without referencing private state.
func (m *MockExtension) String() string {
	return fmt.Sprintf("MockExtension(session=%s channel=%s device=%s)",
		m.sessionID, m.channelID, m.deviceID)
}
