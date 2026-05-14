package xhs

import (
	"context"
	"errors"
	"sync"
)

// DeviceClient is the seam between the xhs adapter and the M1.2 Chrome
// extension WebSocket protocol. Implementations are responsible for
// pushing one outbound `command` frame per Handle call; the inbound
// `/api/device/{id}/callback` HTTP path is intentionally NOT modelled
// here — the framework's `Manager.OnExternalCallback` is the single
// inbound entrypoint, and callback_http.go owns the HTTP→framework
// adaptation.
//
// PushCommand semantics:
//
//   - Returns `ErrDeviceOffline` when the device has no live WS
//     connection. The adapter maps this to a `failed` Respond with
//     reason `device_offline`.
//   - Returns any other error to surface as `device_push_failed`.
//   - Returns nil on a successful send (extension ack pending — the
//     framework leaves the request pending until OnExternalCallback
//     resolves it).
type DeviceClient interface {
	PushCommand(ctx context.Context, deviceID string, cmd Command) error
}

// Command is the outbound WS frame the xhs adapter pushes to the
// Chrome extension. Field shape mirrors the M1.2 Node implementation
// (lightcone/daemon/src/channel-manager.js → pushCommand) so the
// extension client code can stay unchanged through the cutover.
//
// `Cmd` is the type-suffix the extension expects (e.g. "publish" for
// type=xhs.publish). The adapter strips the "xhs." prefix before
// building the frame so the wire shape stays identical to the legacy
// path.
type Command struct {
	Type          string         `json:"type"`           // always "command"
	CorrelationID string         `json:"correlation_id"` // = envelope.id
	Cmd           string         `json:"cmd"`            // e.g. "publish"
	Params        map[string]any `json:"params"`         // domain payload (minus framework keys)
}

// ErrDeviceOffline indicates the WS server has no live connection for
// the target device. Adapters translate it to a failed Respond with
// reason "device_offline" so the originating request resolves promptly
// without waiting on the F3 default timeout.
var ErrDeviceOffline = errors.New("xhs: device offline")

// MockDeviceClient is the in-memory DeviceClient used by tests. Each
// PushCommand call captures the frame in `Sends` (under a mutex) and
// returns the configured `PushErr` (nil by default → success).
//
// Tests typically:
//
//  1. Construct a MockDeviceClient.
//  2. Hand it to xhs.New(...) before plugging into adapter.NewManager.
//  3. Dispatch a request envelope.
//  4. Read `Sends` to assert the WS frame shape.
//  5. Call Manager.OnExternalCallback to simulate the extension reply
//     and assert the resulting `Respond` envelope.
type MockDeviceClient struct {
	mu      sync.Mutex
	sends   []MockSend
	pushErr error
}

// MockSend captures one PushCommand invocation.
type MockSend struct {
	DeviceID string
	Command  Command
}

// NewMockDeviceClient constructs an empty mock client.
func NewMockDeviceClient() *MockDeviceClient {
	return &MockDeviceClient{}
}

// SetPushErr configures the next (and subsequent) PushCommand call to
// return err. Pass nil to clear.
func (m *MockDeviceClient) SetPushErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pushErr = err
}

// Sends returns a snapshot of every recorded PushCommand invocation.
// The slice is safe to mutate — it is a copy of the internal buffer.
func (m *MockDeviceClient) Sends() []MockSend {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]MockSend, len(m.sends))
	copy(out, m.sends)
	return out
}

// PushCommand implements DeviceClient by recording the frame and
// returning the configured pushErr.
func (m *MockDeviceClient) PushCommand(_ context.Context, deviceID string, cmd Command) error {
	m.mu.Lock()
	m.sends = append(m.sends, MockSend{DeviceID: deviceID, Command: cmd})
	err := m.pushErr
	m.mu.Unlock()
	return err
}
