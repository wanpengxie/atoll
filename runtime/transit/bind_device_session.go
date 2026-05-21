package transit

import "github.com/wanpengxie/ActOS/kernel/daemonbus"

// BindDeviceSessionBody is the daemonbus `control.bind_device_session`
// payload. The server (gateway) emits one per successful IssueSession
// call — the daemon mirrors the row into its per-process SessionStore
// so adapter modules can satisfy Handle() routing decisions without a
// server round-trip. The daemon REPLIES with BindDeviceSessionAckBody;
// the server transitions the authoritative session row pending → ready
// on a successful ack.
//
// Field origin (m1.5-tickets §T1.3 + T1.10):
//
//   - SessionID         server-allocated uuid (single-ownership; daemon
//     MUST treat it as opaque).
//   - ChannelID         which channel this session is bound to.
//   - DeviceID          adapter-supplied device identifier (e.g.
//     "xhs-chrome-default").
//   - DeviceType        adapter family ("xhs", "feishu_mobile", …).
//   - DaemonID          owner of the channel at the time the bind was
//     issued; daemons MAY reject a bind frame whose
//     DaemonID does not match the receiving daemon
//     (defence-in-depth — production wiring routes
//     per ConnectionFor before reaching this hook).
//   - TokenFingerprint  hex-truncated HMAC of the device token, used
//     for audit-trail only. The plain token never
//     crosses daemonbus (it lives only between the
//     server and the device).
//   - ExpiresAt         ms epoch — session expiry. Daemon adapter MAY
//     refuse to route frames past this boundary.
//   - BoundAt           ms epoch — server-side issue timestamp.
//
// The wire shape is intentionally close to adapters/device/framework.
// DeviceSession so the cmd/daemon handler can map it 1:1 into the
// SessionStore.
type BindDeviceSessionBody = daemonbus.BindDeviceSessionBody

// BindDeviceSessionAckBody is the daemon → server reply. One ack per
// inbound `control.bind_device_session` frame; the dispatcher emits it
// automatically after invoking the handler.
type BindDeviceSessionAckBody = daemonbus.BindDeviceSessionAckBody

// UnbindDeviceSessionBody is the daemonbus `control.unbind_device_session`
// payload. The server emits one when the device session is revoked /
// expired / explicitly torn down via the devicebus DELETE route. The
// daemon deletes the local mirror row (idempotent — missing rows are
// not an error) and acks.
type UnbindDeviceSessionBody = daemonbus.UnbindDeviceSessionBody

// UnbindDeviceSessionAckBody is the daemon → server reply. Mirrors
// BindDeviceSessionAckBody — Accepted=true when the daemon successfully
// purged the mirror row (or the row was already absent).
type UnbindDeviceSessionAckBody = daemonbus.UnbindDeviceSessionAckBody

// DeviceSessionRejectReason is the closed device-session reason set.
type DeviceSessionRejectReason = daemonbus.DeviceSessionRejectReason

const (
	DeviceSessionRejectBindChannelNotActive      = daemonbus.DeviceSessionRejectBindChannelNotActive
	DeviceSessionRejectBindAdapterNotPresent     = daemonbus.DeviceSessionRejectBindAdapterNotPresent
	DeviceSessionRejectBindAdapterBindingInvalid = daemonbus.DeviceSessionRejectBindAdapterBindingInvalid
	DeviceSessionRejectBindDeviceTypeUnsupported = daemonbus.DeviceSessionRejectBindDeviceTypeUnsupported
	DeviceSessionRejectBindCapacityExceeded      = daemonbus.DeviceSessionRejectBindCapacityExceeded
	DeviceSessionRejectBindInternalError         = daemonbus.DeviceSessionRejectBindInternalError

	DeviceSessionRejectTokenInvalid   = daemonbus.DeviceSessionRejectTokenInvalid
	DeviceSessionRejectTokenExpired   = daemonbus.DeviceSessionRejectTokenExpired
	DeviceSessionRejectSessionRevoked = daemonbus.DeviceSessionRejectSessionRevoked
	DeviceSessionRejectSessionUnknown = daemonbus.DeviceSessionRejectSessionUnknown

	DeviceSessionRejectTransitPayloadTooLarge  = daemonbus.DeviceSessionRejectTransitPayloadTooLarge
	DeviceSessionRejectTransitRouteUnavailable = daemonbus.DeviceSessionRejectTransitRouteUnavailable
	DeviceSessionRejectTransitInternalError    = daemonbus.DeviceSessionRejectTransitInternalError

	DeviceSessionRejectUnbindSessionUnknown = daemonbus.DeviceSessionRejectUnbindSessionUnknown
	DeviceSessionRejectUnbindInternalError  = daemonbus.DeviceSessionRejectUnbindInternalError
)
