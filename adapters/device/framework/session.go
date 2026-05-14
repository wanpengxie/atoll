package framework

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
)

// DeviceState is the closed set of states a device session can occupy on
// the daemon side. Mirrors the T1.10 state machine:
//
//	∅ ──[server INSERT + control.bind_device_session]──→ pending
//	pending ──[daemon ACK]──→ ready
//	ready ──[extension connects to server WS]──→ active
//	active ──[device WS disconnect]──→ offline
//	offline ──[device reconnect]──→ active
//	ready|active|offline ──[token expired]──→ expired
//	ready|active|offline ──[user/server revoke]──→ revoked
//	expired|revoked ──[daemon ACK unbind]──→ ∅ (delete)
//
// On the daemon side the row is a mirror of the server.device_sessions
// authoritative row (L4 §2.6.2 — single ownership). Daemon-side
// transitions are driven by control frames (bind / unbind / device WS
// connect-notification) or by adapter-detected events (timeout / push
// failure).
type DeviceState string

// DeviceState closed set per T1.10.
const (
	StatePending DeviceState = "pending"
	StateReady   DeviceState = "ready"
	StateActive  DeviceState = "active"
	StateOffline DeviceState = "offline"
	StateExpired DeviceState = "expired"
	StateRevoked DeviceState = "revoked"
)

// AllDeviceStates lists every DeviceState value in spec order — used by
// tests to assert the closed-set coverage.
var AllDeviceStates = []DeviceState{
	StatePending,
	StateReady,
	StateActive,
	StateOffline,
	StateExpired,
	StateRevoked,
}

// String returns the wire form.
func (s DeviceState) String() string { return string(s) }

// IsTerminal reports whether the state is a sink (no further transition
// other than deletion). expired / revoked are sinks.
func (s DeviceState) IsTerminal() bool {
	return s == StateExpired || s == StateRevoked
}

// IsReachable reports whether the device WS can carry traffic in this
// state. Only `active` is delivery-eligible; offline holds the session
// open for reconnection but cannot route requests.
func (s DeviceState) IsReachable() bool {
	return s == StateActive
}

// allowedTransitions encodes the T1.10 state machine. Reading: from
// state X, the slice lists every legal `next` value. Idempotent
// re-transition (X → X) is also legal — callers may call SetState with
// the same value defensively without tripping the matrix.
var allowedTransitions = map[DeviceState][]DeviceState{
	StatePending: {StatePending, StateReady, StateExpired, StateRevoked},
	StateReady:   {StateReady, StateActive, StateOffline, StateExpired, StateRevoked},
	StateActive:  {StateActive, StateOffline, StateExpired, StateRevoked},
	StateOffline: {StateOffline, StateActive, StateExpired, StateRevoked},
	// Sink states. Defensive idempotent self-loop; the row is meant to be
	// deleted shortly after daemon ACKs unbind, but callers may re-stamp
	// the sink state on duplicate control frames.
	StateExpired: {StateExpired},
	StateRevoked: {StateRevoked},
}

// CanTransitionTo reports whether (current → next) is a legal state
// machine edge. Treats unknown current states as "no legal edges" so a
// caller that constructed a DeviceSession with a wire-side enum drift
// fails closed rather than silently jumping to a sink.
func (s DeviceState) CanTransitionTo(next DeviceState) bool {
	allowed, ok := allowedTransitions[s]
	if !ok {
		return false
	}
	for _, candidate := range allowed {
		if candidate == next {
			return true
		}
	}
	return false
}

// DeviceSession is the daemon-side mirror row for one server-owned device
// session. The authoritative server row carries more fields (user_id,
// token_hash, etc.); the daemon only needs what it must consult to route
// frames + enforce lifecycle.
//
// Field origins (T1.3 + T1.10):
//
//   - SessionID         server-allocated uuid (covers codex 必修 #5)
//   - ChannelID         which channel this session is bound to
//   - DeviceID          adapter-supplied device identifier (e.g. "xhs-chrome-default")
//   - DeviceType        adapter family ("xhs", "feishu_mobile", …)
//   - State             T1.10 lifecycle state
//   - BoundAt           ms epoch — daemon ACK time
//   - LastActiveAt      ms epoch — last device WS observed activity (0 if never)
//   - TokenFingerprint  hex prefix of HMAC; for log / audit only — never the plain token
//   - ExpiresAt         ms epoch — session expiry (covers T1.10 expiry transition)
//
// Two fields are intentionally absent on the daemon side:
//
//   - The plain token: it lives only at server.devicebus + the device.
//     Daemon ACKs the bind frame using token_fingerprint per T1.3 to
//     prove it has the same token without storing the secret material.
//   - user_id: device sessions are bound to channels, not users. The
//     server-side row carries it for catalog, but the daemon adapter
//     never needs it (envelope sender stays tool:xhs-adapter — L4 §2.6).
type DeviceSession struct {
	SessionID        adapter.DeviceSessionID `json:"session_id"`
	ChannelID        channel.ID              `json:"channel_id"`
	DeviceID         string                  `json:"device_id"`
	DeviceType       string                  `json:"device_type"`
	State            DeviceState             `json:"state"`
	BoundAt          int64                   `json:"bound_at"`
	LastActiveAt     int64                   `json:"last_active_at,omitempty"`
	TokenFingerprint string                  `json:"token_fingerprint"`
	ExpiresAt        int64                   `json:"expires_at,omitempty"`
}

// SessionFields lists every DeviceSession field in spec order. Tests
// assert the schema is 1:1 with this list so silent drift fails closed.
var SessionFields = []string{
	"session_id",
	"channel_id",
	"device_id",
	"device_type",
	"state",
	"bound_at",
	"last_active_at",
	"token_fingerprint",
	"expires_at",
}

// Validate runs the structural invariants every persisted row must
// satisfy. Returns nil on success or a wrapped error describing the
// first violation. Used by Upsert implementations + by the test fakes
// to keep schema drift loud.
func (d DeviceSession) Validate() error {
	if d.SessionID == "" {
		return errors.New("framework.DeviceSession: SessionID is required")
	}
	if d.ChannelID == "" {
		return errors.New("framework.DeviceSession: ChannelID is required")
	}
	if d.DeviceID == "" {
		return errors.New("framework.DeviceSession: DeviceID is required")
	}
	if d.DeviceType == "" {
		return errors.New("framework.DeviceSession: DeviceType is required")
	}
	if _, ok := allowedTransitions[d.State]; !ok {
		return fmt.Errorf("framework.DeviceSession: State %q outside closed set", d.State)
	}
	return nil
}

// SessionStore is the daemon-side mirror persistence contract. Concrete
// backends:
//
//   - runtime/store (T3) — sqlite per-channel table device_sessions_mirror
//   - adapters/device/framework/inmem_store.go — in-memory fake for tests
//
// All methods MUST be safe for concurrent use; the adapter framework
// dispatches Handle / OnExternalCallback / control-frame handlers in
// separate goroutines.
type SessionStore interface {
	// Upsert writes (or overwrites) the row keyed by SessionID. Returns
	// an error if Validate() fails. Idempotent — calling twice with the
	// same row is a no-op other than refreshing the row body.
	Upsert(ctx context.Context, sess DeviceSession) error

	// Get returns the mirror row by SessionID. ok=false when absent
	// (caller decides: pending bind frame? orphan callback? — handler
	// chooses the diagnostic).
	Get(ctx context.Context, sid adapter.DeviceSessionID) (DeviceSession, bool, error)

	// SetState advances the row's State, stamping LastActiveAt with
	// `at` (ms epoch). Returns an error if the transition violates
	// CanTransitionTo. Idempotent (state == next is legal).
	SetState(ctx context.Context, sid adapter.DeviceSessionID, next DeviceState, at int64) error

	// ListByChannel returns every mirror row attached to channelID. Used
	// by adapter boot recovery (T1.6 phase 1.4) so on daemon restart the
	// xhs Module reconstructs which sessions it knows about.
	ListByChannel(ctx context.Context, channelID channel.ID) ([]DeviceSession, error)

	// Delete drops the row by SessionID. Called when daemon ACKs an
	// unbind_device_session control frame after the session reached a
	// sink state. Idempotent (delete missing row is OK).
	Delete(ctx context.Context, sid adapter.DeviceSessionID) error
}
