package framework

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/coagent-ai/coagent/kernel/adapter"
	"github.com/coagent-ai/coagent/kernel/channel"
)

// TokenSpec is the signed payload server.devicebus embeds in a device
// session token. Field set per T1.10 ("server 在 user 登录后签发
// device token = HMAC(server_secret, …)") + T1.3 control.bind_device_session
// (token_fingerprint identifies this exact spec).
//
// The spec deliberately does NOT include user_id or any human identity.
// device sessions are bound to channels, not users (L4 §2.6 — device
// not actor). User-side authentication happens upstream at the
// human_caller_token layer (T1.9).
type TokenSpec struct {
	SessionID adapter.DeviceSessionID `json:"sid"`
	ChannelID channel.ID              `json:"cid"`
	DeviceID  string                  `json:"did"`
	IssuedAt  int64                   `json:"iat"`
	ExpiresAt int64                   `json:"exp"`
}

// Validate enforces the structural rules every TokenSpec must satisfy
// before signing or after parsing.
func (s TokenSpec) Validate() error {
	if s.SessionID == "" {
		return errors.New("framework.TokenSpec: SessionID is required")
	}
	if s.ChannelID == "" {
		return errors.New("framework.TokenSpec: ChannelID is required")
	}
	if s.DeviceID == "" {
		return errors.New("framework.TokenSpec: DeviceID is required")
	}
	if s.IssuedAt <= 0 {
		return errors.New("framework.TokenSpec: IssuedAt must be positive ms epoch")
	}
	if s.ExpiresAt <= s.IssuedAt {
		return errors.New("framework.TokenSpec: ExpiresAt must be greater than IssuedAt")
	}
	return nil
}

// IsExpired reports whether the token is past its ExpiresAt boundary
// relative to `now` (ms epoch). server.devicebus calls this on every
// incoming device WS frame; daemon adapter calls it before treating a
// session as routable.
func (s TokenSpec) IsExpired(now int64) bool {
	return now >= s.ExpiresAt
}

// IsExpiredAt is the time.Time-friendly variant of IsExpired.
func (s TokenSpec) IsExpiredAt(t time.Time) bool {
	return s.IsExpired(t.UnixMilli())
}

// FingerprintLength is the number of hex characters retained from the
// HMAC tail. 16 hex chars = 64 bits — enough to log-identify a token
// without leaking the signing material. Kept exported so test code can
// assert the wire-format expectation.
const FingerprintLength = 16

// fingerprintEncodingBytes is the number of raw bytes we hex-encode to
// reach FingerprintLength hex characters (each byte → 2 hex chars).
const fingerprintEncodingBytes = FingerprintLength / 2

// tokenSeparator splits the body from the signature in the wire form.
// Picked '.' to match JWT-ish ergonomics without claiming JWT
// compatibility (the body is plain JSON, not JOSE; no `alg` header,
// no algorithm negotiation).
const tokenSeparator = "."

// ErrTokenMalformed is returned when the wire bytes do not parse as
// `<body>.<sig>`. Server.devicebus translates it to HTTP 401 on WS
// handshake.
var ErrTokenMalformed = errors.New("framework.Token: malformed (expected <body>.<sig>)")

// ErrTokenSignatureInvalid is returned when the HMAC does not match the
// declared body. Server-side check uses constant-time compare via
// hmac.Equal so the path is timing-safe.
var ErrTokenSignatureInvalid = errors.New("framework.Token: signature does not match")

// ErrTokenBodyDecode is returned when the body section is not valid
// base64url or not valid TokenSpec JSON.
var ErrTokenBodyDecode = errors.New("framework.Token: body decode failed")

// Issue signs the spec with `secret` (server-only) and returns the
// wire-form token plus its Fingerprint. The fingerprint is what daemon
// adapters persist (in DeviceSession.TokenFingerprint) — they MUST NOT
// store the token itself.
//
// Wire form:
//
//	base64url(JSON(spec)) + "." + base64url(HMAC-SHA256(secret, body))
//
// Both halves use RawURLEncoding (no `=` padding) so the value is safe
// in URL query strings + WebSocket subprotocol headers.
func Issue(secret []byte, spec TokenSpec) (token string, fingerprint string, err error) {
	if len(secret) == 0 {
		return "", "", errors.New("framework.Token.Issue: secret is required")
	}
	if err := spec.Validate(); err != nil {
		return "", "", fmt.Errorf("framework.Token.Issue: %w", err)
	}
	body, err := json.Marshal(spec)
	if err != nil {
		return "", "", fmt.Errorf("framework.Token.Issue: marshal spec: %w", err)
	}
	encodedBody := base64.RawURLEncoding.EncodeToString(body)
	sig := computeSignature(secret, []byte(encodedBody))
	encodedSig := base64.RawURLEncoding.EncodeToString(sig)
	token = encodedBody + tokenSeparator + encodedSig
	fingerprint = fingerprintFromSignature(sig)
	return token, fingerprint, nil
}

// Parse validates the wire token against `secret` and returns the
// TokenSpec. The function returns ErrTokenMalformed / ErrTokenBodyDecode
// / ErrTokenSignatureInvalid plus wrapped detail in the failure paths.
// Successful Parse does NOT enforce IsExpired — caller decides whether
// to reject expired tokens (server.devicebus does; daemon adapter
// inspecting a fingerprint replay does not).
func Parse(secret []byte, token string) (TokenSpec, error) {
	if len(secret) == 0 {
		return TokenSpec{}, errors.New("framework.Token.Parse: secret is required")
	}
	if token == "" {
		return TokenSpec{}, ErrTokenMalformed
	}
	parts := strings.SplitN(token, tokenSeparator, 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return TokenSpec{}, ErrTokenMalformed
	}
	encodedBody, encodedSig := parts[0], parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(encodedSig)
	if err != nil {
		return TokenSpec{}, fmt.Errorf("%w: signature decode: %v", ErrTokenSignatureInvalid, err)
	}
	expected := computeSignature(secret, []byte(encodedBody))
	if !hmac.Equal(sig, expected) {
		return TokenSpec{}, ErrTokenSignatureInvalid
	}
	body, err := base64.RawURLEncoding.DecodeString(encodedBody)
	if err != nil {
		return TokenSpec{}, fmt.Errorf("%w: %v", ErrTokenBodyDecode, err)
	}
	var spec TokenSpec
	if err := json.Unmarshal(body, &spec); err != nil {
		return TokenSpec{}, fmt.Errorf("%w: %v", ErrTokenBodyDecode, err)
	}
	if err := spec.Validate(); err != nil {
		return TokenSpec{}, fmt.Errorf("framework.Token.Parse: %w", err)
	}
	return spec, nil
}

// Fingerprint extracts the fingerprint from a wire-form token without
// validating the signature. Used by clients that already trust the
// token (e.g. the device CLI that received it from the server) and
// just want a stable identifier for logs.
//
// Returns empty string when the token is malformed (no panic — fingerprint
// is best-effort observability).
func Fingerprint(token string) string {
	if token == "" {
		return ""
	}
	idx := strings.LastIndex(token, tokenSeparator)
	if idx < 0 || idx == len(token)-1 {
		return ""
	}
	encodedSig := token[idx+1:]
	sig, err := base64.RawURLEncoding.DecodeString(encodedSig)
	if err != nil {
		return ""
	}
	return fingerprintFromSignature(sig)
}

// computeSignature is the HMAC-SHA256 raw-bytes signer. Centralized so
// Issue / Parse / Fingerprint cannot drift on algorithm choice.
func computeSignature(secret, body []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return mac.Sum(nil)
}

// fingerprintFromSignature converts the raw HMAC bytes to the daemon-
// visible fingerprint. Hex-encodes the first FingerprintLength/2 bytes —
// loud enough to identify the token in a log without exposing collision-
// resistant material.
func fingerprintFromSignature(sig []byte) string {
	if len(sig) < fingerprintEncodingBytes {
		return hex.EncodeToString(sig)
	}
	return hex.EncodeToString(sig[:fingerprintEncodingBytes])
}
