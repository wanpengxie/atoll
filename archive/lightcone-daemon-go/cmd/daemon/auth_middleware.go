package main

// auth_middleware.go owns the shared bearer-token gate used by the
// daemon's HTTP composition root (T107 R2-FIX-1).
//
// Two routes were anonymous before this hardening:
//
//   - /api/channel/create  /  /api/channel/list  — bootstrap saga routes
//     could be invoked without an Authorization header. An attacker
//     could provision channels (running DDL + writing channel_created
//     events to bootstrap_registry) and enumerate channel ids + workdir
//     paths simply by reaching the daemon over TCP.
//
//   - /api/rpc/message.send — the per-channel router read up to 1 MiB
//     of body, peeked params.channel_id, and returned 404 for unknown
//     channels BEFORE invoking the per-channel handler's AuthFunc. A
//     no-token attacker could therefore distinguish "channel exists
//     (401)" from "channel missing (404)" — channel-id fingerprinting.
//
// requireBearer fixes both: it sits in front of the affected handlers
// and short-circuits with 401 before any body is read or any channel
// lookup runs. The wrapped handlers (bootstrap.RegisterRoutes outputs,
// newMessageSendRouter) keep their existing semantics intact for
// callers that present a valid token.
//
// The token compare uses subtle.ConstantTimeCompare to avoid leaking
// equality timing per the same threat model the xhs callback handler
// adopted at T102 FIX-2.

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

// requireBearer returns an http.Handler middleware that rejects any
// request whose `Authorization: Bearer <token>` header does not match
// the configured daemon token. The returned wrapper is safe for
// concurrent use.
//
// Error shape (mirrors cmd/daemon/http_helpers.go writeError):
//
//	HTTP 401 {"error": {"reason": "token_required", "detail": "..."}}
//	HTTP 401 {"error": {"reason": "token_invalid", "detail": "..."}}
//
// requireBearer panics on an empty token at construction time so a
// misconfigured daemon never silently accepts anonymous traffic. The
// composition root (Run → validateConfig) already enforces a non-empty
// AuthToken, so this panic is a defensive backstop.
func requireBearer(token string) func(http.Handler) http.Handler {
	if strings.TrimSpace(token) == "" {
		panic("cmd/daemon: requireBearer: token must be non-empty")
	}
	want := []byte(token)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got, terr := extractBearerToken(r.Header.Get("Authorization"))
			if terr != nil {
				writeError(w, http.StatusUnauthorized, "token_required", terr.Error())
				return
			}
			if subtle.ConstantTimeCompare([]byte(got), want) != 1 {
				writeError(w, http.StatusUnauthorized, "token_invalid",
					"bearer token does not match the configured daemon token")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// extractBearerToken parses an `Authorization: Bearer <token>` header.
// Returns the token (non-empty) on success, or an error explaining why
// the header was rejected. Same shape as the helper in
// internal/adapters/xhs/callback_http.go and
// internal/harness/binding_daemon_rpc.go — duplicated rather than
// imported to keep cmd/daemon's dependency graph thin.
func extractBearerToken(header string) (string, error) {
	if header == "" {
		return "", errors.New("missing Authorization header")
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", errors.New("authorization header is not a Bearer token")
	}
	tok := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if tok == "" {
		return "", errors.New("empty bearer token")
	}
	return tok, nil
}
