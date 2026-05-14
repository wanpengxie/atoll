package xhs

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
)

// CallbackManager is the subset of *adapter.Manager the HTTP handler
// needs. Modelled as an interface so tests can inject a recording
// stub without standing up a full Manager + sqlite channel.
type CallbackManager interface {
	OnExternalCallback(ctx context.Context, adapterName string, payload []byte) error
}

// callbackPathRe matches the M1.2 protocol path
// /api/device/{deviceId}/callback. The {deviceId} segment is captured
// so the handler can echo it back inside the body for the adapter.
//
// The handler is path-scoped: callers usually mount it via
// `mux.Handle("/api/device/", NewCallbackHandler(mgr, token))` so any
// non-matching subpath returns 404 instead of being silently absorbed
// by the prefix.
var callbackPathRe = regexp.MustCompile(`^/api/device/([^/]+)/callback/?$`)

// NewCallbackHandler builds the HTTP handler that translates an
// extension callback HTTP request into a framework
// Manager.OnExternalCallback call. The returned handler is safe for
// concurrent use.
//
// Wire contract (M1.2-compatible):
//
//   - METHOD = POST
//   - PATH   = /api/device/{deviceId}/callback
//   - HEADER = Authorization: Bearer <machine_token>
//   - body   = JSON object with {correlation_id, status, result?, error?}
//
// The handler enriches the body with `device_id` parsed from the URL
// path before passing it to the adapter so the adapter sees the same
// device_id regardless of whether the caller put it in the JSON or
// only in the path.
//
// Auth (T102 FIX-2 hardening): the handler refuses to invoke the
// adapter unless the request carries a valid `Authorization: Bearer
// <token>` header matching the configured machine token. `token` is
// the same shared token used by /api/rpc/message.send (Config.AuthToken
// in cmd/daemon). An empty token at construction time panics so a
// misconfigured daemon never silently accepts unauthenticated callbacks.
//
// HTTP responses:
//
//	200 {"ok": true}                      — adapter accepted the callback
//	400 {"error": "<reason>"}             — invalid body / missing fields
//	401 {"error": "token_required"|...}   — missing/malformed Authorization
//	401 {"error": "token_invalid"}        — bearer present but does not match
//	404 — path did not match /api/device/{id}/callback
//	405 — non-POST method
//	500 — adapter / framework returned an error
func NewCallbackHandler(mgr CallbackManager, token string) http.Handler {
	if mgr == nil {
		panic("xhs.NewCallbackHandler: manager is nil")
	}
	if strings.TrimSpace(token) == "" {
		panic("xhs.NewCallbackHandler: token is required (auth hardening, T102 FIX-2)")
	}
	return &callbackHandler{mgr: mgr, token: token}
}

type callbackHandler struct {
	mgr   CallbackManager
	token string
}

func (h *callbackHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
			"error": "method_not_allowed",
		})
		return
	}
	m := callbackPathRe.FindStringSubmatch(r.URL.Path)
	if m == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": "path_not_found",
		})
		return
	}
	deviceID := m[1]

	// Auth check (T102 FIX-2 / claude 98-1 critical) — must happen
	// BEFORE we read the body / call the adapter so an attacker without
	// the daemon token cannot forge publish ok/fail responses.
	tok, terr := extractBearer(r.Header.Get("Authorization"))
	if terr != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"error":  "token_required",
			"detail": terr.Error(),
		})
		return
	}
	if subtle.ConstantTimeCompare([]byte(tok), []byte(h.token)) != 1 {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"error": "token_invalid",
		})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MiB hard cap
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "body_read_failed",
		})
		return
	}
	defer func() { _ = r.Body.Close() }()

	// Decode → re-encode so we can fold the path-derived device_id
	// into the JSON body the adapter parses. We avoid hand-splicing
	// JSON strings to keep the shape canonical (and to surface bad
	// bodies as 400 early instead of inside the adapter).
	var raw map[string]any
	if len(strings.TrimSpace(string(body))) > 0 {
		if err := json.Unmarshal(body, &raw); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error":  "body_invalid_json",
				"detail": err.Error(),
			})
			return
		}
	}
	if raw == nil {
		raw = map[string]any{}
	}
	if _, present := raw["device_id"]; !present {
		raw["device_id"] = deviceID
	}
	if v, _ := raw["correlation_id"].(string); strings.TrimSpace(v) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "correlation_id_required",
		})
		return
	}
	enc, err := json.Marshal(raw)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":  "encode_failed",
			"detail": err.Error(),
		})
		return
	}

	if err := h.mgr.OnExternalCallback(r.Context(), AdapterName, enc); err != nil {
		// Framework errors are infrastructure-level (sql / driver /
		// adapter not installed). Surface them as 500 — ops dashboards
		// pick the per-error class from the response body's `detail`.
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":  "adapter_callback_failed",
			"detail": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":             true,
		"device_id":      deviceID,
		"correlation_id": raw["correlation_id"],
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// extractBearer parses an `Authorization: Bearer <token>` header. Returns
// the token (non-empty) on success, or an error explaining what's wrong.
// Mirrors the equivalent helper in internal/harness/binding_daemon_rpc.go
// (T102 FIX-2 fix-spec § "复用 extractBearer"). Duplicated rather than
// imported because internal/harness imports the harness Deps types and a
// cyclic dependency on the xhs adapter is not worth creating just for a
// 12-line helper.
func extractBearer(header string) (string, error) {
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
