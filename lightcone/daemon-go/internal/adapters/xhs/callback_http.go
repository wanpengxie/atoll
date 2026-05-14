package xhs

import (
	"context"
	"encoding/json"
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
// `mux.Handle("/api/device/", NewCallbackHandler(mgr))` so any non-
// matching subpath returns 404 instead of being silently absorbed by
// the prefix.
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
//   - body   = JSON object with {correlation_id, status, result?, error?}
//
// The handler enriches the body with `device_id` parsed from the URL
// path before passing it to the adapter so the adapter sees the same
// device_id regardless of whether the caller put it in the JSON or
// only in the path.
//
// HTTP responses:
//
//	200 {"ok": true}                      — adapter accepted the callback
//	400 {"error": "<reason>"}             — invalid body / missing fields
//	404 — path did not match /api/device/{id}/callback
//	405 — non-POST method
//	500 — adapter / framework returned an error
func NewCallbackHandler(mgr CallbackManager) http.Handler {
	if mgr == nil {
		panic("xhs.NewCallbackHandler: manager is nil")
	}
	return &callbackHandler{mgr: mgr}
}

type callbackHandler struct {
	mgr CallbackManager
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
