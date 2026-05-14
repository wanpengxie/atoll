package bootstrap

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// CreateChannelPath is the canonical HTTP route the server posts to.
// Exported so the daemon main can mount the handler at the right path
// without duplicating the literal.
const CreateChannelPath = "/api/channel/create"

// ListChannelsPath exposes ListChannels over HTTP (`daemon:list_channels`
// in spec parlance). The handler at this path returns the JSON array
// returned by Saga.ListChannels.
const ListChannelsPath = "/api/channel/list"

// errorResponse is the JSON shape returned for non-2xx responses. The
// `reason` value is one of the L2 §3.6.1 daemon_rpc reason strings:
//   - "bootstrap_in_progress"
//   - "bootstrap_rolled_back"
//   - "params_invalid"
//   - "internal_error"
type errorResponse struct {
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`

	// Optional fields populated for status-bearing failures so the
	// caller can disambiguate (e.g. ChannelID for an in-progress
	// retry, status for the current saga state).
	ChannelID string `json:"channel_id,omitempty"`
	Status    string `json:"status,omitempty"`
}

// NewCreateChannelHandler returns an http.Handler that maps POST
// requests to Saga.ChannelCreate per L2 §3.6.1 daemon_rpc binding:
//
//	200 OK            — completed; body = Result
//	409 Conflict      — bootstrap_in_progress (caller may retry later)
//	409 Conflict      — bootstrap_rolled_back  (caller MUST switch id)
//	400 Bad Request   — params_invalid
//	405 Method Not    — non-POST
//	500 Internal      — anything else
//
// The daemon main wires this with `mux.Handle(CreateChannelPath, ...)`.
func NewCreateChannelHandler(saga *Saga) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed",
				"POST required", "", "")
			return
		}
		if ct := r.Header.Get("Content-Type"); ct != "" &&
			!strings.HasPrefix(ct, "application/json") {
			writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type",
				"Content-Type must be application/json", "", "")
			return
		}

		var p CreateParams
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&p); err != nil {
			writeError(w, http.StatusBadRequest, "params_invalid", err.Error(), "", "")
			return
		}

		res, err := saga.ChannelCreate(r.Context(), p)
		switch {
		case err == nil:
			writeJSON(w, http.StatusOK, res)
		case errors.Is(err, ErrParamsInvalid):
			writeError(w, http.StatusBadRequest, "params_invalid", err.Error(), "", "")
		case errors.Is(err, ErrBootstrapInProgress):
			writeError(w, http.StatusConflict, "bootstrap_in_progress",
				err.Error(), res.ChannelID, res.Status)
		case errors.Is(err, ErrBootstrapRolledBack):
			writeError(w, http.StatusConflict, "bootstrap_rolled_back",
				err.Error(), res.ChannelID, res.Status)
		default:
			writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), "", "")
		}
	})
}

// NewListChannelsHandler returns an http.Handler over Saga.ListChannels.
// GET only; returns 200 + JSON array (empty array when no completed
// channels exist).
func NewListChannelsHandler(saga *Saga) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed",
				"GET required", "", "")
			return
		}
		channels, err := saga.ListChannels(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), "", "")
			return
		}
		if channels == nil {
			channels = []ChannelInfo{}
		}
		writeJSON(w, http.StatusOK, channels)
	})
}

// RegisterRoutes mounts both bootstrap routes on the given mux. Daemon
// main can call this once after constructing the saga to wire all HTTP
// surface this package owns.
func RegisterRoutes(mux *http.ServeMux, saga *Saga) {
	mux.Handle(CreateChannelPath, NewCreateChannelHandler(saga))
	mux.Handle(ListChannelsPath, NewListChannelsHandler(saga))
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, reason, message, channelID, sagaStatus string) {
	writeJSON(w, status, errorResponse{
		Reason:    reason,
		Message:   message,
		ChannelID: channelID,
		Status:    sagaStatus,
	})
}
