package main

// http_helpers.go holds tiny HTTP plumbing the message-send router
// needs. Kept in its own file so server.go stays focused on subsystem
// wiring.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// readAndRestoreBody reads up to maxBytes of r.Body, then replaces
// r.Body with an io.NopCloser over the same bytes so downstream
// handlers (the per-channel internalharness HTTPHandler) can re-read it.
func readAndRestoreBody(r *http.Request, maxBytes int64) ([]byte, error) {
	if r.Body == nil {
		return nil, errors.New("empty body")
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("body exceeds %d bytes", maxBytes)
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}

// peekChannelID extracts `params.channel_id` from a message.send body
// without allocating the full envelope. The harness HTTPHandler does
// the full unmarshal a second time — this helper only needs the
// routing key.
func peekChannelID(body []byte) (string, error) {
	if len(strings.TrimSpace(string(body))) == 0 {
		return "", errors.New("empty body")
	}
	// Cheap two-level decoder — we only need params.channel_id, so
	// decode the outer wrapper into a json.RawMessage and the inner
	// `params` into a tiny struct.
	var outer struct {
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(body, &outer); err != nil {
		return "", fmt.Errorf("decode body: %w", err)
	}
	var inner struct {
		ChannelID string `json:"channel_id"`
	}
	if err := json.Unmarshal(outer.Params, &inner); err != nil {
		return "", fmt.Errorf("decode params: %w", err)
	}
	if strings.TrimSpace(inner.ChannelID) == "" {
		return "", errors.New("params.channel_id is required")
	}
	return inner.ChannelID, nil
}

// writeError writes a JSON error envelope mirroring the L2 §3.6.1 reject
// shape ("{error: {reason, detail}}") with the given HTTP status.
func writeError(w http.ResponseWriter, status int, reason, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	payload := map[string]any{
		"error": map[string]any{
			"reason": reason,
			"detail": detail,
		},
	}
	_ = json.NewEncoder(w).Encode(payload)
}
