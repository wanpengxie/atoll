//go:build e2e

package e2e

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_WSPushReceived asserts the live push contract:
//
//  1. UI subscribes via /ws  → {"type":"subscribe","channel_id":...}
//  2. A human.text POST fans out a {"type":"message", ...} frame
//  3. The pushed envelope shape matches what GET /messages returns
//     (no nested .Envelope wrapper, payload.text accessible).
//
// Catches a regression where push fan-out drifts from the REST shape
// — UI then handles two different envelope contracts and breaks
// either on cold load (GET) or on live update (WS).
func TestE2E_WSPushReceived(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	email := "wspush+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-push-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-push-"+uniqSuffix(), "")
	s.BindChannel(wsID, chID)

	ws := s.DialPushWS()
	defer func() { _ = ws.Close() }()
	_ = ws.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := ws.WriteJSON(map[string]any{
		"type":       "subscribe",
		"channel_id": chID,
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Give the subscribe a moment to register before the POST so the
	// fan-out path picks us up. The hub processes the subscribe on its
	// read pump goroutine; 200ms is generous for an in-process server.
	time.Sleep(200 * time.Millisecond)

	resp := s.PostMessage(chID, "human.text", "ws-hello", "")
	if resp.MessageID == "" {
		t.Fatalf("post failed: %+v", resp)
	}

	// Read frames until we get a `message` frame for our channel.
	deadline := time.Now().Add(8 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("never received push frame within 8s")
		}
		_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
		mt, raw, err := ws.ReadMessage()
		if err != nil {
			// Read timeout — loop, but only until our outer deadline.
			if e, ok := err.(net.Error); ok && e.Timeout() {
				continue
			}
			// gorilla returns close errors typed; just keep looping
			// until outer deadline so transient noise doesn't fail.
			continue
		}
		if mt != websocket.TextMessage {
			continue
		}
		var frame struct {
			Type      string          `json:"type"`
			ChannelID string          `json:"channel_id"`
			Seq       int64           `json:"seq"`
			Envelope  json.RawMessage `json:"envelope"`
		}
		if err := json.Unmarshal(raw, &frame); err != nil {
			continue
		}
		if frame.Type != "message" || frame.ChannelID != chID {
			continue
		}
		if frame.Seq <= 0 {
			t.Errorf("push frame seq=%d want > 0; raw=%s", frame.Seq, string(raw))
		}
		// The .envelope field itself must be a flat envelope (same
		// shape as the REST list endpoint).
		var probe map[string]any
		if err := json.Unmarshal(frame.Envelope, &probe); err != nil {
			t.Fatalf("push envelope not json: %v (raw=%s)", err, string(frame.Envelope))
		}
		for _, field := range []string{"id", "channel_id", "type", "kind", "sender", "payload"} {
			if _, ok := probe[field]; !ok {
				t.Errorf("push envelope missing flat-envelope field %q; raw=%s",
					field, string(frame.Envelope))
			}
		}
		var typed struct {
			Type    string `json:"type"`
			Payload struct {
				Text string `json:"text"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(frame.Envelope, &typed); err != nil {
			t.Fatalf("push envelope typed decode: %v (raw=%s)", err, string(frame.Envelope))
		}
		if typed.Type == "human.text" && typed.Payload.Text != "ws-hello" {
			t.Errorf("push envelope payload.text=%q want ws-hello", typed.Payload.Text)
		}
		return
	}
}
