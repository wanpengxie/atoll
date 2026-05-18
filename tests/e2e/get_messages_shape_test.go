//go:build e2e

package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_GetMessages_ShapeContract asserts the front-end contract on
// GET /api/channels/:chID/messages:
//
//   - response is {"messages": [envelope, ...]}
//   - each element is a FLAT envelope — payload.text accessible at
//     top-level .payload.text, NOT nested under .Envelope.
//
// Catches bug #4 (the JSON shape regression that returned
// `{Seq, Envelope, ReceivedAt}` tuples instead of flat envelopes,
// silently breaking the UI message renderer).
func TestE2E_GetMessages_ShapeContract(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	email := "shape+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-shape-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-shape-"+uniqSuffix(), "")
	s.BindChannel(wsID, chID)

	resp := s.PostMessage(chID, "human.text", "shape-check", "")
	if resp.MessageID == "" {
		t.Fatalf("post failed: %+v", resp)
	}

	// Wait for the view_cache fan-out to land server-side.
	harness.Eventually(t, "view_cache has at least 1 row", 5*time.Second, func() bool {
		raw := s.GetMessages(chID)
		var probe struct {
			Messages []map[string]any `json:"messages"`
		}
		_ = json.Unmarshal(raw, &probe)
		return len(probe.Messages) >= 1
	})

	raw := s.GetMessages(chID)

	// Step 1: top-level wrapper.
	var wrapper struct {
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		t.Fatalf("response not {messages: [...]}: %v (body=%s)", err, string(raw))
	}
	if len(wrapper.Messages) == 0 {
		t.Fatalf("messages array empty (raw=%s)", string(raw))
	}

	// Step 2: every element must be a flat envelope. Probe required
	// envelope fields directly + reject the legacy
	// {Seq, Envelope, ReceivedAt} wrapper shape.
	for i, m := range wrapper.Messages {
		var probe map[string]any
		if err := json.Unmarshal(m, &probe); err != nil {
			t.Fatalf("message[%d] not json object: %v (raw=%s)", i, err, string(m))
		}
		// Reject nested wrapper — bug #4 looked like:
		//   {"Seq": N, "Envelope": {...}, "ReceivedAt": ...}
		if _, found := probe["Envelope"]; found {
			t.Fatalf("message[%d] carries nested .Envelope wrapper (bug #4 regression); raw=%s",
				i, string(m))
		}
		if _, found := probe["envelope"]; found {
			t.Fatalf("message[%d] carries nested .envelope wrapper (bug #4 regression); raw=%s",
				i, string(m))
		}
		// Required flat-envelope fields.
		for _, field := range []string{"id", "channel_id", "type", "kind", "sender", "payload"} {
			if _, ok := probe[field]; !ok {
				t.Errorf("message[%d] missing flat-envelope field %q; raw=%s",
					i, field, string(m))
			}
		}
		// Per-envelope payload.text accessibility — that was the
		// concrete UI breakage. Decode as a typed envelope and prove
		// .payload.text is readable.
		var env struct {
			Type    string `json:"type"`
			Payload struct {
				Text string `json:"text"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(m, &env); err != nil {
			t.Fatalf("message[%d] typed decode failed: %v (raw=%s)", i, err, string(m))
		}
		if env.Type == "human.text" && env.Payload.Text == "" {
			t.Errorf("message[%d] type=human.text but payload.text empty; raw=%s",
				i, string(m))
		}
	}
}
