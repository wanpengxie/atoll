//go:build e2e

package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_ListMessages_AfterAgentReply_NotEmptyPayload asserts every
// returned envelope carries a non-empty payload.
//
// This is the [empty payload] bug guard: a regression in the view
// cache write path once stripped agent.text.payload, leaving the UI
// rendering blank bubbles. The mock bridge in single-shot mode emits
// `{"text":"pong","next_action":"done"}` so every agent envelope
// MUST round-trip with payload.text == "pong".
func TestE2E_ListMessages_AfterAgentReply_NotEmptyPayload(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	email := "payload+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-payload-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-payload-"+uniqSuffix(), "")
	s.BindChannel(wsID, chID)

	resp := s.PostMessage(chID, "human.text", "payload-check", "")
	if resp.MessageID == "" {
		t.Fatalf("post failed: %+v", resp)
	}

	// Wait for both human + agent envelopes to be queryable.
	harness.Eventually(t, "human + agent envelopes visible", 10*time.Second, func() bool {
		raw := s.GetMessages(chID)
		var probe struct {
			Messages []map[string]any `json:"messages"`
		}
		_ = json.Unmarshal(raw, &probe)
		var hasHuman, hasAgent bool
		for _, m := range probe.Messages {
			switch m["type"] {
			case "human.text":
				hasHuman = true
			case "agent.text":
				hasAgent = true
			}
		}
		return hasHuman && hasAgent
	})

	raw := s.GetMessages(chID)
	var wrapper struct {
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		t.Fatalf("messages decode: %v (body=%s)", err, string(raw))
	}

	if len(wrapper.Messages) < 2 {
		t.Fatalf("expected >= 2 messages, got %d (raw=%s)", len(wrapper.Messages), string(raw))
	}

	for i, m := range wrapper.Messages {
		var env struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Payload struct {
				Text       string `json:"text"`
				NextAction string `json:"next_action"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(m, &env); err != nil {
			t.Fatalf("message[%d] decode: %v (raw=%s)", i, err, string(m))
		}
		if env.Payload.Text == "" {
			t.Errorf("message[%d] id=%s type=%s payload.text empty (regression: empty payload bug); raw=%s",
				i, env.ID, env.Type, string(m))
		}
		switch env.Type {
		case "human.text":
			if env.Payload.Text != "payload-check" {
				t.Errorf("human.text payload.text=%q want payload-check", env.Payload.Text)
			}
		case "agent.text":
			if env.Payload.Text != "pong" {
				t.Errorf("agent.text payload.text=%q want pong", env.Payload.Text)
			}
			if env.Payload.NextAction != "done" {
				t.Errorf("agent.text payload.next_action=%q want done", env.Payload.NextAction)
			}
		}
	}
}
