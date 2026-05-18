//go:build e2e

package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_AgentReply_SingleFinalEnvelope asserts the
// "single terminal envelope per turn" contract: a human.text trigger
// reaches the spawned mock worker → exactly ONE agent.text envelope
// comes back with payload.next_action=done, and zero per-chunk
// stream envelopes leak into the channel sqlite.
//
// Catches:
//   - workerhost.ExecSpawner not wired into cmd/daemon (worker never
//     spawned, no agent.text ever appears)
//   - LLM stream chunks emitted as standalone envelopes
//   - mock single-shot mode regression
func TestE2E_AgentReply_SingleFinalEnvelope(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	email := "reply+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-reply-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-reply-"+uniqSuffix(), "")
	s.BindChannel(wsID, chID)

	resp := s.PostMessage(chID, "human.text", "ping", "")
	if resp.MessageID == "" {
		t.Fatalf("post failed: %+v", resp)
	}

	// Poll for agent.text to land. The mock bridge runs single-shot
	// + responds within one IPC round-trip, so 8s is generous.
	var agentRows []harness.StoredMessage
	harness.Eventually(t, "agent.text reply", 8*time.Second, func() bool {
		all := s.ListChannelMessages(chID)
		agentRows = agentRows[:0]
		for _, m := range all {
			if m.Type == "agent.text" {
				agentRows = append(agentRows, m)
			}
		}
		return len(agentRows) >= 1
	})

	// Exactly one agent.text envelope — chunk-spam would push count > 1.
	if got := len(agentRows); got != 1 {
		t.Fatalf("agent.text count=%d want exactly 1 (chunk-spam regression?); rows=%+v",
			got, agentRows)
	}

	reply := agentRows[0]
	if reply.SenderKind != "agent" {
		t.Errorf("agent.text sender_kind=%q want agent", reply.SenderKind)
	}
	if reply.Kind != "event" {
		t.Errorf("agent.text kind=%q want event", reply.Kind)
	}

	var payload struct {
		Text       string `json:"text"`
		NextAction string `json:"next_action"`
	}
	if err := json.Unmarshal(reply.Payload, &payload); err != nil {
		t.Fatalf("agent.text payload not json: %v (raw=%s)", err, string(reply.Payload))
	}
	if payload.Text != "pong" {
		t.Errorf("agent.text payload.text=%q want pong (COAGENT_MOCK_REPLY_TEXT)", payload.Text)
	}
	if payload.NextAction != "done" {
		t.Errorf("agent.text payload.next_action=%q want done (single-shot contract)", payload.NextAction)
	}

	// Same channel must hold the human.text plus the single agent.text;
	// no other envelope types (chunk envelopes would be agent.text with
	// next_action absent, also caught above).
	all := s.ListChannelMessages(chID)
	types := map[string]int{}
	for _, m := range all {
		types[m.Type]++
	}
	if types["human.text"] != 1 {
		t.Errorf("human.text count=%d want 1; types=%v", types["human.text"], types)
	}
	if types["agent.text"] != 1 {
		t.Errorf("agent.text count=%d want 1; types=%v", types["agent.text"], types)
	}
}
