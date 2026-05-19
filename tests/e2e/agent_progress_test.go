//go:build e2e

package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_AgentProgress_EmittedPerTurn covers the per-turn progress
// contract: when the worker bridge produces intermediate `agent.progress`
// envelopes (one per tool-call step) plus a final `agent.text` reply,
// every envelope MUST land in the channel.sqlite messages table and in
// the correct order:
//
//	human.text → agent.progress(step 1) → agent.progress(step 2) → agent.text
//
// The harness drives this by setting COAGENT_MOCK_SCRIPT to the
// `multi-turn-with-progress` script the MockBridge ships for tests.
// Catches:
//   - progress envelopes silently dropped by the harness chain
//   - terminal text envelope swallowed when preceded by progress events
//   - envelope ordering regression (progress arriving AFTER the text)
func TestE2E_AgentProgress_EmittedPerTurn(t *testing.T) {
	s := harness.Start(t, harness.Options{
		ExtraDaemonEnv: []string{
			// Activate the multi-turn-with-progress script (mock bridge
			// emits N agent.progress envelopes + one terminal agent.text
			// per trigger).
			"COAGENT_MOCK_SCRIPT=multi-turn-with-progress",
			"COAGENT_MOCK_PROGRESS_COUNT=2",
			// Disable single-shot so the script path runs instead of the
			// default single-shot ReplyFn (which short-circuits to one
			// agent.text with next_action=done).
			"COAGENT_MOCK_SINGLE_SHOT=0",
		},
	})

	email := "progress+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-prog-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-prog-"+uniqSuffix(), "")
	s.BindChannel(wsID, chID)

	resp := s.PostMessage(chID, "human.text", "show progress", "")
	if resp.MessageID == "" {
		t.Fatalf("post failed: %+v", resp)
	}

	// Poll until the expected envelope set has materialised in the
	// channel sqlite. We expect 1 human.text + 2 agent.progress +
	// 1 agent.text == 4 rows total.
	var (
		humanRows    []harness.StoredMessage
		progressRows []harness.StoredMessage
		textRows     []harness.StoredMessage
	)
	harness.Eventually(t, "agent.progress + agent.text rows", 10*time.Second, func() bool {
		all := s.ListChannelMessages(chID)
		humanRows = humanRows[:0]
		progressRows = progressRows[:0]
		textRows = textRows[:0]
		for _, m := range all {
			switch m.Type {
			case "human.text":
				humanRows = append(humanRows, m)
			case "agent.progress":
				progressRows = append(progressRows, m)
			case "agent.text":
				textRows = append(textRows, m)
			}
		}
		return len(humanRows) >= 1 && len(progressRows) >= 2 && len(textRows) >= 1
	})

	if got := len(humanRows); got != 1 {
		t.Errorf("human.text rows=%d want 1", got)
	}
	if got := len(progressRows); got != 2 {
		t.Errorf("agent.progress rows=%d want 2; rows=%+v", got, progressRows)
	}
	if got := len(textRows); got != 1 {
		t.Errorf("agent.text rows=%d want 1; rows=%+v", got, textRows)
	}

	// Each progress envelope must carry payload.turn_index + tool_calls.
	for i, row := range progressRows {
		var payload struct {
			TurnIndex any              `json:"turn_index"`
			ToolCalls []map[string]any `json:"tool_calls"`
		}
		if err := json.Unmarshal(row.Payload, &payload); err != nil {
			t.Errorf("progress[%d] payload not json: %v (raw=%s)", i, err, row.Payload)
			continue
		}
		if payload.TurnIndex == nil {
			t.Errorf("progress[%d] missing turn_index: %s", i, row.Payload)
		}
		if len(payload.ToolCalls) == 0 {
			t.Errorf("progress[%d] tool_calls empty: %s", i, row.Payload)
		}
		if row.SenderKind != "agent" {
			t.Errorf("progress[%d] sender_kind=%q want agent", i, row.SenderKind)
		}
	}

	// Terminal agent.text must carry next_action=done.
	var finalPayload struct {
		Text       string `json:"text"`
		NextAction string `json:"next_action"`
	}
	if err := json.Unmarshal(textRows[0].Payload, &finalPayload); err != nil {
		t.Fatalf("agent.text payload not json: %v (raw=%s)", err, textRows[0].Payload)
	}
	if finalPayload.NextAction != "done" {
		t.Errorf("agent.text next_action=%q want done", finalPayload.NextAction)
	}
	if finalPayload.Text == "" {
		t.Errorf("agent.text empty body: %s", textRows[0].Payload)
	}

	// Ordering: in seq ASC, expect human.text first, then both progress
	// rows, then agent.text last. The harness sorts ListChannelMessages
	// by seq so we can index by position in `all`.
	all := s.ListChannelMessages(chID)
	if len(all) < 4 {
		t.Fatalf("expected >=4 rows total; got %d", len(all))
	}
	if all[0].Type != "human.text" {
		t.Errorf("row 0 type=%q want human.text", all[0].Type)
	}
	if all[1].Type != "agent.progress" {
		t.Errorf("row 1 type=%q want agent.progress (first step)", all[1].Type)
	}
	if all[2].Type != "agent.progress" {
		t.Errorf("row 2 type=%q want agent.progress (second step)", all[2].Type)
	}
	if all[3].Type != "agent.text" {
		t.Errorf("row 3 type=%q want agent.text (terminal)", all[3].Type)
	}
}
