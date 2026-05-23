//go:build e2e

package e2e

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_AgentProgress_EmittedPerTurn covers the per-turn progress
// contract: when the worker bridge produces intermediate progress
// envelopes (one per tool-call step) plus a final reply, every
// envelope MUST land in the channel.sqlite messages table and in the
// correct order:
//
//	human.text → progress(step 1) → progress(step 2) → final reply
//
// Per impl-vocabulary §2.3 (R5-19) the historical standalone
// `agent.progress` type was collapsed into `agent.text` +
// `visibility=system`; the terminal reply is `agent.text` +
// `visibility=public`. Both share the same type — they're
// distinguished by visibility.
//
// The harness drives this by setting COAGENT_MOCK_SCRIPT to the
// `multi-turn-with-progress` script the MockBridge ships for tests.
// Catches:
//   - progress envelopes silently dropped by the harness chain
//   - terminal reply envelope swallowed when preceded by progress events
//   - envelope ordering regression (progress arriving AFTER the reply)
func TestE2E_AgentProgress_EmittedPerTurn(t *testing.T) {
	s := harness.Start(t, harness.Options{
		ExtraDaemonEnv: []string{
			// Activate the multi-turn-with-progress script (mock bridge
			// emits N progress envelopes (visibility=system) + one
			// terminal agent.text (visibility=public) per trigger).
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

	// classify splits a single agent.text row into the progress bucket
	// (visibility=system) or the terminal reply bucket (visibility=public).
	classify := func(m harness.StoredMessage) (progress, terminal bool) {
		if m.Type != "agent.text" {
			return false, false
		}
		switch m.Visibility {
		case "system":
			return true, false
		case "public", "":
			return false, true
		}
		return false, false
	}

	// Poll until the expected envelope set has materialised in the
	// channel sqlite. We expect 1 human.text + 2 progress + 1 terminal
	// agent.text == 4 rows total.
	var (
		humanRows    []harness.StoredMessage
		progressRows []harness.StoredMessage
		textRows     []harness.StoredMessage
	)
	harness.Eventually(t, "progress + terminal agent.text rows", 10*time.Second, func() bool {
		all := s.ListChannelMessages(chID)
		humanRows = humanRows[:0]
		progressRows = progressRows[:0]
		textRows = textRows[:0]
		for _, m := range all {
			if m.Type == "human.text" {
				humanRows = append(humanRows, m)
				continue
			}
			if p, term := classify(m); p {
				progressRows = append(progressRows, m)
			} else if term {
				textRows = append(textRows, m)
			}
		}
		return len(humanRows) >= 1 && len(progressRows) >= 2 && len(textRows) >= 1
	})

	if got := len(humanRows); got != 1 {
		t.Errorf("human.text rows=%d want 1", got)
	}
	if got := len(progressRows); got != 2 {
		t.Errorf("progress rows (agent.text+visibility=system)=%d want 2; rows=%+v", got, progressRows)
	}
	if got := len(textRows); got != 1 {
		t.Errorf("terminal agent.text rows (visibility=public)=%d want 1; rows=%+v", got, textRows)
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
		if row.Visibility != "system" {
			t.Errorf("progress[%d] visibility=%q want system", i, row.Visibility)
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
	if textRows[0].Visibility != "public" {
		t.Errorf("terminal agent.text visibility=%q want public", textRows[0].Visibility)
	}

	// Ordering: in seq ASC, expect human.text first, then both progress
	// rows, then the terminal reply last. The harness sorts
	// ListChannelMessages by seq so we can index by position in `all`.
	//
	// Skip leading reserved-namespace events (system.*) injected at
	// channel bootstrap (e.g. system.channel.created) — proto-layer0
	// reserved system.* set is part of the bootstrap stream and not
	// part of the business message sequence under test.
	all := s.ListChannelMessages(chID)
	business := make([]harness.StoredMessage, 0, len(all))
	for _, row := range all {
		if strings.HasPrefix(row.Type, "system.") {
			continue
		}
		business = append(business, row)
	}
	if len(business) < 4 {
		t.Fatalf("expected >=4 business rows; got %d (from %d total)", len(business), len(all))
	}
	if business[0].Type != "human.text" {
		t.Errorf("business row 0 type=%q want human.text", business[0].Type)
	}
	if p, _ := classify(business[1]); !p {
		t.Errorf("business row 1 type=%q visibility=%q want agent.text+system (first progress)", business[1].Type, business[1].Visibility)
	}
	if p, _ := classify(business[2]); !p {
		t.Errorf("business row 2 type=%q visibility=%q want agent.text+system (second progress)", business[2].Type, business[2].Visibility)
	}
	if _, term := classify(business[3]); !term {
		t.Errorf("business row 3 type=%q visibility=%q want agent.text+public (terminal)", business[3].Type, business[3].Visibility)
	}
}
