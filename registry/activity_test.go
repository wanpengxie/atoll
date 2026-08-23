package registry

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestActivityEndedStatusVocabulariesAreSeparate(t *testing.T) {
	for _, s := range []string{ActivityTurnEndedStatusOK, ActivityTurnEndedStatusFailed, ActivityTurnEndedStatusInterrupted} {
		if !IsActivityTurnEndedStatus(s) {
			t.Fatalf("turn status %q rejected", s)
		}
	}
	if IsActivityTurnEndedStatus(ActivityToolEndedStatusCompleted) {
		t.Fatal("tool completed leaked into turn vocabulary")
	}
	for _, s := range []string{ActivityToolEndedStatusCompleted, ActivityToolEndedStatusFailed} {
		if !IsActivityToolEndedStatus(s) {
			t.Fatalf("tool status %q rejected", s)
		}
	}
	if IsActivityToolEndedStatus(ActivityTurnEndedStatusOK) || IsActivityToolEndedStatus(ActivityTurnEndedStatusInterrupted) {
		t.Fatal("turn statuses leaked into tool vocabulary")
	}
}

func TestToolActivityPayloadKeepsRawJSONValues(t *testing.T) {
	started, err := json.Marshal(ActivityToolStartedPayload{TurnIndex: 1, ToolCallID: "call-1", Tool: "search", Status: ActivityStartedStatus, Input: json.RawMessage(`{"query":"ledger"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(started), `"input":{"query":"ledger"}`) {
		t.Fatalf("started=%s", started)
	}
	ended, err := json.Marshal(ActivityToolEndedPayload{TurnIndex: 1, ToolCallID: "call-1", Tool: "search", Status: ActivityToolEndedStatusCompleted, Output: json.RawMessage(`{"hits":3}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ended), `"output":{"hits":3}`) {
		t.Fatalf("ended=%s", ended)
	}
}
