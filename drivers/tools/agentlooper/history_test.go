package agentlooper

import (
	"encoding/json"
	"testing"
)

func TestHistoryPairingBatchIdentityAndOrder(t *testing.T) {
	a := json.RawMessage(`{"role":"assistant","content":[{"type":"toolCall","id":"x","name":"read","arguments":{}},{"type":"toolCall","id":"y","name":"read","arguments":{}}]}`)
	x := toolResult(toolCall{ID: "x", Name: "read"}, true, "timeout; effects unknown")
	y := toolResult(toolCall{ID: "y", Name: "read"}, false, "ok")
	for name, h := range map[string][]json.RawMessage{
		"missing": {a, x}, "out of order": {a, y, x}, "duplicate": {a, x, x}, "orphan": {x},
		"interrupted batch": {a, x, json.RawMessage(`{"role":"user","content":"new input"}`), y},
	} {
		t.Run(name, func(t *testing.T) {
			if validateHistory(h) == nil {
				t.Fatal("invalid history accepted")
			}
		})
	}
	h := []json.RawMessage{a, x, y, a, x, y}
	for i := 0; i < 2; i++ {
		if err := validateHistory(h); err != nil {
			t.Fatal(err)
		}
	}
	if len(h) != 6 {
		t.Fatal("validation mutated history")
	}
}

func TestAssistantBatchRejectsAmbiguousIdentityNotBadArguments(t *testing.T) {
	for _, content := range []string{
		`[{"type":"toolCall","id":"x","name":"read","arguments":{}},{"type":"toolCall","id":"x","name":"read","arguments":{}}]`,
		`[{"type":"toolCall","id":"","name":"read","arguments":{}}]`,
	} {
		if _, _, err := assistantParts(json.RawMessage(`{"role":"assistant","content":` + content + `}`)); err == nil {
			t.Fatal("ambiguous identity accepted")
		}
	}
	calls, _, err := assistantParts(json.RawMessage(`{"role":"assistant","content":[{"type":"toolCall","id":"x","name":"read","arguments":[]}]}`))
	if err != nil || len(calls) != 1 || validArguments(calls[0].Arguments) {
		t.Fatalf("bad arguments must remain pairable: %v %v", calls, err)
	}
}
