package base

import (
	"encoding/json"
	"testing"
)

func TestContextTokensUsesPiUsageAndCountsAssistantOutputAndTail(t *testing.T) {
	messages := []json.RawMessage{
		json.RawMessage(`{"role":"user","content":[{"type":"text","text":"prompt"}]}`),
		json.RawMessage(`{"role":"assistant","stopReason":"stop","usage":{"input":100,"output":20,"cacheRead":30,"cacheWrite":5},"content":[{"type":"text","text":"answer"}]}`),
		json.RawMessage(`{"role":"toolResult","content":[{"type":"text","text":"12345678"}]}`),
	}
	wantTail := (len(messages[2]) + 3) / 4
	if got, want := ContextTokens(messages), 100+30+5+20+wantTail; got != want {
		t.Fatalf("ContextTokens=%d want %d", got, want)
	}
}
