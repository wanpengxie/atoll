package codex

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestToolSummaryMaxCharsBoundary(t *testing.T) {
	for _, n := range []int{toolSummaryMaxChars - 1, toolSummaryMaxChars} {
		at := strings.Repeat("界", n)
		if got := boundedToolSummary(itemWire{Command: at}); got != at {
			t.Fatalf("%d-character summary truncated", n)
		}
	}
	at := strings.Repeat("界", toolSummaryMaxChars)
	over := boundedToolSummary(itemWire{Command: at + "界"})
	if len([]rune(over)) > toolSummaryMaxChars || !strings.Contains(over, "truncated") {
		t.Fatalf("over=%q", over[len(over)-20:])
	}
}

func TestToolSummaryCoversSupportedItemFamilies(t *testing.T) {
	tests := []struct {
		name string
		item itemWire
		want string
	}{
		{"command", itemWire{Type: "commandExecution", Command: "go test ./pkg"}, "go test ./pkg"},
		{"files", itemWire{Type: "fileChange", Changes: make([]json.RawMessage, 3)}, "3 file change(s)"},
		{"mcp", itemWire{Type: "mcpToolCall", Server: "github", Tool: "search", Arguments: json.RawMessage(`{"q":"atoll"}`)}, `github · search · {"q":"atoll"}`},
		{"dynamic", itemWire{Type: "dynamicToolCall", Namespace: "plugin", Tool: "run", Arguments: json.RawMessage(`{"x":1}`)}, `plugin · run · {"x":1}`},
		{"web", itemWire{Type: "webSearch", Query: "codex app server"}, "codex app server"},
		{"image-view", itemWire{Type: "imageView", Path: "/tmp/image.png"}, "/tmp/image.png"},
		{"image-generation", itemWire{Type: "imageGeneration", SavedPath: "/tmp/generated.png"}, "/tmp/generated.png"},
		{"collab", itemWire{Type: "collabAgentToolCall", Tool: "spawn", ReceiverThreadIDs: []string{"a", "b"}, Prompt: "review"}, "spawn · a,b · review"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := boundedToolSummary(tt.item); got != tt.want {
				t.Fatalf("summary=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestFinalAnswerCacheLastNonEmptyAndClearedPerTurn(t *testing.T) {
	e, events, c := outputHarness()
	c.turnID = "turn-a"
	c.final["turn-a"] = ""
	e.handleItem(c, "item/completed", itemNotice{ThreadID: "thread", TurnID: "turn-a", Item: itemWire{Type: "agentMessage", Text: "first"}})
	e.handleItem(c, "item/completed", itemNotice{ThreadID: "thread", TurnID: "turn-a", Item: itemWire{Type: "agentMessage", Text: ""}})
	e.handleItem(c, "item/completed", itemNotice{ThreadID: "thread", TurnID: "turn-a", Item: itemWire{Type: "agentMessage", Text: "last"}})
	notify(t, e, c, "turn/completed", map[string]any{"threadId": "thread", "turn": map[string]any{"id": "turn-a", "status": "completed"}})
	records := events.snapshot()
	if len(records) != 1 || records[0].kind != "ended" || records[0].text != "last" {
		t.Fatalf("turn A records=%#v", records)
	}
	if _, ok := c.final["turn-a"]; ok {
		t.Fatal("turn A cache survived terminal")
	}
	c.turnID = "turn-b"
	c.final["turn-b"] = ""
	notify(t, e, c, "turn/completed", map[string]any{"threadId": "thread", "turn": map[string]any{"id": "turn-b", "status": "completed"}})
	records = events.snapshot()
	if len(records) != 2 || records[1].text != "" {
		t.Fatalf("turn B inherited prior final: %#v", records)
	}
}

// The driver must hand the final answer through whole: nothing on our path
// (item cache, turn terminal, RPC line reader) may clip it. Pinned here rather
// than end to end, where the length would be the model's behaviour, not ours.
func TestFinalAnswerSurvivesFarBeyondToolSummaryBound(t *testing.T) {
	e, events, c := outputHarness()
	c.turnID = "turn-long"
	c.final["turn-long"] = ""
	long := "LONG-BEGIN-" + strings.Repeat("abc123", 900) + "-LONG-END"
	e.handleItem(c, "item/completed", itemNotice{ThreadID: "thread", TurnID: "turn-long", Item: itemWire{Type: "agentMessage", Text: long}})
	notify(t, e, c, "turn/completed", map[string]any{"threadId": "thread", "turn": map[string]any{"id": "turn-long", "status": "completed"}})
	records := events.snapshot()
	if len(records) != 1 || records[0].kind != "ended" {
		t.Fatalf("records=%#v", records)
	}
	if records[0].text != long {
		t.Fatalf("final answer altered: got %d runes want %d", len([]rune(records[0].text)), len([]rune(long)))
	}
}

func TestErrorNotificationNeverSettles(t *testing.T) {
	for _, retry := range []bool{false, true} {
		e, events, c := outputHarness()
		c.turnID = "turn"
		notify(t, e, c, "error", map[string]any{"threadId": "thread", "turnId": "turn", "willRetry": retry, "error": map[string]any{"message": "diagnostic"}})
		if got := events.snapshot(); len(got) != 0 || c.turnID != "turn" {
			t.Fatalf("willRetry=%v settled: records=%#v turn=%q", retry, got, c.turnID)
		}
		e.stopWatchdog()
	}
}

func outputHarness() (*engine, *recordingEvents, *connection) {
	events := &recordingEvents{}
	c := &connection{id: 1, final: map[string]string{}}
	e := &engine{cfg: Config{Logger: slog.New(slog.DiscardHandler)}, events: events, life: context.Background(), current: c, threadID: "thread"}
	return e, events, c
}

func notify(t *testing.T, e *engine, c *connection, method string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	e.handleNotification(c, method, raw)
}
func TestAllDeltaMethodsDropped(t *testing.T) {
	for _, m := range deltaNotificationMethods() {
		if !isDeltaMethod(m) {
			t.Fatalf("%q not dropped", m)
		}
	}
}
