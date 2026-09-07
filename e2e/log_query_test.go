package e2e

import (
	"strings"
	"testing"
	"time"
)

// Uses the shipped server and ordinary message submission, never database
// inspection. This exercises registration, manifest, assembly and replies.
func TestLogQueryDiscoverySearchAndReadThroughMessages(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	describe := ws.request(c0ChannelID, "actor.describe", systemActor, map[string]any{})
	words, _ := describe["words"].(map[string]any)
	word, _ := words["system.log.query"].(map[string]any)
	if word["input_schema"] == nil || word["output_schema"] == nil || len(asSlice(word["examples"])) == 0 {
		t.Fatalf("query not discoverable: %v", word)
	}
	text := strings.Repeat("正文", 1000) + "history-query-needle"
	id := ws.submit(c0ChannelID, "e2e.note", "event", nil, map[string]any{"text": text})
	ws.awaitEnvelope(func(e map[string]any) bool { return e["id"] == id }, 15*time.Second)
	result := ws.request(c0ChannelID, "system.log.query", systemActor, map[string]any{"text": "history-query-needle", "limit": 1})
	turns := asSlice(result["turns"])
	if len(turns) != 1 {
		t.Fatalf("search: %v", result)
	}
	turn := turns[0].(map[string]any)
	messages := asSlice(turn["messages"])
	if len(messages) != 1 {
		t.Fatalf("messages: %v", messages)
	}
	m := messages[0].(map[string]any)
	if m["id"] != id || m["truncated"] != true || !strings.Contains(m["payload_text"].(string), "history-query-needle") {
		t.Fatalf("excerpt: %v", m)
	}
	read := ws.request(c0ChannelID, "system.log.query", systemActor, map[string]any{"read_seq": m["seq"], "offset": 0})
	full := read["message"].(map[string]any)
	if full["truncated"] != false || !strings.Contains(full["payload_text"].(string), text) {
		t.Fatalf("read: %v", full)
	}
	// The previous query and its result contain the keyword, but must not
	// recursively become new search hits.
	again := ws.request(c0ChannelID, "system.log.query", systemActor, map[string]any{"text": "history-query-needle"})
	if len(asSlice(again["turns"])) != 1 {
		t.Fatalf("query included its own history: %v", again)
	}
	statsResult := ws.request(c0ChannelID, "system.log.query", systemActor, map[string]any{"group_by": "sender", "text": "history-query-needle"})
	stats, ok := statsResult["stats"].(map[string]any)
	if !ok || stats["scope"] != "scanned_page" || stats["matched_messages"] != float64(1) || len(asSlice(stats["buckets"])) != 1 {
		t.Fatalf("statistics: %v", statsResult)
	}
	nearbyID := ws.submit(c0ChannelID, "e2e.note", "event", nil, map[string]any{"text": "nearby unrelated wording"})
	ws.awaitEnvelope(func(e map[string]any) bool { return e["id"] == nearbyID }, 15*time.Second)
	contextResult := ws.request(c0ChannelID, "system.log.query", systemActor, map[string]any{"around_seq": m["seq"], "radius": 1})
	near, ok := contextResult["context"].(map[string]any)
	if !ok {
		t.Fatalf("context absent: %v", contextResult)
	}
	after := asSlice(near["after"])
	if len(after) != 1 || after[0].(map[string]any)["id"] != nearbyID {
		t.Fatalf("nearby message must ignore earlier keyword and query traffic: %v", near)
	}
	if result["view"] != "conversation" || len(asSlice(result["guidance"])) == 0 || len(asSlice(result["next"])) == 0 {
		t.Fatalf("missing agent usage hints: %v", result)
	}
	for _, action := range asSlice(result["next"]) {
		a := action.(map[string]any)
		follow := ws.request(c0ChannelID, "system.log.query", systemActor, a["request"].(map[string]any))
		if follow["status"] != "completed" {
			t.Fatalf("returned follow-up failed: %v", follow)
		}
	}
	byID := ws.request(c0ChannelID, "system.log.query", systemActor, map[string]any{"read_id": id, "head_seq": result["head_seq"]})
	if byID["message"].(map[string]any)["payload_text"] != text {
		t.Fatalf("read by id lost conversation text: %v", byID)
	}
	rawRead := ws.request(c0ChannelID, "system.log.query", systemActor, map[string]any{"read_id": id, "view": "raw"})
	if rawRead["message"].(map[string]any)["content_source"] != "raw_json" {
		t.Fatalf("raw mode missing: %v", rawRead)
	}
}
