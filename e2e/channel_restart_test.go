package e2e

import (
	"testing"
)

// The break-glass button sends one control request to the channel's system
// actor. This asserts what the human actually needs to be true afterwards:
// the members that do work got a fresh term, everything else was left alone
// and SAID SO, and — the part that separates a restart from a reset — the
// channel, its roster and every actor identity survived unchanged.
func TestChannelRestartRecyclesWorkersAndKeepsEverythingElse(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})

	before := ws.request(c0ChannelID, "system.member.list", systemActor, map[string]any{})
	beforeRows := asSlice(before["actors"])
	if len(beforeRows) == 0 {
		t.Fatalf("member list=%v, want a roster to restart", before)
	}

	result := ws.request(c0ChannelID, "system.member.restart_all", systemActor, map[string]any{})

	restarted := asSlice(result["restarted"])
	skipped := asSlice(result["skipped"])
	if len(restarted) == 0 {
		t.Fatalf("restart_all=%v, want it to have restarted somebody", result)
	}
	if failed := asSlice(result["failed"]); len(failed) != 0 {
		t.Fatalf("failed=%v, want none on a healthy channel", failed)
	}

	// Every member is accounted for: restarted or skipped, never silently
	// dropped from the answer.
	kinds := map[string]string{}
	for _, raw := range beforeRows {
		row, _ := raw.(map[string]any)
		id, _ := row["id"].(string)
		kind, _ := row["kind"].(string)
		kinds[id] = kind
	}
	seen := map[string]bool{}
	for _, raw := range restarted {
		id, _ := raw.(string)
		seen[id] = true
		if kind := kinds[id]; kind != "agent" && kind != "tool" {
			t.Fatalf("restarted %s of kind %q; only agent and tool run work a restart recovers", id, kind)
		}
	}
	for _, raw := range skipped {
		row, _ := raw.(map[string]any)
		id, _ := row["member"].(string)
		seen[id] = true
		if row["reason"] == "" {
			t.Fatalf("skipped %s without saying why: %v", id, row)
		}
		if kind := kinds[id]; kind == "agent" || kind == "tool" {
			t.Fatalf("skipped %s of kind %q; it does work and should have restarted", id, kind)
		}
	}
	for id := range kinds {
		if !seen[id] {
			t.Fatalf("%s appears in neither restarted nor skipped", id)
		}
	}

	// A restart is a new term, not a new member: the same ids are still here
	// afterwards. This is the whole difference between restarting a channel
	// and re-creating one, so it is asserted rather than assumed.
	after := ws.request(c0ChannelID, "system.member.list", systemActor, map[string]any{})
	afterIDs := map[string]bool{}
	for _, raw := range asSlice(after["actors"]) {
		row, _ := raw.(map[string]any)
		id, _ := row["id"].(string)
		afterIDs[id] = true
	}
	for id := range kinds {
		if !afterIDs[id] {
			t.Fatalf("%s did not survive the restart", id)
		}
	}
	if len(afterIDs) != len(kinds) {
		t.Fatalf("roster size changed: before=%d after=%d", len(kinds), len(afterIDs))
	}
}
