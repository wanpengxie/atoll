package e2e

import (
	"testing"
	"time"
)

func TestDoorCarriesChannelRequestThroughSvcactor(t *testing.T) {
	h := newHarness(t)
	api, setupWS := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findRegistrar(t, setupWS)
	sourceRow := createChannelWithRoot(t, setupWS, c0ChannelID, registrar, "e2e-membrane-source")
	sourceID := stringField(t, sourceRow, "channel_id")
	door := awaitDoor(t, setupWS, sourceID)

	// The UI history projection intentionally removes system.* housekeeping.
	// Observe this protocol chain live instead of trying to reconstruct an audit
	// trace through the human-facing historical projection after the fact.
	auditWS := dialWS(t, h.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0, sourceID: 0})
	sourceRequestID, result := setupWS.requestWithID(sourceID, "system.channel.list", door, map[string]any{})
	if _, ok := result["value"]; !ok {
		t.Fatalf("door system channel-list reply omitted registrar value: %v", result)
	}

	var sourceRequest, sourceReply, c0Request, c0Reply map[string]any
	deadline := time.Now().Add(30 * time.Second)
	for (sourceRequest == nil || sourceReply == nil || c0Request == nil || c0Reply == nil) && time.Now().Before(deadline) {
		select {
		case item := <-auditWS.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil || envelope["type"] != "system.channel.list" {
				continue
			}
			channelID, _ := item["channel_id"].(string)
			switch {
			case channelID == sourceID && envelope["id"] == sourceRequestID:
				sourceRequest = envelope
			case channelID == sourceID && envelope["kind"] == "response" && envelope["parent_id"] == sourceRequestID:
				sourceReply = envelope
			case channelID == c0ChannelID && envelope["kind"] == "request" && svcactorOriginMatches(envelope, sourceID):
				c0Request = envelope
			case channelID == c0ChannelID && c0Request != nil && envelope["kind"] == "response" && envelope["parent_id"] == c0Request["id"]:
				c0Reply = envelope
			}
		case <-auditWS.done:
			t.Fatal("audit websocket closed while replaying membrane chain")
		case <-time.After(time.Until(deadline)):
		}
	}
	if sourceRequest == nil || sourceReply == nil || c0Request == nil || c0Reply == nil {
		t.Fatalf("membrane chain incomplete: source_request=%v source_reply=%v c0_request=%v c0_reply=%v", sourceRequest, sourceReply, c0Request, c0Reply)
	}
	if responseStatus(sourceReply) != "completed" || responseStatus(c0Reply) != "completed" {
		t.Fatalf("membrane chain terminal shape invalid: source=%v c0=%v", sourceReply, c0Reply)
	}
}

func TestCoreCreatesAndDeletesMemberThroughTargetSvcactor(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findRegistrar(t, ws)

	const (
		channelName = "e2e-membrane-control"
		declID      = "e2e-membrane-codex"
	)
	created := createChannelWithRoot(t, ws, c0ChannelID, registrar, channelName)
	targetID := stringField(t, created, "channel_id")
	awaitDoor(t, ws, targetID)
	peer := "peer:c0." + channelName

	// Channel creation seats its peer asynchronously in c0. Wait for that
	// local handle before exercising the membrane rather than retrying a value
	// mutation whose first reply might merely have been delayed.
	deadline := time.Now().Add(30 * time.Second)
	var lastPeerErr error
	for time.Now().Before(deadline) {
		_, _, lastPeerErr = ws.tryRequest(c0ChannelID, "system.member.get", systemActor, map[string]any{"member": peer})
		if lastPeerErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastPeerErr != nil {
		t.Fatalf("target peer %s did not become active: %v", peer, lastPeerErr)
	}

	registrarRequest(t, ws, c0ChannelID, registrar, "system.actor.template.create", map[string]any{
		"id": declID, "name": declID, "class": "codex", "config": map[string]any{}, "visibility": "private",
	})
	introduced := ws.request(c0ChannelID, "system.member.create", peer, map[string]any{"decl_id": declID})
	member := stringField(t, introduced, "member")

	removed := ws.request(c0ChannelID, "system.member.delete", peer, map[string]any{"decl_id": declID})
	rows, ok := removed["removed"].([]any)
	if !ok || len(rows) != 1 || rows[0] != member {
		t.Fatalf("remote delete=%v, want removed member %s", removed, member)
	}
}

func svcactorOriginMatches(envelope map[string]any, channelID string) bool {
	payload, _ := envelope["payload"].(map[string]any)
	context, _ := payload["_context"].(map[string]any)
	caller, _ := context["caller"].(map[string]any)
	_, hasBody := payload["body"]
	return caller["channel"] == channelID && caller["actor"] != "" && hasBody
}

func responseStatus(envelope map[string]any) string {
	payload, _ := envelope["payload"].(map[string]any)
	status, _ := payload["status"].(string)
	return status
}
