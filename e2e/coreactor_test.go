package e2e

import (
	"testing"
	"time"
)

func TestCoreactorCarriesChannelRequestThroughSvcactor(t *testing.T) {
	h := newHarness(t)
	api, setupWS := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findTool(t, setupWS)
	sourceRow := registrarRequest(t, setupWS, registrar, "channel.create", map[string]any{"name": "e2e-coreactor-source"})
	sourceID := stringField(t, sourceRow, "id")
	coreactor := awaitCoreactor(t, setupWS, sourceID)

	sourceRequestID, result := setupWS.requestWithID(sourceID, "channel.list", coreactor, map[string]any{})
	if _, ok := result["value"]; !ok {
		t.Fatalf("coreactor channel.list reply omitted registrar value: %v", result)
	}

	auditWS := dialWS(t, h.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0, sourceID: 0})
	var sourceRequest, sourceReply, c0Request, c0Reply map[string]any
	deadline := time.Now().Add(30 * time.Second)
	for (sourceRequest == nil || sourceReply == nil || c0Request == nil || c0Reply == nil) && time.Now().Before(deadline) {
		select {
		case item := <-auditWS.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil || envelope["type"] != "channel.list" {
				continue
			}
			channelID, _ := item["channel_id"].(string)
			switch {
			case channelID == sourceID && envelope["id"] == sourceRequestID:
				sourceRequest = envelope
			case channelID == sourceID && envelope["kind"] == "response" && envelope["parent_id"] == sourceRequestID:
				sourceReply = envelope
			case channelID == c0ChannelID && envelope["kind"] == "request" && svcactorOriginMatches(envelope, sourceID, sourceRequestID):
				c0Request = envelope
			case channelID == c0ChannelID && c0Request != nil && envelope["kind"] == "response" && envelope["parent_id"] == c0Request["id"]:
				c0Reply = envelope
			}
		case <-auditWS.done:
			t.Fatal("audit websocket closed while replaying coreactor chain")
		case <-time.After(time.Until(deadline)):
		}
	}
	if sourceRequest == nil || sourceReply == nil || c0Request == nil || c0Reply == nil {
		t.Fatalf("coreactor chain incomplete: source_request=%v source_reply=%v c0_request=%v c0_reply=%v", sourceRequest, sourceReply, c0Request, c0Reply)
	}
	if responseStatus(sourceReply) != "completed" || responseStatus(c0Reply) != "completed" {
		t.Fatalf("coreactor chain terminal shape invalid: source=%v c0=%v", sourceReply, c0Reply)
	}
}

func svcactorOriginMatches(envelope map[string]any, channelID, requestID string) bool {
	payload, _ := envelope["payload"].(map[string]any)
	origin, _ := payload["origin"].(map[string]any)
	return origin["channel"] == channelID && origin["request_id"] == requestID && payload["args"] != nil
}

func responseStatus(envelope map[string]any) string {
	payload, _ := envelope["payload"].(map[string]any)
	status, _ := payload["status"].(string)
	return status
}
