package e2e

import (
	"testing"
	"time"
)

func TestOrdinaryChannelSpaceToolCarriesTwoLedgerSourceRefs(t *testing.T) {
	h := newHarness(t)
	api, setupWS := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findTool(t, setupWS)
	sourceRow := registrarRequest(t, setupWS, registrar, "channel.create", map[string]any{
		"name": "e2e-space-tool-source",
	})
	sourceID := stringField(t, sourceRow, "id")

	var spaceTool string
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, catalog, err := setupWS.tryRequest(sourceID, "actor.list", systemActor, map[string]any{})
		if err == nil {
			rows, _ := catalog["actors"].([]any)
			for _, raw := range rows {
				row, _ := raw.(map[string]any)
				if row["kind"] == "tool" {
					spaceTool, _ = row["id"].(string)
					if spaceTool != "" {
						break
					}
				}
			}
		}
		if spaceTool != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if spaceTool == "" {
		t.Fatalf("ordinary channel %s never published its space-tool", sourceID)
	}

	sourceRequestID, result := setupWS.requestWithID(sourceID, "channel.create", spaceTool, map[string]any{
		"name": "e2e-space-tool-child",
	})
	if stringField(t, result, "id") == "" {
		t.Fatal("space-tool channel.create returned no child")
	}

	// Replay both ledgers on a fresh connection. The live request waiter may
	// consume interleaved c0 feed frames, while since=0 makes the audit complete.
	auditWS := dialWS(t, h.base, api.cookieHeader(), map[string]int64{
		c0ChannelID: 0,
		sourceID:    0,
	})
	var sourceRequest, sourceReply, c0Request, c0Reply map[string]any
	deadline = time.Now().Add(30 * time.Second)
	for (sourceRequest == nil || sourceReply == nil || c0Request == nil || c0Reply == nil) && time.Now().Before(deadline) {
		select {
		case item := <-auditWS.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil || envelope["type"] != "channel.create" {
				continue
			}
			channelID, _ := item["channel_id"].(string)
			switch {
			case channelID == sourceID && envelope["id"] == sourceRequestID:
				sourceRequest = envelope
			case channelID == sourceID && envelope["kind"] == "response" && envelope["parent_id"] == sourceRequestID:
				sourceReply = envelope
			case channelID == c0ChannelID && envelope["kind"] == "request" && forwardedEnvelopeMatches(envelope, sourceID, sourceRequestID):
				c0Request = envelope
			case channelID == c0ChannelID && c0Request != nil && envelope["kind"] == "response" && envelope["parent_id"] == c0Request["id"]:
				c0Reply = envelope
			}
		case <-auditWS.done:
			t.Fatal("audit websocket closed while replaying the two ledgers")
		case <-time.After(time.Until(deadline)):
		}
	}
	if sourceRequest == nil || sourceReply == nil || c0Request == nil || c0Reply == nil {
		t.Fatalf("two-ledger chain incomplete: source_request=%v source_reply=%v c0_request=%v c0_reply=%v", sourceRequest, sourceReply, c0Request, c0Reply)
	}
	if sourceRequest["kind"] != "request" || responseStatus(sourceReply) != "completed" || responseStatus(c0Reply) != "completed" {
		t.Fatalf("two-ledger question/answer shape invalid: source_request=%v source_reply=%v c0_request=%v c0_reply=%v", sourceRequest, sourceReply, c0Request, c0Reply)
	}
	if !sourceRefMatches(c0Reply, sourceID, sourceRequestID) {
		t.Fatalf("c0 receipt does not point back to source request %s:%s: %v", sourceID, sourceRequestID, c0Reply)
	}
}

func sourceRefMatches(envelope map[string]any, channelID, requestID string) bool {
	payload, _ := envelope["payload"].(map[string]any)
	source, _ := payload["source"].(map[string]any)
	return source["channel_id"] == channelID && source["request_id"] == requestID
}

// forwardedEnvelopeMatches recognizes the corridor's c0 leg: space-tool
// forwards the member's original envelope verbatim, so the source facts are
// the envelope's own channel_id and id rather than a source-ref wrapper.
func forwardedEnvelopeMatches(envelope map[string]any, channelID, requestID string) bool {
	payload, _ := envelope["payload"].(map[string]any)
	return payload["channel_id"] == channelID && payload["id"] == requestID
}

func responseStatus(envelope map[string]any) string {
	payload, _ := envelope["payload"].(map[string]any)
	status, _ := payload["status"].(string)
	return status
}
