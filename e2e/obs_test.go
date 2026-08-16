package e2e

import (
	"net/http"
	"testing"
	"time"
)

func TestObsPullReadsRealStateWithoutAppendingChannelLog(t *testing.T) {
	h := newHarness(t)
	api, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})

	beforeID := ws.submit(c0ChannelID, "e2e.obs.before", "event", nil, map[string]any{"marker": "before"})
	before := awaitFeedItem(t, ws, func(envelope map[string]any) bool { return envelope["id"] == beforeID })
	beforeSeq, _ := before["seq"].(float64)

	answer := api.request(http.MethodGet, "/obs/channel/c0/profile", nil, http.StatusOK)
	if answer["subject"] != "channel/c0/profile" || answer["kind"] != "profile" {
		t.Fatalf("obs identity=%v", answer)
	}
	items, _ := answer["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("profile items=%v", answer["items"])
	}
	item, _ := items[0].(map[string]any)
	actual, _ := item["actual"].(map[string]any)
	measures, _ := actual["measures"].([]any)
	if len(measures) != 1 || measures[0].(map[string]any)["name"] != "open" || measures[0].(map[string]any)["value"] != true {
		t.Fatalf("profile measures=%v", measures)
	}

	afterID := ws.submit(c0ChannelID, "e2e.obs.after", "event", nil, map[string]any{"marker": "after"})
	after := awaitFeedItem(t, ws, func(envelope map[string]any) bool { return envelope["id"] == afterID })
	afterSeq, _ := after["seq"].(float64)
	if beforeSeq == 0 || afterSeq != beforeSeq+1 {
		t.Fatalf("channel log changed across obs pull: before seq=%v after seq=%v", beforeSeq, afterSeq)
	}
}

func TestObsRejectsUnauthenticatedAndRetiredPrincipalWithLiveCookie(t *testing.T) {
	h := newHarness(t)
	unauth := newAPIClient(t, h.base)
	unauth.request(http.MethodGet, "/obs/space/channels", nil, http.StatusUnauthorized)

	user := newAPIClient(t, h.base)
	user.register("obs-retired", "obs-retired@example.test", "obs-password")
	channels := user.request(http.MethodGet, "/obs/space/channels?parent_id=c0", nil, http.StatusOK)
	qualified := false
	for _, raw := range channels["items"].([]any) {
		item := raw.(map[string]any)
		declared := item["declared"].(map[string]any)
		if declared["name"] == "obs-retired" && declared["qualified_name"] == "c0.obs-retired" {
			qualified = true
		}
		if _, leaked := declared["spec"]; leaked {
			t.Fatalf("channel observation leaked genesis spec: %v", declared)
		}
	}
	if !qualified {
		t.Fatalf("registered home channel lacks Registry-qualified name: %v", channels)
	}
	_, rootWS := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findTool(t, rootWS)
	registrarRequest(t, rootWS, registrar, "principal.retire", map[string]any{"principal_id": "obs-retired"})
	answer := user.request(http.MethodGet, "/obs/space/channels", nil, http.StatusForbidden)
	if answer["code"] != "permission_denied" {
		t.Fatalf("retired principal response=%v", answer)
	}
}

func awaitFeedItem(t *testing.T, ws *wsClient, match func(map[string]any) bool) map[string]any {
	t.Helper()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	for {
		select {
		case item := <-ws.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope != nil && match(envelope) {
				return item
			}
		case <-ws.done:
			t.Fatal("websocket closed while awaiting obs test marker")
		case <-timer.C:
			t.Fatal("no matching obs test marker")
		}
	}
}
