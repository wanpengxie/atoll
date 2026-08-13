package e2e

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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

func TestObsDeviceOnlineBecomesUnknownAfterDaemonDies(t *testing.T) {
	h := newHarness(t)
	api, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	registrar := findTool(t, ws)

	device := registrarRequest(t, ws, registrar, "device.mint", map[string]any{"name": "obs-daemon"})
	deviceID := stringField(t, device, "id")
	registrarRequest(t, ws, registrar, "device.attach", map[string]any{
		"channel_id": c0ChannelID, "device_id": deviceID,
	})
	const declarationID = "e2e-obs-kimi"
	registrarRequest(t, ws, registrar, "decl.register", map[string]any{
		"id": declarationID, "name": "E2E obs Kimi", "class": "kimi",
		"config": map[string]any{}, "visibility": "private",
	})
	introduced := ws.request(c0ChannelID, "channel.introduce_actor", systemActor, map[string]any{
		"kind": "tool", "decl_id": declarationID,
	})
	actorID := stringField(t, introduced, "instance_id")

	daemonLog := filepath.Join(h.root, "logs", "obs-daemon.log")
	daemon := startProc(t, "obs-daemon", filepath.Join(e2eBinDir, "atoll-daemon"), []string{
		"--server", fmt.Sprintf("ws://127.0.0.1:%d/compute", h.port),
		"--key", stringField(t, device, "key"), "--name", "obs-daemon", "--home", h.daemonHome,
	}, h.env, filepath.Join(h.root, "work"), daemonLog)
	waitActorPresence(t, ws, actorID, true, daemon, daemonLog)
	daemons := api.request(http.MethodGet, "/obs/space/daemons", nil, http.StatusOK)
	foundDaemon := false
	for _, raw := range daemons["items"].([]any) {
		item := raw.(map[string]any)
		declared := item["declared"].(map[string]any)
		if declared["id"] != deviceID {
			continue
		}
		foundDaemon = true
		if _, leaked := declared["key"]; leaked {
			t.Fatalf("daemon observation leaked key: %v", declared)
		}
		measures := item["actual"].(map[string]any)["measures"].([]any)
		if len(measures) != 1 || measures[0].(map[string]any)["value"] != true {
			t.Fatalf("daemon online measure=%v", measures)
		}
	}
	if !foundDaemon {
		t.Fatalf("daemon missing from observation: %v", daemons)
	}

	extension := dialKimiExtension(t, daemon, daemonLog)
	defer extension.Close()
	waitObsDeviceMeasure(t, api, actorID, func(measure map[string]any) bool {
		return measure["unknown"] == false && measure["value"] == true
	}, "known true")

	daemon.kill9(t)
	waitObsDeviceMeasure(t, api, actorID, func(measure map[string]any) bool {
		value, hasValue := measure["value"]
		return hasValue && value == nil && measure["unknown"] == true && measure["reason"] == "no_testimony"
	}, "unknown no_testimony")
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

func dialKimiExtension(t *testing.T, daemon *proc, daemonLog string) *websocket.Conn {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		conn, _, err := websocket.DefaultDialer.Dial("ws://127.0.0.1:8091/device", nil)
		if err == nil {
			return conn
		}
		if daemon.exited() {
			t.Fatalf("daemon exited before Kimi device endpoint opened\n%s", tailLog(daemonLog, 100))
		}
		if time.Now().After(deadline) {
			t.Fatalf("Kimi device endpoint did not open: %v\n%s", err, tailLog(daemonLog, 100))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitObsDeviceMeasure(t *testing.T, api *apiClient, actorID string, match func(map[string]any) bool, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last map[string]any
	for {
		answer := api.request(http.MethodGet, "/obs/channel/c0/actors", nil, http.StatusOK)
		items, _ := answer["items"].([]any)
		for _, rawItem := range items {
			item, _ := rawItem.(map[string]any)
			if item["key"] != actorID {
				continue
			}
			actual, _ := item["actual"].(map[string]any)
			measures, _ := actual["measures"].([]any)
			for _, rawMeasure := range measures {
				measure, _ := rawMeasure.(map[string]any)
				if measure["name"] == "device_online" {
					last = measure
					if match(measure) {
						return
					}
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("actor %s device_online never became %s; last=%v", actorID, want, last)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
