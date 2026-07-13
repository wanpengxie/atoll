// TestFaultMembershipConservation is a black-box churn-storm fault test (枢纽格
// membership 守恒): it drives the SAME assembly template as TestLoop (loop_test.go)
// — register → workspace → channel → daemon → introduce echo+assistant → start
// daemon — then hammers the channel with N introduce/attach/delete cycles of
// throwaway daemon-placed echo tools, interspersed with daemon kill -9 restarts,
// and asserts the channel's membership ledger converges back to EXACTLY the
// baseline set. This is the black-box half of the soak refcount doubt (attached
// stuck at 2 never falling back) plus the daemon-dereg degrade path.
//
// Black-box law: this file imports ZERO atoll packages — only /api HTTP, binary
// CLI flags and process signals (via the loop_test.go helpers it reuses
// read-only: proc/apiClient/wsClient/pollUntil/startProc/kill9/dialWS/
// submitAndAwaitTerminal).
package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestFaultMembershipConservation(t *testing.T) {
	binDir := os.Getenv("ATOLL_E2E_BIN")
	if binDir == "" {
		t.Skip("ATOLL_E2E_BIN not set; run via `make build-go` + ATOLL_E2E_BIN=$PWD/bin go test ./e2e/")
	}
	serverBin := filepath.Join(binDir, "atoll-server")
	daemonBin := filepath.Join(binDir, "atoll-daemon")
	for _, b := range []string{serverBin, daemonBin} {
		if _, err := os.Stat(b); err != nil {
			t.Fatalf("binary missing: %v", err)
		}
	}

	root := t.TempDir()
	dirs := map[string]string{}
	for _, d := range []string{"serverwd", "daemonwd", "channels", "daemon-ws", "home", "logs"} {
		dirs[d] = filepath.Join(root, d)
		if err := os.MkdirAll(dirs[d], 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	dbPath := filepath.Join(root, "app.db")
	env := scrubbedEnv(dirs["home"])
	var port int
	var base string

	var serverLog, daemonLog string
	t.Cleanup(func() {
		if t.Failed() {
			if serverLog != "" {
				t.Logf("server log tail:\n%s", tailLog(serverLog, 60))
			}
			if daemonLog != "" {
				t.Logf("daemon log tail:\n%s", tailLog(daemonLog, 60))
			}
		}
	})

	serverGen := 0
	startServerProc := func() *proc {
		serverGen++
		serverLog = filepath.Join(dirs["logs"], fmt.Sprintf("fm-server-%d.log", serverGen))
		return startProc(t, fmt.Sprintf("fm-server#%d", serverGen), serverBin, []string{
			"-addr", fmt.Sprintf("127.0.0.1:%d", port),
			"-db", dbPath,
			"-channel-db-dir", dirs["channels"],
		}, dirs["serverwd"], serverLog, env)
	}

	// ---- boot: same assembly template TestLoop uses ------------------------
	var server *proc
	for attempt := 1; ; attempt++ {
		port = freePort(t)
		base = fmt.Sprintf("http://127.0.0.1:%d", port)
		server = startServerProc()
		err := waitHealthzErr(base, server, 30*time.Second)
		if err == nil {
			break
		}
		if server.exited() && attempt < 3 {
			server.reclaim()
			continue
		}
		t.Fatalf("%v; log tail:\n%s", err, tailLog(serverLog, 50))
	}
	api := newAPIClient(t, base)

	reg := api.must("POST", "/api/identity/register",
		map[string]any{"email": "fault-membership@example.com", "password": "secret123", "display_name": "FM"},
		http.StatusCreated)
	userID, _ := reg["id"].(string)

	ws1 := api.must("POST", "/api/workspaces", map[string]any{"name": "fm-ws"}, http.StatusCreated)
	wsID, _ := ws1["id"].(string)

	ch := api.must("POST", "/api/workspaces/"+wsID+"/channels", map[string]any{"name": "home"}, http.StatusCreated)
	chID, _ := ch["id"].(string)

	dm := api.must("POST", "/api/channels/"+chID+"/daemons", map[string]any{"name": "fm-box"}, http.StatusCreated)
	daemonID, _ := dm["id"].(string)
	apiKey, _ := dm["api_key"].(string)
	if daemonID == "" || apiKey == "" {
		t.Fatalf("create-and-attach daemon: %v", dm)
	}

	// Baseline echo tool (server-placed, the assistant's actual tool) + assistant
	// (daemon-placed) — the FIVE members the churn storm must never perturb.
	echoDecl := api.must("POST", "/api/actor-decls",
		map[string]any{"name": "fm-echo-tool", "class": "echo"}, http.StatusCreated)
	echoDeclID, _ := echoDecl["id"].(string)
	echoIntro := api.mustRetry5xx("POST", "/api/channels/"+chID+"/actors",
		map[string]any{"decl_id": echoDeclID, "placement": "server"},
		60*time.Second, http.StatusCreated, http.StatusOK, http.StatusAccepted)
	echoID, _ := echoIntro["instance_id"].(string)
	asstDecl := api.must("POST", "/api/actor-decls",
		map[string]any{"name": "fm-assistant", "class": "script",
			"config": map[string]any{"tool_id": echoID}},
		http.StatusCreated)
	asstDeclID, _ := asstDecl["id"].(string)
	asstIntro := api.mustRetry5xx("POST", "/api/channels/"+chID+"/actors",
		map[string]any{"decl_id": asstDeclID, "placement": "daemon",
			"desired_host": daemonID, "make_default": true},
		60*time.Second, http.StatusCreated, http.StatusOK, http.StatusAccepted)
	assistantID, _ := asstIntro["instance_id"].(string)

	startDaemon := func(gen int) *proc {
		daemonLog = filepath.Join(dirs["logs"], fmt.Sprintf("fm-daemon-%d.log", gen))
		return startProc(t, fmt.Sprintf("fm-daemon#%d", gen), daemonBin, []string{
			"-server", fmt.Sprintf("ws://127.0.0.1:%d/compute?channel=%s", port, chID),
			"-key", apiKey,
			"-name", "fm-box",
			"-workspace", dirs["daemon-ws"],
		}, dirs["daemonwd"], daemonLog, env)
	}
	daemon := startDaemon(1)

	pollUntil(t, "default_agent points at assistant", 30*time.Second, func() bool {
		_, m := api.do("GET", "/api/channels/"+chID, nil)
		return m["default_agent"] == assistantID
	})

	boostID, _ := ch["default_agent"].(string)
	var humanID string
	pollUntil(t, "creator principal is represented by one active human", 30*time.Second, func() bool {
		_, m := api.do("GET", "/api/channels/"+chID+"/actors", nil)
		rows, _ := m["actors"].([]any)
		for _, raw := range rows {
			row, _ := raw.(map[string]any)
			if row["kind"] == "human" && row["principal"] == userID {
				humanID, _ = row["id"].(string)
				return humanID != ""
			}
		}
		return false
	})

	baseline := []string{"system", humanID, boostID, echoID, assistantID}
	sort.Strings(baseline)
	pollUntil(t, "baseline membership is exactly the five expected before the storm", 60*time.Second, func() bool {
		return reflect.DeepEqual(actorIDs(t, api, chID), baseline)
	})

	cookie := api.cookieHeader()
	ws := dialWS(t, base, cookie, chID, 0)

	// ---- churn storm ---------------------------------------------------------
	// N introduce→attach→delete cycles of throwaway daemon-placed echo tools,
	// with a daemon kill9+restart every few cycles. Each cycle's own instance_id
	// must vanish from /actors before the next cycle starts — no cycle is allowed
	// to leave a zombie row behind, and the running set must never exceed
	// baseline+1 (this cycle's live tool).
	const stormCycles = 30
	const killEvery = 7
	chatEvery := 10

	for i := 1; i <= stormCycles; i++ {
		decl := api.mustRetry5xx("POST", "/api/actor-decls",
			map[string]any{"name": fmt.Sprintf("fm-churn-tool-%d", i), "class": "echo"},
			30*time.Second, http.StatusCreated)
		declID, _ := decl["id"].(string)

		intro := api.mustRetry5xx("POST", "/api/channels/"+chID+"/actors",
			map[string]any{"decl_id": declID, "placement": "daemon", "desired_host": daemonID},
			60*time.Second, http.StatusCreated, http.StatusOK, http.StatusAccepted)
		instanceID, _ := intro["instance_id"].(string)
		if instanceID == "" {
			t.Fatalf("churn cycle %d: introduce carried no instance_id: %v", i, intro)
		}

		// Brief wait for it to attach (show up in the membership ledger) before
		// tearing it down — deleting before admission would under-test the
		// membership-add half of the cycle.
		pollUntil(t, fmt.Sprintf("churn cycle %d: instance %s attached", i, instanceID), 30*time.Second, func() bool {
			for _, id := range actorIDs(t, api, chID) {
				if id == instanceID {
					return true
				}
			}
			return false
		})

		api.mustRetry5xx("DELETE", "/api/channels/"+chID+"/actors/"+instanceID,
			nil, 30*time.Second, http.StatusOK, http.StatusAccepted, http.StatusNoContent)

		// Core assertion ①: this cycle's instance must disappear from /actors —
		// no zombie row survives its own DELETE.
		pollUntil(t, fmt.Sprintf("churn cycle %d: instance %s reclaimed from /actors", i, instanceID), 30*time.Second, func() bool {
			for _, id := range actorIDs(t, api, chID) {
				if id == instanceID {
					return false
				}
			}
			return true
		})

		if i%killEvery == 0 {
			daemon.kill9(t)
			daemon = startDaemon(i/killEvery + 1)
			// Give the daemon a moment to redial + reconcile before resuming the
			// storm (bounded by the chat check right below, which itself retries).
		}

		if i%chatEvery == 0 {
			// Core assertion ③: the assistant is not dragged down by the churn —
			// a fresh chat still completes end to end mid-storm.
			payload := json.RawMessage(fmt.Sprintf(`{"text":"storm chat %d"}`, i))
			_, term := submitAndAwaitTerminal(t, ws, "loop.chat", payload, 120*time.Second)
			_ = assertChatReply(t, term, payload)
		}
	}

	// ---- post-storm convergence ----------------------------------------------
	// Core assertion ②: once the storm stops, the membership set converges back
	// to EXACTLY the baseline — no churn tool left as a zombie row, nothing
	// missing either (a strict sorted-set comparison: one extra or one short both
	// fail).
	pollUntil(t, "membership converges back to exactly baseline after the storm", 60*time.Second, func() bool {
		return reflect.DeepEqual(actorIDs(t, api, chID), baseline)
	})

	// A final chat proves the assistant survived the whole storm healthy, not
	// just limping through the mid-storm checks.
	finalPayload := json.RawMessage(`{"text":"storm settled"}`)
	_, finalTerm := submitAndAwaitTerminal(t, ws, "loop.chat", finalPayload, 120*time.Second)
	_ = assertChatReply(t, finalTerm, finalPayload)

	ws.close()
	daemon.kill9(t)
	server.kill9(t)
}

// actorIDs returns the sorted list of channel-actor ids from /api/channels/:ch/actors.
func actorIDs(t *testing.T, api *apiClient, chID string) []string {
	t.Helper()
	_, m := api.do("GET", "/api/channels/"+chID+"/actors", nil)
	rows, _ := m["actors"].([]any)
	var got []string
	for _, r := range rows {
		row, _ := r.(map[string]any)
		id, _ := row["id"].(string)
		got = append(got, id)
	}
	sort.Strings(got)
	return got
}
