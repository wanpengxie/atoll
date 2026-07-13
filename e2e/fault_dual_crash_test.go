// TestFaultDualCrash is a black-box fault-injection test: server AND daemon
// crash CONCURRENTLY, then are restarted in the production order (server
// first + healthz, daemon second — its -server URL is welded to the server's
// port). Black-box law (unchanged from loop_test.go): this file imports ZERO
// atoll packages and speaks only /api HTTP, /ws frames, binary CLI flags, and
// process signals. It only READS proc/apiClient/wsClient/frame helpers
// defined in loop_test.go and gateway_frames_test.go — never edits them.
package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

// TestFaultDualCrash: normal assembly + one successful chat round, then a
// CONCURRENT kill -9 of both server and daemon, then a fixed-order restart
// (server first, daemon second). Three invariants:
//   ① 自愈  — default_agent reconverges to the assistant, membership
//     reconverges to the five-member baseline, the daemon re-attaches —
//     none of that deadlocks (neither side wedges waiting on the other).
//   ② 不丢  — the pre-crash chat request+response replay byte-exact from
//     seq 0 after the restart.
//   ③ 走通  — a brand-new chat completes end-to-end post-restart (daemon
//     redial → reconcile revival → routing → call → resource all live).
func TestFaultDualCrash(t *testing.T) {
	binDir := requireE2EBin(t)
	serverBin := filepath.Join(binDir, "atoll-server")
	daemonBin := filepath.Join(binDir, "atoll-daemon")
	if _, err := os.Stat(daemonBin); err != nil {
		t.Fatalf("binary missing: %v", err)
	}

	root := t.TempDir()
	dirs := makeDirs(t, root, "serverwd", "daemonwd", "channels", "daemon-ws", "home", "logs")
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
		serverLog = filepath.Join(dirs["logs"], fmt.Sprintf("server-%d.log", serverGen))
		return startProc(t, fmt.Sprintf("dualcrash-server#%d", serverGen), serverBin, []string{
			"-addr", fmt.Sprintf("127.0.0.1:%d", port),
			"-db", dbPath,
			"-channel-db-dir", dirs["channels"],
		}, dirs["serverwd"], serverLog, env)
	}

	// ---- L1 起: bind-retry, same discipline as TestLoop -----------------
	var server *proc
	for attempt := 1; ; attempt++ {
		port = freePort(t)
		base = fmt.Sprintf("http://127.0.0.1:%d", port)
		server = startServerProc()
		if err := waitHealthzErr(base, server, 30*time.Second); err == nil {
			break
		} else if server.exited() && attempt < 3 {
			server.reclaim()
			continue
		} else {
			t.Fatalf("%v; log tail:\n%s", err, tailLog(serverLog, 50))
		}
	}
	api := newAPIClient(t, base)

	reg := api.must("POST", "/api/identity/register",
		map[string]any{"email": "dualcrash@example.com", "password": "secret123", "display_name": "DualCrash"},
		http.StatusCreated)
	userID, _ := reg["id"].(string)

	wsRow := api.must("POST", "/api/workspaces", map[string]any{"name": "dualcrash-ws"}, http.StatusCreated)
	wsID, _ := wsRow["id"].(string)

	ch := api.must("POST", "/api/workspaces/"+wsID+"/channels", map[string]any{"name": "home"}, http.StatusCreated)
	chID, _ := ch["id"].(string)

	dm := api.must("POST", "/api/channels/"+chID+"/daemons", map[string]any{"name": "dualcrash-box"}, http.StatusCreated)
	daemonID, _ := dm["id"].(string)
	apiKey, _ := dm["api_key"].(string)
	if daemonID == "" || apiKey == "" {
		t.Fatalf("create-and-attach daemon: %v", dm)
	}

	echoDecl := api.must("POST", "/api/actor-decls",
		map[string]any{"name": "echo-tool", "class": "echo"}, http.StatusCreated)
	echoDeclID, _ := echoDecl["id"].(string)
	echoIntro := api.mustRetry5xx("POST", "/api/channels/"+chID+"/actors",
		map[string]any{"decl_id": echoDeclID, "placement": "server"},
		60*time.Second, http.StatusCreated, http.StatusOK, http.StatusAccepted)
	echoID, _ := echoIntro["instance_id"].(string)

	asstDecl := api.must("POST", "/api/actor-decls",
		map[string]any{"name": "assistant", "class": "script",
			"config": map[string]any{"tool_id": echoID}},
		http.StatusCreated)
	asstDeclID, _ := asstDecl["id"].(string)
	asstIntro := api.mustRetry5xx("POST", "/api/channels/"+chID+"/actors",
		map[string]any{"decl_id": asstDeclID, "placement": "daemon",
			"desired_host": daemonID, "make_default": true},
		60*time.Second, http.StatusCreated, http.StatusOK, http.StatusAccepted)
	assistantID, _ := asstIntro["instance_id"].(string)

	daemonGen := 0
	startDaemon := func() *proc {
		daemonGen++
		daemonLog = filepath.Join(dirs["logs"], fmt.Sprintf("daemon-%d.log", daemonGen))
		return startProc(t, fmt.Sprintf("dualcrash-daemon#%d", daemonGen), daemonBin, []string{
			"-server", fmt.Sprintf("ws://127.0.0.1:%d/compute?channel=%s", port, chID),
			"-key", apiKey,
			"-name", "dualcrash-box",
			"-workspace", dirs["daemon-ws"],
		}, dirs["daemonwd"], daemonLog, env)
	}
	daemon := startDaemon()

	// baseline convergence, mirroring TestLoop's L1 assertions.
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
	wantMembers := []string{"system", humanID, boostID, echoID, assistantID}
	sort.Strings(wantMembers)
	assertMembershipBaseline := func(label string) {
		pollUntil(t, label, 60*time.Second, func() bool {
			_, m := api.do("GET", "/api/channels/"+chID+"/actors", nil)
			rows, _ := m["actors"].([]any)
			var got []string
			for _, r := range rows {
				row, _ := r.(map[string]any)
				id, _ := row["id"].(string)
				got = append(got, id)
			}
			sort.Strings(got)
			return reflect.DeepEqual(got, wantMembers)
		})
	}
	assertMembershipBaseline("membership is exactly the five expected (pre-crash)")

	// ---- L2-L5: one successful chat round pre-crash ----------------------
	cookie := api.cookieHeader()
	ws := dialWS(t, base, cookie, chID, 0)

	chatPayload1 := json.RawMessage(`{"text":"dual crash one"}`)
	chat1ID, term1 := submitAndAwaitTerminal(t, ws, "loop.chat", chatPayload1, 120*time.Second)
	rid1 := assertChatReply(t, term1, chatPayload1)
	verifyResource(t, ws, rid1, chatPayload1)

	// ---- FAULT: kill -9 server AND daemon CONCURRENTLY --------------------
	// Two goroutines fire reclaim() (the same idempotent SIGKILL-the-group +
	// bounded Wait that proc.kill9 uses) at the same time — no ordering
	// between the two signals, exercising both "server dies while daemon is
	// mid-attach" and "daemon dies while server is mid-teardown" without
	// favoring either race direction. t.Fatalf must run only on the test's
	// own goroutine, so the goroutines just reclaim(); the assertion that
	// each process actually died happens back on the test goroutine below.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); server.reclaim() }()
	go func() { defer wg.Done(); daemon.reclaim() }()
	wg.Wait()
	if !server.exited() {
		t.Fatalf("server not reclaimed after concurrent kill -9")
	}
	if !daemon.exited() {
		t.Fatalf("daemon not reclaimed after concurrent kill -9")
	}
	ws.close()

	// ---- RESTART: fixed production order — server first (+healthz), then
	// daemon (its -server URL is welded to the server's port) -------------
	server = startServerProc()
	waitHealthz(t, base, server, 30*time.Second)
	daemon = startDaemon()

	// ① 自愈: assembly order does not deadlock — default_agent and
	// membership both reconverge to the pre-crash baseline, proving neither
	// side is wedged waiting on the other (server waiting on daemon reattach,
	// or daemon waiting on a server that never advances past assembly).
	pollUntil(t, "default_agent reconverges to assistant after dual crash", 30*time.Second, func() bool {
		_, m := api.do("GET", "/api/channels/"+chID, nil)
		return m["default_agent"] == assistantID
	})
	assertMembershipBaseline("membership reconverges to the five-member baseline after dual crash")

	// Fresh session (server db survives; a fresh login is the honest client
	// behaviour after a server crash, same as TestLoop's restart leg).
	api2 := newAPIClient(t, base)
	api2.must("POST", "/api/identity/login",
		map[string]any{"email": "dualcrash@example.com", "password": "secret123"}, http.StatusOK)
	ws2 := dialWS(t, base, api2.cookieHeader(), chID, 0)

	// ② 不丢: the pre-crash request+response replay byte-exact from seq 0.
	if _, ok := ws2.awaitTail(func(env map[string]any) bool {
		return env["id"] == chat1ID && env["kind"] == "request"
	}, 30*time.Second); !ok {
		t.Fatalf("dual crash: pre-crash request envelope %s not replayed from seq 0", chat1ID)
	}
	if _, ok := ws2.awaitTail(func(env map[string]any) bool {
		return env["kind"] == "response" && env["parent_id"] == chat1ID && terminalStatus(env) == "completed"
	}, 30*time.Second); !ok {
		t.Fatalf("dual crash: pre-crash response for %s not replayed from seq 0", chat1ID)
	}

	// ③ 走通: daemon reattached — a brand-new chat completes end-to-end
	// (daemon redial → reconcile revival → routing → call → resource all
	// live), and the ORIGINAL pre-crash resource still reads back
	// byte-exact across the dual crash.
	chatPayload2 := json.RawMessage(`{"text":"dual crash two"}`)
	_, term2 := submitAndAwaitTerminal(t, ws2, "loop.chat", chatPayload2, 180*time.Second)
	_ = assertChatReply(t, term2, chatPayload2)
	verifyResource(t, ws2, rid1, chatPayload1)

	daemon.kill9(t)
	server.kill9(t)
}
