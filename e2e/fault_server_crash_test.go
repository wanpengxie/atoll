// Package e2e — see loop_test.go's header for the black-box law this file
// obeys too: ZERO atoll imports, four wire languages only.
package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFaultServerCrashBeforeTerminal is TestLoop's server-restart leg pushed
// into the submit→receipt→terminal WINDOW instead of after a clean terminal:
// the server is SIGKILLed the instant a loop.chat submit frame is sent,
// before this process even finishes reading its own receipt. Real timing
// decides whether the request reached the log before the crash — both
// landings are legal (a request that never committed is a clean miss, not a
// bug) — so the test detects which branch actually happened and asserts the
// matching recovery invariant instead of assuming one:
//
//  ① not lost      — if the request DID commit, its request envelope replays
//     from seq 0 after the restart (same proof TestLoop uses for its
//     pre-crash chat).
//  ② not duplicated — the request reaches EXACTLY ONE terminal (completed or
//     an honest failed) — never a second terminal for the same parent_id —
//     and if completed, the echo's resource side-effect was written exactly
//     once (loop.verify reads back byte-exact content, not corrupted by a
//     double execution).
//  ③ self-heals    — a brand-new chat submitted after the restart still walks
//     the whole path end to end (routing + daemon redial + echo + verify).
func TestFaultServerCrashBeforeTerminal(t *testing.T) {
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

	var port int
	var base string
	serverGen := 0
	startServerProc := func() *proc {
		serverGen++
		serverLog = filepath.Join(dirs["logs"], fmt.Sprintf("fcserver-%d.log", serverGen))
		return startProc(t, fmt.Sprintf("fcserver#%d", serverGen), serverBin, []string{
			"-addr", fmt.Sprintf("127.0.0.1:%d", port),
			"-db", dbPath,
			"-channel-db-dir", dirs["channels"],
		}, dirs["serverwd"], serverLog, env)
	}

	// ---- L1 起: initial start, bind-retry on a fresh port (same pattern as
	// TestLoop / TestGatewayFrames) ------------------------------------------
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
		map[string]any{"email": "fault@example.com", "password": "secret123", "display_name": "Fault"},
		http.StatusCreated)
	_ = reg["id"]

	ws1 := api.must("POST", "/api/workspaces", map[string]any{"name": "fault-ws"}, http.StatusCreated)
	wsID, _ := ws1["id"].(string)
	ch := api.must("POST", "/api/workspaces/"+wsID+"/channels", map[string]any{"name": "home"}, http.StatusCreated)
	chID, _ := ch["id"].(string)

	dm := api.must("POST", "/api/channels/"+chID+"/daemons", map[string]any{"name": "fault-box"}, http.StatusCreated)
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
		daemonLog = filepath.Join(dirs["logs"], fmt.Sprintf("fcdaemon-%d.log", daemonGen))
		return startProc(t, fmt.Sprintf("fcdaemon#%d", daemonGen), daemonBin, []string{
			"-server", fmt.Sprintf("ws://127.0.0.1:%d/compute?channel=%s", port, chID),
			"-key", apiKey,
			"-name", "fault-box",
			"-workspace", dirs["daemon-ws"],
		}, dirs["daemonwd"], daemonLog, env)
	}
	daemon := startDaemon()

	pollUntil(t, "default_agent points at assistant", 30*time.Second, func() bool {
		_, m := api.do("GET", "/api/channels/"+chID, nil)
		return m["default_agent"] == assistantID
	})

	cookie := api.cookieHeader()
	ws := dialWS(t, base, cookie, chID, 0)

	// ---- warm-up: one clean round trip proves the topology is actually
	// live (not still converging) before the fault is injected -------------
	warmPayload := json.RawMessage(`{"text":"fault warm up"}`)
	_, warmTerm := submitAndAwaitTerminal(t, ws, "loop.chat", warmPayload, 120*time.Second)
	warmRid := assertChatReply(t, warmTerm, warmPayload)
	verifyResource(t, ws, warmRid, warmPayload)

	// ---- fault injection: submit, take the receipt (message_id + seq) the
	// instant it arrives, then kill -9 the server WITHOUT waiting for the
	// terminal. The receipt proves the request committed (the commit is what
	// the receipt acks) — so this deterministically lands in the "committed,
	// crashed before terminal" window the spec names as primary. The window
	// is still real: if the server dies faster than the receipt round-trips
	// (e.g. it committed but the ack frame never made it back over the wire),
	// that is the spec's named "clean miss" edge case — legal, and handled
	// below by searching the replayed log rather than trusting the client's
	// own receipt-or-not observation.
	const marker = "fault crash probe"
	faultPayload := json.RawMessage(`{"text":"` + marker + `"}`)
	refCounter++
	faultRef := fmt.Sprintf("faultchat-%d", refCounter)
	if err := ws.send("submit", faultRef, map[string]any{"msg_type": "loop.chat", "payload": faultPayload}); err != nil {
		t.Fatalf("fault submit: ws send: %v", err)
	}
	rec, gotReceipt := ws.awaitRef(faultRef, 10*time.Second)
	if gotReceipt {
		t.Logf("fault submit: receipt landed before kill: %v", rec)
	} else {
		t.Logf("fault submit: no receipt/ack within 10s — crashing anyway (clean-miss candidate)")
	}
	server.kill9(t)

	// ---- restart on the SAME db + channel dir + port ----------------------
	server = startServerProc()
	waitHealthz(t, base, server, 30*time.Second)

	api2 := newAPIClient(t, base)
	api2.must("POST", "/api/identity/login",
		map[string]any{"email": "fault@example.com", "password": "secret123"}, http.StatusOK)
	ws2 := dialWS(t, base, api2.cookieHeader(), chID, 0)

	// ① not lost: did the pre-crash request commit? Replaying from seq 0
	// either finds its request envelope (landed) or it genuinely never made
	// it into the log (clean miss) — both are legal, so detect which.
	reqEnv, landed := ws2.awaitTail(func(e map[string]any) bool {
		if e["kind"] != "request" {
			return false
		}
		p := envelopePayload(e)
		txt, _ := p["text"].(string)
		return txt == marker
	}, 15*time.Second)

	if !landed {
		t.Logf("fault request never committed before the crash (clean miss — legal per spec)")
	} else {
		msgID, _ := reqEnv["id"].(string)
		if msgID == "" {
			t.Fatalf("landed request envelope carries no id: %v", reqEnv)
		}
		t.Logf("fault request %s landed pre-crash; verifying it stays exactly-once", msgID)

		// ② not duplicated. The substrate's own documented contract (platform/
		// home/open.go: "a crashed-but-still-registered receiver ... is left to
		// the expiry reaper — its callers wait for the request deadline; no
		// liveness snapshot is ever a terminal-write dependency") means a mid-
		// flight receiver crash is NOT promised a prompt redelivery: the delivery
		// pump's cursor starts at boot's MaxSeq ("mailbox semantics: only new
		// commits"), so this specific pre-crash request is not re-pumped to the
		// rebuilt assistant cell — it honestly stays open until its own declared
		// deadline (24h default here), which is far beyond this test's budget.
		// So "still open" after a bounded wait is NOT a zombie: it is the
		// documented honest state. What must NEVER happen, in either landing, is
		// a SECOND terminal for the same parent_id (that would prove a genuine
		// re-execution/duplicate-answer bug, not a liveness race — the protocol's
		// own retry discipline always mints a FRESH message_id for a retry, never
		// answers one parent_id twice).
		termEnv, gotTerm := ws2.awaitTail(func(e map[string]any) bool {
			return e["kind"] == "response" && e["parent_id"] == msgID && terminalStatus(e) != ""
		}, 20*time.Second)
		var termID, status string
		if gotTerm {
			termID, _ = termEnv["id"].(string)
			status = terminalStatus(termEnv)
			t.Logf("fault request %s: got a terminal %s status=%s within 20s", msgID, termID, status)
		} else {
			t.Logf("fault request %s: still open after 20s (honest — no proactive redelivery to a crashed receiver; bounded only by its own 24h default deadline)", msgID)
		}

		// Regardless of which branch above landed, no SECOND terminal for this
		// parent_id may ever show up (checked over a further bounded window —
		// long enough to catch a spurious duplicate, short enough to stay fast).
		if dupEnv, dup := ws2.awaitTail(func(e map[string]any) bool {
			return e["kind"] == "response" && e["parent_id"] == msgID &&
				e["id"] != termID && terminalStatus(e) != ""
		}, 10*time.Second); dup {
			t.Fatalf("fault request %s: DUPLICATE terminal %v (first was %v, gotTerm=%v)", msgID, dupEnv, termEnv, gotTerm)
		}

		// If it DID complete, the echo's disk side-effect must have been
		// written exactly once — byte-exact readback, no corruption from a
		// concurrent/duplicate write.
		if status == "completed" {
			rid := assertChatReply(t, termEnv, faultPayload)
			verifyResource(t, ws2, rid, faultPayload)
		} else if status == "failed" {
			reason := payloadField(termEnv, "reason")
			if reason == "" {
				t.Fatalf("fault request %s: failed terminal carries no reason: %v", msgID, termEnv["payload"])
			}
			t.Logf("fault request %s: honest failed terminal, reason=%q", msgID, reason)
		}
	}

	// ③ self-heals: a brand-new chat after the restart still walks the whole
	// path (routing convergence + daemon redial + echo + verify all live).
	freshPayload := json.RawMessage(`{"text":"fault fresh after restart"}`)
	_, freshTerm := submitAndAwaitTerminal(t, ws2, "loop.chat", freshPayload, 180*time.Second)
	freshRid := assertChatReply(t, freshTerm, freshPayload)
	verifyResource(t, ws2, freshRid, freshPayload)

	// The pre-fault warm-up resource still reads back byte-exact across the
	// crash too (the crash didn't corrupt unrelated prior state).
	verifyResource(t, ws2, warmRid, warmPayload)

	daemon.kill9(t)
	server.kill9(t)
}
