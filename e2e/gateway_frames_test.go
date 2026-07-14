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

// requireE2EBin skips unless ATOLL_E2E_BIN points at a dir with atoll-server (the
// binaries `make e2e-loop` builds). Bare `go test ./...` leaves it unset → skip.
func requireE2EBin(t *testing.T) string {
	t.Helper()
	binDir := os.Getenv("ATOLL_E2E_BIN")
	if binDir == "" {
		t.Skip("ATOLL_E2E_BIN not set; run via `make e2e-loop`")
	}
	if _, err := os.Stat(filepath.Join(binDir, "atoll-server")); err != nil {
		t.Fatalf("binary missing: %v", err)
	}
	return binDir
}

// makeDirs creates the named subdirs under root and returns their absolute paths.
func makeDirs(t *testing.T, root string, names ...string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, n := range names {
		p := filepath.Join(root, n)
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", n, err)
		}
		out[n] = p
	}
	return out
}

// TestGatewayFrames is the gateway-期 frame-protocol black-box (DoD-2): a home-side
// human member drives ALL five business frames (submit / resolve / cancel / after /
// cancel_timer) plus the opening attach control frame over the real /ws, and its
// presence自报 is read back out-of-band through the status API. It is server-ONLY —
// the human embodiment is home-side (the reconcile ring keeps the admitted member's
// cell up), so no daemon is needed; keeping it daemon-free makes the frame contract
// deterministic and fast. (detach is整删 in the连接模型勘误期 v2 — the client-visible
// unbind verb has no ontology; a connection is an authenticated person + one pipe.)
//
// The four composition controls (introduce / remove / restart / set_default_agent)
// ride the same subjectgate submit-frame path and are exercised end-to-end by
// TestDaemonBinaryCanonicalControl with the real daemon binary. 非成员 tail-only (a workspace
// member who is not a channel member gets a read-only session whose business
// frames are refused not_member) is NOT reachable through today's HTTP surface —
// there is no endpoint to add a second workspace member, and a channel's creator is
// auto-admitted as a member — so it is covered at the gateway Session unit layer,
// not here (申报, S6 return).
func TestGatewayFrames(t *testing.T) {
	binDir := requireE2EBin(t)
	serverBin := filepath.Join(binDir, "atoll-server")

	root := t.TempDir()
	dirs := makeDirs(t, root, "serverwd", "channels", "home", "logs")
	dbPath := filepath.Join(root, "app.db")
	env := scrubbedEnv(dirs["home"])

	var serverLog string
	t.Cleanup(func() {
		if t.Failed() && serverLog != "" {
			t.Logf("server log tail:\n%s", tailLog(serverLog, 60))
		}
	})

	// Start the server with a bounded probe→listen retry.
	var server *proc
	var base string
	gen := 0
	for attempt := 1; ; attempt++ {
		port := freePort(t)
		base = fmt.Sprintf("http://127.0.0.1:%d", port)
		gen++
		serverLog = filepath.Join(dirs["logs"], fmt.Sprintf("server-%d.log", gen))
		args := []string{
			"-addr", fmt.Sprintf("127.0.0.1:%d", port),
			"-db", dbPath,
			"-channel-db-dir", dirs["channels"],
		}
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			args = append(args, "-init")
		}
		server = startProc(t, fmt.Sprintf("gwframes-server#%d", gen), serverBin, args, dirs["serverwd"], serverLog, env)
		if waitHealthzErr(base, server, 30*time.Second) == nil {
			break
		}
		if server.exited() && attempt < 3 {
			server.reclaim()
			continue
		}
		t.Fatalf("server not healthy; log tail:\n%s", tailLog(serverLog, 50))
	}

	api := newAPIClient(t, base)
	reg := api.must("POST", "/api/identity/register",
		map[string]any{"email": "gw@example.com", "password": "secret123", "display_name": "GW"},
		http.StatusCreated)
	userID, _ := reg["id"].(string)
	ws1 := api.must("POST", "/api/workspaces", map[string]any{"name": "gw-ws"}, http.StatusCreated)
	wsID, _ := ws1["id"].(string)
	ch := api.must("POST", "/api/workspaces/"+wsID+"/channels", map[string]any{"name": "home"}, http.StatusCreated)
	chID, _ := ch["id"].(string)

	// Channel creation returns the creator's admitted subject identity. Channel
	// internals have no parallel HTTP roster transport.
	humanID, _ := ch["creator_actor_id"].(string)
	if humanID == "" || userID == "" {
		t.Fatalf("channel creation omitted creator subject identity: %v", ch)
	}

	cookie := api.cookieHeader()
	ws := dialWS(t, base, cookie, chID, 0)

	// ---- submit (event) ----------------------------------------------------
	// An explicit audience skips gateway routing; a public event lands in the log
	// and shows on the feed投影.
	eventID := frameSubmit(t, ws, map[string]any{
		"msg_type":   "gw.note",
		"kind":       "event",
		"visibility": "public",
		"audience":   []string{humanID},
		"payload":    json.RawMessage(`{"n":1}`),
	})
	if _, ok := ws.awaitTail(func(env map[string]any) bool { return env["id"] == eventID }, 15*time.Second); !ok {
		t.Fatalf("event %s never appeared on the feed", eventID)
	}

	// ---- submit (request) + resolve ---------------------------------------
	// A self-addressed human.approve request is left OPEN by the mailbox serve loop
	// (default-deferred); the person's resolve frame is the real answer.
	reqID := frameSubmit(t, ws, map[string]any{
		"msg_type": "human.approve",
		"kind":     "request",
		"audience": []string{humanID},
		"payload":  json.RawMessage(`{}`),
	})
	rec := frameVerb(t, ws, "resolve", "resolve-1", map[string]any{
		"req_id": reqID, "decision": "approved", "payload": json.RawMessage(`{"note":"ok"}`),
	})
	if got := receiptField(rec, "req_id"); got != reqID {
		t.Fatalf("resolve receipt req_id=%q want %q", got, reqID)
	}
	respEnv, ok := ws.awaitTail(func(env map[string]any) bool {
		return env["kind"] == "response" && env["parent_id"] == reqID && terminalStatus(env) == "completed"
	}, 15*time.Second)
	if !ok {
		t.Fatalf("resolve: no completed terminal for %s", reqID)
	}
	if d := payloadField(respEnv, "decision"); d != "approved" {
		t.Fatalf("resolve terminal decision=%q want approved (payload %v)", d, respEnv["payload"])
	}

	// ---- submit (request) + cancel (self-cancel) --------------------------
	// The sender may cancel its own open request → a failed terminal (义务归位: a
	// subject's own request, self-closed).
	req2 := frameSubmit(t, ws, map[string]any{
		"msg_type": "human.approve",
		"kind":     "request",
		"audience": []string{humanID},
		"payload":  json.RawMessage(`{}`),
	})
	rec = frameVerb(t, ws, "cancel", "cancel-1", map[string]any{"req_id": req2})
	if got := receiptField(rec, "req_id"); got != req2 {
		t.Fatalf("cancel receipt req_id=%q want %q", got, req2)
	}
	if _, ok := ws.awaitTail(func(env map[string]any) bool {
		return env["kind"] == "response" && env["parent_id"] == req2 && terminalStatus(env) == "failed"
	}, 15*time.Second); !ok {
		t.Fatalf("cancel: no failed terminal for %s", req2)
	}

	// ---- after + cancel_timer ---------------------------------------------
	// A durable timer far in the future, then cancelled before it can fire.
	rec = frameVerb(t, ws, "after", "after-1", map[string]any{
		"duration_ms": 60000, "msg_type": "human.message", "payload": json.RawMessage(`{}`),
	})
	timerID := receiptField(rec, "timer_id")
	if timerID == "" {
		t.Fatalf("after receipt carries no timer_id: %v", rec)
	}
	rec = frameVerb(t, ws, "cancel_timer", "ct-1", map[string]any{"timer_id": timerID})
	if got := receiptField(rec, "timer_id"); got != timerID {
		t.Fatalf("cancel_timer receipt timer_id=%q want %q", got, timerID)
	}

	// ---- a SECOND connection for the same principal is independent -----------
	// 连接即人 v2: two connections are just two pipes for the same person — closing
	// one leaves the other fully live (there is NO shared 频道臂 to seal, no detach to
	// propagate). Prove it: open ws2, close it, and confirm the primary `ws` still
	// drives a frame round-trip.
	ws2 := dialWSMulti(t, base, cookie, chID, map[string]int64{chID: 0})
	ws2.close()
	select {
	case <-ws.done:
		t.Fatal("closing a second connection must NOT tear down the primary (连接即人: pipes are independent)")
	case <-time.After(500 * time.Millisecond):
	}
	rec = frameVerb(t, ws, "after", "after-2", map[string]any{
		"duration_ms": 60000, "msg_type": "human.message", "payload": json.RawMessage(`{}`),
	})
	if receiptField(rec, "timer_id") == "" {
		t.Fatalf("primary session dead after peer close: %v", rec)
	}

	server.kill9(t)
}

// frameSubmit sends one submit frame and asserts a receipt, returning the minted
// message id (an error frame fails the test).
func frameSubmit(t *testing.T, ws *wsClient, payload map[string]any) string {
	t.Helper()
	refCounter++
	ref := fmt.Sprintf("gwsubmit-%d", refCounter)
	rec := sendAndAwait(t, ws, "submit", ref, payload)
	return receiptField(rec, "message_id")
}

// frameVerb sends one upstream verb frame and returns its receipt (an error frame
// fails the test).
func frameVerb(t *testing.T, ws *wsClient, frameType, ref string, payload map[string]any) map[string]any {
	t.Helper()
	return sendAndAwait(t, ws, frameType, ref, payload)
}

func sendAndAwait(t *testing.T, ws *wsClient, frameType, ref string, payload map[string]any) map[string]any {
	t.Helper()
	if err := ws.send(frameType, ref, payload); err != nil {
		t.Fatalf("%s: ws send: %v", frameType, err)
	}
	rec, ok := ws.awaitRef(ref, 10*time.Second)
	if !ok {
		t.Fatalf("%s: no receipt/error frame for ref %s within 10s", frameType, ref)
	}
	if rec["frame_type"] == "error" {
		t.Fatalf("%s: frame error %q (detail %q)", frameType, frameErrCode(rec), frameErrDetail(rec))
	}
	if rec["frame_type"] != "receipt" {
		t.Fatalf("%s: unexpected frame %v", frameType, rec)
	}
	return rec
}

// receiptField reads one string field from a receipt frame's payload.
func receiptField(rec map[string]any, key string) string {
	p, _ := rec["payload"].(map[string]any)
	v, _ := p[key].(string)
	return v
}
