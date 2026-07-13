package e2e

// ============================================================================
// soak 观察 harness —— 这是一个「观察工具」，不是 pass/fail 测试，别当废测试删。
// ============================================================================
//
// 它干什么：起真的 server+daemon 两进程，用混合流量 + 混沌剧本长时间跑（默认 60s，
// 可调），把所有子进程日志持久化到一个固定目录，供跑完后 grep 分析。目的是抓
// **静态代码审查和普通单测抓不到的一类问题**——只在「多机制交互 + 时间 + 负载」下
// 才浮现的涌现性问题：遥测盲区、日志刷屏、账本不守恒（如 refcount 只增不减）、
// 累积态泄漏。
//
// 战绩（它真挖出过的）：no_factory 每 pass 重复 warn 的 74× 刷屏、member admit 有痕
// 但 remove 零痕的遥测不对称、gateway/presence 链路默认级别零遥测、churn 后 attached
// 计数疑似不回落。这些 happy-path 测试和静态审都照不到。
//
// 为什么默认不跑：它慢（分钟级）、产大量日志、是「按需观察」不是「每次回归」。所以用
// ATOLL_SOAK=1 门控——普通 `go test ./...` 和 `make e2e-loop` 都会 skip 它，零成本。
//
// 怎么跑（做遥测验收 / 排查涌现性问题时）：
//   ATOLL_SOAK=1 ATOLL_SOAK_SECONDS=120 ATOLL_SOAK_LOGDIR=/tmp/atoll-soak \
//     ATOLL_E2E_BIN=$PWD/bin go test -run TestSoak -v -timeout 600s ./e2e/
// 然后分析日志：
//   cat /tmp/atoll-soak/*.log | <按 level×msg 频率聚合，找刷屏/盲区/不对称>
//
// 它从不因业务判决 t.Fatal（混沌下一个请求被取消/过期是常态，不是失败）——只在
// harness 级坏掉（起不来世界）时 fatal。其余一切计数后 dump，留给人/agent 分析。

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSoak(t *testing.T) {
	if os.Getenv("ATOLL_SOAK") != "1" {
		t.Skip("soak observation run; set ATOLL_SOAK=1 to enable (see file header)")
	}
	binDir := os.Getenv("ATOLL_E2E_BIN")
	if binDir == "" {
		t.Skip("ATOLL_E2E_BIN not set")
	}
	serverBin := filepath.Join(binDir, "atoll-server")
	daemonBin := filepath.Join(binDir, "atoll-daemon")

	seconds := 60
	if v := os.Getenv("ATOLL_SOAK_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			seconds = n
		}
	}
	logDir := os.Getenv("ATOLL_SOAK_LOGDIR")
	if logDir == "" {
		logDir = filepath.Join("/tmp", "atoll-soak")
	}
	// Durable log dir (NOT t.TempDir — the logs must survive for post-run analysis).
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("mkdir logdir: %v", err)
	}
	t.Logf("soak: %ds, logs persisted to %s", seconds, logDir)

	// Isolated data world (db/channels/workspace ephemeral; logs durable above).
	root := t.TempDir()
	dirs := makeDirs(t, root, "serverwd", "daemonwd", "channels", "daemon-ws", "home")
	dbPath := filepath.Join(root, "app.db")
	env := scrubbedEnv(dirs["home"])

	var port int
	var base string
	serverGen := 0
	startServer := func() *proc {
		serverGen++
		lp := filepath.Join(logDir, fmt.Sprintf("server-%d.log", serverGen))
		return startProc(t, fmt.Sprintf("server#%d", serverGen), serverBin, []string{
			"-addr", fmt.Sprintf("127.0.0.1:%d", port),
			"-db", dbPath,
			"-channel-db-dir", dirs["channels"],
		}, dirs["serverwd"], lp, env)
	}

	var server *proc
	for attempt := 1; ; attempt++ {
		port = freePort(t)
		base = fmt.Sprintf("http://127.0.0.1:%d", port)
		server = startServer()
		if waitHealthzErr(base, server, 30*time.Second) == nil {
			break
		}
		if server.exited() && attempt < 3 {
			server.reclaim()
			continue
		}
		t.Fatalf("server not healthy; tail:\n%s", tailLog(filepath.Join(logDir, fmt.Sprintf("server-%d.log", serverGen)), 40))
	}

	api := newAPIClient(t, base)
	api.must("POST", "/api/identity/register",
		map[string]any{"email": "soak@example.com", "password": "secret123", "display_name": "Soak"},
		http.StatusCreated)
	ws1 := api.must("POST", "/api/workspaces", map[string]any{"name": "soak-ws"}, http.StatusCreated)
	wsID, _ := ws1["id"].(string)
	ch := api.must("POST", "/api/workspaces/"+wsID+"/channels", map[string]any{"name": "home"}, http.StatusCreated)
	chID, _ := ch["id"].(string)

	dm := api.must("POST", "/api/channels/"+chID+"/daemons", map[string]any{"name": "soak-box"}, http.StatusCreated)
	apiKey, _ := dm["api_key"].(string)
	daemonID, _ := dm["id"].(string)

	// echo tool (server-placed) + scripted assistant (daemon-placed, default).
	echoDecl := api.must("POST", "/api/actor-decls", map[string]any{"name": "echo-tool", "class": "echo"}, http.StatusCreated)
	echoDeclID, _ := echoDecl["id"].(string)
	echoIntro := api.mustRetry5xx("POST", "/api/channels/"+chID+"/actors",
		map[string]any{"decl_id": echoDeclID, "placement": "server"},
		60*time.Second, http.StatusCreated, http.StatusOK, http.StatusAccepted)
	echoID, _ := echoIntro["instance_id"].(string)
	asstDecl := api.must("POST", "/api/actor-decls",
		map[string]any{"name": "assistant", "class": "script", "config": map[string]any{"tool_id": echoID}},
		http.StatusCreated)
	asstDeclID, _ := asstDecl["id"].(string)
	api.mustRetry5xx("POST", "/api/channels/"+chID+"/actors",
		map[string]any{"decl_id": asstDeclID, "placement": "daemon", "desired_host": daemonID, "make_default": true},
		60*time.Second, http.StatusCreated, http.StatusOK, http.StatusAccepted)

	daemonGen := 0
	startDaemon := func() *proc {
		daemonGen++
		lp := filepath.Join(logDir, fmt.Sprintf("daemon-%d.log", daemonGen))
		return startProc(t, fmt.Sprintf("daemon#%d", daemonGen), daemonBin, []string{
			"-server", fmt.Sprintf("ws://127.0.0.1:%d/compute?channel=%s", port, chID),
			"-key", apiKey, "-name", "soak-box", "-workspace", dirs["daemon-ws"],
		}, dirs["daemonwd"], lp, env)
	}
	daemon := startDaemon()

	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	var (
		chats       atomic.Int64
		chatOK      atomic.Int64
		chatMiss    atomic.Int64
		introRemove atomic.Int64
		badDecls    atomic.Int64
		presenceCh  atomic.Int64
		daemonKills atomic.Int64
	)
	var wg sync.WaitGroup

	cookie := api.cookieHeader()

	// --- Workload 1: steady chat traffic over a long-lived ws --------------
	wg.Add(1)
	go func() {
		defer wg.Done()
		ws := dialWS(t, base, cookie, chID, 0)
		i := 0
		for time.Now().Before(deadline) {
			i++
			chats.Add(1)
			payload := json.RawMessage(fmt.Sprintf(`{"text":"soak msg %d"}`, i))
			id, term := trySubmit(ws, "loop.chat", payload, 8*time.Second)
			if id != "" && terminalStatus(term) == "completed" {
				chatOK.Add(1)
			} else {
				chatMiss.Add(1)
			}
			time.Sleep(150 * time.Millisecond)
		}
	}()

	// --- Workload 2: introduce/remove churn (refcount/membership 守恒 territory) -
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		for time.Now().Before(deadline) {
			n++
			decl := api.must("POST", "/api/actor-decls",
				map[string]any{"name": fmt.Sprintf("churn-echo-%d", n), "class": "echo"}, http.StatusCreated)
			declID, _ := decl["id"].(string)
			intro := api.mustRetry5xx("POST", "/api/channels/"+chID+"/actors",
				map[string]any{"decl_id": declID, "placement": "daemon", "desired_host": daemonID},
				20*time.Second, http.StatusCreated, http.StatusOK, http.StatusAccepted)
			iid, _ := intro["instance_id"].(string)
			introRemove.Add(1)
			time.Sleep(400 * time.Millisecond)
			if iid != "" {
				api.do("DELETE", "/api/channels/"+chID+"/actors/"+iid, nil)
			}
			time.Sleep(400 * time.Millisecond)
		}
	}()

	// --- Workload 3: bad-config declarations (no_factory storm probe) ------
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		for time.Now().Before(deadline) {
			n++
			decl := api.must("POST", "/api/actor-decls",
				map[string]any{"name": fmt.Sprintf("bad-%d", n), "class": "nonexistent-class"}, http.StatusCreated)
			declID, _ := decl["id"].(string)
			api.do("POST", "/api/channels/"+chID+"/actors",
				map[string]any{"decl_id": declID, "placement": "server"})
			badDecls.Add(1)
			time.Sleep(3 * time.Second)
		}
	}()

	// --- Workload 4: presence churn — multi-device attach/detach ----------
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			d := dialWS(t, base, cookie, chID, 0)
			presenceCh.Add(1)
			time.Sleep(500 * time.Millisecond)
			d.close()
			time.Sleep(500 * time.Millisecond)
		}
	}()

	// --- Chaos: periodic daemon kill -9 -----------------------------------
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			time.Sleep(time.Duration(seconds/3+1) * time.Second)
			if !time.Now().Before(deadline) {
				break
			}
			daemon.reclaim()
			daemonKills.Add(1)
			daemon = startDaemon()
		}
	}()

	wg.Wait()
	daemon.reclaim()
	server.kill9(t)

	t.Logf("soak done: chats=%d ok=%d miss=%d | intro/remove=%d | bad-decls=%d | presence-cycles=%d | daemon-kills=%d",
		chats.Load(), chatOK.Load(), chatMiss.Load(), introRemove.Load(), badDecls.Load(), presenceCh.Load(), daemonKills.Load())
	t.Logf("logs for analysis: %s (grep by level×msg to find storms / blind spots / asymmetries)", logDir)
}

// trySubmit is submitAndAwaitTerminal without the t.Fatal — returns ("","") on
// any miss (frame error / no terminal in budget). Soak wants misses counted,
// not fatal (under chaos, misses are data, not failures).
func trySubmit(ws *wsClient, msgType string, payload json.RawMessage, budget time.Duration) (string, map[string]any) {
	refCounter++
	ref := fmt.Sprintf("%s-soak-%d", msgType, refCounter)
	if err := ws.send("submit", ref, map[string]any{"msg_type": msgType, "payload": payload}); err != nil {
		return "", nil
	}
	rec, ok := ws.awaitRef(ref, budget)
	if !ok || rec["frame_type"] == "error" {
		return "", nil
	}
	rp, _ := rec["payload"].(map[string]any)
	id, _ := rp["message_id"].(string)
	if id == "" {
		return "", nil
	}
	env, got := ws.awaitTail(func(env map[string]any) bool {
		return env["kind"] == "response" && env["parent_id"] == id && terminalStatus(env) != ""
	}, budget)
	if !got {
		return id, nil
	}
	return id, env
}
