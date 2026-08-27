package xhs

import (
	"bufio"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/drivers/tools/plugindevice"
)

// The end-side forwarder (tools/plugin-bridge) exists for one situation: the
// browser is on the operator's laptop and the adapter is not. Everything else
// here is tested against a plugin dialling the adapter directly, which is
// exactly the case the bridge does NOT cover — so this is the only test that
// exercises the shipped forwarder against the real adapter.
//
// It skips rather than fails where node or the package's dependencies are
// absent: this asserts a Node artifact works, and a Go checkout without it is
// not a broken adapter.
func TestPluginBridgeCarriesARequestToTheRealAdapter(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed; the bridge is an optional end-side tool")
	}
	root, err := filepath.Abs("../../../tools/plugin-bridge")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "node_modules", "ws")); err != nil {
		t.Skip("tools/plugin-bridge dependencies are not installed (npm install there to run this)")
	}

	// The adapter, exactly as it runs in production.
	a, sys := startActor(t, Config{})

	// The bridge, standing in for the adapter on a local port the plugin dials.
	local := freeLoopbackAddr(t)
	cmd := exec.Command(node, filepath.Join(root, "cli.js"),
		"--upstream", "ws://"+a.ListenAddr()+"/device",
		"--listen", local)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	waitForLine(t, stderr, "listening on")

	// From here on this is the plugin's point of view: it dials localhost and
	// never learns that the adapter is somewhere else.
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+local+"/device", nil)
	if err != nil {
		t.Fatalf("dial the bridge: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	deadline := time.Now().Add(3 * time.Second)
	for !a.Online() {
		if time.Now().After(deadline) {
			t.Fatal("the adapter never saw the bridged connection")
		}
		time.Sleep(5 * time.Millisecond)
	}

	sys.push(request("bridged-1", TypeSearch, map[string]any{"keyword": "go"}))

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var down plugindevice.DownFrame
	if err := conn.ReadJSON(&down); err != nil {
		t.Fatalf("read the bridged command: %v", err)
	}
	if down.Cmd != "search" || down.CorrelationID != "bridged-1" {
		t.Fatalf("down=%+v, want the search command for bridged-1", down)
	}

	result, _ := json.Marshal(map[string]any{"results": []any{}})
	if err := conn.WriteJSON(plugindevice.UpFrame{CorrelationID: "bridged-1", OK: true, Result: result}); err != nil {
		t.Fatalf("reply through the bridge: %v", err)
	}
	if _, ok := sys.waitReply(t, "bridged-1", 3*time.Second); !ok {
		t.Fatal("the reply never made it back through the bridge to the adapter")
	}
}

// freeLoopbackAddr picks a loopback host:port nothing is using, and releases it
// so the bridge can take it.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	a := NewActor(Config{ListenAddr: "127.0.0.1:0"})
	if err := a.dev.Bind("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	addr := a.dev.Addr()
	if err := a.dev.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitForLine(t *testing.T, r interface{ Read([]byte) (int, error) }, want string) {
	t.Helper()
	found := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, want) {
				select {
				case found <- line:
				default:
				}
			}
		}
	}()
	select {
	case <-found:
	case <-time.After(10 * time.Second):
		t.Fatalf("the bridge never reported %q", want)
	}
}
