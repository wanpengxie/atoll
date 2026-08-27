package kimi

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/drivers/tools/plugindevice"
)

// These pin the parts of kimi-webbridge's protocol that are NOT ours to choose.
// They are written from that package's own server (npm: kimi-webbridge, MIT),
// because this adapter stands in for exactly that server: if the real extension
// is going to talk to us, every one of these has to match its expectations
// rather than our preferences.

// The extension dials /ws. It has that path compiled in, so serving anything
// else means the handshake fails and nothing else in this file can ever happen.
func TestTheExtensionsPathIsWhatWeServe(t *testing.T) {
	a, _ := startActor(t, Config{})

	if _, _, err := websocket.DefaultDialer.Dial("ws://"+a.ListenAddr()+"/ws", nil); err != nil {
		t.Fatalf("dial /ws: %v", err)
	}
	// And nothing else: a stray path must not quietly work, or a
	// misconfiguration would look fine right up until the real extension.
	if conn, _, err := websocket.DefaultDialer.Dial("ws://"+a.ListenAddr()+"/device", nil); err == nil {
		_ = conn.Close()
		t.Fatal("/device accepted a connection; the extension's path is /ws")
	}
}

// hello must be answered with hello_ack. The extension does not consider itself
// ready until it arrives, so an adapter that merely accepts the socket looks
// connected and answers nothing — which is the exact failure this whole change
// was made to fix.
func TestHelloIsAcknowledgedBeforeAnyRequestExists(t *testing.T) {
	a, _ := startActor(t, Config{})
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+a.ListenAddr()+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// No channel request has been sent. The handshake is transport business and
	// must complete on its own.
	if err := conn.WriteJSON(map[string]any{
		"type":    "hello",
		"payload": map[string]any{"extensionVersion": "1.2.3"},
	}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var ack struct {
		Type string `json:"type"`
	}
	if err := conn.ReadJSON(&ack); err != nil {
		t.Fatalf("hello went unanswered: %v", err)
	}
	if ack.Type != "hello_ack" {
		t.Fatalf("answered %q, want hello_ack", ack.Type)
	}
}

// The command frame is the extension's shape, field for field. A rename here is
// invisible to every test that uses our own helper, and fatal in production.
func TestTheCommandFrameIsTheExtensionsShape(t *testing.T) {
	a, sys := startActor(t, Config{})
	ext := dialExtension(t, a)

	sys.push(command("req-shape", "evaluate", map[string]any{"code": "1+1"}))

	_ = ext.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var raw json.RawMessage
	if err := ext.conn.ReadJSON(&raw); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got["type"] != "tool_call" {
		t.Errorf(`type=%v want "tool_call"`, got["type"])
	}
	if got["requestId"] != "req-shape" {
		t.Errorf(`requestId=%v want the channel request id`, got["requestId"])
	}
	payload, _ := got["payload"].(map[string]any)
	if payload == nil {
		t.Fatalf("no payload object in %s", raw)
	}
	if payload["name"] != "evaluate" {
		t.Errorf(`payload.name=%v want "evaluate"`, payload["name"])
	}
	args, _ := payload["args"].(map[string]any)
	if args == nil || args["code"] != "1+1" {
		t.Errorf("payload.args=%v want the request's args passed through", payload["args"])
	}
	// The names we do NOT use are worth asserting: this frame family and the
	// one this adapter used to speak are easy to confuse by eye.
	for _, stale := range []string{"correlation_id", "cmd", "params"} {
		if _, present := got[stale]; present {
			t.Errorf("frame still carries %q; that is the old wire, not the extension's", stale)
		}
	}
}

// A failure from the extension arrives as payload.error, and it is free-form —
// a bare string on the common path. It has to reach the channel as a sentence,
// not as a sentence wrapped in quotes.
func TestAnExtensionErrorBecomesTheChannelFailure(t *testing.T) {
	a, sys := startActor(t, Config{})
	ext := dialExtension(t, a)

	sys.push(command("req-err", "click", map[string]any{"ref": "nope"}))
	call := ext.read(t)
	ext.replyError(t, call.RequestID, "element not found: nope")

	fail, ok := sys.waitFail(t, "req-err", 2*time.Second)
	if !ok {
		t.Fatal("the extension's error never became a channel failure")
	}
	if fail.detail != "element not found: nope" {
		t.Errorf("detail=%q, want the extension's own words, unquoted", fail.detail)
	}
}

// The heartbeat is an APPLICATION frame in this protocol, not a WS control
// ping: the extension answers {"type":"pong"} and ignores protocol-level pings.
// An adapter sending only control frames would satisfy itself and reach nobody.
func TestTheHeartbeatIsTheOneTheExtensionAnswers(t *testing.T) {
	every, frame, ok := plugindevice.WebbridgeProtocol{}.Heartbeat()
	if !ok {
		t.Fatal("no heartbeat; the extension expects one every 15s")
	}
	if every != 15*time.Second {
		t.Errorf("interval=%v, want 15s to match kimi-webbridge's own server", every)
	}
	var got map[string]any
	if err := json.Unmarshal(frame, &got); err != nil {
		t.Fatal(err)
	}
	if got["type"] != "ping" {
		t.Errorf(`heartbeat frame is %s, want {"type":"ping"}`, frame)
	}
}

// A pong must not be mistaken for anything. It closes no request and provokes
// no answer — it is only evidence the socket is alive.
func TestAPongIsNotMistakenForAResult(t *testing.T) {
	a, sys := startActor(t, Config{})
	ext := dialExtension(t, a)

	sys.push(command("req-live", "snapshot", nil))
	call := ext.read(t)

	if err := ext.conn.WriteJSON(map[string]any{"type": "pong"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	if _, closed := sys.waitReply(t, "req-live", 10*time.Millisecond); closed {
		t.Fatal("a pong closed an open request")
	}
	if _, closed := sys.waitFail(t, "req-live", 10*time.Millisecond); closed {
		t.Fatal("a pong failed an open request")
	}

	// And the request is still answerable afterwards.
	ext.reply(t, call.RequestID, map[string]any{"ok": true})
	if _, closed := sys.waitReply(t, "req-live", 2*time.Second); !closed {
		t.Fatal("the request stopped being answerable after a pong")
	}
}
