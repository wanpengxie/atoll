package xhs

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/drivers/tools/plugindevice"
	"github.com/wanpengxie/atoll/protocol/message"
)

// listenReply decodes a listen word's reply body.
type listenReply struct {
	DesiredAddr string `json:"desired_addr"`
	ActualAddr  string `json:"actual_addr"`
	Online      bool   `json:"online"`
	Loopback    bool   `json:"loopback"`
	Persisted   bool   `json:"persisted"`
}

func decodeListen(t *testing.T, v any) listenReply {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}
	var out listenReply
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	return out
}

// The whole point of the word: an operator whose browser is on a different
// machine from the adapter moves the endpoint to an address the browser can
// reach, and the plugin then connects there. Asserted end to end — the new
// address is not merely reported, it actually accepts a connection and carries
// a request.
func TestListenSetMovesTheEndpointAndItReallyServes(t *testing.T) {
	a, sys := startActor(t, Config{})
	old := a.ListenAddr()

	sys.push(request("set-1", TypeListenSet, map[string]any{"listen_addr": "127.0.0.1:0"}))
	rep, ok := sys.waitReply(t, "set-1", 2*time.Second)
	if !ok {
		t.Fatal("no reply to xhs.listen.set")
	}
	got := decodeListen(t, rep.v)
	if got.ActualAddr == "" || got.ActualAddr == old {
		t.Fatalf("actual_addr=%q old=%q, want a new address", got.ActualAddr, old)
	}
	if !got.Persisted {
		t.Error("persisted=false; a set that is forgotten on restart is not a setting")
	}

	// The old address is gone. Dialing it must fail rather than silently keep
	// serving a listener nobody knows about.
	if conn, _, err := websocket.DefaultDialer.Dial("ws://"+old+"/device", nil); err == nil {
		_ = conn.Close()
		t.Fatalf("the old address %s is still accepting connections", old)
	}

	// The new one really works: a plugin connects and a request round-trips.
	ext := dialExtension(t, a)
	waitOnline(t, a)
	sys.push(request("req-after", TypeSearch, map[string]any{"keyword": "go"}))
	down := ext.read(t)
	if down.Cmd != "search" {
		t.Fatalf("cmd=%q want search over the rebound endpoint", down.Cmd)
	}
	result, _ := json.Marshal(map[string]any{"results": []any{}})
	ext.reply(t, plugindevice.UpFrame{CorrelationID: down.CorrelationID, OK: true, Result: result})
	if _, ok := sys.waitReply(t, "req-after", 2*time.Second); !ok {
		t.Fatal("the rebound endpoint did not carry a request through")
	}
}

// A set that cannot bind must cost the operator nothing. This is the guarantee
// that makes the word safe to use on a live channel: the request fails, and the
// endpoint they were already using is exactly where they left it.
func TestListenSetFailureLeavesTheOldEndpointServing(t *testing.T) {
	a, sys := startActor(t, Config{})
	old := a.ListenAddr()
	ext := dialExtension(t, a)
	waitOnline(t, a)

	// An address this host cannot bind. 240.0.0.0/4 is reserved and assigned to
	// no interface, so listen() refuses it without depending on what else is
	// running on this machine.
	sys.push(request("set-bad", TypeListenSet, map[string]any{"listen_addr": "240.0.0.1:9"}))
	f, ok := sys.waitFail(t, "set-bad", 2*time.Second)
	if !ok {
		t.Fatal("a bind that cannot succeed was not reported as a failure")
	}
	if f.code != "bind_failed" {
		t.Fatalf("code=%q want bind_failed", f.code)
	}

	if a.ListenAddr() != old {
		t.Fatalf("listen addr moved to %q after a failed set; want it left at %q", a.ListenAddr(), old)
	}
	if !a.Online() {
		t.Fatal("the live plugin connection was dropped by a set that never took effect")
	}
	// And it still carries work.
	sys.push(request("req-old", TypeSearch, map[string]any{"keyword": "go"}))
	down := ext.read(t)
	result, _ := json.Marshal(map[string]any{"results": []any{}})
	ext.reply(t, plugindevice.UpFrame{CorrelationID: down.CorrelationID, OK: true, Result: result})
	if _, ok := sys.waitReply(t, "req-old", 2*time.Second); !ok {
		t.Fatal("the untouched endpoint stopped working after a failed set")
	}
}

// A wildcard bind is refused, and refused BEFORE anything moves. The endpoint
// has no key, so "listen everywhere" would hand it to every network this host
// happens to be on — including ones nobody thought about.
func TestListenSetRefusesAWildcardBind(t *testing.T) {
	a, sys := startActor(t, Config{})
	old := a.ListenAddr()

	for i, addr := range []string{"0.0.0.0:8090", "[::]:8090", ":8090"} {
		id := message.ID("set-wild-" + string(rune('a'+i)))
		sys.push(request(string(id), TypeListenSet, map[string]any{"listen_addr": addr}))
		f, ok := sys.waitFail(t, id, 2*time.Second)
		if !ok {
			t.Fatalf("%s was accepted; a keyless endpoint must not bind a wildcard", addr)
		}
		if f.code != "invalid_args" {
			t.Errorf("%s: code=%q want invalid_args", addr, f.code)
		}
	}
	if a.ListenAddr() != old {
		t.Fatalf("listen addr moved to %q; a refused set must change nothing", a.ListenAddr())
	}
}

// get answers about the endpoint itself, so it has to work when no plugin is
// attached — that is precisely the moment someone is trying to find out why.
func TestListenGetAnswersWhenNoPluginIsAttached(t *testing.T) {
	a, sys := startActor(t, Config{})

	sys.push(request("get-1", TypeListenGet, map[string]any{}))
	rep, ok := sys.waitReply(t, "get-1", 2*time.Second)
	if !ok {
		t.Fatal("no reply to xhs.listen.get")
	}
	got := decodeListen(t, rep.v)
	if got.ActualAddr != a.ListenAddr() {
		t.Errorf("actual_addr=%q want %q", got.ActualAddr, a.ListenAddr())
	}
	if got.Online {
		t.Error("online=true with nothing connected")
	}
	if !got.Loopback {
		t.Error("loopback=false for a 127.0.0.1 bind")
	}

	dialExtension(t, a)
	waitOnline(t, a)
	sys.push(request("get-2", TypeListenGet, map[string]any{}))
	rep, ok = sys.waitReply(t, "get-2", 2*time.Second)
	if !ok {
		t.Fatal("no second reply to xhs.listen.get")
	}
	if !decodeListen(t, rep.v).Online {
		t.Error("online=false with a plugin attached")
	}
}

// A set is a decision, and a restart must not quietly undo it: the next
// incarnation binds where the operator put the endpoint, not where the config
// default points.
func TestStoredListenAddrSurvivesRestart(t *testing.T) {
	// Built inline rather than through startActor because this test has to KILL
	// the first incarnation before the second starts: otherwise they fight over
	// the same port, which is a different story than the one under test.
	first := NewActor(Config{ListenAddr: "127.0.0.1:0", BindRetryInterval: 20 * time.Millisecond})
	sys := newFakeSys(DefaultActorID)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.run(sys) }()
	waitListening(t, first)

	sys.push(request("set-1", TypeListenSet, map[string]any{"listen_addr": "127.0.0.1:0"}))
	rep, ok := sys.waitReply(t, "set-1", 2*time.Second)
	sys.stop()
	<-firstDone
	if !ok {
		t.Fatal("no reply to xhs.listen.set")
	}
	chosen := decodeListen(t, rep.v).ActualAddr

	// A fresh incarnation over the SAME durable state — which is what a restart
	// is: a new term, not a new actor.
	next := NewActor(Config{ListenAddr: DefaultListenAddr, BindRetryInterval: 20 * time.Millisecond})
	nextSys := newFakeSys(DefaultActorID)
	nextSys.state = sys.state
	done := make(chan error, 1)
	go func() { done <- next.run(nextSys) }()
	t.Cleanup(func() {
		nextSys.stop()
		<-done
	})

	deadline := time.Now().Add(2 * time.Second)
	for next.ListenAddr() != chosen {
		select {
		case err := <-done:
			t.Fatalf("the restarted actor died instead of binding %s: %v", chosen, err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("restart bound %q (wanted %q), want the stored %q",
				next.ListenAddr(), next.Desired(), chosen)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A stored address that no longer validates must not take the actor down. An
// actor that refuses to start is far harder to recover than one listening at
// its default.
func TestUnusableStoredAddrFallsBackInsteadOfKillingTheActor(t *testing.T) {
	sys := newFakeSys(DefaultActorID)
	if _, err := sys.State().Put(plugindevice.StateKey, []byte("0.0.0.0:8090")); err != nil {
		t.Fatal(err)
	}
	a := NewActor(Config{ListenAddr: "127.0.0.1:0", BindRetryInterval: 20 * time.Millisecond})
	done := make(chan error, 1)
	go func() { done <- a.run(sys) }()
	t.Cleanup(func() {
		sys.stop()
		<-done
	})
	waitListening(t, a)
	if !plugindevice.IsLoopbackAddr(a.ListenAddr()) {
		t.Fatalf("fell back to %q, want the loopback default", a.ListenAddr())
	}
}
