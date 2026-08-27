package plugindevice

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
)

// A gorilla connection takes ONE writer at a time, and this transport has three
// goroutines that write: the worker dispatches, the read loop answers a
// handshake, and the beat loop sends the protocol's heartbeat. That was a single
// writer once, and the discipline that replaced it is invisible — nothing about
// adding a fourth write site tells you it has to be serialised. So it is pinned
// here, under -race, where breaking it fails rather than corrupts a frame in
// production.
//
// beatProtocol beats every millisecond so the three writers genuinely overlap
// within a test's lifetime.
type beatProtocol struct{ WebbridgeProtocol }

func (beatProtocol) Heartbeat() (time.Duration, []byte, bool) {
	return time.Millisecond, []byte(`{"type":"ping"}`), true
}

func TestEveryOutboundFrameIsSerialised(t *testing.T) {
	dev := New(Deps{
		Tool:       "test",
		Sys:        func() actorbase.Sys { return nil },
		Protocol:   beatProtocol{},
		OnPresence: func(bool) {},
	})
	srv := httptest.NewServer(http.HandlerFunc(dev.handleAccept))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { _ = dev.Stop(t.Context()) })

	// handleAccept reads d.desired to decide keepalive; give it the server's own
	// address so the test exercises the same branch a real bind would.
	dev.mu.Lock()
	dev.desired = srv.Listener.Addr().String()
	dev.mu.Unlock()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+srv.URL[len("http"):], nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// Drain whatever arrives; the assertion is that -race sees no torn write and
	// that every frame is complete JSON, which a corrupted interleave would not
	// be.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var probe map[string]any
			if err := json.Unmarshal(raw, &probe); err != nil {
				t.Errorf("received a frame that is not whole JSON (%v): %s", err, raw)
				return
			}
		}
	}()

	// The read loop writes hello_ack while the beat loop is already ticking.
	if err := conn.WriteJSON(map[string]any{
		"type": "hello", "payload": map[string]any{"extensionVersion": "test"},
	}); err != nil {
		t.Fatal(err)
	}

	// And the worker dispatches concurrently with both.
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg := actorbase.NewMsg(actorbase.OriginMailbox, t.Context(), message.Envelope{
				ID:      message.ID(fmt.Sprintf("req-%d", i)),
				Kind:    message.KindRequest,
				Type:    "test.command",
				Payload: json.RawMessage(`{"body":{"k":"v"}}`),
			})
			_ = dev.Dispatch(msg, Spec{Cmd: "noop", Deadline: time.Minute}, json.RawMessage(`{"k":"v"}`))
		}(i)
	}
	wg.Wait()
	time.Sleep(20 * time.Millisecond)
	_ = conn.Close()
	<-done
}
