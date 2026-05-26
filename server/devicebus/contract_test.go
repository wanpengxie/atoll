package devicebus

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	proxycontract "github.com/wanpengxie/ActOS/internal/proxy/contract"
)

func TestProxyDaemonV2ContractRoundTrip(t *testing.T) {
	t.Parallel()

	const apiKey = "dk_test_contract"
	readySeen := make(chan proxycontract.DeviceFrameV2, 1)
	handlerErr := make(chan error, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != proxycontract.WSPathV2 {
			http.NotFound(w, r)
			return
		}
		if got := r.URL.Query().Get(proxycontract.QueryParamApiKey); got != apiKey {
			http.Error(w, "invalid api key", http.StatusUnauthorized)
			return
		}
		upgrader := websocket.Upgrader{
			CheckOrigin:  func(*http.Request) bool { return true },
			Subprotocols: []string{proxycontract.WSSubprotocolV2},
		}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			handlerErr <- err
			return
		}
		defer func() { _ = ws.Close() }()

		var ready proxycontract.DeviceFrameV2
		if err := ws.ReadJSON(&ready); err != nil {
			handlerErr <- err
			return
		}
		readySeen <- ready

		ack := proxycontract.DeviceFrameV2{
			Direction: "to_device",
			Payload:   json.RawMessage(`{"status":"ready_received"}`),
		}
		if err := ws.WriteJSON(ack); err != nil {
			handlerErr <- err
			return
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") +
		proxycontract.WSPathV2 + "?" + url.Values{proxycontract.QueryParamApiKey: {apiKey}}.Encode()
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second
	dialer.Subprotocols = []string{proxycontract.WSSubprotocolV2}

	conn, resp, err := dialer.Dial(wsURL, nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial proxy daemon v2 contract ws status=%d: %v", status, err)
	}
	defer func() { _ = conn.Close() }()
	if got := conn.Subprotocol(); got != proxycontract.WSSubprotocolV2 {
		t.Fatalf("selected subprotocol = %q, want %q", got, proxycontract.WSSubprotocolV2)
	}

	ready := proxycontract.DeviceFrameV2{
		Direction:    "from_device",
		FrameType:    proxycontract.FrameTypeReady,
		Hostname:     "contract-host",
		HostLabel:    "Contract Host",
		ProxyVersion: "0.1.0",
		Actors: []proxycontract.ReadyActorV2{
			{
				ActorID:       "tool:kimi",
				CapabilitySet: json.RawMessage(`{"types":["kimi.ask"]}`),
			},
		},
	}
	if err := conn.WriteJSON(ready); err != nil {
		t.Fatalf("write ready frame: %v", err)
	}

	var ack proxycontract.DeviceFrameV2
	if err := conn.ReadJSON(&ack); err != nil {
		t.Fatalf("read test ack frame: %v", err)
	}
	if ack.FrameType != "" {
		t.Fatalf("ready ack frame_type = %q, want empty non-reserved response", ack.FrameType)
	}
	if string(ack.Payload) != `{"status":"ready_received"}` {
		t.Fatalf("ack payload = %s", ack.Payload)
	}

	select {
	case err := <-handlerErr:
		t.Fatalf("handler error: %v", err)
	case got := <-readySeen:
		if got.FrameType != proxycontract.FrameTypeReady {
			t.Fatalf("ready frame_type = %q, want %q", got.FrameType, proxycontract.FrameTypeReady)
		}
		if len(got.Actors) != 1 || got.Actors[0].ActorID != "tool:kimi" {
			t.Fatalf("ready actors = %+v", got.Actors)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not observe ready frame")
	}
}
