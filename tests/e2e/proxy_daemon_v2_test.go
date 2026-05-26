//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	proxycontract "github.com/wanpengxie/ActOS/internal/proxy/contract"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/pkg/coagentsdk"
	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

const proxyEchoActorID = "tool:proxy-echo"

type proxyDaemonResp struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	APIKeyPrefix string `json:"api_key_prefix"`
	APIKey       string `json:"apiKey,omitempty"`
	Status       string `json:"status"`
}

type proxyDeviceFrame struct {
	Direction     string                       `json:"direction"`
	FrameType     proxycontract.FrameType      `json:"frame_type,omitempty"`
	ActorID       string                       `json:"actor_id"`
	ChannelID     string                       `json:"channel_id"`
	RequestID     string                       `json:"request_id,omitempty"`
	ParentID      string                       `json:"parent_id,omitempty"`
	CorrelationID string                       `json:"correlation_id,omitempty"`
	Payload       json.RawMessage              `json:"payload,omitempty"`
	ExpiresAt     int64                        `json:"expires_at,omitempty"`
	Hostname      string                       `json:"hostname,omitempty"`
	HostLabel     string                       `json:"host_label,omitempty"`
	Actors        []proxycontract.ReadyActorV2 `json:"actors,omitempty"`
	ProxyVersion  string                       `json:"proxy_version,omitempty"`
}

func TestE2E_ProxyDaemonV2_FullRoundTrip(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	email := "proxyv2+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-proxyv2-" + uniqSuffix())
	channelID := s.CreateChannel(wsID, "ch-proxyv2-"+uniqSuffix(), "group")
	s.BindChannel(wsID, channelID)

	client := &coagentsdk.Client{
		BaseURL:      s.ServerURLBase(),
		SessionToken: s.SessionToken(),
	}
	daemon := createProxyDaemon(t, s, channelID, "Mock Proxy "+uniqSuffix())
	if daemon.APIKey == "" || daemon.APIKeyPrefix == "" || daemon.Status != "offline" {
		t.Fatalf("created daemon missing one-shot key or baseline fields: %+v", daemon)
	}
	assertListDaemonsHidesAPIKey(t, s, channelID, daemon.ID)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn := dialProxyDaemonV2(t, ctx, s, daemon.APIKey)
	defer func() { _ = conn.Close() }()

	if err := conn.WriteJSON(proxyDeviceFrame{
		Direction:    "from_device",
		FrameType:    proxycontract.FrameTypeReady,
		Hostname:     "proxy-host-e2e",
		HostLabel:    "Proxy Host E2E",
		ProxyVersion: "e2e-proxy/0.1.0",
		Actors: []proxycontract.ReadyActorV2{
			{
				ActorID: proxyEchoActorID,
				CapabilitySet: json.RawMessage(`{
					"name":"proxy-echo",
					"description":"E2E proxy echo actor",
					"types":["proxy.echo"],
					"type_declarations":{"proxy.echo":{
						"AllowedKinds":["request","response"],
						"TerminalConvention":"payload_status",
						"Description":"Echo a payload through the proxy daemon transport"
					}},
					"max_pending_ms":30000
				}`),
			},
			{
				ActorID: "tool:proxy-metadata",
				CapabilitySet: json.RawMessage(`{
					"name":"proxy-metadata",
					"description":"Second ready actor for multi-actor projection",
					"types":["proxy.metadata"],
					"max_pending_ms":30000
				}`),
			},
		},
	}); err != nil {
		t.Fatalf("write ready: %v", err)
	}

	beforeReady := waitSDKActor(t, client, channelID, proxyEchoActorID, 10*time.Second, func(a coagentsdk.ActorInfo) bool {
		return actorInfoHasType(a, "proxy.echo")
	})
	if beforeReady.Ready {
		t.Fatalf("proxy actor became ready before actor.readiness.changed event: %+v", beforeReady)
	}
	if beforeReady.DaemonID != daemon.ID || beforeReady.DaemonName != daemon.Name {
		t.Fatalf("proxy actor daemon projection=%+v want id=%s name=%s", beforeReady, daemon.ID, daemon.Name)
	}
	_ = waitSDKActor(t, client, channelID, "tool:proxy-metadata", 10*time.Second, func(a coagentsdk.ActorInfo) bool {
		return actorInfoHasType(a, "proxy.metadata")
	})

	now := time.Now().UnixMilli()
	readiness := message.Envelope{
		ID:        message.ID("evt-proxy-ready-" + uniqSuffix()),
		TS:        now,
		ChannelID: channel.ID(channelID),
		Sender:    message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:      message.KindEvent,
		Type:      "actor.readiness.changed",
		Payload: json.RawMessage(fmt.Sprintf(`{
			"actor_id":%q,
			"changed_at":%d,
			"current":{
				"ready":true,
				"reason":"proxy_ready",
				"detail":{"transport":"devicebus.v2"},
				"last_ready_at":%d,
				"last_state_change_at":%d
			}
		}`, proxyEchoActorID, now, now, now)),
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{actor.SystemActorID},
	}
	if err := writeProxyEnvelope(t, conn, proxyEchoActorID, readiness); err != nil {
		t.Fatalf("write readiness event: %v", err)
	}
	readyActor := waitSDKActor(t, client, channelID, proxyEchoActorID, 10*time.Second, func(a coagentsdk.ActorInfo) bool {
		return a.Ready && a.ReadyReason == "proxy_ready" && actorInfoHasType(a, "proxy.echo")
	})
	if readyActor.LastReadyAt == 0 || readyActor.LastStateChangeAt == 0 {
		t.Fatalf("readiness timestamps not projected: %+v", readyActor)
	}

	status, err := client.ActorStatus(ctx, channelID, proxyEchoActorID)
	if err != nil {
		t.Fatalf("ActorStatus: %v", err)
	}
	if !status.Available || status.Reason != "proxy_ready" || status.Binding != string(actor.BindingRuntimeInboundViaRelay) {
		t.Fatalf("ActorStatus=%+v", status)
	}
	describe, err := client.DescribeActor(ctx, channelID, proxyEchoActorID)
	if err != nil {
		t.Fatalf("DescribeActor: %v", err)
	}
	if describe.ActorID != proxyEchoActorID || describe.Binding != string(actor.BindingRuntimeInboundViaRelay) {
		t.Fatalf("DescribeActor=%+v", describe)
	}
	if _, ok := describe.Types["proxy.echo"]; !ok {
		t.Fatalf("DescribeActor types missing proxy.echo: %+v", describe.Types)
	}

	callDone := make(chan *coagentsdk.CallActorResult, 1)
	callErr := make(chan error, 1)
	go func() {
		res, err := client.CallActor(ctx, coagentsdk.CallActorRequest{
			ChannelID: channelID,
			ActorID:   proxyEchoActorID,
			Type:      "proxy.echo",
			Payload:   json.RawMessage(`{"prompt":"ping"}`),
			Timeout:   10 * time.Second,
		})
		if err != nil {
			callErr <- err
			return
		}
		callDone <- res
	}()

	reqFrame := readProxyBusinessFrame(t, conn, proxyEchoActorID)
	var reqEnv message.Envelope
	if err := json.Unmarshal(reqFrame.Payload, &reqEnv); err != nil {
		t.Fatalf("decode proxied request envelope: %v raw=%s", err, string(reqFrame.Payload))
	}
	if reqEnv.Type != "proxy.echo" || reqEnv.Kind != message.KindRequest ||
		len(reqEnv.Audience) != 1 || reqEnv.Audience[0] != actor.ActorID(proxyEchoActorID) {
		t.Fatalf("proxied request envelope=%+v", reqEnv)
	}
	if !bytes.Contains(reqEnv.Payload, []byte(`"prompt":"ping"`)) {
		t.Fatalf("proxied request payload=%s", string(reqEnv.Payload))
	}
	if err := writeProxyEnvelope(t, conn, proxyEchoActorID, message.Envelope{
		ID:            message.ID("resp-proxy-echo-" + uniqSuffix()),
		TS:            time.Now().UnixMilli(),
		ChannelID:     reqEnv.ChannelID,
		Sender:        message.Sender{Kind: actor.KindTool, ID: actor.ActorID(proxyEchoActorID)},
		Kind:          message.KindResponse,
		Type:          reqEnv.Type,
		ParentID:      reqEnv.ID,
		CorrelationID: reqEnv.ID,
		Payload:       json.RawMessage(`{"status":"completed","echo":"ping","via":"devicebus.v2"}`),
		Visibility:    message.VisibilityPublic,
		Audience:      message.Audience{reqEnv.Sender.ID},
	}); err != nil {
		t.Fatalf("write proxy response: %v", err)
	}

	select {
	case err := <-callErr:
		t.Fatalf("CallActor: %v", err)
	case res := <-callDone:
		if res == nil || !res.OK {
			t.Fatalf("CallActor result=%+v", res)
		}
		var data struct {
			Echo string `json:"echo"`
			Via  string `json:"via"`
		}
		if err := json.Unmarshal(res.Data, &data); err != nil {
			t.Fatalf("decode CallActor data: %v raw=%s", err, string(res.Data))
		}
		if data.Echo != "ping" || data.Via != "devicebus.v2" {
			t.Fatalf("CallActor data=%+v raw=%s", data, string(res.Data))
		}
	case <-ctx.Done():
		t.Fatalf("CallActor did not finish: %v", ctx.Err())
	}
}

func createProxyDaemon(t *testing.T, s *harness.Stack, channelID, name string) proxyDaemonResp {
	t.Helper()
	var out proxyDaemonResp
	doE2EJSON(t, s.Client(), http.MethodPost, s.ServerURLBase()+"/api/channels/"+url.PathEscape(channelID)+"/daemons",
		map[string]any{"name": name}, http.StatusCreated, &out)
	return out
}

func assertListDaemonsHidesAPIKey(t *testing.T, s *harness.Stack, channelID, daemonID string) {
	t.Helper()
	var out struct {
		Daemons []proxyDaemonResp `json:"daemons"`
	}
	doE2EJSON(t, s.Client(), http.MethodGet, s.ServerURLBase()+"/api/channels/"+url.PathEscape(channelID)+"/daemons",
		nil, http.StatusOK, &out)
	for _, d := range out.Daemons {
		if d.ID != daemonID {
			continue
		}
		if d.APIKey != "" {
			t.Fatalf("list daemons leaked one-shot api key: %+v", d)
		}
		return
	}
	t.Fatalf("created daemon %s missing from list: %+v", daemonID, out.Daemons)
}

func doE2EJSON(t *testing.T, client *http.Client, method, endpoint string, body any, wantStatus int, out any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, endpoint, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s status=%d want=%d body=%s", method, endpoint, resp.StatusCode, wantStatus, string(raw))
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("decode response: %v raw=%s", err, string(raw))
		}
	}
}

func dialProxyDaemonV2(t *testing.T, ctx context.Context, s *harness.Stack, apiKey string) *websocket.Conn {
	t.Helper()
	u, err := url.Parse(s.ServerURLBase())
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	u.Scheme = "ws"
	u.Path = proxycontract.WSPathV2
	u.RawQuery = url.Values{proxycontract.QueryParamApiKey: {apiKey}}.Encode()
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second
	dialer.Subprotocols = []string{proxycontract.WSSubprotocolV2}
	conn, resp, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial proxy daemon v2 status=%d: %v", status, err)
	}
	if got := conn.Subprotocol(); got != proxycontract.WSSubprotocolV2 {
		t.Fatalf("proxy daemon subprotocol=%q want %q", got, proxycontract.WSSubprotocolV2)
	}
	return conn
}

func writeProxyEnvelope(t *testing.T, conn *websocket.Conn, routeActorID string, env message.Envelope) error {
	t.Helper()
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return conn.WriteJSON(proxyDeviceFrame{
		Direction: "from_device",
		ActorID:   routeActorID,
		Payload:   raw,
	})
}

func readProxyBusinessFrame(t *testing.T, conn *websocket.Conn, actorID string) proxyDeviceFrame {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		var frame proxyDeviceFrame
		if err := conn.ReadJSON(&frame); err != nil {
			if strings.Contains(err.Error(), "i/o timeout") {
				continue
			}
			t.Fatalf("read proxy frame: %v", err)
		}
		if frame.FrameType == "" && frame.ActorID == actorID && len(frame.Payload) > 0 {
			return frame
		}
	}
	t.Fatalf("proxy business frame for %s not received", actorID)
	return proxyDeviceFrame{}
}

func actorInfoHasType(info coagentsdk.ActorInfo, typeName string) bool {
	for _, typ := range info.Types {
		if typ.Type == typeName {
			return true
		}
	}
	return false
}
