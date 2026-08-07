package app_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// TestRouting_DefaultAgentReady uses no instance-discovery test helper. It
// drives creation, source-key setting, removal, actor.list discovery,
// instance-key resetting, and unaddressed delivery through public frames.
func TestRouting_DefaultAgentReady(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)

	reg, cookies := register(t, env, "routing@example.com", "secret123", "Routing")
	_, loginCookies := login(t, env, "routing@example.com", "secret123")
	cookies = mergeCookies(cookies, loginCookies)
	created, cookies := createChannel(t, env, cookies, "routing")
	s := setupResult{
		cookies: cookies, userID: reg["id"].(string), chID: created["id"].(string),
	}

	c := dialWS(t, srv, s.cookies, s.chID, 0)
	defer c.close()

	unset := c.sendMessage(map[string]any{
		"msg_type": "chat.text", "payload": map[string]any{"text": "before set"},
	})
	if unset["type"] != "error" || unset["error"] != "routing_unavailable" {
		t.Fatalf("created channel must be Unset, got %v", unset)
	}

	boost := setBoostDefault(t, env, s, c)
	ack := c.sendMessage(map[string]any{
		"msg_type": "chat.text", "payload": map[string]any{"text": "to boost"},
	})
	if ack["type"] != "ack" {
		t.Fatalf("default submit=%v", ack)
	}
	var boostReply struct {
		Status string `json:"status"`
		Text   string `json:"text"`
	}
	raw := waitForResponse(t, env, s, ack["message_id"].(string), 3*time.Second)
	if err := json.Unmarshal(raw, &boostReply); err != nil ||
		boostReply.Status != string(message.StatusCompleted) || boostReply.Text != "stub-ok" {
		t.Fatalf("boost reply=%s decoded=%+v err=%v", raw, boostReply, err)
	}

	remove := c.sendMessage(map[string]any{
		"msg_type":   "channel.remove_actor",
		"kind":       "request",
		"audience":   []string{string(actor.SystemActorID)},
		"visibility": "public",
		"payload":    map[string]any{"instance_id": boost},
	})
	if remove["type"] != "ack" {
		t.Fatalf("remove submit=%v", remove)
	}
	_ = waitForResponse(t, env, s, remove["message_id"].(string), 3*time.Second)
	dangling := c.sendMessage(map[string]any{
		"msg_type": "chat.text", "payload": map[string]any{"text": "after end"},
	})
	if dangling["type"] != "error" || dangling["error"] != "routing_unavailable" {
		t.Fatalf("dangling default must ask for reset, got %v", dangling)
	}

	list := c.sendMessage(map[string]any{
		"msg_type": introspect.QueryList, "kind": "request",
		"audience": []string{string(actor.SystemActorID)}, "payload": map[string]any{},
	})
	if list["type"] != "ack" {
		t.Fatalf("actor.list submit=%v", list)
	}
	var catalog struct {
		Actors []introspect.CatalogEntry `json:"actors"`
	}
	raw = waitForResponse(t, env, s, list["message_id"].(string), 3*time.Second)
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatalf("actor.list response=%s err=%v", raw, err)
	}
	var human actor.ActorID
	for _, entry := range catalog.Actors {
		if entry.Kind == string(actor.KindHuman) {
			human = actor.ActorID(entry.ID)
			break
		}
	}
	if human == "" {
		t.Fatalf("actor.list contains no human: %+v", catalog.Actors)
	}

	reset := c.sendMessage(map[string]any{
		"msg_type":   "channel.set_default_agent",
		"kind":       "request",
		"audience":   []string{string(actor.SystemActorID)},
		"visibility": "public",
		"payload":    map[string]any{"instance_id": human},
	})
	if reset["type"] != "ack" {
		t.Fatalf("instance-key reset submit=%v", reset)
	}
	_ = waitForResponse(t, env, s, reset["message_id"].(string), 3*time.Second)

	recovered := c.sendMessage(map[string]any{
		"msg_type": "human.message", "payload": map[string]any{"text": "recovered"},
	})
	if recovered["type"] != "ack" {
		t.Fatalf("recovered submit=%v", recovered)
	}
	raw = waitForResponse(t, env, s, recovered["message_id"].(string), 3*time.Second)
	var delivered struct {
		Status    string `json:"status"`
		Delivered bool   `json:"delivered"`
	}
	if err := json.Unmarshal(raw, &delivered); err != nil ||
		delivered.Status != string(message.StatusCompleted) || !delivered.Delivered {
		t.Fatalf("human reply=%s decoded=%+v err=%v", raw, delivered, err)
	}
}
