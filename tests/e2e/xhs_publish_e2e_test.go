//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

var _ = time.Second // keep harness time import alive for sub-cases
var _ = context.TODO

// TestE2E_XHSPublish_AgentEmitsRequest covers phase-2 case 3 — the
// agent → adapter request leg.
//
// What this test pins (positive):
//   - The mock bridge's xhs-publish script correctly translates a
//     human.text trigger containing "publish" into a kind=request
//     envelope of type=xhs.publish addressed to tool:xhs,
//     authored as sender=agent:channel-agent.
//   - The harness chain steps 1-9 accept the request (no rejections),
//     so the channel-local sqlite carries both the trigger and the
//     request row in monotonic seq order.
//
// This test pins the agent-emit leg. The full device round trip is
// covered by adapter-focused e2e cases that register the device actor
// and attach a mock extension before sending the request.
func TestE2E_XHSPublish_AgentEmitsRequest(t *testing.T) {
	s := harness.Start(t, harness.Options{
		ExtraDaemonEnv: []string{
			"COAGENT_MOCK_SCRIPT=xhs-publish",
		},
	})

	email := "xhspub+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-xhspub-" + uniqSuffix())
	channelID := s.CreateChannel(wsID, "ch-xhs-"+uniqSuffix(), "xhs-creator")
	s.BindChannel(wsID, channelID)

	// Drive the agent: human POST mentioning "publish" triggers the
	// scripted xhs.publish emission inside the mock bridge.
	resp := s.PostMessage(channelID, "human.text",
		"请帮我 publish 一条标题为 hello、正文为 world 的笔记", "")
	if !resp.Accepted {
		t.Fatalf("trigger POST not accepted: %+v", resp)
	}

	// The channel sqlite must contain the xhs.publish request envelope
	// soon after the trigger lands. We're NOT asserting on the
	// downstream device round-trip (see header docstring) — only the
	// agent-emit leg.
	harness.Eventually(t, "xhs.publish request emitted", 15*time.Second, func() bool {
		for _, m := range s.ListChannelMessages(channelID) {
			if m.Type == "xhs.publish" && m.Kind == "request" {
				return true
			}
		}
		return false
	})

	all := s.ListChannelMessages(channelID)
	var req *harness.StoredMessage
	for i := range all {
		if all[i].Type == "xhs.publish" && all[i].Kind == "request" {
			req = &all[i]
			break
		}
	}
	if req == nil {
		t.Fatalf("xhs.publish request envelope missing — channel msgs: %d", len(all))
	}
	if req.SenderKind != "agent" || req.SenderID != "agent:channel-agent" {
		t.Errorf("xhs.publish sender=%s/%s want agent/agent:channel-agent",
			req.SenderKind, req.SenderID)
	}

	// Payload must carry the bridge's stamped title/content.
	var payload struct {
		Title   string `json:"title"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		t.Errorf("xhs.publish payload not json: %v (raw=%s)", err, string(req.Payload))
	}
	if payload.Title != "hello" || payload.Content != "world" {
		t.Errorf("xhs.publish payload mismatched: %+v", payload)
	}

	// Trigger row sanity (the human.text seq must precede the request).
	humans := filterMessages(all, "human.text", "")
	if len(humans) < 1 {
		t.Fatalf("human.text trigger missing")
	}
	if humans[0].Seq >= req.Seq {
		t.Errorf("trigger seq=%d not before request seq=%d", humans[0].Seq, req.Seq)
	}

	// Drain — keep ctx alive to avoid leaks.
	_ = context.Background
}

// filterMessages returns the rows matching type + kind. Empty kind
// matches any.
func filterMessages(all []harness.StoredMessage, envType, kind string) []harness.StoredMessage {
	out := []harness.StoredMessage{}
	for _, m := range all {
		if m.Type != envType {
			continue
		}
		if kind != "" && m.Kind != kind {
			continue
		}
		out = append(out, m)
	}
	return out
}
