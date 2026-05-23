//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_DaemonRestartMidXHSPublishRequest_RecoverPending covers the
// adapter durability path with real server/daemon/worker binaries:
//
//  1. xhs.publish request reaches the production device adapter.
//  2. The adapter reserves a pending correlation and sends a device
//     command.
//  3. The daemon restarts before the device callback is released.
//  4. The restarted daemon boots the same channel sqlite, recovers the
//     pending correlation, and accepts the delayed callback.
func TestE2E_DaemonRestartMidXHSPublishRequest_RecoverPending(t *testing.T) {
	s := harness.Start(t, harness.Options{
		DeviceAllowedOrigins: []string{harness.MockExtensionOriginID},
	})

	email := "xhsrestart+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-xhsrestart-" + uniqSuffix())
	channelID := s.CreateChannel(wsID, "ch-xhs-"+uniqSuffix(), "xhs-creator")
	s.BindChannel(wsID, channelID)

	placement := harness.EventuallyValue(t, "placement reaches active", 5*time.Second, func() (harness.PlacementRow, bool) {
		p, ok := s.GetPlacement(channelID)
		return p, ok && p.State == "active"
	})

	deviceID := "device-" + uniqSuffix()
	issued := s.IssueDeviceSession(channelID, deviceID, placement.DaemonID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCallback := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseCallback()

	commandSeen := make(chan harness.CommandPayload, 1)
	_ = harness.NewMockExtension(t, ctx, harness.MockExtensionConfig{
		WSURL:     s.DevicebusWSURL(issued.DeviceSessionID, issued.Token),
		SessionID: issued.DeviceSessionID,
		Token:     issued.Token,
		ChannelID: channelID,
		DeviceID:  deviceID,
		OnCommand: func(cmd harness.CommandPayload) harness.CallbackPayload {
			select {
			case commandSeen <- cmd:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return harness.CallbackPayload{
				CorrelationID: cmd.CorrelationID,
				Status:        "ok",
				DeviceID:      deviceID,
				Result: map[string]any{
					"note_id": "note-recovered",
					"url":     "https://www.xiaohongshu.com/explore/note-recovered",
				},
			}
		},
	})

	harness.Eventually(t, "device session active", 5*time.Second, func() bool {
		row, ok := s.GetDeviceSession(issued.DeviceSessionID)
		return ok && row.State == "active"
	})

	req := postRawMessage(t, s, channelID, map[string]any{
		"id":       "msg-adapter-restart-1",
		"type":     "xhs.publish",
		"kind":     "request",
		"audience": []string{"tool:xhs-adapter"},
		"payload": map[string]any{
			"device_session_id": issued.DeviceSessionID,
			"title":             "restart recovery",
			"content":           "callback after daemon restart",
		},
	})
	if !req.Accepted || req.MessageID == "" {
		t.Fatalf("xhs.publish request not accepted: %+v", req)
	}

	var cmd harness.CommandPayload
	select {
	case cmd = <-commandSeen:
	case <-time.After(10 * time.Second):
		t.Fatal("mock extension did not receive xhs.publish command")
	}
	if cmd.Cmd != "publish" {
		t.Fatalf("command cmd=%q want publish", cmd.Cmd)
	}
	if cmd.CorrelationID != req.MessageID {
		t.Fatalf("command correlation_id=%q want request id %q", cmd.CorrelationID, req.MessageID)
	}

	hbBefore := readDaemonHeartbeat(t, s, "daemon-e2e")
	s.RestartDaemon()
	harness.Eventually(t, "daemon heartbeat advances after restart", 30*time.Second, func() bool {
		return readDaemonHeartbeat(t, s, "daemon-e2e") > hbBefore
	})
	if !waitPlacementActive(t, s, channelID, 15*time.Second) {
		t.Fatalf("placement for channel %s never returned active after restart", channelID)
	}
	ready := s.PostMessage(channelID, "human.text", "after-restart-ready", "")
	if !ready.Accepted {
		t.Fatalf("post-restart readiness write rejected: %+v", ready)
	}

	releaseCallback()

	harness.Eventually(t, "xhs.publish response after daemon restart", 20*time.Second, func() bool {
		for _, m := range s.ListChannelMessages(channelID) {
			if m.Type == "xhs.publish" && m.Kind == "response" && m.SenderID == "tool:xhs-adapter" {
				return true
			}
		}
		return false
	})
}

func postRawMessage(t *testing.T, s *harness.Stack, channelID string, body map[string]any) harness.PostMessageResponse {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal raw message: %v", err)
	}
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodPost,
		s.ServerURLBase()+"/api/channels/"+channelID+"/messages",
		bytes.NewReader(raw),
	)
	if err != nil {
		t.Fatalf("build raw message request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.Client().Do(req)
	if err != nil {
		t.Fatalf("post raw message: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post raw message status=%d body=%s", resp.StatusCode, string(respBody))
	}
	var out harness.PostMessageResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		t.Fatalf("decode raw message response: %v body=%s", err, string(respBody))
	}
	return out
}
