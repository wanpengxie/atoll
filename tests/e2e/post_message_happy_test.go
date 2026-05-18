//go:build e2e

package e2e

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_PostMessage_HappyPath is the spine of every other e2e:
// register → workspace → channel → bind → POST message. Failure here
// almost always traces to a daemonbus wiring bug (write deadline,
// ack frame_id pairing, SendAndAwait timeout). Keep this test fast
// and noisy on failure.
func TestE2E_PostMessage_HappyPath(t *testing.T) {
	s := harness.Start(t, harness.Options{})

	email := "happy+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-happy-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-happy-"+uniqSuffix(), "")
	s.BindChannel(wsID, chID)

	start := time.Now()
	resp := s.PostMessage(chID, "human.text", "hi", "")
	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		t.Fatalf("POST /messages took %s, want < 5s — likely SendAndAwait stall", elapsed)
	}

	if resp.MessageID == "" {
		t.Fatalf("response missing message_id: %+v", resp)
	}
	if resp.Seq <= 0 {
		t.Fatalf("response seq=%d want > 0: %+v", resp.Seq, resp)
	}
	if !resp.Accepted {
		t.Fatalf("response accepted=%v want true: %+v", resp.Accepted, resp)
	}
	if resp.CorrelationID == "" {
		t.Fatalf("response missing correlation_id: %+v", resp)
	}
	if resp.RejectReason != "" {
		t.Fatalf("response carries reject_reason=%q: %+v", resp.RejectReason, resp)
	}

	// Channel sqlite must now have exactly one human.text envelope.
	// Use Eventually because the daemon writes async after acking the
	// frame, even though the harness contract is "ack means committed".
	harness.Eventually(t, "human.text in channel sqlite", 3*time.Second, func() bool {
		msgs := s.ListChannelMessages(chID)
		for _, m := range msgs {
			if m.Type == "human.text" {
				return true
			}
		}
		return false
	})

	msgs := s.ListChannelMessages(chID)
	var human *harness.StoredMessage
	for i := range msgs {
		if msgs[i].Type == "human.text" {
			human = &msgs[i]
			break
		}
	}
	if human == nil {
		t.Fatalf("no human.text row in channel sqlite (count=%d)", len(msgs))
	}
	if human.SenderKind != "human" {
		t.Errorf("human.text sender_kind=%q want human", human.SenderKind)
	}
	if !strings.HasPrefix(human.SenderID, "user:") {
		t.Errorf("human.text sender_id=%q want user:* prefix", human.SenderID)
	}
	var payload struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(human.Payload, &payload); err != nil {
		t.Fatalf("human.text payload not json: %v (raw=%s)", err, string(human.Payload))
	}
	if payload.Text != "hi" {
		t.Errorf("human.text payload.text=%q want hi", payload.Text)
	}
}
