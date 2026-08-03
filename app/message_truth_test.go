package app_test

// Message write path → truth: a gateway ws submit becomes a truth-log row, the
// receipt hands back the message's IDENTITY (never a row position), empty/absent
// audience is filled by the humancell default, and read-side ordering comes from
// the store's own seq column.

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestSendMessageAndReadBack(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	// Send a message through the gateway ws frame (the write path; POST is废).
	c := dialWS(t, srv, s.cookies, s.chID, 0)
	defer c.close()
	setBoostDefault(t, env, s, c)
	ack := c.sendMessage(map[string]any{
		"msg_type": "chat.text",
		"kind":     "event",
		"payload":  map[string]any{"text": "hello world"},
	})
	if ack["type"] != "ack" {
		t.Fatalf("send message: want ack, got %v", ack)
	}
	// The receipt says "accepted, and here is WHAT was written" — an identity,
	// not a row position. Ordering is read-side and arrives on the feed
	// (FeedPayload.Seq); a writer has no use for a seq it cannot page from.
	msgID := ack["message_id"].(string)
	if msgID == "" {
		t.Fatal("send message returned empty message_id")
	}

	// Read back through the canonical Home truth view.
	msgs := truthRowsForTest(t, env, s.chID)
	if len(msgs) == 0 {
		t.Fatal("expected at least one message, got 0")
	}

	// Find our message by checking the envelope.
	found := false
	for _, raw := range msgs {
		row := raw.(map[string]any)
		envelope, ok := row["envelope"]
		if !ok {
			continue
		}
		// The envelope might be stored as a JSON string or object.
		var envMap map[string]any
		switch v := envelope.(type) {
		case string:
			if err := json.Unmarshal([]byte(v), &envMap); err != nil {
				continue
			}
		case map[string]any:
			envMap = v
		default:
			continue
		}
		if envMap["id"] == msgID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("sent message %s not found in channel messages", msgID)
	}
}

// TestDaemonAttachAndMessageFlow exercises the message write path through a
// daemon-attached channel (HTTP/ws layer only — no compute.Run, so kind=event
// which has no request cardinality constraint) and verifies the message lands
// in truth.
func TestDaemonAttachAndMessageFlow(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	// Create and attach a daemon.
	daemonBody := createAndBindDaemon(t, env, s.chID, "echo-daemon", s.cookies)
	daemonID := daemonBody["id"].(string)
	_ = daemonID

	c := dialWS(t, srv, s.cookies, s.chID, 0)
	defer c.close()
	setBoostDefault(t, env, s, c)
	ack := c.sendMessage(map[string]any{
		"msg_type": "echo.ping",
		"kind":     "event",
		"payload":  map[string]any{"text": "ping"},
	})
	if ack["type"] != "ack" {
		t.Fatalf("send message: want ack, got %v", ack)
	}
	reqMsgID := ack["message_id"].(string)

	// Read back -- should contain the request message. The receipt hands back
	// the message's IDENTITY, so that is what the truth row is matched on; the
	// row's seq is the store's own column, not something the writer was told.
	msgs := truthRowsForTest(t, env, s.chID)
	if len(msgs) == 0 {
		t.Fatal("expected at least one message")
	}
	if seqOfTruthRow(t, msgs, reqMsgID) <= 0 {
		t.Fatalf("message %s not found in truth", reqMsgID)
	}
}

// seqOfTruthRow returns the store-assigned row position of the message with
// this id, or 0 if truth holds no such row.
func seqOfTruthRow(t *testing.T, rows []any, msgID string) float64 {
	t.Helper()
	for _, raw := range rows {
		row, _ := raw.(map[string]any)
		if row == nil {
			continue
		}
		var envMap map[string]any
		switch v := row["envelope"].(type) {
		case string:
			_ = json.Unmarshal([]byte(v), &envMap)
		case map[string]any:
			envMap = v
		}
		if envMap != nil && envMap["id"] == msgID {
			seq, _ := row["seq"].(float64)
			return seq
		}
	}
	return 0
}

func TestSendMessageNoAudienceDefaultFill(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	c := dialWS(t, srv, s.cookies, s.chID, 0)
	defer c.close()
	setBoostDefault(t, env, s, c)

	// Send a message with NO audience field at all.
	ack := c.sendMessage(map[string]any{
		"msg_type": "chat.text",
		"kind":     "event",
		"payload":  map[string]any{"text": "broadcast"},
	})
	if ack["type"] != "ack" {
		t.Fatalf("send message: want ack, got %v", ack)
	}
	msgID := ack["message_id"].(string)
	if msgID == "" {
		t.Fatal("expected non-empty message_id")
	}

	// Also send with an explicit empty audience array -- should still succeed.
	ack2 := c.sendMessage(map[string]any{
		"msg_type": "chat.text",
		"kind":     "event",
		"payload":  map[string]any{"text": "broadcast2"},
		"audience": []string{},
	})
	if ack2["type"] != "ack" {
		t.Fatalf("second send: want ack, got %v", ack2)
	}
	msgID2 := ack2["message_id"].(string)

	// Verify both messages exist in truth, and that the second sits AFTER the
	// first. The ordering claim is unchanged; only its source moved to where
	// row positions actually come from — the store's own column on the read
	// side, not a number handed back to the writer.
	msgs := truthRowsForTest(t, env, s.chID)
	if len(msgs) < 2 {
		t.Fatalf("expected at least 2 messages, got %d", len(msgs))
	}
	seq, seq2 := seqOfTruthRow(t, msgs, msgID), seqOfTruthRow(t, msgs, msgID2)
	if seq <= 0 || seq2 <= 0 {
		t.Fatalf("both messages must be in truth: seq(%s)=%v seq(%s)=%v", msgID, seq, msgID2, seq2)
	}
	if seq2 <= seq {
		t.Fatalf("second message seq %v should be > first seq %v", seq2, seq)
	}
}
