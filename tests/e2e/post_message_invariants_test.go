//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_PostMessage_Dedupe_IdempotentRetry covers launch-checklist §1.1
// invariant: 同 message id 重 POST 同 envelope 命中 harness Step 3 dedupe,
// 第二次返回 Deduped=true + 相同 message_id / seq, 不写入新 row.
//
// Path verified: HTTP POST → daemon harness Step 3 LookupCanonicalHash
// hit + same hash → short-circuit with original row's seq.
func TestE2E_PostMessage_Dedupe_IdempotentRetry(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	email := "dedupe+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-dedupe-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-dedupe-"+uniqSuffix(), "")
	s.BindChannel(wsID, chID)

	fixedID := "msg-dedupe-fixed-" + uniqSuffix()
	// Idempotent retry requires the caller to resend the SAME envelope —
	// including ts (part of L1 §2.3 canonical_hash domain). We fix ts here
	// so both POSTs hash identically.
	fixedTS := time.Now().UnixMilli()

	first := postMessageRaw(t, s, chID, map[string]any{
		"id":      fixedID,
		"type":    "human.text",
		"payload": json.RawMessage(`{"text":"hello"}`),
		"ts":      fixedTS,
		"audience": []string{"agent:channel-agent"},
	}, http.StatusOK)
	if !first.Accepted {
		t.Fatalf("first POST not accepted: %+v", first)
	}
	if first.Deduped {
		t.Fatalf("first POST should not be deduped: %+v", first)
	}
	if first.MessageID != fixedID {
		t.Errorf("first.MessageID=%q want %q", first.MessageID, fixedID)
	}
	if first.Seq <= 0 {
		t.Errorf("first.Seq=%d want > 0", first.Seq)
	}

	second := postMessageRaw(t, s, chID, map[string]any{
		"id":      fixedID,
		"type":    "human.text",
		"payload": json.RawMessage(`{"text":"hello"}`),
		"ts":      fixedTS,
		"audience": []string{"agent:channel-agent"},
	}, http.StatusOK)
	if !second.Accepted {
		t.Fatalf("second POST not accepted: %+v", second)
	}
	if !second.Deduped {
		t.Errorf("second POST should be deduped (idempotent retry): %+v", second)
	}
	if second.MessageID != first.MessageID {
		t.Errorf("second.MessageID=%q want %q", second.MessageID, first.MessageID)
	}
	if second.Seq != first.Seq {
		t.Errorf("dedupe should return original seq=%d, got %d", first.Seq, second.Seq)
	}
}

// postMessageRaw is a write-message helper that decouples from harness
// PostMessageWithID's fixed shape — accepts an arbitrary body map +
// expected status, returns the decoded PostMessageResponse.
func postMessageRaw(t *testing.T, s *harness.Stack, channelID string, body map[string]any, wantStatus int) harness.PostMessageResponse {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		s.ServerURLBase()+"/api/channels/"+channelID+"/messages", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("build raw POST: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.Client().Do(req)
	if err != nil {
		t.Fatalf("post raw: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("status=%d want=%d body=%s", resp.StatusCode, wantStatus, string(respBody))
	}
	var out harness.PostMessageResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		t.Fatalf("decode response: %v body=%s", err, string(respBody))
	}
	return out
}

// TestE2E_PostMessage_PayloadChanged_DuplicateConflict covers
// launch-checklist §1.1 R7-54 caller-education invariant: 同 message id 但
// 不同 envelope (canonical_hash mismatch) 触发 harness Step 3 reject
// `harness_id_duplicate_conflict` with HTTP 409.
//
// The canonical_hash domain includes payload / audience / sender — any
// caller-controlled field that differs between the original write and the
// retry trips the conflict. We use a payload text change as the simplest
// concrete trigger; the audience-reorder variant follows the same code
// path because Audience participates in the hash.
func TestE2E_PostMessage_PayloadChanged_DuplicateConflict(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	email := "conflict+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-conflict-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-conflict-"+uniqSuffix(), "")
	s.BindChannel(wsID, chID)

	fixedID := "msg-conflict-fixed-" + uniqSuffix()

	first := s.PostMessageWithID(chID, fixedID, "human.text", "original", "")
	if !first.Accepted {
		t.Fatalf("first POST not accepted: %+v", first)
	}

	// Same id, different payload text → canonical_hash mismatch → reject.
	payload, _ := json.Marshal(map[string]string{"text": "tampered"})
	body, _ := json.Marshal(map[string]any{
		"id":       fixedID,
		"type":     "human.text",
		"payload":  json.RawMessage(payload),
		"audience": []string{"agent:channel-agent"},
	})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		s.ServerURLBase()+"/api/channels/"+chID+"/messages", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build raw POST: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.Client().Do(req)
	if err != nil {
		t.Fatalf("post raw: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status=%d want 409 (body=%s)", resp.StatusCode, string(raw))
	}
	var out harness.PostMessageResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode response: %v body=%s", err, string(raw))
	}
	if out.RejectReason != "harness_id_duplicate_conflict" {
		t.Errorf("reject_reason=%q want harness_id_duplicate_conflict (body=%s)",
			out.RejectReason, string(raw))
	}
	if out.Accepted {
		t.Errorf("conflict response should not have accepted=true: %+v", out)
	}
}

// TestE2E_PostMessage_NoSession_Unauthorized covers launch-checklist §1.1
// invariant: caller without valid session cookie returns 401.
//
// Expired-cookie and missing-cookie share the identity AuthMiddleware
// reject path — middleware reads the session cookie, looks it up in the
// session store, and rejects unknown / expired ids with the same 401.
// A bare http.Client (no cookie jar) exercises the same code path
// without the test having to manipulate the server clock.
func TestE2E_PostMessage_NoSession_Unauthorized(t *testing.T) {
	s := harness.Start(t, harness.Options{})
	email := "noauth+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-noauth-" + uniqSuffix())
	chID := s.CreateChannel(wsID, "ch-noauth-"+uniqSuffix(), "")
	s.BindChannel(wsID, chID)

	// Fresh client with no cookie jar — never logged in.
	bareClient := &http.Client{Timeout: 5 * time.Second}

	body, _ := json.Marshal(map[string]any{
		"id":      "msg-noauth-" + uniqSuffix(),
		"type":    "human.text",
		"payload": json.RawMessage(`{"text":"hi"}`),
	})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		s.ServerURLBase()+"/api/channels/"+chID+"/messages", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build raw POST: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := bareClient.Do(req)
	if err != nil {
		t.Fatalf("post raw: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		raw, _ := io.ReadAll(resp.Body)
		t.Errorf("status=%d want 401 (body=%s)", resp.StatusCode, string(raw))
	}
}
