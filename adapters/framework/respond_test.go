package framework

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestValidateRespondReasonClosedSet(t *testing.T) {
	cases := []struct {
		name    string
		status  string
		reason  string
		wantErr string
	}{
		{
			name:   "empty reason allowed",
			status: "completed",
		},
		{
			name:   "terminal failure reason allowed",
			status: "failed",
			reason: string(message.TerminalReceiverInternalError),
		},
		{
			name:    "reason requires failed status",
			status:  "completed",
			reason:  string(message.TerminalReceiverUnavailable),
			wantErr: "requires status=failed",
		},
		{
			name:    "open set rejected",
			status:  "failed",
			reason:  "adapter_route_missing",
			wantErr: "closed set",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRespondReason(tc.status, tc.reason)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateRespondReason: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validateRespondReason err=%v want substring %q", err, tc.wantErr)
			}
		})
	}
}

// TestRespondRejectsProvisionalStatus locks down the codex r1 review fix
// for A7/E21: ctx.Respond / ctx.Fail must reject any non-final status
// (Layer 2 core provisional set, Layer 3 namespace extensions, or stray
// strings) BEFORE any closure side-effect. Otherwise a misrouted
// provisional status would write a response envelope, cancel F3, and
// close the pending correlation prematurely — breaking INVARIANT-11
// (Closure: every request gets exactly one final response or substrate
// fallback).
func TestRespondRejectsProvisionalStatus(t *testing.T) {
	cases := []struct {
		name   string
		status string
	}{
		{name: "layer2_received", status: "received"},
		{name: "layer2_queued", status: "queued"},
		{name: "layer2_processing", status: "processing"},
		{name: "layer2_deferred", status: "deferred"},
		{name: "layer2_unavailable", status: "unavailable"},
		{name: "layer3_xhs_extension", status: "xhs.login_queued"},
		{name: "arbitrary_garbage", status: "in_progress_42"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var respondErr error
			mod := &stubModule{
				decl: adapter.Declaration{
					Name:         "xhs",
					ActorID:      "tool:xhs",
					Types:        []string{"xhs.publish"},
					Binding:      actor.BindingRuntimeOutbound,
					MaxPendingMs: 30_000,
				},
			}
			mod.handle = func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
				_, respondErr = mctx.Respond(ctx, adapter.CorrelationKey(env.ID),
					json.RawMessage(`{}`),
					adapter.RespondOptions{Status: tc.status},
				)
				// Surface a final to drain the pending entry so the test
				// cleanup is clean; this is post-violation so it does not
				// affect the assertion below.
				_, _ = mctx.Respond(ctx, adapter.CorrelationKey(env.ID),
					json.RawMessage(`{}`),
					adapter.RespondOptions{Status: "completed"},
				)
				return nil
			}

			mgr, chain, lookup, _, _ := newTestManager(t, mod)
			req := newTestRequest("channel:test", "agent:author", "xhs.publish", "req-respond-"+tc.name)
			req.Audience = message.Audience{"tool:xhs"}
			lookup.Put(req)
			if err := mgr.Dispatch(context.Background(), req); err != nil {
				t.Fatalf("Dispatch: %v", err)
			}

			if respondErr == nil {
				t.Fatalf("Respond with provisional status %q must error, got nil", tc.status)
			}
			if !strings.Contains(respondErr.Error(), "must be final") {
				t.Fatalf("Respond err=%v want substring \"must be final\"", respondErr)
			}

			// Only the trailing recovery final must have hit the chain —
			// the rejected provisional Respond must NOT have written
			// anything (pre-write rejection).
			written := chain.Written()
			if len(written) != 1 {
				t.Fatalf("expected 1 write (recovery final), got %d", len(written))
			}
			var payload map[string]any
			if err := json.Unmarshal(written[0].Payload, &payload); err != nil {
				t.Fatalf("payload unmarshal: %v", err)
			}
			if payload["status"] != "completed" {
				t.Fatalf("recovery write status=%v want completed", payload["status"])
			}
		})
	}
}

// TestRespondAcceptsFinalStatus verifies the positive path is unaffected:
// "completed" and "failed" both proceed through chain.Write and close
// the correlation. This guards against the validator overfitting and
// regressing the happy path.
func TestRespondAcceptsFinalStatus(t *testing.T) {
	cases := []struct {
		name   string
		status string
		reason string
	}{
		{name: "completed", status: "completed"},
		{name: "completed_default", status: ""}, // empty defaults to completed
		{name: "failed_with_reason", status: "failed", reason: string(message.TerminalReceiverInternalError)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mod := &stubModule{
				decl: adapter.Declaration{
					Name:         "xhs",
					ActorID:      "tool:xhs",
					Types:        []string{"xhs.publish"},
					Binding:      actor.BindingRuntimeOutbound,
					MaxPendingMs: 30_000,
				},
			}
			mod.handle = func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
				_, err := mctx.Respond(ctx, adapter.CorrelationKey(env.ID),
					json.RawMessage(`{}`),
					adapter.RespondOptions{Status: tc.status, Reason: tc.reason},
				)
				return err
			}

			mgr, chain, lookup, _, _ := newTestManager(t, mod)
			req := newTestRequest("channel:test", "agent:author", "xhs.publish", "req-final-"+tc.name)
			req.Audience = message.Audience{"tool:xhs"}
			lookup.Put(req)
			if err := mgr.Dispatch(context.Background(), req); err != nil {
				t.Fatalf("Dispatch: %v", err)
			}

			written := chain.Written()
			if len(written) != 1 {
				t.Fatalf("expected 1 write, got %d", len(written))
			}
			bm := mgr.byName["xhs"]
			entry, ok, err := bm.correlation.Get(context.Background(), adapter.CorrelationKey(req.ID))
			if err != nil || !ok {
				t.Fatalf("correlation get ok=%v err=%v", ok, err)
			}
			if entry.State != adapter.CorrelationDone {
				t.Fatalf("correlation state=%s want done", entry.State)
			}
		})
	}
}

// TestCompleteExternalResponseRejectsProvisionalPayloadStatus locks down
// the same A7/E21 fix for the external-callback final-completion path:
// CompleteExternalResponse must refuse envelopes whose payload.status is
// not in the final set. Otherwise an upstream proxy_facade misroute would
// close the correlation on a provisional, breaking INVARIANT-11.
func TestCompleteExternalResponseRejectsProvisionalPayloadStatus(t *testing.T) {
	cases := []struct {
		name    string
		payload string
	}{
		{name: "layer2_processing", payload: `{"status":"processing","progress_percent":0.4}`},
		{name: "layer2_received", payload: `{"status":"received"}`},
		{name: "layer3_xhs_namespace", payload: `{"status":"xhs.uploading"}`},
		{name: "missing_status", payload: `{}`},
		{name: "empty_status_string", payload: `{"status":""}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mod := &stubModule{
				decl: adapter.Declaration{
					Name:         "kimi",
					ActorID:      "tool:kimi",
					Types:        []string{"kimi.ask"},
					Binding:      actor.BindingRuntimeOutbound,
					MaxPendingMs: 30_000,
				},
			}
			mgr, chain, lookup, _, _ := newTestManager(t, mod)
			defer func() { _ = mgr.Shutdown(context.Background()) }()

			req := newTestRequest("channel:test", "agent:a", "kimi.ask", "req-cer-"+tc.name)
			req.Audience = message.Audience{"tool:kimi"}
			req.CorrelationID = message.ID("corr-cer-" + tc.name)
			lookup.Put(req)
			if err := mgr.Dispatch(context.Background(), req); err != nil {
				t.Fatalf("Dispatch: %v", err)
			}

			resp := &message.Envelope{
				ID:            message.ID("resp-cer-" + tc.name),
				ChannelID:     "channel:test",
				Sender:        message.Sender{Kind: actor.KindTool, ID: "tool:kimi"},
				Kind:          message.KindResponse,
				Type:          "kimi.ask",
				ParentID:      req.ID,
				CorrelationID: req.CorrelationID,
				Payload:       json.RawMessage(tc.payload),
				Audience:      message.Audience{"agent:a"},
			}

			bm := mgr.byName["kimi"]
			_, err := bm.mctx.CompleteExternalResponse(context.Background(), resp)
			if err == nil {
				t.Fatalf("CompleteExternalResponse with provisional payload %q must error, got nil", tc.payload)
			}
			if !strings.Contains(err.Error(), "must be final") {
				t.Fatalf("CompleteExternalResponse err=%v want substring \"must be final\"", err)
			}
			// Pre-write rejection: chain must be untouched.
			if written := chain.Written(); len(written) != 0 {
				t.Fatalf("expected 0 chain writes, got %d", len(written))
			}
			// Pending entry must remain so a real final can still close it.
			entry, ok, err := bm.correlation.Get(context.Background(), adapter.CorrelationKey(req.ID))
			if err != nil || !ok {
				t.Fatalf("correlation get ok=%v err=%v", ok, err)
			}
			if entry.State != adapter.CorrelationPending {
				t.Fatalf("correlation state=%s want pending (must not close on provisional)", entry.State)
			}
		})
	}
}

// TestCompleteExternalResponseAcceptsFinalPayloadStatus mirrors the
// negative test: completed / failed proceed through the chain and close
// the correlation. Guards against validator overfit.
func TestCompleteExternalResponseAcceptsFinalPayloadStatus(t *testing.T) {
	cases := []struct {
		name    string
		payload string
	}{
		{name: "completed", payload: `{"status":"completed","answer":"hi"}`},
		{name: "failed", payload: `{"status":"failed","reason":"receiver_internal_error"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mod := &stubModule{
				decl: adapter.Declaration{
					Name:         "kimi",
					ActorID:      "tool:kimi",
					Types:        []string{"kimi.ask"},
					Binding:      actor.BindingRuntimeOutbound,
					MaxPendingMs: 30_000,
				},
			}
			mgr, chain, lookup, _, _ := newTestManager(t, mod)
			defer func() { _ = mgr.Shutdown(context.Background()) }()

			req := newTestRequest("channel:test", "agent:a", "kimi.ask", "req-cer-ok-"+tc.name)
			req.Audience = message.Audience{"tool:kimi"}
			req.CorrelationID = message.ID("corr-cer-ok-" + tc.name)
			lookup.Put(req)
			if err := mgr.Dispatch(context.Background(), req); err != nil {
				t.Fatalf("Dispatch: %v", err)
			}

			resp := &message.Envelope{
				ID:            message.ID("resp-cer-ok-" + tc.name),
				ChannelID:     "channel:test",
				Sender:        message.Sender{Kind: actor.KindTool, ID: "tool:kimi"},
				Kind:          message.KindResponse,
				Type:          "kimi.ask",
				ParentID:      req.ID,
				CorrelationID: req.CorrelationID,
				Payload:       json.RawMessage(tc.payload),
				Audience:      message.Audience{"agent:a"},
			}

			bm := mgr.byName["kimi"]
			if _, err := bm.mctx.CompleteExternalResponse(context.Background(), resp); err != nil {
				t.Fatalf("CompleteExternalResponse final %s: %v", tc.name, err)
			}
			if written := chain.Written(); len(written) != 1 {
				t.Fatalf("expected 1 chain write, got %d", len(written))
			}
			entry, ok, err := bm.correlation.Get(context.Background(), adapter.CorrelationKey(req.ID))
			if err != nil || !ok {
				t.Fatalf("correlation get ok=%v err=%v", ok, err)
			}
			if entry.State != adapter.CorrelationDone {
				t.Fatalf("correlation state=%s want done", entry.State)
			}
		})
	}
}

// TestExtractPayloadStatusEdgeCases pins down the helper behaviour to
// avoid surprising callers (empty bytes, null, non-object, non-string
// status) — all of these should land as "missing/invalid" rather than
// silently passing IsFinalStatus.
func TestExtractPayloadStatusEdgeCases(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "empty_bytes", raw: "", want: ""},
		{name: "null_literal", raw: "null", want: ""},
		{name: "empty_object", raw: "{}", want: ""},
		{name: "completed", raw: `{"status":"completed"}`, want: "completed"},
		{name: "non_object", raw: `"completed"`, wantErr: true},
		{name: "non_string_status", raw: `{"status":42}`, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := extractPayloadStatus(json.RawMessage(tc.raw))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("extractPayloadStatus(%q) err=nil want error", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("extractPayloadStatus(%q): %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("extractPayloadStatus(%q)=%q want %q", tc.raw, got, tc.want)
			}
		})
	}
}
