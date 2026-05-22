package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// chainCallerCtx wraps a caller into ctx for use in tests.
func chainCallerCtx(actorID actor.ActorID) context.Context {
	return CtxWithCaller(context.Background(), CallerContext{
		ActorID:   actorID,
		ChannelID: channel.ID("ch-1"),
	})
}

func TestChain_PanicRecoveryReturnsError(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	c.steps = []khar.Step{panicHarnessStep{}}
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"panic"}`))

	res, err := c.Write(chainCallerCtx("agent:alpha"), env)
	if err == nil {
		t.Fatalf("Write err=nil result=%+v", res)
	}
	if !strings.Contains(err.Error(), "harness: panic") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("panic error missing context: %v", err)
	}
}

func TestChain_RejectObservabilityIncludesReasonAndCorrelation(t *testing.T) {
	logger := &recordingHarnessLogger{}
	metrics := newRecordingHarnessMetrics()
	c, _, _, _ := newTestChain(t, func(d *Deps) {
		d.Logger = logger
		d.Metrics = metrics
	})
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.CorrelationID = "corr-harness-1"
	env.Kind = "bogus"

	res, err := c.Write(chainCallerCtx("agent:alpha"), env)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.RejectReason != message.HarnessKindInvalid {
		t.Fatalf("RejectReason=%s want %s", res.RejectReason, message.HarnessKindInvalid)
	}
	if got := metrics.counter("harness.reject", "reason", string(message.HarnessKindInvalid), "step", "envelope_shape"); got != 1 {
		t.Fatalf("harness.reject counter=%d want 1", got)
	}
	if !logger.has("warn", "harness.write.reject", "reason", string(message.HarnessKindInvalid), "correlation_id", "corr-harness-1") {
		t.Fatalf("missing reject log with reason + correlation_id: %+v", logger.lines())
	}
}

type panicHarnessStep struct{}

func (panicHarnessStep) ID() khar.StepID { return khar.StepCallerAuth }

func (panicHarnessStep) Run(context.Context, *message.Envelope) (khar.Outcome, error) {
	panic("boom")
}

type recordingHarnessLogger struct {
	mu   sync.Mutex
	logs []recordedHarnessLog
}

type recordedHarnessLog struct {
	level string
	msg   string
	args  map[string]string
}

func (l *recordingHarnessLogger) Debug(msg string, args ...any) { l.record("debug", msg, args...) }
func (l *recordingHarnessLogger) Warn(msg string, args ...any)  { l.record("warn", msg, args...) }
func (l *recordingHarnessLogger) Error(msg string, args ...any) { l.record("error", msg, args...) }

func (l *recordingHarnessLogger) record(level, msg string, args ...any) {
	line := recordedHarnessLog{level: level, msg: msg, args: map[string]string{}}
	for i := 0; i < len(args); i += 2 {
		key := fmt.Sprint(args[i])
		value := ""
		if i+1 < len(args) {
			value = fmt.Sprint(args[i+1])
		}
		line.args[key] = value
	}
	l.mu.Lock()
	l.logs = append(l.logs, line)
	l.mu.Unlock()
}

func (l *recordingHarnessLogger) has(level, msg string, fields ...string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.logs {
		if line.level != level || line.msg != msg {
			continue
		}
		ok := true
		for i := 0; i < len(fields); i += 2 {
			if line.args[fields[i]] != fields[i+1] {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func (l *recordingHarnessLogger) lines() []recordedHarnessLog {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]recordedHarnessLog, len(l.logs))
	copy(out, l.logs)
	return out
}

type recordingHarnessMetrics struct {
	mu       sync.Mutex
	counters map[string]int64
}

func newRecordingHarnessMetrics() *recordingHarnessMetrics {
	return &recordingHarnessMetrics{counters: map[string]int64{}}
}

func (m *recordingHarnessMetrics) IncCounter(name string, tags ...string) {
	m.mu.Lock()
	m.counters[metricKey(name, tags...)]++
	m.mu.Unlock()
}

func (m *recordingHarnessMetrics) counter(name string, tags ...string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[metricKey(name, tags...)]
}

func metricKey(name string, tags ...string) string {
	key := name
	for i := 0; i < len(tags); i += 2 {
		value := ""
		if i+1 < len(tags) {
			value = tags[i+1]
		}
		key += "|" + tags[i] + "=" + value
	}
	return key
}

// TestChain_Step1_MissingCaller covers Step 0/1 caller-principal reject
// (harness_engine_acl_denied).
func TestChain_Step1_MissingCaller(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	res, err := c.Write(context.Background(), env)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.RejectReason != message.HarnessEngineACLDenied {
		t.Fatalf("expected harness_engine_acl_denied, got %s", res.RejectReason)
	}
}

// TestChain_Step1_ChannelMismatch — caller ctx for ch-2 against deps ch-1.
func TestChain_Step1_ChannelMismatch(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	ctx := CtxWithCaller(context.Background(), CallerContext{
		ActorID: "agent:alpha", ChannelID: "ch-other",
	})
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	res, _ := c.Write(ctx, env)
	if res.RejectReason != message.HarnessEngineACLDenied {
		t.Fatalf("expected harness_engine_acl_denied, got %s", res.RejectReason)
	}
}

// TestChain_Step2_MissingFields covers nil payload + missing kind etc.
// (Round-3 cluster F: HarnessEnvelopeFieldMissing was the pre-round-3
// reason; the new envelope-shape stage emits HarnessEnvelopeFieldMissing.)
func TestChain_Step2_MissingFields(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := &message.Envelope{
		Sender: message.Sender{ID: "agent:alpha"},
		// no ID, no Type — required-field reject expected.
	}
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessEnvelopeFieldMissing {
		t.Fatalf("expected harness_envelope_field_missing, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step2_KindInvalid covers junk in envelope.kind.
func TestChain_Step2_KindInvalid(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.Kind = "bogus"
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessKindInvalid {
		t.Fatalf("expected harness_kind_invalid, got %s", res.RejectReason)
	}
}

// TestChain_Step2_ResponseMissingParent covers the One Law pairing
// extra-strong constraint.
func TestChain_Step2_ResponseMissingParent(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newResponse("r-1", "agent:alpha", "", "agent.text", json.RawMessage(`{"status":"completed"}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessResponseMissingParent {
		t.Fatalf("expected harness_response_missing_parent, got %s", res.RejectReason)
	}
}

// TestChain_Step3_SenderMismatch covers step 3 sender_mismatch.
func TestChain_Step3_SenderMismatch(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.Sender.ID = "agent:other"
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessSenderMismatch {
		t.Fatalf("expected harness_sender_mismatch, got %s", res.RejectReason)
	}
}

// TestChain_Step3_SenderKindTamper covers sender_kind_mismatch when
// caller fabricates a different kind.
func TestChain_Step3_SenderKindTamper(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.Sender.Kind = actor.KindSystem // alpha is an agent.
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessSenderKindMismatch {
		t.Fatalf("expected harness_sender_kind_mismatch, got %s", res.RejectReason)
	}
}

// TestChain_Step3_SenderKindOverwrite — caller omits kind, harness
// forces overwrite from actor_registry.
func TestChain_Step3_SenderKindOverwrite(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.Sender.Kind = ""
	res, err := c.Write(chainCallerCtx("agent:alpha"), env)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !res.Accepted() {
		t.Fatalf("expected accept, got reject=%s detail=%s", res.RejectReason, res.RejectDetail)
	}
	if env.Sender.Kind != actor.KindAgent {
		t.Fatalf("expected sender.kind=agent (forced), got %s", env.Sender.Kind)
	}
}

// TestChain_Step3_Deregistered covers sender_deregistered after
// soft-delete.
func TestChain_Step3_Deregistered(t *testing.T) {
	c, areg, _, _ := newTestChain(t)
	_ = areg.Deregister(context.Background(), "agent:alpha", 999999)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessSenderDeregistered {
		t.Fatalf("expected harness_sender_deregistered, got %s", res.RejectReason)
	}
}

// TestChain_Step4_UnknownType covers business type unknown to registry.
func TestChain_Step4_UnknownType(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := &message.Envelope{
		ID:        "m-1",
		ChannelID: "ch-1",
		TS:        testTS,
		Type:      "biz.nonexistent",
		Kind:      message.KindEvent,
		Sender:    message.Sender{ID: "agent:alpha"},
		Payload:   json.RawMessage(`{}`),
		Audience:  message.Audience{"*"},
	}
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessTypeUnknown {
		t.Fatalf("expected harness_type_unknown, got %s", res.RejectReason)
	}
}

// TestChain_Step5_RequestAudienceInvalid covers request without single
// concrete receiver.
func TestChain_Step5_RequestAudienceInvalid(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})
	env := newRequest("req-1", "agent:alpha", "feishu.chat.send", "*", json.RawMessage(`{"title":"x"}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessRequestAudienceInvalid {
		t.Fatalf("expected harness_request_audience_invalid, got %s", res.RejectReason)
	}
}

// TestChain_Step5_AudienceActorNotRegistered.
func TestChain_Step5_AudienceActorNotRegistered(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})
	env := newRequest("req-1", "agent:alpha", "feishu.chat.send", "tool:does-not-exist", json.RawMessage(`{"title":"x"}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessAudienceMemberNotActive {
		t.Fatalf("expected harness_audience_member_not_active, got %s", res.RejectReason)
	}
}

// TestChain_Step5_AudienceHandlerMismatch.
func TestChain_Step5_AudienceHandlerMismatch(t *testing.T) {
	c, areg, _, treg := newTestChain(t)
	_ = areg.Insert(context.Background(), actorreg.Record{ID: "tool:other", Kind: actor.KindTool, CreatedAt: 1})
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})
	env := newRequest("req-1", "agent:alpha", "feishu.chat.send", "tool:other", json.RawMessage(`{"title":"x"}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessAudienceHandlerMismatch {
		t.Fatalf("expected harness_audience_handler_mismatch, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step5_KindNotAllowed.
func TestChain_Step5_KindNotAllowed(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	// Type only allows request — agent emits event → reject.
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest},
		HandlerActorID: "tool:feishu-adapter",
	})
	env := newEvent("agent:alpha", "feishu.chat.send", json.RawMessage(`{}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessKindNotAllowedForType {
		t.Fatalf("expected harness_kind_not_allowed, got %s", res.RejectReason)
	}
}

// Payload schema validation was removed in Level A (proto-layer0
// §1.4.1 / proto-layer1 §1.3 §2): payload is opaque to the protocol
// layer, type_registry stores no payload schemas, and the harness has
// no payload-schema step. The former TestChain_Step6_PayloadSchema /
// TestChain_Step6_PayloadSchemaFailsClosedWithoutValidator tests are
// intentionally absent.

func TestChain_Step5_DefaultExpiresAtByReceiverKind(t *testing.T) {
	c, areg, _, treg := newTestChain(t)
	_ = areg.Insert(context.Background(), actorreg.Record{ID: "agent:beta", Kind: actor.KindAgent, CreatedAt: 1})
	treg.Add(TypeView{
		Type:           "tool.exec",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   2500,
		HandlerActorID: "tool:feishu-adapter",
	})

	cases := []struct {
		name     string
		env      *message.Envelope
		want     *int64
		wantNil  bool
		callerID actor.ActorID
	}{
		{
			name:     "tool max_pending_ms",
			env:      newRequest("req-tool", "agent:alpha", "tool.exec", "tool:feishu-adapter", json.RawMessage(`{}`)),
			want:     int64Ptr(1700000002500),
			callerID: "agent:alpha",
		},
		{
			name:     "agent default",
			env:      newRequest("req-agent", "agent:alpha", "human.text", "agent:beta", json.RawMessage(`{"text":"hi"}`)),
			want:     int64Ptr(1700000000000 + defaultAgentMaxPendingMs),
			callerID: "agent:alpha",
		},
		{
			name:     "system default",
			env:      newRequest("req-system", "agent:alpha", "human.text", string(actor.SystemActorID), json.RawMessage(`{"text":"hi"}`)),
			want:     int64Ptr(1700000000000 + defaultSystemMaxPendingMs),
			callerID: "agent:alpha",
		},
		{
			name:     "human null baseline",
			env:      newRequest("req-human", "agent:alpha", "human.text", "user:demo", json.RawMessage(`{"text":"hi"}`)),
			wantNil:  true,
			callerID: "agent:alpha",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := c.Write(chainCallerCtx(tc.callerID), tc.env)
			if err != nil {
				t.Fatalf("Write: %v", err)
			}
			if !res.Accepted() {
				t.Fatalf("Write rejected: %s %s", res.RejectReason, res.RejectDetail)
			}
			if tc.wantNil {
				if tc.env.ExpiresAt != nil {
					t.Fatalf("ExpiresAt=%d want nil", *tc.env.ExpiresAt)
				}
				return
			}
			if tc.env.ExpiresAt == nil || *tc.env.ExpiresAt != *tc.want {
				t.Fatalf("ExpiresAt=%v want %d", tc.env.ExpiresAt, *tc.want)
			}
		})
	}
}

func int64Ptr(v int64) *int64 { return &v }

// TestChain_Step8_ResponseParentNotFound — parent_id doesn't exist in
// message_log → harness_response_parent_not_found per proto-layer1 §2.8.
func TestChain_Step8_ResponseParentNotFound(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newResponse("r-1", "agent:alpha", "missing-parent-id", "agent.text",
		json.RawMessage(`{"status":"completed"}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessResponseParentNotFound {
		t.Fatalf("expected harness_response_parent_not_found, got %s", res.RejectReason)
	}
}

// TestChain_Step8_ResponseParentNotRequest — parent_id resolves but
// points at a kind=event → harness_response_parent_not_request.
func TestChain_Step8_ResponseParentNotRequest(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	// Seed an event message that the test response will erroneously
	// reference as parent.
	parent := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	parent.ID = "evt-not-request"
	if r, err := c.Write(chainCallerCtx("agent:alpha"), parent); err != nil || !r.Accepted() {
		t.Fatalf("seed event: r=%+v err=%v", r, err)
	}
	resp := newResponse("resp-bad-parent", "agent:alpha", "evt-not-request", "agent.text",
		json.RawMessage(`{"status":"completed"}`))
	resp.Audience = message.Audience{"agent:alpha"}
	res, _ := c.Write(chainCallerCtx("agent:alpha"), resp)
	if res.RejectReason != message.HarnessResponseParentNotRequest {
		t.Fatalf("expected harness_response_parent_not_request, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step8_ResponseStatusInvalid — payload.status not in the
// strict {completed, failed} closed set → harness_response_status_invalid
// per proto-layer1 §2.8 #4.
func TestChain_Step8_ResponseStatusInvalid(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})
	req := newRequest("req-status", "agent:alpha", "feishu.chat.send", "tool:feishu-adapter",
		json.RawMessage(`{"title":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}
	resp := newResponse("resp-bogus-status", "tool:feishu-adapter", "req-status", "feishu.chat.send",
		json.RawMessage(`{"status":"in_progress"}`))
	resp.Audience = message.Audience{"agent:alpha"}
	res, _ := c.Write(chainCallerCtx("tool:feishu-adapter"), resp)
	if res.RejectReason != message.HarnessResponseStatusInvalid {
		t.Fatalf("expected harness_response_status_invalid, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

func TestChain_Step8_ResponsePairingAcceptsAuthorizedResponder(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})

	req := newRequest("req-1", "agent:alpha", "feishu.chat.send", "tool:feishu-adapter",
		json.RawMessage(`{"title":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}

	resp := newResponse("resp-1", "tool:feishu-adapter", "req-1", "feishu.chat.send",
		json.RawMessage(`{"status":"completed"}`))
	resp.Audience = message.Audience{"agent:alpha"}
	res, err := c.Write(chainCallerCtx("tool:feishu-adapter"), resp)
	if err != nil {
		t.Fatalf("Write response: %v", err)
	}
	if !res.Accepted() {
		t.Fatalf("expected response accepted, got reject=%s detail=%s", res.RejectReason, res.RejectDetail)
	}
}

func TestChain_Step8_ResponseUnauthorizedSender(t *testing.T) {
	c, areg, _, treg := newTestChain(t)
	_ = areg.Insert(context.Background(), actorreg.Record{ID: "agent:mallory", Kind: actor.KindAgent, CreatedAt: 1})
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})

	req := newRequest("req-1", "agent:alpha", "feishu.chat.send", "tool:feishu-adapter",
		json.RawMessage(`{"title":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}

	resp := newResponse("resp-mallory", "agent:mallory", "req-1", "feishu.chat.send",
		json.RawMessage(`{"status":"failed","reason":"permission_denied"}`))
	resp.Audience = message.Audience{"agent:alpha"}
	res, err := c.Write(chainCallerCtx("agent:mallory"), resp)
	if err != nil {
		t.Fatalf("Write mallory response: %v", err)
	}
	if res.RejectReason != message.HarnessResponseUnauthorizedSender {
		t.Fatalf("expected harness_response_unauthorized_sender, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

func TestChain_Step8_SystemTerminalFallbackRejectsInvalidReason(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})

	req := newRequest("req-1", "agent:alpha", "feishu.chat.send", "tool:feishu-adapter",
		json.RawMessage(`{"title":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}

	legacyPanicReason := "adapter" + "_panic"
	payload := json.RawMessage(`{"status":"failed","reason":"` + legacyPanicReason + `"}`)
	resp := newResponse("resp-invalid-reason", actor.SystemActorID, "req-1", "feishu.chat.send", payload)
	resp.Audience = message.Audience{"agent:alpha"}
	res, err := c.Write(chainCallerCtx(actor.SystemActorID), resp)
	if err != nil {
		t.Fatalf("Write invalid system fallback: %v", err)
	}
	if res.RejectReason != message.HarnessResponseReasonInvalid {
		t.Fatalf("expected harness_response_reason_invalid, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

func TestChain_Step8_ResponseAudienceMismatch(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})

	req := newRequest("req-1", "agent:alpha", "feishu.chat.send", "tool:feishu-adapter",
		json.RawMessage(`{"title":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}

	resp := newResponse("resp-bad-audience", "tool:feishu-adapter", "req-1", "feishu.chat.send",
		json.RawMessage(`{"status":"failed"}`))
	resp.Audience = message.Audience{"user:demo"}
	res, err := c.Write(chainCallerCtx("tool:feishu-adapter"), resp)
	if err != nil {
		t.Fatalf("Write mismatched response: %v", err)
	}
	if res.RejectReason != message.HarnessResponseAudienceMismatch {
		t.Fatalf("expected harness_response_audience_mismatch, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step8_TerminalDuplicate.
func TestChain_Step8_TerminalDuplicate(t *testing.T) {
	c, _, log, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})
	// Seed a parent request and a terminal response.
	req := newRequest("req-1", "agent:alpha", "feishu.chat.send", "tool:feishu-adapter",
		json.RawMessage(`{"title":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}
	resp := newResponse("resp-1", "tool:feishu-adapter", "req-1", "feishu.chat.send",
		json.RawMessage(`{"status":"completed"}`))
	resp.Audience = message.Audience{"agent:alpha"}
	if r, err := c.Write(chainCallerCtx("tool:feishu-adapter"), resp); err != nil || !r.Accepted() {
		t.Fatalf("seed response: r=%+v err=%v", r, err)
	}
	// Second different response for the same parent → terminal_duplicate.
	resp2 := newResponse("resp-2", "tool:feishu-adapter", "req-1", "feishu.chat.send",
		json.RawMessage(`{"status":"failed"}`))
	resp2.Audience = message.Audience{"agent:alpha"}
	res, err := c.Write(chainCallerCtx("tool:feishu-adapter"), resp2)
	if err != nil {
		t.Fatalf("Write second response: %v", err)
	}
	if res.RejectReason != message.HarnessTerminalDuplicate {
		t.Fatalf("expected harness_terminal_duplicate, got %s detail=%s", res.RejectReason, res.RejectDetail)
	}
	_ = log
}

// TestChain_Step0_5_Dedupe — same id same content returns Deduped=true.
func TestChain_Step0_5_Dedupe(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	payload := json.RawMessage(`{"text":"hi"}`)
	env1 := newEvent("agent:alpha", "agent.text", payload)
	env1.ID = "evt-shared-id"
	if r, err := c.Write(chainCallerCtx("agent:alpha"), env1); err != nil || !r.Accepted() {
		t.Fatalf("first append: %+v %v", r, err)
	}

	env2 := newEvent("agent:alpha", "agent.text", payload)
	env2.ID = "evt-shared-id"
	r, err := c.Write(chainCallerCtx("agent:alpha"), env2)
	if err != nil {
		t.Fatalf("dedupe append: %v", err)
	}
	if !r.Deduped {
		t.Fatalf("expected Deduped=true, got %+v", r)
	}
}

// TestChain_Step0_5_IDDuplicateConflict — same id different content.
// (Round-3 cluster G3: renamed from MessageIDConflict; wire value
// harness_id_duplicate_conflict per proto-layer1 §2.3 / §2.11.1.)
func TestChain_Step0_5_IDDuplicateConflict(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env1 := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env1.ID = "evt-conflict"
	if r, err := c.Write(chainCallerCtx("agent:alpha"), env1); err != nil || !r.Accepted() {
		t.Fatalf("first append: %+v %v", r, err)
	}
	env2 := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"different"}`))
	env2.ID = "evt-conflict"
	r, err := c.Write(chainCallerCtx("agent:alpha"), env2)
	if err != nil {
		t.Fatalf("conflict append: %v", err)
	}
	if r.RejectReason != message.HarnessIDDuplicateConflict {
		t.Fatalf("expected harness_id_duplicate_conflict, got %+v", r)
	}
}

// TestChain_AcceptAndAppendSetsSeq covers the happy path.
func TestChain_AcceptAndAppendSetsSeq(t *testing.T) {
	c, _, log, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.ID = "evt-ok"
	r, err := c.Write(chainCallerCtx("agent:alpha"), env)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !r.Accepted() || r.Seq == 0 {
		t.Fatalf("expected accept with seq>0, got %+v", r)
	}
	if env.TSReceived == 0 {
		t.Fatalf("expected TSReceived populated")
	}
	if env.Visibility != message.VisibilityPublic {
		t.Fatalf("expected default visibility=public, got %s", env.Visibility)
	}
	if _, ok, _ := log.FindByID(context.Background(), "ch-1", "evt-ok"); !ok {
		t.Fatalf("expected row persisted to log")
	}
}

// TestChain_CoreTypeKindLocked — core.system_event must stay kind=event.
func TestChain_CoreTypeKindLocked(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := &message.Envelope{
		ID:        "evt-sys-1",
		ChannelID: "ch-1",
		TS:        testTS,
		Type:      "core.system_event",
		Kind:      message.KindRequest, // not allowed for core.system_event
		Sender:    message.Sender{ID: actor.SystemActorID},
		Payload:   json.RawMessage(`{}`),
		Audience:  message.Audience{"*"},
	}
	res, _ := c.Write(chainCallerCtx(actor.SystemActorID), env)
	if res.RejectReason != message.HarnessKindNotAllowedForType {
		t.Fatalf("expected harness_kind_not_allowed, got %s", res.RejectReason)
	}
}

// ---------------------------------------------------------------------------
// Round-3 Cluster F — Step 2 Envelope Shape Validate coverage.
//
// Each new reject reason from proto-layer1 §2.11.1 added by cluster F gets
// at least one explicit case. The cases are exhaustive in the sense that
// every numbered branch in §2.2 of the spec maps to a test below.
// ---------------------------------------------------------------------------

// TestChain_Step2_ChannelMismatch — envelope.channel_id != bound channel.
// Caller is bound to the same ch-1 as the harness (so step 0+1 accepts);
// the envelope itself carries a stray channel_id.
func TestChain_Step2_ChannelMismatch(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.ChannelID = "ch-other"
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessChannelMismatch {
		t.Fatalf("expected harness_channel_mismatch, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step2_VisibilityInvalid — caller-provided visibility outside
// the closed {public, private} set.
func TestChain_Step2_VisibilityInvalid(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.Visibility = "internal"
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessVisibilityInvalid {
		t.Fatalf("expected harness_visibility_invalid, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step2_VisibilityAudienceInvalid — visibility=private +
// audience=['*'] is a semantic contradiction (private with broadcast).
func TestChain_Step2_VisibilityAudienceInvalid(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.Visibility = message.VisibilityPrivate
	env.Audience = message.Audience{"*"}
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessVisibilityAudienceInvalid {
		t.Fatalf("expected harness_visibility_audience_invalid, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step2_AudienceEmpty — explicit empty audience after the
// shape stage (caller can construct one even though newEvent fills ['*']).
func TestChain_Step2_AudienceEmpty(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.Audience = message.Audience{}
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessAudienceEmpty {
		t.Fatalf("expected harness_audience_empty, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step2_AudienceMixedWildcard — '*' mixed with concrete actors.
func TestChain_Step2_AudienceMixedWildcard(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.Audience = message.Audience{"*", "agent:alpha"}
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessAudienceMixedWildcard {
		t.Fatalf("expected harness_audience_mixed_wildcard, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step2_RequestAudienceInvalid_TwoConcrete — kind=request with
// two concrete recipients (no wildcard, len != 1).
func TestChain_Step2_RequestAudienceInvalid_TwoConcrete(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})
	env := newRequest("req-multi", "agent:alpha", "feishu.chat.send", "tool:feishu-adapter",
		json.RawMessage(`{"title":"x"}`))
	env.Audience = message.Audience{"tool:feishu-adapter", "agent:alpha"}
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessRequestAudienceInvalid {
		t.Fatalf("expected harness_request_audience_invalid, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step2_ResponseAudienceInvalid — kind=response with len != 1.
func TestChain_Step2_ResponseAudienceInvalid(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newResponse("r-multi", "agent:alpha", "parent-1", "agent.text",
		json.RawMessage(`{"status":"completed"}`))
	env.Audience = message.Audience{"agent:alpha", "user:demo"}
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessResponseAudienceInvalid {
		t.Fatalf("expected harness_response_audience_invalid, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step2_EnvelopeUnknownField — raw-JSON path. Step 2 enforces
// fail-closed unknown top-level fields per proto-layer0 §7.3 when the
// caller plumbs the raw envelope via CtxWithRawEnvelope.
func TestChain_Step2_EnvelopeUnknownField(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	raw := []byte(`{"id":"evt-x","channel_id":"ch-1","kind":"event","type":"agent.text",` +
		`"sender":{"id":"agent:alpha"},"audience":["*"],"ts":1700000000000,` +
		`"payload":{"text":"hi"},"future_field":"future"}`)
	ctx := CtxWithRawEnvelope(chainCallerCtx("agent:alpha"), raw)
	res, _ := c.Write(ctx, env)
	if res.RejectReason != message.HarnessEnvelopeUnknownField {
		t.Fatalf("expected harness_envelope_unknown_field, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// ---------------------------------------------------------------------------
// Round-3 Cluster F — Step 4 time-relation reject cases (harness_time_invalid).
// ---------------------------------------------------------------------------

// TestChain_Step4_TimeInvalid_NotBeforeBeforeTS — not_before < ts.
func TestChain_Step4_TimeInvalid_NotBeforeBeforeTS(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	earlier := env.TS - 1
	env.NotBefore = &earlier
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessTimeInvalid {
		t.Fatalf("expected harness_time_invalid (not_before < ts), got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step4_TimeInvalid_ExpiresAtBeforeTS — expires_at <= ts.
func TestChain_Step4_TimeInvalid_ExpiresAtBeforeTS(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})
	env := newRequest("req-exp", "agent:alpha", "feishu.chat.send", "tool:feishu-adapter",
		json.RawMessage(`{"title":"x"}`))
	exp := env.TS // equal → reject
	env.ExpiresAt = &exp
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessTimeInvalid {
		t.Fatalf("expected harness_time_invalid (expires_at <= ts), got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step4_TimeInvalid_ExpiresAtBeforeNotBefore — expires_at <=
// not_before. Both fields are explicitly set so the post-normalize check
// fires regardless of default fill.
func TestChain_Step4_TimeInvalid_ExpiresAtBeforeNotBefore(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})
	env := newRequest("req-nbe", "agent:alpha", "feishu.chat.send", "tool:feishu-adapter",
		json.RawMessage(`{"title":"x"}`))
	nb := env.TS + 100
	exp := env.TS + 50 // exp < nb → reject
	env.NotBefore = &nb
	env.ExpiresAt = &exp
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessTimeInvalid {
		t.Fatalf("expected harness_time_invalid (expires_at <= not_before), got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step4_TimeRelation_AcceptsValidFuture — sanity: not_before >
// ts AND expires_at > not_before → accept.
func TestChain_Step4_TimeRelation_AcceptsValidFuture(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	nb := env.TS + 1000
	exp := env.TS + 2000
	env.NotBefore = &nb
	env.ExpiresAt = &exp
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if !res.Accepted() {
		t.Fatalf("expected accept with valid future timing, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// ---------------------------------------------------------------------------
// Dedupe ordering — proto-layer1 §2.3.
// ---------------------------------------------------------------------------

// TestChain_DedupeRunsBeforeNormalize asserts the dedupe hash is computed
// from the pre-normalize sender-provided envelope. A retry that omits
// runtime-derived defaults (visibility / not_before) MUST still match.
func TestChain_DedupeRunsBeforeNormalize(t *testing.T) {
	c, _, log, _ := newTestChain(t)
	payload := json.RawMessage(`{"text":"hi"}`)

	// First write: sender provides the minimum. Normalize will fill
	// visibility=public + not_before=ts.
	env1 := newEvent("agent:alpha", "agent.text", payload)
	env1.ID = "evt-prenormalize-dedupe"
	env1.Visibility = "" // explicit zero — normalize fills
	env1.NotBefore = nil
	r1, err := c.Write(chainCallerCtx("agent:alpha"), env1)
	if err != nil || !r1.Accepted() {
		t.Fatalf("first append: r=%+v err=%v", r1, err)
	}

	// Confirm the stored row's CanonicalHash matches what dedupe step
	// would compute on a raw retry.
	storedHash, ok, err := log.LookupCanonicalHash(context.Background(), "ch-1", "evt-prenormalize-dedupe")
	if err != nil || !ok {
		t.Fatalf("LookupCanonicalHash: ok=%v err=%v", ok, err)
	}
	if storedHash == "" {
		t.Fatalf("stored canonical_hash empty; StepEngineAppend must persist it")
	}

	// Retry — same sender-provided shape (no normalize defaults).
	env2 := newEvent("agent:alpha", "agent.text", payload)
	env2.ID = "evt-prenormalize-dedupe"
	env2.Visibility = ""
	env2.NotBefore = nil
	r2, err := c.Write(chainCallerCtx("agent:alpha"), env2)
	if err != nil {
		t.Fatalf("retry write: %v", err)
	}
	if !r2.Deduped {
		t.Fatalf("expected Deduped=true on raw retry, got %+v", r2)
	}
}

// TestChain_DedupeIgnoresSenderKindOverwrite — sender retries without
// providing sender.kind; StepSenderConsistent forces kind=agent on first
// write but the stored row's canonical_hash (sender-provided) MUST match
// the retry's hash.
func TestChain_DedupeIgnoresSenderKindOverwrite(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	payload := json.RawMessage(`{"text":"hi"}`)

	env1 := newEvent("agent:alpha", "agent.text", payload)
	env1.ID = "evt-kind-retry"
	env1.Sender.Kind = "" // sender omits — registry forces agent
	r1, err := c.Write(chainCallerCtx("agent:alpha"), env1)
	if err != nil || !r1.Accepted() {
		t.Fatalf("first append: %+v %v", r1, err)
	}
	if env1.Sender.Kind != actor.KindAgent {
		t.Fatalf("expected forced kind=agent after first write, got %s", env1.Sender.Kind)
	}

	env2 := newEvent("agent:alpha", "agent.text", payload)
	env2.ID = "evt-kind-retry"
	env2.Sender.Kind = "" // retry also omits
	r2, err := c.Write(chainCallerCtx("agent:alpha"), env2)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !r2.Deduped {
		t.Fatalf("expected Deduped on sender.kind-omitted retry, got %+v", r2)
	}
}

// ---------------------------------------------------------------------------
// Reserved namespace authority — proto-layer1 §2.5 + §6.2.0.
// ---------------------------------------------------------------------------

// TestChain_Step5_ReservedTypeUnauthorizedSender — non-system actor
// emits a reserved system type → harness_reserved_type_unauthorized_sender.
func TestChain_Step5_ReservedTypeUnauthorizedSender(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	// Add system.actor.registered to the type registry so type lookup
	// would succeed if the reserved gate let it through (we want to be
	// sure the reserved check runs *first*).
	treg.Add(TypeView{
		Type:         "system.actor.registered",
		AllowedKinds: []message.Kind{message.KindEvent},
	})
	env := &message.Envelope{
		ID:        "evt-fake-system",
		ChannelID: "ch-1",
		TS:        testTS,
		Type:      "system.actor.registered",
		Kind:      message.KindEvent,
		Sender:    message.Sender{ID: "agent:alpha"},
		Payload:   json.RawMessage(`{}`),
		Audience:  message.Audience{"*"},
	}
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessReservedTypeUnauthorizedSender {
		t.Fatalf("expected harness_reserved_type_unauthorized_sender, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step5_ReservedTypeSystemSenderAccepted — system actor
// emitting a reserved type passes the authority gate.
func TestChain_Step5_ReservedTypeSystemSenderAccepted(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:         "system.actor.registered",
		AllowedKinds: []message.Kind{message.KindEvent},
	})
	env := &message.Envelope{
		ID:        "evt-real-system",
		ChannelID: "ch-1",
		TS:        testTS,
		Type:      "system.actor.registered",
		Kind:      message.KindEvent,
		Sender:    message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Payload:   json.RawMessage(`{}`),
		Audience:  message.Audience{"*"},
	}
	res, _ := c.Write(chainCallerCtx(actor.SystemActorID), env)
	if !res.Accepted() {
		t.Fatalf("expected accept for system-sender reserved type, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

func TestChain_Step5_NonReservedSystemTypeRejectedBeforeRegistry(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:         "system.foo",
		AllowedKinds: []message.Kind{message.KindEvent},
	})
	env := &message.Envelope{
		ID:        "evt-forged-system",
		ChannelID: "ch-1",
		TS:        testTS,
		Type:      "system.foo",
		Kind:      message.KindEvent,
		Sender:    message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Payload:   json.RawMessage(`{}`),
		Audience:  message.Audience{"*"},
	}
	res, _ := c.Write(chainCallerCtx(actor.SystemActorID), env)
	if res.RejectReason != message.HarnessTypeUnknown {
		t.Fatalf("expected harness_type_unknown, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}
