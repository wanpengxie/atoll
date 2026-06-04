package harness

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// ---------------------------------------------------------------------
// Injectable stubs — the only seams the frozen harness exposes for the
// error / defensive branches: a Registry and a MessageLog whose method
// behaviour is supplied per-test. We never touch production .go.
// ---------------------------------------------------------------------

type stubRegistry struct {
	lookup func(ctx context.Context, id actor.ActorID) (storespec.Record, bool, error)
}

func (s stubRegistry) Lookup(ctx context.Context, id actor.ActorID) (storespec.Record, bool, error) {
	return s.lookup(ctx, id)
}
func (s stubRegistry) Exists(context.Context, actor.ActorID) (bool, error) { return false, nil }
func (s stubRegistry) ListActive(context.Context) ([]storespec.Record, error) {
	return nil, nil
}

type stubLog struct {
	appendFn   func(ctx context.Context, env *message.Envelope, isTerminal bool) (storespec.AppendResult, error)
	findByID   func(ctx context.Context, id message.ID) (*storespec.StoredRow, bool, error)
	hasFinalFn func(ctx context.Context, parentID message.ID) (bool, error)
}

func (s stubLog) Append(ctx context.Context, env *message.Envelope, isTerminal bool) (storespec.AppendResult, error) {
	return s.appendFn(ctx, env, isTerminal)
}
func (s stubLog) FindByID(ctx context.Context, id message.ID) (*storespec.StoredRow, bool, error) {
	return s.findByID(ctx, id)
}
func (s stubLog) HasFinalResponse(ctx context.Context, parentID message.ID) (bool, error) {
	return s.hasFinalFn(ctx, parentID)
}

// spyMetrics records every IncCounter call so observe* paths can be asserted.
type spyMetrics struct {
	calls [][]string
}

func (m *spyMetrics) IncCounter(name string, tags ...string) {
	m.calls = append(m.calls, append([]string{name}, tags...))
}

// activeRecord returns an active agent record for sender/audience resolution.
func activeRecord(id actor.ActorID) storespec.Record {
	return storespec.Record{ID: id, Kind: actor.KindAgent, CreatedAt: fixedNowMs}
}

// ---------------------------------------------------------------------
// deps.go — Validate + NoopMetrics.IncCounter + New defaults
// ---------------------------------------------------------------------

func TestDeps_ValidateMissingFields(t *testing.T) {
	if err := (Deps{}).Validate(); err == nil {
		t.Fatalf("empty Deps should fail Validate")
	}
	// ChannelID set, ActorRegistry nil → line 87-89.
	if err := (Deps{ChannelID: testChannelID}).Validate(); err == nil {
		t.Fatalf("missing ActorRegistry should fail")
	}
	// ChannelID + Registry set, Log nil → line 90-92.
	reg := stubRegistry{lookup: func(context.Context, actor.ActorID) (storespec.Record, bool, error) {
		return storespec.Record{}, false, nil
	}}
	if err := (Deps{ChannelID: testChannelID, ActorRegistry: reg}).Validate(); err == nil {
		t.Fatalf("missing Log should fail")
	}
	// Fully wired → nil.
	lg := stubLog{}
	if err := (Deps{ChannelID: testChannelID, ActorRegistry: reg, Log: lg}).Validate(); err != nil {
		t.Fatalf("fully-wired Deps Validate = %v, want nil", err)
	}
}

func TestNoopMetrics_IncCounter(t *testing.T) {
	// Pure no-op — exercises deps.go:33. Must not panic.
	NoopMetrics{}.IncCounter("anything", "k", "v")
}

// New must fill NowMs / Logger / Metrics defaults when nil (chain.go:36-44).
func TestNew_FillsDefaults(t *testing.T) {
	reg := stubRegistry{lookup: func(context.Context, actor.ActorID) (storespec.Record, bool, error) {
		return activeRecord("agent:p"), true, nil
	}}
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{Seq: 1}, nil
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	// NowMs / Logger / Metrics all nil → defaults filled.
	c, err := New(Deps{ChannelID: testChannelID, ActorRegistry: reg, Log: lg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Drive a successful write so the default NowMs is actually invoked and a
	// real (non-test) clock value lands in ts_received.
	e := validEvent("m-default", "agent:p")
	res, err := c.Write(ctxCaller("agent:p"), e)
	if err != nil || !res.Accepted() {
		t.Fatalf("write with defaulted deps: err=%v reason=%q", err, res.RejectReason)
	}
	if e.TSReceived == 0 {
		t.Fatalf("default NowMs not applied (ts_received still 0)")
	}
}

// ---------------------------------------------------------------------
// reason.go — String()
// ---------------------------------------------------------------------

func TestHarnessRejectReason_String(t *testing.T) {
	if got := HarnessChannelMismatch.String(); got != "harness_channel_mismatch" {
		t.Fatalf("String() = %q", got)
	}
}

// ---------------------------------------------------------------------
// chain.go — WithRequestID, stepName default, observe paths, panic recover,
// step-error path, append-error paths
// ---------------------------------------------------------------------

func TestWithRequestID_RoundTrips(t *testing.T) {
	ctx := WithRequestID(context.Background(), "req-42")
	if got := requestIDFromCtx(ctx); got != "req-42" {
		t.Fatalf("requestID = %q, want req-42", got)
	}
	// Absent → empty.
	if got := requestIDFromCtx(context.Background()); got != "" {
		t.Fatalf("absent requestID = %q, want empty", got)
	}
}

func TestStepName_DefaultUnknownID(t *testing.T) {
	if got := stepName(stepID(99)); got != "step_99" {
		t.Fatalf("stepName(99) = %q, want step_99", got)
	}
}

// chainWith builds a Chain with stub deps + a spy Metrics, exercising the
// observe* metric paths.
func chainWith(t *testing.T, reg storespec.Registry, lg storespec.MessageLog) (*Chain, *spyMetrics) {
	t.Helper()
	spy := &spyMetrics{}
	c, err := New(Deps{
		ChannelID:     testChannelID,
		ActorRegistry: reg,
		Log:           lg,
		NowMs:         func() int64 { return fixedNowMs },
		Metrics:       spy,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, spy
}

// A step that returns a hard error (not a reject) → Write maps it through
// observeError (chain.go:85-88 + 160-173) and returns the wrapped error. We
// trigger this at StepKindAndAudience by making the registry Lookup fail for a
// request audience target.
func TestWrite_StepError_ObservedAndReturned(t *testing.T) {
	lookupErr := errors.New("boom-registry")
	reg := stubRegistry{lookup: func(_ context.Context, id actor.ActorID) (storespec.Record, bool, error) {
		if id == "agent:p" {
			return activeRecord("agent:p"), true, nil // sender resolves
		}
		return storespec.Record{}, false, lookupErr // audience lookup fails
	}}
	lg := stubLog{
		appendFn:   func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) { return storespec.AppendResult{}, nil },
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c, spy := chainWith(t, reg, lg)

	req := &message.Envelope{
		ID: "rq", TS: fixedNowMs - 1000, ChannelID: testChannelID,
		Sender: message.Sender{ID: "agent:p"}, Kind: message.KindRequest, Type: "xhs.publish",
		Audience: message.Audience{"tool:dead"},
	}
	res, err := c.Write(ctxCaller("agent:p"), req)
	if err == nil {
		t.Fatalf("expected wrapped step error, got nil (res=%+v)", res)
	}
	if !errors.Is(err, lookupErr) {
		t.Fatalf("error = %v, want wrapping %v", err, lookupErr)
	}
	// observeError must have incremented harness.error.
	if !hasMetric(spy, "harness.error") {
		t.Fatalf("observeError did not record harness.error counter; calls=%v", spy.calls)
	}
}

// Append returns a typed *storespec.AppendError → mapped to a closed-set
// reject via observeReject (chain.go:108-116 + 141-157).
func TestWrite_AppendTypedError_MapsToReject(t *testing.T) {
	reg := stubRegistry{lookup: func(context.Context, actor.ActorID) (storespec.Record, bool, error) {
		return activeRecord("agent:p"), true, nil
	}}
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, &storespec.AppendError{
				Reason:           string(HarnessTerminalDuplicate),
				Detail:           "store says dup",
				PartialMessageID: "m-partial",
			}
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c, spy := chainWith(t, reg, lg)

	res, err := c.Write(ctxCaller("agent:p"), validEvent("m-app", "agent:p"))
	if err != nil {
		t.Fatalf("typed AppendError should be a reject, not error: %v", err)
	}
	if res.Accepted() || res.RejectReason != HarnessTerminalDuplicate {
		t.Fatalf("reject = %q accepted=%v, want terminal_duplicate", res.RejectReason, res.Accepted())
	}
	if res.MessageID != "m-partial" {
		t.Fatalf("MessageID = %q, want m-partial", res.MessageID)
	}
	if !hasMetricTag(spy, "harness.reject", string(HarnessTerminalDuplicate)) {
		t.Fatalf("observeReject did not record reject counter; calls=%v", spy.calls)
	}
}

// Append returns a PLAIN error (not *AppendError) → chain.go:117-118 wraps it
// and observeError records harness.error.
func TestWrite_AppendPlainError_WrappedAsError(t *testing.T) {
	appendErr := errors.New("disk on fire")
	reg := stubRegistry{lookup: func(context.Context, actor.ActorID) (storespec.Record, bool, error) {
		return activeRecord("agent:p"), true, nil
	}}
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, appendErr
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c, spy := chainWith(t, reg, lg)

	_, err := c.Write(ctxCaller("agent:p"), validEvent("m-plain", "agent:p"))
	if err == nil || !errors.Is(err, appendErr) {
		t.Fatalf("plain append error = %v, want wrapping %v", err, appendErr)
	}
	if !hasMetric(spy, "harness.error") {
		t.Fatalf("observeError missing for append plain error; calls=%v", spy.calls)
	}
}

// A step that panics → Write's deferred recover (chain.go:68-72) converts it to
// an error. We make the audience Lookup panic at StepKindAndAudience.
func TestWrite_PanicRecovered(t *testing.T) {
	reg := stubRegistry{lookup: func(_ context.Context, id actor.ActorID) (storespec.Record, bool, error) {
		if id == "agent:p" {
			return activeRecord("agent:p"), true, nil
		}
		panic("step blew up")
	}}
	lg := stubLog{
		appendFn:   func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) { return storespec.AppendResult{}, nil },
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c, _ := chainWith(t, reg, lg)

	req := &message.Envelope{
		ID: "rq", TS: fixedNowMs - 1000, ChannelID: testChannelID,
		Sender: message.Sender{ID: "agent:p"}, Kind: message.KindRequest, Type: "xhs.publish",
		Audience: message.Audience{"tool:boom"},
	}
	_, err := c.Write(ctxCaller("agent:p"), req)
	if err == nil {
		t.Fatalf("panic should be recovered into an error")
	}
}

// observeReject's reason=="" early-return (chain.go:142-144) is reachable only
// through the engine-append path: a typed *AppendError carrying an EMPTY Reason
// makes Chain.Write call observeReject with reason "". We assert no
// harness.reject counter is recorded (the metric branch is skipped).
func TestWrite_AppendEmptyReason_ObserveRejectNoOp(t *testing.T) {
	reg := stubRegistry{lookup: func(context.Context, actor.ActorID) (storespec.Record, bool, error) {
		return activeRecord("agent:p"), true, nil
	}}
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, &storespec.AppendError{Reason: "", Detail: "no reason"}
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c, spy := chainWith(t, reg, lg)

	res, err := c.Write(ctxCaller("agent:p"), validEvent("m-empty", "agent:p"))
	if err != nil {
		t.Fatalf("empty-reason AppendError should map to a WriteResult, got err=%v", err)
	}
	// An empty Reason produces a WriteResult with empty RejectReason, so
	// Accepted() is degenerately true — but the partial message id is carried
	// and observeReject's metric branch is SKIPPED (reason=="" early return).
	if res.MessageID != "" {
		t.Fatalf("empty-reason result MessageID = %q, want empty (no PartialMessageID set)", res.MessageID)
	}
	if hasMetric(spy, "harness.reject") {
		t.Fatalf("observeReject should skip metric on empty reason; calls=%v", spy.calls)
	}
}

func hasMetric(spy *spyMetrics, name string) bool {
	for _, c := range spy.calls {
		if len(c) > 0 && c[0] == name {
			return true
		}
	}
	return false
}

func hasMetricTag(spy *spyMetrics, name, tagVal string) bool {
	for _, c := range spy.calls {
		if len(c) > 0 && c[0] == name {
			for _, t := range c[1:] {
				if t == tagVal {
					return true
				}
			}
		}
	}
	return false
}

// ---------------------------------------------------------------------
// step_normalize.go:28-30 — nil envelope short-circuit. Chain.Write guards nil
// before the loop, so the step's own nil branch is only reachable by calling
// the step directly.
// ---------------------------------------------------------------------

func TestStepNormalize_NilEnvelope(t *testing.T) {
	out, err := newStepNormalize(Deps{NowMs: func() int64 { return fixedNowMs }}).Run(context.Background(), nil)
	if err != nil || !out.Continue() {
		t.Fatalf("nil envelope normalize = out=%+v err=%v, want continue/no-error", out, err)
	}
}

// ---------------------------------------------------------------------
// step_sender_consistent.go — Lookup error (51-53) + ctx canceled (83-85)
// ---------------------------------------------------------------------

func TestStepSenderConsistent_LookupError(t *testing.T) {
	wantErr := errors.New("registry down")
	reg := stubRegistry{lookup: func(context.Context, actor.ActorID) (storespec.Record, bool, error) {
		return storespec.Record{}, false, wantErr
	}}
	deps := Deps{ChannelID: testChannelID, ActorRegistry: reg}
	env := validEvent("m1", "agent:p")
	_, err := runStep(t, newStepSenderConsistent, deps, ctxCaller("agent:p"), env)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("lookup error = %v, want wrapping %v", err, wantErr)
	}
}

func TestStepSenderConsistent_CtxCanceled(t *testing.T) {
	reg := stubRegistry{lookup: func(context.Context, actor.ActorID) (storespec.Record, bool, error) {
		return activeRecord("agent:p"), true, nil
	}}
	deps := Deps{ChannelID: testChannelID, ActorRegistry: reg}
	ctx, cancel := context.WithCancel(ctxCaller("agent:p"))
	cancel() // canceled before run → final guard returns ctx.Err()
	env := validEvent("m1", "agent:p")
	_, err := runStep(t, newStepSenderConsistent, deps, ctx, env)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// ---------------------------------------------------------------------
// step_kind_audience.go:87-89 — request audience Lookup error
// ---------------------------------------------------------------------

func TestStepKindAudience_LookupError(t *testing.T) {
	wantErr := errors.New("audience lookup down")
	reg := stubRegistry{lookup: func(context.Context, actor.ActorID) (storespec.Record, bool, error) {
		return storespec.Record{}, false, wantErr
	}}
	deps := Deps{ChannelID: testChannelID, ActorRegistry: reg}
	req := &message.Envelope{
		ID: "rq", TS: fixedNowMs - 1000, ChannelID: testChannelID,
		Sender: message.Sender{ID: "agent:p"}, Kind: message.KindRequest, Type: "xhs.publish",
		Audience: message.Audience{"tool:x"},
	}
	_, err := runStep(t, newStepKindAndAudience, deps, ctxCaller("agent:p"), req)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("kind+audience lookup error = %v, want wrapping %v", err, wantErr)
	}
}

// ---------------------------------------------------------------------
// step_envelope_shape.go:122-124 + 164-168 — malformed raw JSON plumbed via
// CtxWithRawEnvelope makes checkUnknownTopLevelFields' json.Unmarshal fail,
// surfaced as a hard error (not a reject).
// ---------------------------------------------------------------------

func TestStepEnvelopeShape_MalformedRawJSONError(t *testing.T) {
	deps := Deps{ChannelID: testChannelID}
	env := validEvent("m1", "agent:p")
	ctx := CtxWithRawEnvelope(ctxCaller("agent:p"), []byte("{not-json"))
	_, err := runStep(t, newStepEnvelopeShape, deps, ctx, env)
	if err == nil {
		t.Fatalf("malformed raw JSON should surface as error")
	}
}

// checkUnknownTopLevelFields directly: valid JSON, valid error path covered
// above; here assert the function itself on malformed input returns the error.
func TestCheckUnknownTopLevelFields_Malformed(t *testing.T) {
	if _, err := checkUnknownTopLevelFields([]byte("nope")); err == nil {
		t.Fatalf("malformed JSON should error")
	}
}

// ---------------------------------------------------------------------
// step_response_pairing.go — FindByID error (86-88), HasFinalResponse error
// (176-178), and the checkFailedResponseReason / extractPayloadStatus /
// senderLocalName uncovered arms.
// ---------------------------------------------------------------------

// responsePairingDeps builds Deps whose Log returns a request parent for the
// given parentID and lets the test control HasFinalResponse.
func responsePairingDeps(parent *storespec.StoredRow, findErr error, hasFinal func(context.Context, message.ID) (bool, error)) Deps {
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, nil
		},
		findByID: func(_ context.Context, id message.ID) (*storespec.StoredRow, bool, error) {
			if findErr != nil {
				return nil, false, findErr
			}
			if parent == nil {
				return nil, false, nil
			}
			return parent, true, nil
		},
		hasFinalFn: hasFinal,
	}
	return Deps{ChannelID: testChannelID, Log: lg}
}

func makeResponse(payload string) *message.Envelope {
	return &message.Envelope{
		ID: "resp", TS: fixedNowMs, ChannelID: testChannelID,
		Sender: message.Sender{ID: "tool:xhs"}, Kind: message.KindResponse, Type: "xhs.publish",
		ParentID: "req1", Audience: message.Audience{"agent:caller"},
		Payload: message.Envelope{}.Payload, // placeholder; overwritten below
	}
}

func requestParent() *storespec.StoredRow {
	return &storespec.StoredRow{Envelope: message.Envelope{
		ID: "req1", ChannelID: testChannelID,
		Sender: message.Sender{ID: "agent:caller"}, Kind: message.KindRequest, Type: "xhs.publish",
		Audience: message.Audience{"tool:xhs"},
	}}
}

func TestStepResponsePairing_FindByIDError(t *testing.T) {
	wantErr := errors.New("find down")
	deps := responsePairingDeps(nil, wantErr, func(context.Context, message.ID) (bool, error) { return false, nil })
	resp := makeResponse("")
	resp.Payload = []byte(`{"status":"completed"}`)
	_, err := runStep(t, newStepResponsePairing, deps, ctxCaller("tool:xhs"), resp)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("FindByID error = %v, want wrapping %v", err, wantErr)
	}
}

func TestStepResponsePairing_HasFinalResponseError(t *testing.T) {
	wantErr := errors.New("hasfinal down")
	deps := responsePairingDeps(requestParent(), nil, func(context.Context, message.ID) (bool, error) {
		return false, wantErr
	})
	resp := makeResponse("")
	resp.Payload = []byte(`{"status":"completed"}`)
	_, err := runStep(t, newStepResponsePairing, deps, ctxCaller("tool:xhs"), resp)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("HasFinalResponse error = %v, want wrapping %v", err, wantErr)
	}
}

// checkFailedResponseReason branches:
//   - len==0 (220-222): empty payload → zero check.
//   - unmarshal err (224-226): malformed payload.
//   - no "status" (228-230).
//   - status=failed, no "reason" (237-239): failed=true,hasReason=false.
//   - reason non-string (241-245): invalid=true.
func TestCheckFailedResponseReason_Branches(t *testing.T) {
	if c := checkFailedResponseReason(nil); c.failed {
		t.Fatalf("nil payload should not be failed")
	}
	if c := checkFailedResponseReason([]byte("{bad")); c.failed {
		t.Fatalf("malformed payload should not be failed")
	}
	if c := checkFailedResponseReason([]byte(`{}`)); c.failed {
		t.Fatalf("no status should not be failed")
	}
	// status present but not failed (e.g. completed) → status unmarshal ok, !=failed.
	if c := checkFailedResponseReason([]byte(`{"status":"completed"}`)); c.failed {
		t.Fatalf("completed should not be failed")
	}
	// status non-string → unmarshal into string fails → not failed.
	if c := checkFailedResponseReason([]byte(`{"status":123}`)); c.failed {
		t.Fatalf("non-string status should not be failed")
	}
	// failed, no reason → failed=true, hasReason=false, invalid=false.
	c := checkFailedResponseReason([]byte(`{"status":"failed"}`))
	if !c.failed || c.hasReason || c.invalid {
		t.Fatalf("failed-no-reason = %+v", c)
	}
	// failed, reason non-string → invalid=true.
	c = checkFailedResponseReason([]byte(`{"status":"failed","reason":123}`))
	if !c.invalid || c.detail == "" {
		t.Fatalf("failed-nonstring-reason = %+v, want invalid", c)
	}
	// failed, reason out of closed set → invalid via terminalFailureReasonAllowed.
	c = checkFailedResponseReason([]byte(`{"status":"failed","reason":"not_a_reason"}`))
	if !c.invalid {
		t.Fatalf("failed-bad-reason should be invalid: %+v", c)
	}
	// failed, valid reason → valid.
	c = checkFailedResponseReason([]byte(`{"status":"failed","reason":"receiver_internal_error"}`))
	if c.invalid || c.reason != "receiver_internal_error" {
		t.Fatalf("failed-good-reason = %+v", c)
	}
}

// extractPayloadStatus arms: len==0 (341-343) and malformed JSON (345-347).
func TestExtractPayloadStatus_Branches(t *testing.T) {
	if _, ok := extractPayloadStatus(nil); ok {
		t.Fatalf("empty payload should be ok=false")
	}
	if _, ok := extractPayloadStatus([]byte("{nope")); ok {
		t.Fatalf("malformed payload should be ok=false")
	}
	if _, ok := extractPayloadStatus([]byte(`{}`)); ok {
		t.Fatalf("missing status should be ok=false")
	}
	if _, ok := extractPayloadStatus([]byte(`{"status":5}`)); ok {
		t.Fatalf("non-string status should be ok=false")
	}
	s, ok := extractPayloadStatus([]byte(`{"status":"processing"}`))
	if !ok || s != "processing" {
		t.Fatalf("extract = %q %v", s, ok)
	}
}

// senderLocalName fallback when no ':' present (372).
func TestSenderLocalName_NoColonFallback(t *testing.T) {
	if got := senderLocalName(actor.ActorID("daemon")); got != "daemon" {
		t.Fatalf("no-colon = %q, want daemon", got)
	}
	if got := senderLocalName(actor.ActorID("a:b:c")); got != "c" {
		t.Fatalf("nested = %q, want c", got)
	}
}
