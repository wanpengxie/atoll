package harness

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// ---------------------------------------------------------------------
// Injectable stubs — the only seams the frozen harness exposes for the
// error / defensive branches: a Registry and a MessageLog whose method
// behaviour is supplied per-test. We never touch production .go.
// ---------------------------------------------------------------------

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

// ---------------------------------------------------------------------
// deps.go — Validate + New defaults
// ---------------------------------------------------------------------

func TestDeps_ValidateMissingFields(t *testing.T) {
	if err := (Deps{}).Validate(); err == nil {
		t.Fatalf("empty Deps should fail Validate")
	}
	// ChannelID set, Log nil → missing-Log branch.
	if err := (Deps{ChannelID: testChannelID}).Validate(); err == nil {
		t.Fatalf("missing Log should fail")
	}
	// Fully wired → nil. (No ActorRegistry dep — the sender door trusts the
	// pen weld.)
	lg := stubLog{}
	if err := (Deps{ChannelID: testChannelID, Log: lg}).Validate(); err != nil {
		t.Fatalf("fully-wired Deps Validate = %v, want nil", err)
	}
}

// New must fill NowMs / Logger defaults when nil.
func TestNew_FillsDefaults(t *testing.T) {
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{Seq: 1}, nil
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	// NowMs / Logger nil → defaults filled.
	m, err := New(Deps{ChannelID: testChannelID, Log: lg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c := m.(*minter).chain
	// Drive a successful write so the default NowMs is actually invoked and a
	// real (non-test) clock value lands in ts_received.
	e := validEvent("m-default", "agent:p")
	res, err := c.write(ctxCallerKind("agent:p", actor.KindAgent), e)
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
// chain.go — stepName default, observe paths, panic recover,
// step-error path, append-error paths
// ---------------------------------------------------------------------

func TestStepName_DefaultUnknownID(t *testing.T) {
	if got := stepName(stepID(99)); got != "step_99" {
		t.Fatalf("stepName(99) = %q, want step_99", got)
	}
}

// chainWith builds the internal chain with stub deps.
func chainWith(t *testing.T, lg storespec.MessageLog) *chain {
	t.Helper()
	m, err := New(Deps{
		ChannelID: testChannelID,
		Log:       lg,
		NowMs:     func() int64 { return fixedNowMs },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m.(*minter).chain
}

// A step that returns a hard error (not a reject) → Write maps it through
// observeError (chain.go step-error path) and returns the wrapped error. We
// trigger this at StepResponsePairing by making Log.FindByID fail for the
// parent lookup — StepSenderConsistent no longer has an error-producing seam
// of its own (no registry lookup left: identity is pen-welded + livePen-gated,
// not registry-checked).
func TestWrite_StepError_ObservedAndReturned(t *testing.T) {
	findErr := errors.New("boom-find")
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, nil
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, findErr },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c := chainWith(t, lg)

	resp := makeResponse("")
	resp.Payload = []byte(`{"status":"completed"}`)
	res, err := c.write(ctxCallerKind("tool:xhs", actor.KindTool), resp)
	if err == nil {
		t.Fatalf("expected wrapped step error, got nil (res=%+v)", res)
	}
	if !errors.Is(err, findErr) {
		t.Fatalf("error = %v, want wrapping %v", err, findErr)
	}
}

// Append returns a typed *storespec.AppendError → mapped to a closed-set
// reject via observeReject (chain.go:108-116 + 141-157).
func TestWrite_AppendTypedError_MapsToReject(t *testing.T) {
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
	c := chainWith(t, lg)

	res, err := c.write(ctxCallerKind("agent:p", actor.KindAgent), validEvent("m-app", "agent:p"))
	if err != nil {
		t.Fatalf("typed AppendError should be a reject, not error: %v", err)
	}
	if res.Accepted() || res.RejectReason != HarnessTerminalDuplicate {
		t.Fatalf("reject = %q accepted=%v, want terminal_duplicate", res.RejectReason, res.Accepted())
	}
	if res.MessageID != "m-partial" {
		t.Fatalf("MessageID = %q, want m-partial", res.MessageID)
	}
}

// Append returns a PLAIN error (not *AppendError) → chain.go:117-118 wraps it
// and observeError logs it.
func TestWrite_AppendPlainError_WrappedAsError(t *testing.T) {
	appendErr := errors.New("disk on fire")
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, appendErr
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c := chainWith(t, lg)

	_, err := c.write(ctxCallerKind("agent:p", actor.KindAgent), validEvent("m-plain", "agent:p"))
	if err == nil || !errors.Is(err, appendErr) {
		t.Fatalf("plain append error = %v, want wrapping %v", err, appendErr)
	}
}

// A step that panics → Write's deferred recover converts it to an error. We
// make Log.FindByID panic at StepResponsePairing (StepSenderConsistent no
// longer has a seam to panic through — no registry lookup left).
func TestWrite_PanicRecovered(t *testing.T) {
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, nil
		},
		findByID: func(context.Context, message.ID) (*storespec.StoredRow, bool, error) {
			panic("step blew up")
		},
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c := chainWith(t, lg)

	resp := makeResponse("")
	resp.Payload = []byte(`{"status":"completed"}`)
	_, err := c.write(ctxCallerKind("tool:xhs", actor.KindTool), resp)
	if err == nil {
		t.Fatalf("panic should be recovered into an error")
	}
}

// observeReject's reason=="" early-return (chain.go:142-144) is reachable only
// through the engine-append path: a typed *AppendError carrying an EMPTY Reason
// makes Chain.Write call observeReject with reason "".
func TestWrite_AppendEmptyReason_ObserveRejectNoOp(t *testing.T) {
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{}, &storespec.AppendError{Reason: "", Detail: "no reason"}
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	c := chainWith(t, lg)

	res, err := c.write(ctxCallerKind("agent:p", actor.KindAgent), validEvent("m-empty", "agent:p"))
	if err != nil {
		t.Fatalf("empty-reason AppendError should map to a WriteResult, got err=%v", err)
	}
	// An empty Reason produces a WriteResult with empty RejectReason, so
	// Accepted() is degenerately true — but the partial message id is carried
	// and observeReject returns immediately for an empty reason.
	if res.MessageID != "" {
		t.Fatalf("empty-reason result MessageID = %q, want empty (no PartialMessageID set)", res.MessageID)
	}
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
// step_sender_consistent.go — ctx canceled (final guard). The Lookup-error
// seam this section used to cover is gone: the step no longer calls
// ActorRegistry.Lookup at all — identity is pen-welded + livePen-gated one
// layer up, not registry-checked here. That correctness now lives in
// platform/internal/link/livepen_test.go (ErrWriterNotLive).
// ---------------------------------------------------------------------

func TestStepSenderConsistent_CtxCanceled(t *testing.T) {
	deps := Deps{ChannelID: testChannelID}
	ctx, cancel := context.WithCancel(ctxCallerKind("agent:p", actor.KindAgent))
	cancel() // canceled before run → final guard returns ctx.Err()
	env := validEvent("m1", "agent:p")
	_, err := runStep(t, newStepSenderConsistent, deps, ctx, env)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
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
