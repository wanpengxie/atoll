package cmd_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/adapters/cmd"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

type respondCall struct {
	correlation adapter.CorrelationKey
	payload     json.RawMessage
	options     adapter.RespondOptions
}

type fakeMCtx struct {
	mu    sync.Mutex
	calls []respondCall
}

func (f *fakeMCtx) ctx() *adapter.ModuleContext {
	return &adapter.ModuleContext{
		AdapterActorID: cmd.DefaultAdapterActorID,
		Respond: func(_ context.Context, key adapter.CorrelationKey, payload json.RawMessage, opts adapter.RespondOptions) (adapter.RespondResult, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.calls = append(f.calls, respondCall{correlation: key, payload: append(json.RawMessage(nil), payload...), options: opts})
			return adapter.RespondResult{}, nil
		},
		Correlation: noopCorrelation{},
		ErrorPolicy: noopPolicy{},
	}
}

func (f *fakeMCtx) last() respondCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		panic("no Respond calls captured")
	}
	return f.calls[len(f.calls)-1]
}

type noopCorrelation struct{}

func (noopCorrelation) Reserve(_ context.Context, e adapter.CorrelationEntry) (adapter.CorrelationEntry, error) {
	return e, nil
}
func (noopCorrelation) Get(_ context.Context, _ adapter.CorrelationKey) (adapter.CorrelationEntry, bool, error) {
	return adapter.CorrelationEntry{}, false, nil
}
func (noopCorrelation) MarkDone(_ context.Context, _ adapter.CorrelationKey) error    { return nil }
func (noopCorrelation) MarkExpired(_ context.Context, _ adapter.CorrelationKey) error { return nil }
func (noopCorrelation) MarkRejected(_ context.Context, _ adapter.CorrelationKey, _ string) error {
	return nil
}
func (noopCorrelation) ListPending(_ context.Context) ([]adapter.CorrelationEntry, error) {
	return nil, nil
}

type noopPolicy struct{}

func (noopPolicy) RegisterTimer(_ context.Context, _ adapter.CorrelationKey, _ time.Time) error {
	return nil
}
func (noopPolicy) CancelTimer(_ context.Context, _ adapter.CorrelationKey) error { return nil }
func (noopPolicy) OnExternalError(_ context.Context, _ adapter.CorrelationKey, _ message.TerminalFailureReason, _ string) error {
	return nil
}

func TestModule_Exec_Allowed(t *testing.T) {
	t.Parallel()
	m := cmd.New(cmd.Config{
		AllowedBinaries: []string{"echo"},
	})
	f := &fakeMCtx{}
	if err := m.Init(context.Background(), f.ctx()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	env := &message.Envelope{
		ID:      "req-1",
		Kind:    message.KindRequest,
		Type:    cmd.TypeExec,
		Payload: json.RawMessage(`{"binary":"echo","args":["hello"]}`),
	}
	if err := m.Handle(context.Background(), env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	call := f.last()
	if call.options.Status != "completed" {
		t.Fatalf("status=%q want completed (payload=%s)", call.options.Status, string(call.payload))
	}
	var resp cmd.ExecResponse
	if err := json.Unmarshal(call.payload, &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Stdout != "hello\n" {
		t.Fatalf("stdout=%q want \"hello\\n\"", resp.Stdout)
	}
	if resp.ExitCode != 0 {
		t.Fatalf("exit_code=%d want 0", resp.ExitCode)
	}
}

func TestModule_Exec_NonZeroExitCode_StillCompletes(t *testing.T) {
	t.Parallel()
	m := cmd.New(cmd.Config{AllowedBinaries: []string{"false"}})
	f := &fakeMCtx{}
	_ = m.Init(context.Background(), f.ctx())
	env := &message.Envelope{
		ID:      "req-false",
		Kind:    message.KindRequest,
		Type:    cmd.TypeExec,
		Payload: json.RawMessage(`{"binary":"false"}`),
	}
	if err := m.Handle(context.Background(), env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	call := f.last()
	// Non-zero exit_code is a success-shaped terminal — the binary ran.
	if call.options.Status != "completed" {
		t.Fatalf("status=%q want completed", call.options.Status)
	}
	var resp cmd.ExecResponse
	_ = json.Unmarshal(call.payload, &resp)
	if resp.ExitCode == 0 {
		t.Fatalf("exit_code=0 want non-zero")
	}
}

func TestModule_Exec_BinaryNotAllowed(t *testing.T) {
	t.Parallel()
	m := cmd.New(cmd.Config{AllowedBinaries: []string{"echo"}})
	f := &fakeMCtx{}
	_ = m.Init(context.Background(), f.ctx())
	env := &message.Envelope{
		ID:      "req-deny",
		Kind:    message.KindRequest,
		Type:    cmd.TypeExec,
		Payload: json.RawMessage(`{"binary":"rm","args":["-rf","/"]}`),
	}
	if err := m.Handle(context.Background(), env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	call := f.last()
	if call.options.Status != "failed" {
		t.Fatalf("status=%q want failed", call.options.Status)
	}
	var p map[string]any
	_ = json.Unmarshal(call.payload, &p)
	if p["error_code"] != "binary_not_allowed" {
		t.Fatalf("error_code=%v want binary_not_allowed", p["error_code"])
	}
}

func TestModule_Exec_EmptyAllowlist_FailsClosed(t *testing.T) {
	t.Parallel()
	m := cmd.New(cmd.Config{AllowedBinaries: []string{}}) // explicit empty != nil
	f := &fakeMCtx{}
	_ = m.Init(context.Background(), f.ctx())
	env := &message.Envelope{
		ID:      "req-closed",
		Kind:    message.KindRequest,
		Type:    cmd.TypeExec,
		Payload: json.RawMessage(`{"binary":"echo"}`),
	}
	_ = m.Handle(context.Background(), env)
	call := f.last()
	if call.options.Status != "failed" {
		t.Fatalf("empty allowlist must fail-closed; got status=%q", call.options.Status)
	}
}

func TestModule_Exec_Timeout(t *testing.T) {
	t.Parallel()
	m := cmd.New(cmd.Config{
		AllowedBinaries: []string{"sleep"},
		MaxPendingMs:    1_000,
	})
	f := &fakeMCtx{}
	_ = m.Init(context.Background(), f.ctx())
	env := &message.Envelope{
		ID:      "req-timeout",
		Kind:    message.KindRequest,
		Type:    cmd.TypeExec,
		Payload: json.RawMessage(`{"binary":"sleep","args":["3"],"timeout_ms":300}`),
	}
	start := time.Now()
	_ = m.Handle(context.Background(), env)
	elapsed := time.Since(start)
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("timeout took %v, want <1.5s", elapsed)
	}
	call := f.last()
	if call.options.Status != "failed" {
		t.Fatalf("status=%q want failed", call.options.Status)
	}
	var p map[string]any
	_ = json.Unmarshal(call.payload, &p)
	if p["error_code"] != "exec_timeout" {
		t.Fatalf("error_code=%v want exec_timeout", p["error_code"])
	}
}

func TestModule_Exec_BinaryNotFound(t *testing.T) {
	t.Parallel()
	m := cmd.New(cmd.Config{
		AllowedBinaries: []string{"definitely-not-a-real-binary-xyz"},
		LookPath: func(name string) (string, error) {
			return "", errors.New("not found")
		},
	})
	f := &fakeMCtx{}
	_ = m.Init(context.Background(), f.ctx())
	env := &message.Envelope{
		ID:      "req-nf",
		Kind:    message.KindRequest,
		Type:    cmd.TypeExec,
		Payload: json.RawMessage(`{"binary":"definitely-not-a-real-binary-xyz"}`),
	}
	_ = m.Handle(context.Background(), env)
	call := f.last()
	if call.options.Status != "failed" {
		t.Fatalf("status=%q want failed", call.options.Status)
	}
	var p map[string]any
	_ = json.Unmarshal(call.payload, &p)
	if p["error_code"] != "binary_not_found" {
		t.Fatalf("error_code=%v want binary_not_found", p["error_code"])
	}
}

func TestModule_Which(t *testing.T) {
	t.Parallel()
	m := cmd.New(cmd.Config{AllowedBinaries: []string{"echo"}})
	f := &fakeMCtx{}
	_ = m.Init(context.Background(), f.ctx())
	env := &message.Envelope{
		ID:      "req-which",
		Kind:    message.KindRequest,
		Type:    cmd.TypeWhich,
		Payload: json.RawMessage(`{"binary":"echo"}`),
	}
	if err := m.Handle(context.Background(), env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	call := f.last()
	if call.options.Status != "completed" {
		t.Fatalf("status=%q want completed", call.options.Status)
	}
	var resp cmd.WhichResponse
	_ = json.Unmarshal(call.payload, &resp)
	if !resp.Allowed {
		t.Fatalf("allowed=false want true")
	}
	if resp.Path == "" {
		t.Fatalf("path empty (echo should be on PATH)")
	}
}

func TestModule_Declares_AllTypesInTypeDeclarations(t *testing.T) {
	t.Parallel()
	m := cmd.New(cmd.Config{})
	decl := m.Declares()
	if len(decl.TypeDeclarations) != len(cmd.AllTypes) {
		t.Fatalf("TypeDeclarations count=%d want %d", len(decl.TypeDeclarations), len(cmd.AllTypes))
	}
	for _, ty := range cmd.AllTypes {
		td, ok := decl.TypeDeclarations[ty]
		if !ok {
			t.Errorf("type %s missing from TypeDeclarations", ty)
			continue
		}
		if td.Description == "" {
			t.Errorf("type %s missing Description", ty)
		}
		if len(td.PayloadExample) == 0 {
			t.Errorf("type %s missing PayloadExample", ty)
		}
	}
	if decl.Description == "" || decl.SkillDoc == "" {
		t.Errorf("Declaration missing Description / SkillDoc")
	}
}
