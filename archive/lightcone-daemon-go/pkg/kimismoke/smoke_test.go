package kimismoke

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestRun_EchoProvider exercises the M1.3-T0 acceptance check:
// "能跑通 go-kimi examples/01_basic_turn 改编版".
//
// We swap OpenAI for the in-process echo provider so the smoke runs in CI
// without any API key or network access. A successful Run proves that
// NewAgent + Run + LastResult + Close link cleanly against the pinned
// go-kimi version — which is the whole point of T0's "工程化" deliverable.
func TestRun_EchoProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := Run(ctx, Options{
		Prompt:  "hello daemon-go",
		WorkDir: t.TempDir(),
		Model:   "echo-smoke-test",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if res == nil {
		t.Fatal("Run() returned nil result")
	}
	if res.Model != "echo-smoke-test" {
		t.Fatalf("Result.Model = %q, want %q", res.Model, "echo-smoke-test")
	}
	if strings.TrimSpace(res.Reply) == "" {
		t.Fatalf("Result.Reply is empty; want non-empty echo response")
	}
}

// TestRun_DefaultsApplied checks that callers can invoke Run with a
// zero-valued Options and still get a usable result back. This keeps the
// integration seam ergonomic for higher-level callers (cmd/worker, T10
// worker runtime).
func TestRun_DefaultsApplied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := Run(ctx, Options{WorkDir: t.TempDir()})
	if err != nil {
		t.Fatalf("Run() with defaults error = %v", err)
	}
	if res == nil || strings.TrimSpace(res.Reply) == "" {
		t.Fatalf("expected non-empty reply, got %+v", res)
	}
	if res.Model == "" {
		t.Fatalf("expected default model, got empty string")
	}
}

// TestRun_NilContextRejected guards the contract that callers always
// pass a context, since the embedded daemon-go worker (T10) will always
// have one.
func TestRun_NilContextRejected(t *testing.T) {
	//nolint:staticcheck // SA1012: passing nil context is the intended unit-under-test.
	if _, err := Run(nil, Options{}); err == nil {
		t.Fatal("Run(nil ctx) error = nil, want non-nil")
	}
}
