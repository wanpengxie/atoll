package codex

import (
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/base"
)

func TestControlErrorClassification(t *testing.T) {
	cases := map[string]base.ControlVerdict{"no active turn": base.ControlNoActiveTurn, "expected turn id mismatch": base.ControlMismatch, "empty input": base.ControlEmptyInput, "future provider text": base.ControlRPCError}
	for text, want := range cases {
		if got := controlVerdict(errors.New(text)); got != want {
			t.Fatalf("%q=%q want %q", text, got, want)
		}
	}
}
func TestResumeErrorPatterns(t *testing.T) {
	for _, text := range []string{
		"no rollout found for thread",
		"thread not found",
		"conversation not found",
		"session abc is archived",
	} {
		if !isInvalidResumeError(errors.New(text)) {
			t.Fatalf("%q not classified as invalid resume", text)
		}
	}
	if !isClosingError(errors.New("thread is closing")) {
		t.Fatal("closing not classified")
	}
}
