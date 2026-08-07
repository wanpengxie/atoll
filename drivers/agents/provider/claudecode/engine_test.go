package claudecode

import (
	"testing"

	claude "github.com/wanpengxie/go-claude-agent-sdk"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

func TestComposeUserInputLabelsSender(t *testing.T) {
	env := message.Envelope{Sender: message.Sender{ID: actor.ActorID("human:a")}, Payload: []byte(`{"text":"hello"}`)}
	if got := composeUserInput(env); got != "[from human:a]\nhello" {
		t.Fatalf("compose = %q", got)
	}
}

func TestClassifyAssistantErrorClosed(t *testing.T) {
	if got := classifyAssistantError(claude.AssistantErrorRateLimit); got != "llm_rate_limit" {
		t.Fatalf("rate limit = %q", got)
	}
	if got := classifyAssistantError(claude.AssistantMessageError("future")); got != "llm_unknown" {
		t.Fatalf("unknown = %q", got)
	}
}
