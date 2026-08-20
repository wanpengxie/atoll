package behavior

import (
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

func builderClock() time.Time { return time.UnixMilli(1_000) }

func TestBuildRequest(t *testing.T) {
	env, err := BuildRequest(builderClock, RequestSpec{
		Type:     "x.y",
		Audience: message.Audience{"tool:t"},
		Cause:    message.Anchored("p1", "p1"),
		Payload:  []byte(`{"body":null}`),
	})
	if err != nil {
		t.Fatalf("BuildRequest: %v", err)
	}
	if env.Kind != message.KindRequest || env.Type != "x.y" || env.TS != 1_000 ||
		env.ParentID != "p1" || env.ID == "" {
		t.Fatalf("request env = %+v", env)
	}
	// Sender / ChannelID are substrate-injected by the pen, not filled by the
	// builder — they MUST be zero on the freshly built envelope (sealed-pen).
	if env.Sender != (message.Sender{}) {
		t.Fatalf("request sender = %+v, want zero (pen-injected)", env.Sender)
	}
	if env.ChannelID != "" {
		t.Fatalf("request channel_id = %q, want empty (pen-injected)", env.ChannelID)
	}

	// Required fields enforced. Each case supplies every OTHER required field so
	// the error it observes is the one it names.
	if _, err := BuildRequest(builderClock, RequestSpec{Audience: message.Audience{"a"}, Cause: message.Root()}); err == nil {
		t.Fatal("missing type: want error")
	}
	if _, err := BuildRequest(builderClock, RequestSpec{Type: "x", Cause: message.Root()}); err == nil {
		t.Fatal("missing audience: want error")
	}
	// A zero Cause is silence, not a root: the builder refuses it rather than
	// guessing that a caller who said nothing meant "this starts here".
	if _, err := BuildRequest(builderClock, RequestSpec{Type: "x", Audience: message.Audience{"a"}}); err == nil {
		t.Fatal("missing cause: want error")
	}

	// Caller id scheme override.
	env, _ = BuildRequest(builderClock, RequestSpec{
		Type: "x", Audience: message.Audience{"a"}, ID: "my-id", Cause: message.Root(), Payload: []byte(`{"body":null}`),
	})
	if env.ID != "my-id" {
		t.Fatalf("id override = %q", env.ID)
	}
}

func TestBuildEvent(t *testing.T) {
	env, err := BuildEvent(builderClock, EventSpec{
		Type: "agent.text", Cause: message.Anchored("p1", "c1"),
	})
	if err != nil {
		t.Fatalf("BuildEvent: %v", err)
	}
	if env.Kind != message.KindEvent || env.ParentID != "p1" || env.CorrelationID != "c1" || env.ID == "" {
		t.Fatalf("event env = %+v", env)
	}
	// Sender / ChannelID are pen-injected, not builder-filled (sealed-pen).
	if env.Sender != (message.Sender{}) {
		t.Fatalf("event sender = %+v, want zero (pen-injected)", env.Sender)
	}
	if env.ChannelID != "" {
		t.Fatalf("event channel_id = %q, want empty (pen-injected)", env.ChannelID)
	}
	if _, err := BuildEvent(builderClock, EventSpec{Cause: message.Root()}); err == nil {
		t.Fatal("missing type: want error")
	}
	// Same law as a request's: an event with no stated cause is refused rather
	// than silently rooted.
	if _, err := BuildEvent(builderClock, EventSpec{Type: "agent.text"}); err == nil {
		t.Fatal("missing cause: want error")
	}
}
