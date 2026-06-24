package behavior

import (
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/protocol/message"
)

func builderClock() time.Time { return time.UnixMilli(1_000) }

func TestBuildRequest(t *testing.T) {
	env, err := BuildRequest(builderClock, RequestSpec{
		Type:     "x.y",
		Audience: message.Audience{"tool:t"},
		ParentID: "p1",
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

	// Required fields enforced.
	if _, err := BuildRequest(builderClock, RequestSpec{Audience: message.Audience{"a"}}); err == nil {
		t.Fatal("missing type: want error")
	}
	if _, err := BuildRequest(builderClock, RequestSpec{Type: "x"}); err == nil {
		t.Fatal("missing audience: want error")
	}

	// Caller id scheme override.
	env, _ = BuildRequest(builderClock, RequestSpec{
		Type: "x", Audience: message.Audience{"a"}, ID: "my-id",
	})
	if env.ID != "my-id" {
		t.Fatalf("id override = %q", env.ID)
	}
}

func TestBuildEvent(t *testing.T) {
	env, err := BuildEvent(builderClock, EventSpec{
		Type: "agent.text", ParentID: "p1", CorrelationID: "c1",
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
	if _, err := BuildEvent(builderClock, EventSpec{}); err == nil {
		t.Fatal("missing type: want error")
	}
}
