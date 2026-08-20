package behavior

import (
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

func causeClock() func() time.Time {
	at := time.UnixMilli(1_700_000_000_000)
	return func() time.Time { return at }
}

// The builders are where the cause stops being optional. Everything upstream
// can be careless; nothing gets onto the ledger without answering.
func TestABuilderRefusesAnEnvelopeThatWillNotSayWhyItExists(t *testing.T) {
	if _, err := BuildRequest(causeClock(), RequestSpec{
		Type: "demo.word", Audience: message.Audience{"agent:demo:1"},
	}); err == nil {
		t.Fatal("BuildRequest accepted a request with no cause")
	}
	if _, err := BuildEvent(causeClock(), EventSpec{Type: "demo.happened"}); err == nil {
		t.Fatal("BuildEvent accepted an event with no cause")
	}
	// A broken anchor is silence too, not a root.
	if _, err := BuildRequest(causeClock(), RequestSpec{
		Type: "demo.word", Audience: message.Audience{"agent:demo:1"},
		Cause: message.Anchored("", "some-errand"),
	}); err == nil {
		t.Fatal("BuildRequest accepted an anchor with no parent")
	}
}

// Whatever a builder does produce carries the derivation, not two fields the
// caller filled in separately and could have disagreed on.
func TestBuiltEnvelopesCarryTheCausesDerivation(t *testing.T) {
	root, err := BuildRequest(causeClock(), RequestSpec{
		Type: "demo.word", Audience: message.Audience{"agent:demo:1"}, Cause: message.Root(),
	})
	if err != nil {
		t.Fatalf("BuildRequest(root): %v", err)
	}
	if root.ParentID != "" || root.CorrelationID != root.ID {
		t.Fatalf("root built as parent %q correlation %q", root.ParentID, root.CorrelationID)
	}

	child, err := BuildRequest(causeClock(), RequestSpec{
		Type: "demo.word", Audience: message.Audience{"agent:demo:1"}, Cause: message.From(*root),
	})
	if err != nil {
		t.Fatalf("BuildRequest(child): %v", err)
	}
	if child.ParentID != root.ID || child.CorrelationID != root.ID {
		t.Fatalf("child built as parent %q correlation %q", child.ParentID, child.CorrelationID)
	}

	// A response's cause is never in doubt, and it goes through the same
	// derivation rather than a second copy of it.
	child.Sender.ID = "agent:demo:1"
	answer, err := BuildResponseFromRequest(child, causeClock(), ResponseSpec{Status: message.StatusCompleted})
	if err != nil {
		t.Fatalf("BuildResponseFromRequest: %v", err)
	}
	if answer.ParentID != child.ID || answer.CorrelationID != root.ID {
		t.Fatalf("response built as parent %q correlation %q", answer.ParentID, answer.CorrelationID)
	}
}
