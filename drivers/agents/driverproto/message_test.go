package driverproto

import (
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/runtime/harness"
)

// The from line is the model's only statement of who is asking, and the seed
// segment means two different things by kind. Labelling an agent's declaration
// "principal" would tell the model a declaration is a login account; labelling
// a human's principal "declaration" hides the one fact that is stable across
// channels. Either way the model reasons about the sender from a false premise.
func TestCallerLineNamesSeedByKind(t *testing.T) {
	human := CallerLine(harness.Caller{Channel: "c0", Actor: "human:root:1787128257816"})
	for _, want := range []string{"channel=c0", "kind=human", "principal=root", "actor=human:root:1787128257816"} {
		if !strings.Contains(human, want) {
			t.Errorf("human from line missing %q: %s", want, human)
		}
	}
	if strings.Contains(human, "declaration=") {
		t.Errorf("human principal labelled as a declaration: %s", human)
	}

	agent := CallerLine(harness.Caller{Channel: "c0", Actor: "agent:steward-decl:1787128257816"})
	if !strings.Contains(agent, "declaration=steward-decl") {
		t.Errorf("agent declaration not labelled: %s", agent)
	}
	if strings.Contains(agent, "principal=") {
		t.Errorf("agent declaration labelled as a principal: %s", agent)
	}

	// The fixed system actor has no segments; it must still be nameable.
	system := CallerLine(harness.Caller{Channel: "c0", Actor: "system"})
	if !strings.Contains(system, "actor=system") || strings.Contains(system, "kind=") {
		t.Errorf("system caller line invented segments it does not have: %s", system)
	}
}
