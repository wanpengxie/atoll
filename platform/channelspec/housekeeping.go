package channelspec

import "strings"

// HousekeepingWord reports whether a request word is bookkeeping rather than
// conversation: capability probes, agent controls, and system-door verbs.
//
// These rows are legitimate ledger facts, but they are not what a reader
// scrolls back to see, and the timeline never renders them as turns. They
// must therefore never count as "complete root turns" when a history window
// decides it has read far enough, and they never ride a history page at all —
// a quiet channel's last hundreds of rows can be nothing but actor.describe /
// agent.context / system.log.recent, and a window bounded by those delivers a
// first screen with no conversation on it while claiming twenty turns.
func HousekeepingWord(word string) bool {
	switch word {
	case "actor.describe",
		"agent.context", "agent.hold", "agent.unhold", "agent.interrupt",
		"agent.fork", "agent.select", "agent.new", "agent.steer", "agent.compact":
		return true
	}
	return strings.HasPrefix(word, "system.")
}
