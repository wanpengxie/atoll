package channelspec

import "testing"

func TestHousekeepingWord(t *testing.T) {
	for _, word := range []string{"actor.describe", "agent.context", "agent.hold", "agent.interrupt", "agent.select", "system.log.recent", "system.member.list"} {
		if !HousekeepingWord(word) {
			t.Errorf("%s should be housekeeping", word)
		}
	}
	for _, word := range []string{"agent.ask", "human.message", "human.note", "human.approve", "agent.replace", "agent.queue", "code.run", "echo.say", "github.issue_get"} {
		if HousekeepingWord(word) {
			t.Errorf("%s is conversation, not housekeeping", word)
		}
	}
}
