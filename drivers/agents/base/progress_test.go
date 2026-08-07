package base

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
)

func TestProgressTransitionPoints(t *testing.T) {
	t.Run("idle direct has processing without queued", func(t *testing.T) {
		l, _ := newUnitLoop()
		l.enqueue(bufferedMsg("direct", "actor:a", 1), false)
		steps := l.sys.(*testSys).steps
		if len(steps) != 1 || steps[0].status != message.StatusProcessing {
			t.Fatalf("steps=%#v", steps)
		}
	})
	t.Run("rejection to buffer writes queued again", func(t *testing.T) {
		l, _ := newUnitLoop()
		l.state, l.turnID, l.active = stateTurnActive, "turn", bufferedMsg("old", "actor:a", 1)
		item := bufferedMsg("steer", "actor:a", 1)
		l.acceptContent(item, false)
		var op OpID
		for id := range l.committing {
			op = id
		}
		l.controlDone(providerEvent{op: op, verdict: ControlNotSteerable, turnID: "turn"})
		steps := l.sys.(*testSys).steps
		if len(steps) != 2 || steps[0].status != message.StatusProcessing || steps[1].status != message.StatusQueued {
			t.Fatalf("steps=%#v", steps)
		}
	})
	t.Run("accepted adds no progress", func(t *testing.T) {
		l, _ := newUnitLoop()
		l.state, l.turnID, l.active = stateTurnActive, "turn", bufferedMsg("old", "actor:a", 1)
		item := bufferedMsg("steer", "actor:a", 1)
		l.acceptContent(item, false)
		var op OpID
		for id := range l.committing {
			op = id
		}
		before := len(l.sys.(*testSys).steps)
		l.controlDone(providerEvent{op: op, verdict: ControlAccepted, turnID: "turn"})
		if len(l.sys.(*testSys).steps) != before {
			t.Fatalf("accepted added progress: %#v", l.sys.(*testSys).steps)
		}
	})
}

func TestMergedPrefixNeverGetsProcessing(t *testing.T) {
	l, _ := newUnitLoop()
	l.state = stateStarting
	l.enqueue(bufferedMsg("prefix", "actor:a", 1), true)
	l.enqueue(bufferedMsg("tail", "actor:a", 1), true)
	l.state = stateIdle
	l.startNext()
	for _, step := range l.sys.(*testSys).steps {
		if step.id == "prefix" && step.status == message.StatusProcessing {
			t.Fatalf("prefix got processing: %#v", l.sys.(*testSys).steps)
		}
	}
}
