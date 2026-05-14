package supervisor_test

import (
	"errors"
	"testing"

	"github.com/wanpengxie/ActOS/runtime/supervisor"
)

func TestLifecycle_StateMachine(t *testing.T) {
	l := supervisor.NewLifecycle()
	l.MarkRunning("ch-1", "agent:a")
	if e, ok := l.Get("ch-1", "agent:a"); !ok || e.State != supervisor.ActorRunning {
		t.Errorf("state = %+v ok=%v", e, ok)
	}
	if !l.HasActive("ch-1") {
		t.Error("HasActive should be true")
	}
	l.MarkFaulted("ch-1", "agent:a", errors.New("boom"))
	if e, _ := l.Get("ch-1", "agent:a"); e.State != supervisor.ActorFaulted || e.LastError != "boom" {
		t.Errorf("post-fault: %+v", e)
	}
	if l.HasActive("ch-1") {
		t.Error("faulted actor should not count active")
	}
	l.MarkStopped("ch-1", "agent:a")
	if e, _ := l.Get("ch-1", "agent:a"); e.State != supervisor.ActorStopped {
		t.Errorf("post-stop: %+v", e)
	}
	if got := len(l.Snapshot()); got != 1 {
		t.Errorf("snapshot len = %d", got)
	}
}
