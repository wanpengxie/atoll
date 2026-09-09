package native

import (
	"time"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

const pulseType = "agent.internal.pulse"

func (c *controller) pulse(sys actorbase.Sys, msg actorbase.Msg) {
	if msg.Sender.ID != sys.Self() {
		_, _ = sys.Fail(msg, "permission_denied", "internal pulse")
		return
	}
	now := nowMillis()
	for _, s := range c.sessions {
		if s.Freeze == "hold" && now >= s.HoldUntil {
			if s.RestoreInterrupt {
				s.Freeze = "interrupt"
			} else {
				s.Freeze = ""
			}
			s.RestoreInterrupt = false
		}
		if w := c.data.Works[string(s.Owner)]; w != nil && s.Execution != "" && now-w.UpdatedAt > (5*time.Minute).Milliseconds() {
			// No execution heartbeat: end the work honestly. Never recreate an
			// assignment in a new incarnation or resend a possibly executed tool.
			s.Freeze = "interrupt"
			s.Rebuffer = false
			_ = c.stop(sys, msg, w, false)
			w.State = agentproto.WorkClosed
			w.Stage = ""
			w.Outcome = agentproto.OutcomeFailed
			w.ExecutionState = "execution_unknown"
			w.Result = mustJSON(map[string]any{"error_code": "execution_unavailable", "detail": "execution stopped reporting; external effects unknown; no replay"})
			_ = c.commit(sys, msg.Cause(), w)
			c.finishWaiters(sys, w)
			s.Execution = ""
			s.Owner = ""
		}
	}
	for id, waiters := range c.wait {
		w := c.data.Works[string(id)]
		if w == nil || w.State != agentproto.WorkOpen {
			continue
		}
		for _, waiting := range waiters {
			if waiting.Ctx().Err() == nil {
				_, _ = sys.Progress(waiting, "processing", map[string]any{"work_id": w.ID, "view_id": w.ViewID, "stage": w.Stage})
			}
		}
	}
	c.scheduleQueued(sys, msg.Cause())
	_, _ = sys.Reply(msg, map[string]any{"disposition": "observed"})
	_, _ = sys.After(10*time.Second, pulseType, map[string]any{}, schedule.TimerHomeMemory)
}
