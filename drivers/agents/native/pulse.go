package native

import (
	"sort"
	"time"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

const pulseType = "agent.internal.pulse"

func (c *controller) pulse(sys actorbase.Sys, msg actorbase.Msg) error {
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
	}
	ordered := make([]*session, 0, len(c.sessions))
	for _, s := range c.sessions {
		if !s.Archived {
			ordered = append(ordered, s)
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].LastUsed > ordered[j].LastUsed })
	for i, s := range ordered {
		if i < c.cfg.Archive.KeepAlive || !c.idleInThisProcess(s) || c.cfg.Archive.IdleMS == 0 || now-s.LastUsed < c.cfg.Archive.IdleMS {
			continue
		}
		err := c.postSessionCommand(sys, msg, s, agentloop.TypeStop, agentloop.StopRequest{SessionID: s.ID, Archive: true, Reason: "idle"})
		if err == nil {
			s.Archived = true
		}
	}
	for id, waiters := range c.wait {
		w := c.data.Works[string(id)]
		if w == nil || w.State != agentproto.WorkOpen {
			continue
		}
		for _, waiting := range waiters {
			if waiting.Ctx().Err() == nil {
				_, _ = sys.Progress(waiting, "processing", map[string]any{"work_id": w.ID, "session_id": w.SessionID, "stage": w.Stage})
			}
		}
	}
	c.scheduleQueued(sys, msg.Cause(), msg.Context())
	_, err := sys.After(10*time.Second, pulseType, map[string]any{}, schedule.TimerHomeMemory)
	return err
}

// A missing local execution does not prove that a historical holder is idle.
// Automatic archive requires a terminal report handled by this process and no
// remaining local work. Explicit session commands need no such inference.
func (c *controller) idleInThisProcess(s *session) bool {
	if s.Execution != "" || s.Control != nil || len(s.Buffer) != 0 {
		return false
	}
	completedHere := false
	for _, w := range c.data.Works {
		if w.SessionID != s.ID {
			continue
		}
		if w.State == agentproto.WorkOpen {
			return false
		}
		completedHere = completedHere || (w.BoundaryID != "" && w.BoundaryID == s.LastBoundary)
	}
	return completedHere
}
