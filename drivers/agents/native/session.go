package native

import (
	"encoding/json"
	"errors"
	"time"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
)

const (
	maxSessions     = 256
	sessionIdle     = 30 * time.Minute
	maxSessionQueue = 128
	batchMaxCount   = 16
)

// Sessions are normal-incarnation conversation state, never restart jobs.
type session struct {
	ID               string
	Scope            string
	Buffer           []agentproto.WorkID
	Execution        string
	Owner            agentproto.WorkID
	History          []json.RawMessage
	Version          int64
	LastUsed         int64
	Freeze           string
	HoldUntil        int64
	RestoreInterrupt bool
	Control          *pendingControl
	Rebuffer         bool
}

func (c *controller) sessionForAsk(msg actorbase.Msg, req agentproto.AskRequest) (*session, error) {
	if c.sessions == nil {
		c.sessions = map[string]*session{}
	}
	scope := workScopeKey(actorbase.EffectiveCaller(msg))
	if req.RelatedWorkID != "" {
		w, ok := c.owned(msg, req.RelatedWorkID)
		if !ok {
			return nil, errors.New("work_not_found")
		}
		if req.ViewID != "" && req.ViewID != w.ViewID {
			return nil, errors.New("invalid_args")
		}
		req.ViewID = w.ViewID
		if c.sessions[req.ViewID] == nil {
			return nil, errors.New("context_unavailable")
		}
	}
	if req.ViewID == "" {
		return nil, errors.New("scope_required")
	}
	if s := c.sessions[req.ViewID]; s != nil {
		if s.Scope != scope {
			return nil, errors.New("view_not_found")
		}
		return s, nil
	}

	if len(c.sessions) >= maxSessions {
		return nil, errors.New("session_capacity")
	}
	s := &session{ID: req.ViewID, Scope: scope, LastUsed: nowMillis()}
	c.sessions[s.ID] = s
	c.sessionOrder = append(c.sessionOrder, s.ID)
	return s, nil
}

func (c *controller) selectSession(msg actorbase.Msg, id string, work agentproto.WorkID, target string) (*session, error) {
	scope := workScopeKey(actorbase.EffectiveCaller(msg))
	for _, selector := range []struct {
		value   string
		request bool
	}{{string(work), false}, {target, true}} {
		if selector.value == "" {
			continue
		}
		w := c.data.Works[selector.value]
		if selector.request {
			w = c.requestWork(selector.value)
		}
		if w == nil || workScopeKey(w.Owner) != scope {
			return nil, errors.New("work_not_found")
		}
		if id != "" && id != w.ViewID {
			return nil, errors.New("invalid_args")
		}
		id = w.ViewID
	}

	if id != "" {
		s := c.sessions[id]
		if s == nil || s.Scope != scope {
			return nil, errors.New("view_not_found")
		}
		return s, nil
	}
	var selected *session
	for _, s := range c.sessions {
		if s.Scope == scope {
			if selected != nil {
				return nil, errors.New("scope_required")
			}
			selected = s
		}
	}
	if selected == nil {
		return nil, errors.New("view_not_found")
	}
	return selected, nil
}

func (c *controller) requestWork(target string) *workRecord {
	for _, w := range c.data.Works {
		if w.SourceRequest == target {
			return w
		}
	}
	return nil
}

func queueIndex(s *session, id agentproto.WorkID) int {
	for i, v := range s.Buffer {
		if v == id {
			return i
		}
	}
	return -1
}

func removeQueue(s *session, id agentproto.WorkID) {
	if i := queueIndex(s, id); i >= 0 {
		s.Buffer = append(s.Buffer[:i], s.Buffer[i+1:]...)
	}
}

func (c *controller) closeLinked(sys actorbase.Sys, cause message.Cause, w *workRecord, key string, to *workRecord) {
	w.State = agentproto.WorkClosed
	w.Stage = ""
	w.Outcome = agentproto.OutcomeAnswered
	w.Result = mustJSON(map[string]any{key: to.SourceRequest, "work_id": to.ID})
	w.AssignmentID = ""
	w.Looper = ""
	w.UpdatedAt = nowMillis()
	_ = c.commit(sys, cause, w)
	c.finishWaiters(sys, w)
}

func (c *controller) scheduleSessions(sys actorbase.Sys, cause message.Cause) {
	start := c.nextSession
	for offset := 0; offset < len(c.sessionOrder); offset++ {
		if len(c.sessionOrder) == 0 {
			return
		}
		i := (start + offset) % len(c.sessionOrder)
		s := c.sessions[c.sessionOrder[i]]
		if s == nil || s.Execution != "" || s.Control != nil || s.Freeze != "" || len(s.Buffer) == 0 {
			continue
		}
		first := c.data.Works[string(s.Buffer[0])]
		if first == nil || first.State != agentproto.WorkOpen {
			s.Buffer = s.Buffer[1:]
			continue
		}
		batch := []*workRecord{first}
		for _, id := range s.Buffer[1:] {
			w := c.data.Works[string(id)]
			if w == nil || w.State != agentproto.WorkOpen || w.Owner != first.Owner || w.Resumed || first.Resumed || len(batch) >= batchMaxCount {
				break
			}
			combined := 0
			size := inputRecordsSize(w.Inputs)
			for _, item := range batch {
				combined += len(item.Inputs)
				size += inputRecordsSize(item.Inputs)
			}
			if (c.cfg.MaxInputsPerWork > 0 && combined+len(w.Inputs) > c.cfg.MaxInputsPerWork) || size > maxWorkInputBytes {
				break
			}
			batch = append(batch, w)
		}
		owner := batch[len(batch)-1]
		// Reserve capacity before merging requests: no ownership changes on a full lane.
		savedCursor := c.data.NextLooper
		_, capacity := c.freeLooper()
		c.data.NextLooper = savedCursor
		if !capacity {
			owner.ExecutionState = "waiting_capacity"
			return
		}
		var inputs []inputRecord
		for _, w := range batch {
			for _, in := range w.Inputs {
				in.Seq = int64(len(inputs) + 1)
				inputs = append(inputs, in)
			}
		}
		owner.Inputs = inputs
		owner.Context = append([]json.RawMessage(nil), s.History...)
		owner.ContextVersion = s.Version
		if !c.dispatch(sys, owner, cause) {
			return
		}
		s.Buffer = s.Buffer[len(batch):]
		s.Execution = owner.AssignmentID
		s.Owner = owner.ID
		s.LastUsed = nowMillis()
		for _, w := range batch[:len(batch)-1] {
			c.closeLinked(sys, cause, w, "merged_into", owner)
		}
		c.nextSession = (i + 1) % len(c.sessionOrder)
	}
}
