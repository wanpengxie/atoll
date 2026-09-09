package native

import (
	"errors"
	"time"

	"github.com/google/uuid"
	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/message"
)

const (
	maxSessions     = 256
	sessionIdle     = time.Hour
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
	LastUsed         int64
	Freeze           string
	HoldUntil        int64
	RestoreInterrupt bool
	Control          *pendingControl
	Rebuffer         bool
	Holder           string
	Base             *agentloop.BoundaryRef
	Opened           bool
	Archived         bool
	Merge            string
	Merged           []string
	Skipped          []string
	Synced           string
	LastBoundary     string
	ForkPoint        string
}

func (c *controller) sessionForAsk(sys actorbase.Sys, msg actorbase.Msg, req agentproto.AskRequest) (*session, error) {
	if c.sessions == nil {
		c.sessions = map[string]*session{}
	}
	scope := workScopeKey(actorbase.EffectiveCaller(msg))
	id := msg.Context().Session
	if id == "" {
		id = req.SessionID
	}
	var base *agentloop.BoundaryRef
	if req.RelatedWorkID != "" {
		w, ok := c.owned(msg, req.RelatedWorkID)
		if !ok {
			return nil, errors.New("work_not_found")
		}
		if c.sessions[w.SessionID] == nil {
			return nil, errors.New("context_unavailable")
		}
		base = &agentloop.BoundaryRef{Session: w.SessionID, At: w.BoundaryID}
	}
	if id == "" {
		return nil, errors.New("scope_required")
	}
	if s := c.sessions[id]; s != nil {
		if s.Scope != scope {
			return nil, errors.New("session_not_found")
		}
		if !s.Archived {
			return s, nil
		}
		// An archived branch is immutable routing truth. Refresh its ledger
		// projection so the fork is pinned to a concrete boundary, then assign
		// the root request a fresh session before any response is written.
		if s.ForkPoint == "" {
			if err := c.recoverSessionProjection(sys); err != nil {
				return nil, errors.New("ledger_unavailable")
			}
			s = c.sessions[id]
		}
		if s == nil || !s.Archived || s.ForkPoint == "" {
			return nil, errors.New("context_unavailable")
		}
		if base == nil {
			base = &agentloop.BoundaryRef{Session: s.ID, At: s.ForkPoint}
		}
		id = "s-" + uuid.NewString()
		if err := actorbase.AssignSession(sys, msg, id); err != nil {
			return nil, errors.New("session_assignment_failed")
		}
	}

	if len(c.sessions) >= maxSessions {
		return nil, errors.New("session_capacity")
	}
	s := &session{ID: id, Scope: scope, LastUsed: nowMillis(), Base: base, Merge: "auto"}
	c.sessions[s.ID] = s
	c.sessionOrder = append(c.sessionOrder, s.ID)
	return s, nil
}

func (c *controller) selectSession(msg actorbase.Msg, id string, work agentproto.WorkID, target string) (*session, error) {
	scope := workScopeKey(actorbase.EffectiveCaller(msg))
	if contextual := msg.Context().Session; contextual != "" {
		if id != "" && id != contextual {
			return nil, errors.New("invalid_args")
		}
		id = contextual
	}
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
		if id != "" && id != w.SessionID {
			return nil, errors.New("invalid_args")
		}
		id = w.SessionID
	}

	if id != "" {
		s := c.sessions[id]
		if s == nil || s.Scope != scope {
			return nil, errors.New("session_not_found")
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
		return nil, errors.New("session_not_found")
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
		var inputs []inputRecord
		for _, w := range batch {
			for _, in := range w.Inputs {
				in.Seq = int64(len(inputs) + 1)
				inputs = append(inputs, in)
			}
		}
		owner.Inputs = inputs
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
