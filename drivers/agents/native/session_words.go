package native

import (
	"errors"
	"strings"

	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
)

func (c *controller) handleSession(sys actorbase.Sys, msg actorbase.Msg) {
	if msg.Type == agentproto.TypeSessionList || msg.Type == agentproto.TypeSessionGet {
		if err := c.refreshSessionRelations(sys); err != nil {
			code := "ledger_unavailable"
			if errors.Is(err, errRelationHistoryLimit) {
				code = errRelationHistoryLimit.Error()
			}
			_, _ = sys.Fail(msg, code, err.Error())
			return
		}
		if msg.Type == agentproto.TypeSessionList {
			rows := make([]map[string]any, 0, len(c.sessionOrder))
			for _, id := range c.sessionOrder {
				if s := c.sessions[id]; s != nil {
					rows = append(rows, sessionProjection(s))
				}
			}
			_, _ = sys.Reply(msg, map[string]any{"sessions": rows})
			return
		}
	}
	id := msg.Context().Session
	if id == "" {
		_, _ = sys.Fail(msg, "scope_required", "_context.session is required")
		return
	}
	s := c.sessions[id]
	if s == nil {
		_, _ = sys.Fail(msg, "session_not_found", "session does not exist")
		return
	}
	if msg.Type == agentproto.TypeSessionGet {
		_, _ = sys.Reply(msg, sessionProjection(s))
		return
	}
	if s.Archived {
		_, _ = sys.Fail(msg, "session_archived", "session is archived")
		return
	}
	switch msg.Type {
	case agentproto.TypeSessionArchive:
		s.Archived = true
		err := c.postSessionCommand(sys, msg, s, agentloop.TypeStop, agentloop.StopRequest{SessionID: id, TurnID: s.Execution, AssignmentID: s.Execution, Archive: true, Reason: "archive"})
		if err != nil {
			s.Archived = false
			_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
			return
		}
		_, _ = sys.Reply(msg, map[string]any{"disposition": "archived", "session_id": id})
	case agentproto.TypeSessionReset:
		if s.Execution != "" {
			_, _ = sys.Fail(msg, "busy", "session has active turn")
			return
		}
		err := c.postSessionCommand(sys, msg, s, agentloop.TypeReset, agentloop.ResetRequest{SessionID: id})
		if err != nil {
			_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
			return
		}
		_, _ = sys.Reply(msg, map[string]any{"disposition": "reset_requested", "session_id": id})
	case agentproto.TypeSessionRename:
		var req struct {
			Name string `json:"name"`
		}
		if actorbase.DecodeStrict(msg.Payload, &req) != nil || strings.TrimSpace(req.Name) == "" {
			_, _ = sys.Fail(msg, "invalid_args", "name required")
			return
		}
		err := c.postSessionCommand(sys, msg, s, agentloop.TypeRename, agentloop.RenameRequest{SessionID: id, Name: req.Name})
		if err != nil {
			_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
			return
		}
		_, _ = sys.Reply(msg, map[string]any{"disposition": "rename_requested", "session_id": id, "name": req.Name})
	case agentproto.TypeSessionSync:
		if s.Execution != "" {
			_, _ = sys.Fail(msg, "busy", "session has active turn")
			return
		}
		var req struct {
			FromSession string `json:"from_session"`
			After       string `json:"after"`
			Through     string `json:"through"`
		}
		if actorbase.DecodeStrict(msg.Payload, &req) != nil || req.FromSession == "" {
			_, _ = sys.Fail(msg, "invalid_args", "from_session required")
			return
		}
		err := c.postSessionCommand(sys, msg, s, agentloop.TypeSync, agentloop.SyncRequest{SessionID: id, From: agentloop.SyncRange{Session: req.FromSession, After: req.After, Through: req.Through}})
		if err != nil {
			_, _ = sys.Fail(msg, "ledger_unavailable", err.Error())
			return
		}
		_, _ = sys.Reply(msg, map[string]any{"disposition": "sync_requested", "session_id": id})
	}
}

func (c *controller) postSessionCommand(sys actorbase.Sys, msg actorbase.Msg, s *session, typ string, payload any) error {
	tried := map[string]bool{}
	for attempt := 0; attempt <= len(c.cfg.Loopers); attempt++ {
		target := s.Holder
		if target == "" || tried[target] {
			var ok bool
			target, ok = c.freeLooper()
			if !ok || tried[target] {
				continue
			}
		}
		tried[target] = true
		_, err := sys.Post(behavior.RequestSpec{Cause: message.Anchored(msg.ID, msg.ID), Type: typ, Audience: message.Audience{actorID(target)}, Payload: mustJSON(payload)})
		if err == nil {
			s.Holder = target
			return nil
		}
		s.Holder = ""
	}
	return errors.New("no live Looper can accept the session command")
}

func sessionProjection(s *session) map[string]any {
	merged := append([]string(nil), s.Merged...)
	skipped := append([]string(nil), s.Skipped...)
	mergedLast := false
	for _, ref := range merged {
		mergedLast = mergedLast || ref == s.LastBoundary
	}
	for _, ref := range skipped {
		mergedLast = mergedLast || ref == s.LastBoundary
	}
	return map[string]any{
		"session_id": s.ID, "holder": s.Holder, "archived": s.Archived, "active_turn": s.Execution, "last_used": s.LastUsed,
		"relation": map[string]any{"merge": s.Merge, "merged": merged, "pending": s.Archived && s.LastBoundary != "" && !mergedLast, "skipped": skipped, "synced": s.Synced},
	}
}
