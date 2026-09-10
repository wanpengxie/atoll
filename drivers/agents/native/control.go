package native

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	agentproto "github.com/wanpengxie/atoll/drivers/agents/workapi"
	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
)

const controlDoneType = "agent.internal.control_done"

type pendingControl struct {
	ID        string
	Execution string
	Message   actorbase.Msg
	Targets   []agentproto.WorkID
	Indices   []int
	Text      bool
	Key       string
	Hash      string
	Request   agentloop.InputRequest
}
type controlDone struct {
	Session   string                  `json:"session_id"`
	Execution string                  `json:"assignment_id"`
	ID        string                  `json:"control_id"`
	Decision  agentloop.ControlResult `json:"decision"`
	Unknown   bool                    `json:"unknown,omitempty"`
}

func (c *controller) steer(sys actorbase.Sys, msg actorbase.Msg) {
	req, err := agentproto.DecodeSteer(msg.Payload)
	if err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	s, err := c.selectSession(msg, req.SessionID, req.WorkID, req.Target)
	if err != nil {
		_, _ = sys.Fail(msg, err.Error(), "cannot select control session")
		return
	}
	key := operationIndexKey(actorbase.EffectiveCaller(msg), req.OperationKey)
	hash := requestHash(req)
	if req.OperationKey != "" {
		for _, w := range c.data.Works {
			if prior, ok := w.Operations[key]; ok {
				if prior.Kind != agentproto.TypeSteer || prior.Hash != hash {
					_, _ = sys.Fail(msg, "operation_conflict", "operation payload changed")
					return
				}
				_, _ = sys.Reply(msg, json.RawMessage(prior.Response))
				return
			}
		}
		if s.Control != nil && s.Control.Key == key {
			if s.Control.Hash != hash {
				_, _ = sys.Fail(msg, "operation_conflict", "operation payload changed")
				return
			}
			_, _ = sys.Reply(msg, map[string]any{"disposition": "control_pending", "control_id": s.Control.ID})
			return
		}
	}
	if s.Control != nil {
		_, _ = sys.Fail(msg, "busy", "session control slot occupied")
		return
	}
	if req.ExpectedTurnID != "" && req.ExpectedTurnID != s.Execution {
		_, _ = sys.Fail(msg, "cas_mismatch", "execution changed")
		return
	}
	if req.WorkID != "" {
		if w := c.data.Works[string(req.WorkID)]; w.State == agentproto.WorkClosed {
			_, _ = sys.Fail(msg, "work_closed", "the selected work is closed")
			return
		}
	}
	if owner := c.data.Works[string(s.Owner)]; owner != nil {
		if req.OperationKey != "" && c.cfg.MaxOperationKeys > 0 && len(owner.Operations) >= c.cfg.MaxOperationKeys {
			_, _ = sys.Fail(msg, "limit_exceeded", "max_operation_keys reached")
			return
		}
		if c.cfg.MaxInputsPerWork > 0 && len(owner.Inputs) >= c.cfg.MaxInputsPerWork {
			_, _ = sys.Fail(msg, "limit_exceeded", "max_inputs_per_work reached")
			return
		}
	}
	pc := &pendingControl{ID: "control-" + uuid.NewString(), Execution: s.Execution, Message: msg, Key: key, Hash: hash}
	if req.OperationKey == "" {
		pc.Key = ""
	}
	if req.Target != "" {
		w := c.requestWork(req.Target)
		if w == nil || w.State != agentproto.WorkOpen || queueIndex(s, w.ID) < 0 {
			_, _ = sys.Fail(msg, "cas_mismatch", "target is not waiting")
			return
		}
		pc.Targets = []agentproto.WorkID{w.ID}
	} else if req.All {
		caller := actorbase.EffectiveCaller(msg)
		for _, id := range s.Buffer {
			w := c.data.Works[string(id)]
			if w != nil && w.Owner == caller {
				pc.Targets = append(pc.Targets, id)
			}
		}
	} else {
		if s.Execution == "" {
			_, _ = sys.Fail(msg, "target_gone", "no active execution for text steer")
			return
		}
		if c.data.openCount() >= c.cfg.MaxOpenWorks {
			_, _ = sys.Fail(msg, "capacity", "max_open_works reached")
			return
		}
		caller := actorbase.EffectiveCaller(msg)
		now := nowMillis()
		w := &workRecord{ID: newWorkID(), SessionID: s.ID, Owner: caller, SourceRequest: string(msg.ID), SourceCause: msg.Cause(), State: agentproto.WorkOpen, Stage: "control_pending", Delivery: agentproto.DeliveryReceipt, CreatedAt: now, UpdatedAt: now,
			Inputs: []inputRecord{{Input: agentloop.Input{ID: string(msg.ID), Seq: 1, Text: req.Text, CallerActor: caller.Actor, CallerChannel: caller.Channel}, Disposition: "accepted"}}}
		c.data.Works[string(w.ID)] = w
		c.data.Order = append(c.data.Order, string(w.ID))
		pc.Targets = []agentproto.WorkID{w.ID}
		pc.Text = true
	}
	if len(pc.Targets) == 0 {
		_, _ = sys.Reply(msg, map[string]any{"disposition": "no_waiting_input"})
		return
	}
	for _, id := range pc.Targets {
		pc.Indices = append(pc.Indices, queueIndex(s, id))
	}
	for _, id := range pc.Targets {
		removeQueue(s, id)
		c.data.Works[string(id)].Stage = "control_pending"
	}
	if s.Execution == "" {
		s.Buffer = append(append([]agentproto.WorkID(nil), pc.Targets...), s.Buffer...)
		s.Freeze = ""
		s.RestoreInterrupt = false
		for _, id := range pc.Targets {
			c.data.Works[string(id)].Stage = "queued"
		}
		response := mustJSON(map[string]any{"disposition": "queued_first", "session_id": s.ID})
		for _, id := range pc.Targets {
			w := c.data.Works[string(id)]
			if pc.Key != "" {
				if w.Operations == nil {
					w.Operations = map[string]operationRecord{}
				}
				w.Operations[pc.Key] = operationRecord{Kind: agentproto.TypeSteer, Hash: pc.Hash, Response: response}
			}
		}
		_, _ = sys.Reply(msg, json.RawMessage(response))
		c.scheduleQueued(sys, msg.Cause())
		return
	}
	owner := c.data.Works[string(s.Owner)]
	if owner == nil || owner.Stage == "stopping" {
		c.restoreControl(s, pc)
		_, _ = sys.Fail(msg, "busy", "execution stopping")
		return
	}
	seq := owner.AssignedThrough
	var inputs []agentloop.Input
	for _, id := range pc.Targets {
		for _, in := range c.data.Works[string(id)].Inputs {
			seq++
			in.Seq = seq
			inputs = append(inputs, in.Input)
		}
	}
	if (c.cfg.MaxInputsPerWork > 0 && len(owner.Inputs)+len(inputs) > c.cfg.MaxInputsPerWork) || inputRecordsSize(owner.Inputs)+len(mustJSON(inputs)) > maxWorkInputBytes {
		c.restoreControl(s, pc)
		_, _ = sys.Fail(msg, "limit_exceeded", "execution input limit reached")
		return
	}
	pc.Request = agentloop.InputRequest{WorkID: owner.ID, AssignmentID: s.Execution, SessionID: s.ID, ControlID: pc.ID, Inputs: inputs}
	s.Control = pc
	go deliverControl(sys, s.ID, owner.Looper, pc)
}

func deliverControl(sys actorbase.Sys, sessionID, looper string, pc *pendingControl) {
	ctx, cancel := context.WithTimeout(sys.Life(), 15*time.Second)
	defer cancel()
	decision := agentloop.ControlResult{ControlID: pc.ID}
	raw, err := controlCall(ctx, sys, pc.Message.Cause(), looper, agentloop.TypeInput, pc.Request)
	unknown := false
	if err == nil {
		err = json.Unmarshal(raw, &decision)
	}
	if err != nil {
		// An absent acknowledgement is unknown admission. Report that result;
		// do not inspect another actor or replay the control to reconstruct it.
		unknown = true
	}
	if sys.Life().Err() != nil {
		return
	}
	_, _ = sys.Post(behavior.RequestSpec{Cause: pc.Message.Cause(), Type: controlDoneType, Audience: message.Audience{sys.Self()}, Payload: mustJSON(controlDone{Session: sessionID, Execution: pc.Execution, ID: pc.ID, Decision: decision, Unknown: unknown})})
}

func controlCall(ctx context.Context, sys actorbase.Sys, cause message.Cause, target, word string, payload any) (json.RawMessage, error) {
	p, err := sys.Call(cause, actorID(target), word, payload)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	progressCtx, stopProgress := context.WithCancel(ctx)
	defer stopProgress()
	go func() {
		defer close(done)
		for {
			select {
			case _, ok := <-p.Progress():
				if !ok {
					return
				}
			case <-progressCtx.Done():
				return
			}
		}
	}()
	response, err := p.Wait(ctx, 0)
	if err != nil && sys.Life().Err() == nil {
		_ = p.Cancel()
	}
	stopProgress()
	<-done
	if err != nil {
		return nil, err
	}
	var h struct {
		Status    string `json:"status"`
		ErrorCode string `json:"error_code"`
	}
	if json.Unmarshal(response.Payload, &h) != nil || h.Status != "completed" {
		return nil, errors.New(h.ErrorCode)
	}
	return response.Payload, nil
}

func (c *controller) restoreControl(s *session, pc *pendingControl) {
	for i, id := range pc.Targets {
		w := c.data.Works[string(id)]
		if w == nil {
			continue
		}
		if pc.Text {
			w.State = agentproto.WorkClosed
			w.Stage = ""
			w.Outcome = agentproto.OutcomeFailed
			continue
		}
		w.Stage = "queued"
		index := pc.Indices[i]
		if index < 0 || index > len(s.Buffer) {
			index = len(s.Buffer)
		}
		s.Buffer = append(s.Buffer, "")
		copy(s.Buffer[index+1:], s.Buffer[index:])
		s.Buffer[index] = id
	}
}

func (c *controller) settleControl(sys actorbase.Sys, s *session, d agentloop.ControlResult, unknown bool) {
	pc := s.Control
	if pc == nil || pc.ID != d.ControlID {
		return
	}
	owner := c.data.Works[string(s.Owner)]
	if !unknown && (d.Disposition == "accepted" || d.Disposition == "already_accepted") && owner != nil {
		tail := c.data.Works[string(pc.Targets[len(pc.Targets)-1])]
		tail.Operations = make(map[string]operationRecord, len(owner.Operations)+1)
		for k, v := range owner.Operations {
			tail.Operations[k] = v
		}
		tail.Inputs = append([]inputRecord(nil), owner.Inputs...)
		for _, in := range pc.Request.Inputs {
			tail.Inputs = append(tail.Inputs, inputRecord{Input: in, Disposition: "assigned"})
			tail.AssignedThrough = in.Seq
		}
		tail.AssignmentID = owner.AssignmentID
		tail.Looper = owner.Looper
		tail.Stage = owner.Stage
		tail.ExecutionState = owner.ExecutionState
		s.Owner = tail.ID
		c.closeLinked(sys, pc.Message.Cause(), owner, "preempted_by", tail)
		for _, id := range pc.Targets[:len(pc.Targets)-1] {
			c.closeLinked(sys, pc.Message.Cause(), c.data.Works[string(id)], "merged_into", tail)
		}
	} else if unknown {
		s.Freeze = "interrupt"
		if owner != nil {
			_ = c.stop(sys, pc.Message, owner, false)
		}
		for _, id := range pc.Targets {
			w := c.data.Works[string(id)]
			w.State = agentproto.WorkClosed
			w.Stage = ""
			w.Outcome = agentproto.OutcomeFailed
			w.ExecutionState = "control_unknown"
			w.Result = mustJSON(map[string]any{"error_code": "control_unknown", "detail": "input admission unknown; it was not automatically requeued"})
			c.finishWaiters(sys, w)
		}
	} else {
		c.restoreControl(s, pc)
		for _, id := range pc.Targets {
			if w := c.data.Works[string(id)]; w.State == agentproto.WorkClosed {
				w.Result = mustJSON(map[string]any{"error_code": "target_gone"})
				c.finishWaiters(sys, w)
			}
		}
	}
	response := mustJSON(map[string]any{"disposition": d.Disposition, "control_id": pc.ID, "session_id": s.ID, "work_id": pc.Targets[len(pc.Targets)-1], "included": false})
	if unknown {
		response = mustJSON(map[string]any{"disposition": "control_unknown", "control_id": pc.ID, "session_id": s.ID})
	}
	for _, id := range pc.Targets {
		w := c.data.Works[string(id)]
		if pc.Key != "" {
			if w.Operations == nil {
				w.Operations = map[string]operationRecord{}
			}
			w.Operations[pc.Key] = operationRecord{Kind: agentproto.TypeSteer, Hash: pc.Hash, Response: response}
		}
	}
	s.Control = nil
	_, _ = sys.Reply(pc.Message, json.RawMessage(response))
}

func (c *controller) controlDone(sys actorbase.Sys, msg actorbase.Msg) {
	if msg.Sender.ID != sys.Self() {
		_, _ = sys.Fail(msg, "permission_denied", "internal message")
		return
	}
	var d controlDone
	if actorbase.DecodeStrict(msg.Payload, &d) != nil {
		return
	}
	s := c.sessions[d.Session]
	if s == nil || s.Execution != d.Execution || s.Control == nil || s.Control.ID != d.ID {
		return
	}
	d.Decision.ControlID = d.ID
	c.settleControl(sys, s, d.Decision, d.Unknown)
	_, _ = sys.Reply(msg, map[string]any{"disposition": "adopted"})
}

type editRequest struct {
	WorkID     agentproto.WorkID `json:"work_id,omitempty"`
	Target     string            `json:"target,omitempty"`
	OldText    string            `json:"old_text,omitempty"`
	NewText    string            `json:"new_text,omitempty"`
	DurationMS *int64            `json:"duration_ms,omitempty"`
}

func (c *controller) editControl(sys actorbase.Sys, msg actorbase.Msg) {
	var req editRequest
	if err := actorbase.DecodeStrictEmpty(msg.Payload, &req); err != nil {
		_, _ = sys.Fail(msg, "invalid_args", err.Error())
		return
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(msg.Payload, &fields)
	for key, value := range fields {
		allowed := key == "work_id" || (msg.Type == agentproto.TypeReplace && (key == "target" || key == "old_text" || key == "new_text")) || (msg.Type == agentproto.TypeHold && (key == "target" || key == "duration_ms"))
		if !allowed || string(value) == "null" {
			_, _ = sys.Fail(msg, "invalid_args", "field is not valid for this control")
			return
		}
	}
	s, err := c.selectSession(msg, "", req.WorkID, req.Target)
	if err != nil {
		_, _ = sys.Fail(msg, err.Error(), "cannot select session")
		return
	}
	w := c.requestWork(req.Target)
	switch msg.Type {
	case agentproto.TypeReplace:
		if w == nil || queueIndex(s, w.ID) < 0 || len(w.Inputs) != 1 || w.Inputs[0].Text != req.OldText || strings.TrimSpace(req.NewText) == "" {
			_, _ = sys.Fail(msg, "cas_mismatch", "replace requires a matching waiting request and old_text")
			return
		}
		updated := cloneWork(w)
		updated.ID = newWorkID()
		updated.Owner = actorbase.EffectiveCaller(msg)
		updated.SourceRequest = string(msg.ID)
		updated.SourceCause = msg.Cause()
		updated.Inputs[0].ID = string(msg.ID)
		updated.Inputs[0].Text = req.NewText
		updated.Inputs[0].CallerActor = updated.Owner.Actor
		updated.Inputs[0].CallerChannel = updated.Owner.Channel
		updated.Inputs[0].Origin = nil
		updated.CreatedAt = nowMillis()
		updated.UpdatedAt = updated.CreatedAt
		updated.Operations = nil
		updated.SubmissionKey = ""
		updated.SubmissionHash = ""
		c.data.Works[string(updated.ID)] = updated
		c.data.Order = append(c.data.Order, string(updated.ID))
		s.Buffer[queueIndex(s, w.ID)] = updated.ID
		c.closeLinked(sys, msg.Cause(), w, "replaced_by", updated)
		c.attachWaiter(sys, msg, updated)
	case agentproto.TypeHold:
		duration := 30 * time.Minute
		if req.DurationMS != nil {
			if *req.DurationMS < 1 || *req.DurationMS > duration.Milliseconds() {
				_, _ = sys.Fail(msg, "invalid_args", "duration_ms must be 1..1800000")
				return
			}
			duration = time.Duration(*req.DurationMS) * time.Millisecond
		}
		if req.Target != "" {
			if w == nil || (queueIndex(s, w.ID) < 0 && w.ID != s.Owner) {
				_, _ = sys.Fail(msg, "cas_mismatch", "target is not waiting or current owner")
				return
			}
			if w.Owner != actorbase.EffectiveCaller(msg) {
				_, _ = sys.Fail(msg, "target_not_owned", "hold target belongs to another sender")
				return
			}
			if w.ID == s.Owner && (s.Control != nil || w.Stage == "dispatching" || w.Stage == "stopping") {
				_, _ = sys.Fail(msg, "busy", "execution control occupied")
				return
			}
		}
		if s.Freeze == "interrupt" {
			s.RestoreInterrupt = true
		}
		s.Freeze = "hold"
		s.HoldUntil = nowMillis() + duration.Milliseconds()
		if w != nil && s.Owner == w.ID && s.Execution != "" {
			s.Rebuffer = true
			_ = c.stop(sys, msg, w, false)
		}
		_, _ = sys.Reply(msg, map[string]any{"disposition": "held", "session_id": s.ID})
	case agentproto.TypeUnhold:
		if s.Freeze != "hold" {
			_, _ = sys.Reply(msg, map[string]any{"disposition": "released", "session_id": s.ID})
			return
		}
		if s.RestoreInterrupt {
			s.Freeze = "interrupt"
		} else {
			s.Freeze = ""
		}
		s.RestoreInterrupt = false
		_, _ = sys.Reply(msg, map[string]any{"disposition": "released", "session_id": s.ID})
	}
	c.scheduleQueued(sys, msg.Cause())
}
